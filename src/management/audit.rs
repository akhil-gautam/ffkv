//! A serialized append-only journal. A mutation cannot begin before sync_data succeeds.
use super::{ApiError, auth::Principal};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
const LINE_LIMIT: usize = 8192;
#[derive(Clone, Serialize, Deserialize)]
pub struct Row {
    pub timestamp_ms: u64,
    pub principal: String,
    pub role: super::auth::Role,
    pub operation: String,
    pub target: Option<String>,
    pub outcome: String,
    pub request_id: String,
}
pub struct Audit {
    file: Mutex<File>,
    failed: AtomicBool,
    sequence: AtomicU64,
}
impl Audit {
    pub fn open(path: &Path) -> io::Result<Self> {
        use std::os::fd::AsRawFd;
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(path)?;
        let meta = file.metadata()?;
        if !meta.is_file() || meta.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "audit journal must be a private regular file",
            ));
        }
        // SAFETY: file owns a valid descriptor, retained for the lock lifetime.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "audit journal already in use",
            ));
        }
        if meta.len() > 0 {
            file.seek(SeekFrom::End(-1))?;
            let mut last = [0];
            file.read_exact(&mut last)?;
            if last[0] != b'\n' {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "audit journal has an incomplete record",
                ));
            }
        }
        file.sync_data()?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            File::open(parent)?.sync_all()?;
        }
        Ok(Self {
            file: Mutex::new(file),
            failed: AtomicBool::new(false),
            sequence: AtomicU64::new(1),
        })
    }
    pub fn row(&self, p: &Principal, operation: &str, target: Option<String>) -> Row {
        Row {
            timestamp_ms: crate::metrics::now_ms(),
            principal: p.name.clone(),
            role: p.role,
            operation: operation.into(),
            target,
            outcome: "intent".into(),
            request_id: format!(
                "{}-{}-{}",
                std::process::id(),
                crate::metrics::now_ms(),
                self.sequence.fetch_add(1, Ordering::Relaxed)
            ),
        }
    }
    pub async fn append(self: &Arc<Self>, row: Row) -> Result<(), ApiError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            if this.failed.load(Ordering::Acquire) {
                return Err(ApiError::unavailable("audit unavailable"));
            }
            let mut data =
                serde_json::to_vec(&row).map_err(|_| ApiError::unavailable("audit unavailable"))?;
            if data.len() >= LINE_LIMIT {
                return Err(ApiError::unavailable("audit record exceeds bound"));
            }
            data.push(b'\n');
            let result = (|| {
                let mut file = this
                    .file
                    .lock()
                    .map_err(|_| io::Error::other("audit lock"))?;
                file.write_all(&data)?;
                file.sync_data()
            })();
            if result.is_err() {
                this.failed.store(true, Ordering::Release);
                return Err(ApiError::unavailable("audit unavailable"));
            }
            Ok(())
        })
        .await
        .map_err(|_| ApiError::unavailable("audit unavailable"))?
    }
    pub async fn page(
        self: &Arc<Self>,
        offset: u64,
        count: usize,
    ) -> Result<serde_json::Value, ApiError> {
        if !(1..=200).contains(&count) {
            return Err(ApiError::bad("invalid audit page size"));
        }
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::{BufRead, BufReader};
            let mut file = this
                .file
                .lock()
                .map_err(|_| ApiError::unavailable("audit unavailable"))?;
            let end = file
                .metadata()
                .map_err(|_| ApiError::unavailable("audit unavailable"))?
                .len();
            if offset > end {
                return Err(ApiError::bad("invalid audit cursor"));
            }
            file.seek(SeekFrom::Start(offset))
                .map_err(|_| ApiError::unavailable("audit unavailable"))?;
            if offset > 0 {
                file.seek(SeekFrom::Start(offset - 1))
                    .map_err(|_| ApiError::unavailable("audit unavailable"))?;
                let mut previous = [0];
                file.read_exact(&mut previous)
                    .map_err(|_| ApiError::bad("invalid audit cursor"))?;
                if previous[0] != b'\n' {
                    return Err(ApiError::bad("invalid audit cursor"));
                }
            }
            let mut reader = BufReader::with_capacity(LINE_LIMIT, &mut *file);
            let mut cursor = offset;
            let mut rows = Vec::with_capacity(count);
            for _ in 0..count {
                if cursor >= end {
                    break;
                }
                let mut line = Vec::new();
                reader
                    .by_ref()
                    .take((LINE_LIMIT + 1) as u64)
                    .read_until(b'\n', &mut line)
                    .map_err(|_| ApiError::unavailable("audit unavailable"))?;
                if line.len() > LINE_LIMIT || line.last() != Some(&b'\n') {
                    return Err(ApiError::unavailable("invalid audit record"));
                }
                cursor += line.len() as u64;
                let row: Row = serde_json::from_slice(&line)
                    .map_err(|_| ApiError::unavailable("invalid audit record"))?;
                rows.push(row);
            }
            Ok(serde_json::json!({"cursor":cursor.to_string(),"rows":rows}))
        })
        .await
        .map_err(|_| ApiError::unavailable("audit unavailable"))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "ffkv-audit-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ))
    }
    #[tokio::test]
    async fn durable_rows_paginate_at_exact_offsets() {
        let path = path();
        let audit = Arc::new(Audit::open(&path).unwrap());
        let principal = Principal {
            name: "operator".into(),
            role: super::super::auth::Role::Admin,
            can_reveal: true,
        };
        let mut row = audit.row(&principal, "FQ.CANCEL", Some("a2V5".into()));
        audit.append(row.clone()).await.unwrap();
        row.outcome = "success".into();
        audit.append(row).await.unwrap();
        let first = audit.page(0, 1).await.unwrap();
        assert_eq!(first["rows"][0]["outcome"], "intent");
        let cursor = first["cursor"].as_str().unwrap().parse().unwrap();
        let second = audit.page(cursor, 1).await.unwrap();
        assert_eq!(second["rows"][0]["outcome"], "success");
        assert!(audit.page(1, 1).await.is_err());
        assert!(audit.page(0, 201).await.is_err());
        drop(audit);
        let reopened = Arc::new(Audit::open(&path).unwrap());
        assert_eq!(
            reopened.page(0, 200).await.unwrap()["rows"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn rejects_symlink_permissive_and_torn_journals() {
        let path = path();
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        file.write_all(b"partial").unwrap();
        assert!(Audit::open(&path).is_err());
        file.set_len(0).unwrap();
        file.set_permissions(std::fs::Permissions::from_mode(0o644))
            .unwrap();
        assert!(Audit::open(&path).is_err());
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .unwrap();
        let link = path.with_extension("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(Audit::open(&link).is_err());
        std::fs::remove_file(link).unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
