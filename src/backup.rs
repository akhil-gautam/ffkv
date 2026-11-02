//! Native standalone snapshot and sealed online backups. Manifests name no paths.
//! Restore validates a private copy without opening sources for writing or repair.
//!
//! Legacy FFKVBAK1 manifests remain exactly 88 bytes and describe wal.ffkv.
//! Online FFKVBAK2 manifests describe immutable base.ffkv and incremental.ffkv:
//! magic, two (length:u64, data-frames:u64, SHA256:[u8;32]) entries, then
//! SHA256 of the preceding 104 bytes. Restore rebases file-local sequences.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};

const MANIFEST_MAGIC: &[u8; 8] = b"FFKVBAK1";
const MANIFEST_LEN: usize = 88;
const COPY_BUFFER_LEN: usize = 64 * 1024;

#[derive(Debug)]
pub struct Backup {
    pub wal_bytes: u64,
    pub data_frames: u64,
    pub sha256: [u8; 32],
}

#[derive(Debug)]
pub struct Restore {
    pub wal_bytes: u64,
    pub data_frames: u64,
    pub sha256: [u8; 32],
}

// Own only the names created by this invocation. In particular, do not recursively
// remove a staging directory that another process may have populated.
pub(crate) struct Scratch {
    path: PathBuf,
    directory: bool,
    armed: bool,
    members: u8,
}

impl Scratch {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn file(parent: &Path) -> io::Result<(Self, File)> {
        for _ in 0..128 {
            let path = scratch_name(parent)?;
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => {
                    return Ok((
                        Self {
                            path,
                            directory: false,
                            armed: true,
                            members: 0,
                        },
                        file,
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "scratch file name collisions",
        ))
    }

    pub(crate) fn directory(target: &Path) -> io::Result<Self> {
        require_absent(target)?;
        let parent = parent(target)?;
        for _ in 0..128 {
            let path = scratch_name(parent)?;
            match fs::create_dir(&path) {
                Ok(()) => {
                    return Ok(Self {
                        path,
                        directory: true,
                        armed: true,
                        members: 0,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "scratch directory name collisions",
        ))
    }

    pub(crate) fn create_member(&mut self, name: &str) -> io::Result<File> {
        let bit = match name {
            "wal.ffkv" => 1,
            "manifest.ffkv" => 2,
            "LOCK" => 4,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unknown snapshot member",
                ));
            }
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(self.path.join(name))?;
        self.members |= bit;
        Ok(file)
    }

    fn publish(mut self, target: &Path) -> io::Result<()> {
        sync_directory(&self.path)?;
        rename_new(&self.path, target)?;
        self.armed = false;
        // A failure here is a publication/durability error, not a rollback. Never
        // delete the now-visible target, whose ownership may already have changed.
        sync_directory(parent(target)?)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.directory {
            for (index, name) in ["wal.ffkv", "manifest.ffkv", "LOCK"]
                .into_iter()
                .enumerate()
            {
                if self.members & (1 << index) != 0 {
                    let _ = fs::remove_file(self.path.join(name));
                }
            }
            let _ = fs::remove_dir(&self.path);
        } else {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn scratch_name(parent: &Path) -> io::Result<PathBuf> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    Ok(parent.join(format!(
        ".ffkv-snapshot-{}-{time}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )))
}

fn parent(path: &Path) -> io::Result<&Path> {
    if path.file_name().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination must name a new directory",
        ));
    }
    Ok(path
        .parent()
        .filter(|value| !value.as_os_str().is_empty())
        .unwrap_or(Path::new(".")))
}

fn require_absent(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "destination already exists",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

// Opening with O_NOFOLLOW also closes the leaf symlink check/open race. Fixed
// manifest/payload names cannot escape the source directory through path traversal.
fn open_source(path: &Path) -> io::Result<File> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(invalid(
            "backup member must be a regular file, not a symlink",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        #[cfg(target_os = "linux")]
        options.custom_flags(0x20000); // O_NOFOLLOW
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        options.custom_flags(0x100); // O_NOFOLLOW
    }
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("backup member is not a regular file"));
    }
    Ok(file)
}

// rename() alone can replace an empty directory. Use the OS's atomic exclusive
// rename primitive instead; there is deliberately no racy/destructive fallback.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios"))]
fn rename_new(source: &Path, target: &Path) -> io::Result<()> {
    use std::{ffi::CString, os::unix::ffi::OsStrExt};
    let source = CString::new(source.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in source path"))?;
    let target = CString::new(target.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL in target path"))?;
    #[cfg(target_os = "linux")]
    let result = {
        unsafe extern "C" {
            fn renameat2(
                olddirfd: i32,
                oldpath: *const std::ffi::c_char,
                newdirfd: i32,
                newpath: *const std::ffi::c_char,
                flags: u32,
            ) -> i32;
        }
        // AT_FDCWD = -100, RENAME_NOREPLACE = 1.
        unsafe { renameat2(-100, source.as_ptr(), -100, target.as_ptr(), 1) }
    };
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    let result = {
        unsafe extern "C" {
            fn renamex_np(
                from: *const std::ffi::c_char,
                to: *const std::ffi::c_char,
                flags: u32,
            ) -> i32;
        }
        // RENAME_EXCL = 4.
        unsafe { renamex_np(source.as_ptr(), target.as_ptr(), 4) }
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
fn rename_new(_source: &Path, _target: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic exclusive directory publication is unsupported on this OS",
    ))
}

fn hash_copy(
    mut source: impl Read,
    mut destination: Option<&mut File>,
) -> io::Result<(u64, [u8; 32])> {
    let mut buffer = [0; COPY_BUFFER_LEN];
    let mut digest = Sha256::new();
    let mut length = 0_u64;
    loop {
        let count = match source.read(&mut buffer) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            break;
        }
        length = length
            .checked_add(count as u64)
            .ok_or_else(|| invalid("backup file length exhausted"))?;
        digest.update(&buffer[..count]);
        if let Some(file) = destination.as_deref_mut() {
            file.write_all(&buffer[..count])?;
        }
    }
    Ok((length, digest.finalize().into()))
}

fn manifest(stats: &Backup) -> [u8; MANIFEST_LEN] {
    let mut bytes = [0; MANIFEST_LEN];
    bytes[..8].copy_from_slice(MANIFEST_MAGIC);
    bytes[8..16].copy_from_slice(&stats.wal_bytes.to_le_bytes());
    bytes[16..24].copy_from_slice(&stats.data_frames.to_le_bytes());
    bytes[24..56].copy_from_slice(&stats.sha256);
    let checksum = Sha256::digest(&bytes[..56]);
    bytes[56..].copy_from_slice(&checksum);
    bytes
}

fn read_manifest(source: &Path) -> io::Result<Backup> {
    let mut file = open_source(&source.join("manifest.ffkv"))?;
    if file.metadata()?.len() != MANIFEST_LEN as u64 {
        return Err(invalid("invalid backup manifest length"));
    }
    let mut bytes = [0; MANIFEST_LEN];
    file.read_exact(&mut bytes)?;
    let mut extra = [0];
    if file.read(&mut extra)? != 0 || &bytes[..8] != MANIFEST_MAGIC {
        return Err(invalid(
            "unsupported backup manifest version or trailing bytes",
        ));
    }
    if Sha256::digest(&bytes[..56])[..] != bytes[56..] {
        return Err(invalid("backup manifest SHA256 mismatch"));
    }
    Ok(Backup {
        wal_bytes: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        data_frames: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        sha256: bytes[24..56].try_into().unwrap(),
    })
}

pub(crate) fn publish_backup(
    mut scratch: Scratch,
    target: &Path,
    expected_bytes: u64,
    data_frames: u64,
) -> io::Result<Backup> {
    let (wal_bytes, sha256) = hash_copy(open_source(&scratch.path().join("wal.ffkv"))?, None)?;
    if wal_bytes != expected_bytes {
        return Err(invalid(
            "snapshot WAL length changed before backup publication",
        ));
    }
    let stats = Backup {
        wal_bytes,
        data_frames,
        sha256,
    };
    let mut file = scratch.create_member("manifest.ffkv")?;
    file.write_all(&manifest(&stats))?;
    file.sync_all()?;
    scratch.publish(target)?;
    Ok(stats)
}

/// Strict offline restore into a fresh database directory. Source files are never
/// modified; corrupt/truncated files and incomplete logical groups are rejected.
/// Publication is atomic and never replaces even an empty existing destination.
/// A parent-directory fsync failure after publication is reported without rollback.
pub fn restore(source: &Path, target: &Path) -> io::Result<Restore> {
    require_absent(target)?;
    if !fs::symlink_metadata(source)?.is_dir() {
        return Err(invalid("backup source must be a directory, not a symlink"));
    }
    let mut prefix = [0; 8];
    open_source(&source.join("manifest.ffkv"))?.read_exact(&mut prefix)?;
    if &prefix == b"FFKVBAK2" {
        return restore_incremental(source, target);
    }
    let expected = read_manifest(source)?;
    let input = open_source(&source.join("wal.ffkv"))?;
    if input.metadata()?.len() != expected.wal_bytes {
        return Err(invalid("backup WAL length mismatch"));
    }
    let mut scratch = Scratch::directory(target)?;
    let mut output = scratch.create_member("wal.ffkv")?;
    let (wal_bytes, sha256) = hash_copy(
        input.take(expected.wal_bytes.saturating_add(1)),
        Some(&mut output),
    )?;
    if wal_bytes != expected.wal_bytes || sha256 != expected.sha256 {
        return Err(invalid("backup WAL length or SHA256 mismatch"));
    }
    let recovery = crate::wal::validate_complete(output.try_clone()?)?;
    if recovery.records != expected.data_frames {
        return Err(invalid("backup WAL data frame count mismatch"));
    }
    output.sync_all()?;
    scratch.create_member("LOCK")?.sync_all()?;
    scratch.publish(target)?;
    Ok(Restore {
        wal_bytes,
        data_frames: recovery.records,
        sha256,
    })
}

// Version two retains two independent, immutable native WALs. Restoring streams
// them into the normal single-WAL format, rebasing physical and group sequences.
const INCREMENTAL_MANIFEST_LEN: usize = 136;

pub(crate) fn seal_incremental(directory: &Path) -> io::Result<()> {
    let mut manifest = Vec::with_capacity(INCREMENTAL_MANIFEST_LEN);
    manifest.extend_from_slice(b"FFKVBAK2");
    for name in ["base.ffkv", "incremental.ffkv"] {
        let file = open_source(&directory.join(name))?;
        let recovery = crate::wal::validate_complete(open_source(&directory.join(name))?)?;
        let (length, hash) = hash_copy(file, None)?;
        manifest.extend_from_slice(&length.to_le_bytes());
        manifest.extend_from_slice(&recovery.records.to_le_bytes());
        manifest.extend_from_slice(&hash);
    }
    let checksum = Sha256::digest(&manifest);
    manifest.extend_from_slice(&checksum);
    let path = directory.join("manifest.ffkv");
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&manifest)?;
    file.sync_all()?;
    sync_directory(directory)
}

fn restore_incremental(source: &Path, target: &Path) -> io::Result<Restore> {
    let mut manifest_file = open_source(&source.join("manifest.ffkv"))?;
    if manifest_file.metadata()?.len() != INCREMENTAL_MANIFEST_LEN as u64 {
        return Err(invalid("invalid incremental backup manifest length"));
    }
    let mut manifest = [0; INCREMENTAL_MANIFEST_LEN];
    manifest_file.read_exact(&mut manifest)?;
    if &manifest[..8] != b"FFKVBAK2" || Sha256::digest(&manifest[..104])[..] != manifest[104..] {
        return Err(invalid("incremental backup manifest checksum mismatch"));
    }
    let mut scratch = Scratch::directory(target)?;
    let mut output = scratch.create_member("wal.ffkv")?;
    output.write_all(b"FFKVWAL1")?;
    let mut sequence = 1u64;
    let mut expected_records = 0u64;
    for (index, name) in ["base.ffkv", "incremental.ffkv"].into_iter().enumerate() {
        let offset = 8 + index * 48;
        let length = u64::from_le_bytes(manifest[offset..offset + 8].try_into().unwrap());
        let records = u64::from_le_bytes(manifest[offset + 8..offset + 16].try_into().unwrap());
        let mut input = open_source(&source.join(name))?;
        let (actual_length, hash) =
            hash_copy(input.try_clone()?.take(length.saturating_add(1)), None)?;
        if actual_length != length || hash[..] != manifest[offset + 16..offset + 48] {
            return Err(invalid("incremental backup payload checksum mismatch"));
        }
        if crate::wal::validate_complete(input.try_clone()?)?.records != records {
            return Err(invalid("incremental backup frame count mismatch"));
        }
        use std::io::{Seek, SeekFrom};
        input.seek(SeekFrom::Start(0))?;
        let mut file_header = [0; 8];
        input.read_exact(&mut file_header)?;
        if &file_header != b"FFKVWAL1" {
            return Err(invalid("incremental WAL header changed"));
        }
        let mut copied_hash = Sha256::new();
        copied_hash.update(file_header);
        let mut copied = 8u64;
        let sequence_base = sequence - 1;
        while copied < length {
            let mut header = [0; 24];
            input.read_exact(&mut header)?;
            let size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
            if size > 16 * 1024 * 1024 || length.saturating_sub(copied) < 24 + size as u64 {
                return Err(invalid("incremental frame exceeds limit"));
            }
            let mut payload = vec![0; size];
            input.read_exact(&mut payload)?;
            copied_hash.update(header);
            copied_hash.update(&payload);
            copied += 24 + size as u64;
            // Native BEGIN/COMMIT controls have zero mutations and carry the
            // BEGIN's physical sequence at payload offset thirteen.
            if payload.len() >= 21 && payload[8..12] == [0; 4] {
                let id = u64::from_le_bytes(payload[13..21].try_into().unwrap());
                let id = id
                    .checked_add(sequence_base)
                    .ok_or_else(|| invalid("group sequence exhausted"))?;
                payload[13..21].copy_from_slice(&id.to_le_bytes());
            }
            header[8..16].copy_from_slice(&sequence.to_le_bytes());
            let crc = crc32c::crc32c(&header[..16]);
            header[16..20].copy_from_slice(&crc.to_le_bytes());
            header[20..24].copy_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
            output.write_all(&header)?;
            output.write_all(&payload)?;
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| invalid("physical sequence exhausted"))?;
        }
        let mut extra = [0];
        if copied != length
            || input.read(&mut extra)? != 0
            || copied_hash.finalize()[..] != manifest[offset + 16..offset + 48]
        {
            return Err(invalid("incremental payload changed during restore"));
        }
        expected_records = expected_records
            .checked_add(records)
            .ok_or_else(|| invalid("frame count exhausted"))?;
    }
    let recovery = crate::wal::validate_complete(output.try_clone()?)?;
    if recovery.records != expected_records {
        return Err(invalid("combined backup frame count mismatch"));
    }
    output.sync_all()?;
    use std::io::{Seek, SeekFrom};
    output.seek(SeekFrom::Start(0))?;
    let (wal_bytes, sha256) = hash_copy(output.try_clone()?, None)?;
    scratch.create_member("LOCK")?.sync_all()?;
    scratch.publish(target)?;
    Ok(Restore {
        wal_bytes,
        data_frames: recovery.records,
        sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        journal::{Batch, Mutation},
        wal::{Durability, Wal},
    };
    use bytes::Bytes;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let path = scratch_name(&std::env::temp_dir()).unwrap();
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn backup(&self) -> PathBuf {
            self.0.join("backup")
        }
        fn restored(&self) -> PathBuf {
            self.0.join("restored")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn batch() -> Batch {
        Batch {
            timestamp_ms: 123,
            mutations: vec![Mutation::Set {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
                expires_at: Some(987654321),
            }],
        }
    }

    async fn create_backup(dir: &TempDir) -> Backup {
        let (mut wal, _) = Wal::open(&dir.0.join("live"), Durability::Strict, |_| Ok(())).unwrap();
        wal.backup(&dir.backup(), std::iter::once(Ok(batch())))
            .await
            .unwrap()
    }

    fn refresh_manifest(dir: &Path, data_frames: u64) {
        let (wal_bytes, sha256) =
            hash_copy(File::open(dir.join("wal.ffkv")).unwrap(), None).unwrap();
        fs::write(
            dir.join("manifest.ffkv"),
            manifest(&Backup {
                wal_bytes,
                data_frames,
                sha256,
            }),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn standalone_backup_preserves_live_wal_and_restores_appendable_database() {
        let dir = TempDir::new();
        let live = dir.0.join("live");
        let (mut wal, _) = Wal::open(&live, Durability::Everysec, |_| Ok(())).unwrap();
        wal.append(&batch()).await.unwrap();
        let live_bytes = fs::read(live.join("wal.ffkv")).unwrap();
        let sequence = wal.next_sequence();
        let backup = wal
            .backup(&dir.backup(), std::iter::once(Ok(batch())))
            .await
            .unwrap();
        assert_eq!(
            backup.wal_bytes,
            fs::metadata(dir.backup().join("wal.ffkv")).unwrap().len()
        );
        assert_eq!(backup.data_frames, 1);
        assert_eq!(
            backup.sha256,
            hash_copy(File::open(dir.backup().join("wal.ffkv")).unwrap(), None)
                .unwrap()
                .1
        );
        assert_eq!(fs::read(live.join("wal.ffkv")).unwrap(), live_bytes);
        assert_eq!(wal.next_sequence(), sequence);
        assert_eq!(
            Wal::open(&live, Durability::Strict, |_| Ok(()))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        fs::write(dir.backup().join("unknown-user-file"), b"untouched").unwrap();
        let restored = restore(&dir.backup(), &dir.restored()).unwrap();
        assert_eq!(restored.wal_bytes, backup.wal_bytes);
        assert_eq!(restored.sha256, backup.sha256);
        assert_eq!(
            fs::read(dir.backup().join("unknown-user-file")).unwrap(),
            b"untouched"
        );
        let mut seen = 0;
        let (mut restored_wal, recovery) = Wal::open(&dir.restored(), Durability::Strict, |batch| {
            assert_eq!(batch.timestamp_ms, 123);
            assert!(matches!(&batch.mutations[0], Mutation::Set { key, value, expires_at }
                if key.as_ref() == b"key" && value.as_ref() == b"value" && *expires_at == Some(987654321)));
            seen += 1;
            Ok(())
        }).unwrap();
        assert_eq!(seen, 1);
        assert_eq!(recovery.records, restored.data_frames);
        assert_eq!(recovery.truncated_bytes, 0);
        restored_wal.append(&batch()).await.unwrap();
        drop(restored_wal);
        assert_eq!(
            Wal::open(&dir.restored(), Durability::Strict, |_| Ok(()))
                .unwrap()
                .1
                .records,
            2
        );
    }

    #[tokio::test]
    async fn empty_and_large_group_backups_restore_with_real_frame_counts() {
        for large in [false, true] {
            let dir = TempDir::new();
            let (mut wal, _) =
                Wal::open(&dir.0.join("live"), Durability::Strict, |_| Ok(())).unwrap();
            let batches: Vec<io::Result<Batch>> = if large {
                vec![Ok(Batch {
                    timestamp_ms: 456,
                    mutations: (0..20)
                        .map(|_| Mutation::Queue {
                            key: Bytes::from_static(b"queue"),
                            effect: Bytes::from(vec![9; 1024 * 1024]),
                        })
                        .collect(),
                })]
            } else {
                Vec::new()
            };
            let stats = wal
                .backup(&dir.backup(), batches.into_iter())
                .await
                .unwrap();
            assert_eq!(stats.data_frames, if large { 2 } else { 0 });
            if !large {
                assert_eq!(stats.wal_bytes, 8);
            }
            let restored = restore(&dir.backup(), &dir.restored()).unwrap();
            assert_eq!(restored.data_frames, stats.data_frames);
            let mut count = 0;
            let (_wal, recovery) = Wal::open(&dir.restored(), Durability::Strict, |batch| {
                count += batch.mutations.len();
                Ok(())
            })
            .unwrap();
            assert_eq!(count, if large { 20 } else { 0 });
            assert_eq!(recovery.truncated_bytes, 0);
        }
    }

    #[tokio::test]
    async fn corrupt_truncated_oversized_and_unknown_manifest_versions_never_publish() {
        for corruption in 0..5 {
            let dir = TempDir::new();
            create_backup(&dir).await;
            let path = dir.backup().join("manifest.ffkv");
            let mut bytes = fs::read(&path).unwrap();
            match corruption {
                0 => {
                    bytes.pop();
                }
                1 => {
                    bytes.push(0);
                }
                2 => {
                    bytes[24] ^= 1;
                }
                3 => {
                    bytes[7] = b'2';
                    let checksum = Sha256::digest(&bytes[..56]);
                    bytes[56..].copy_from_slice(&checksum);
                }
                _ => {
                    bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
                    let checksum = Sha256::digest(&bytes[..56]);
                    bytes[56..].copy_from_slice(&checksum);
                }
            }
            fs::write(&path, &bytes).unwrap();
            assert!(restore(&dir.backup(), &dir.restored()).is_err());
            assert!(!dir.restored().exists());
            assert_eq!(fs::read(path).unwrap(), bytes);
            assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 2);
        }
    }

    #[tokio::test]
    async fn corrupted_or_truncated_wal_never_repairs_source_or_publishes_target() {
        for corruption in 0..5 {
            let dir = TempDir::new();
            create_backup(&dir).await;
            let path = dir.backup().join("wal.ffkv");
            let mut bytes = fs::read(&path).unwrap();
            match corruption {
                0 => {
                    bytes.pop();
                }
                1 => {
                    *bytes.last_mut().unwrap() ^= 1;
                }
                2 => {
                    bytes.extend_from_slice(b"torn header");
                }
                3 => {
                    bytes.truncate(7);
                }
                _ => {
                    bytes[8] ^= 1;
                }
            }
            fs::write(&path, &bytes).unwrap();
            // Cases 2..4 authenticate the damaged bytes in a valid manifest:
            // physical WAL validation, not SHA alone, must reject them.
            if corruption >= 2 {
                refresh_manifest(&dir.backup(), 1);
            }
            let manifest_before = fs::read(dir.backup().join("manifest.ffkv")).unwrap();
            assert!(restore(&dir.backup(), &dir.restored()).is_err());
            assert!(!dir.restored().exists());
            assert_eq!(fs::read(path).unwrap(), bytes);
            assert_eq!(
                fs::read(dir.backup().join("manifest.ffkv")).unwrap(),
                manifest_before
            );
            assert_eq!(fs::read_dir(&dir.0).unwrap().count(), 2);
        }
    }

    #[tokio::test]
    async fn authenticated_uncommitted_group_is_rejected_without_tail_repair() {
        let dir = TempDir::new();
        let (mut wal, _) = Wal::open(&dir.0.join("live"), Durability::Strict, |_| Ok(())).unwrap();
        let batch = Batch {
            timestamp_ms: 5,
            mutations: (0..2049)
                .map(|_| Mutation::Delete {
                    key: Bytes::from_static(b"k"),
                })
                .collect(),
        };
        wal.backup(&dir.backup(), std::iter::once(Ok(batch)))
            .await
            .unwrap();
        let path = dir.backup().join("wal.ffkv");
        let bytes = fs::read(&path).unwrap();
        // COMMIT has 24 header + 29 payload bytes; remove it entirely.
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(bytes.len() as u64 - 53)
            .unwrap();
        refresh_manifest(&dir.backup(), 3);
        let truncated = fs::read(&path).unwrap();
        assert!(restore(&dir.backup(), &dir.restored()).is_err());
        assert_eq!(fs::read(path).unwrap(), truncated);
        assert!(!dir.restored().exists());
    }

    #[tokio::test]
    async fn existing_targets_and_iterator_io_failures_are_nondestructive() {
        let dir = TempDir::new();
        create_backup(&dir).await;
        let original = fs::read(dir.backup().join("manifest.ffkv")).unwrap();
        let (mut wal, _) =
            Wal::open(&dir.0.join("other-live"), Durability::Strict, |_| Ok(())).unwrap();
        assert_eq!(
            wal.backup(&dir.backup(), std::iter::empty())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            fs::read(dir.backup().join("manifest.ffkv")).unwrap(),
            original
        );
        for nonempty in [false, true] {
            let target = dir.0.join(if nonempty { "nonempty" } else { "empty" });
            fs::create_dir(&target).unwrap();
            if nonempty {
                fs::write(target.join("user"), b"keep").unwrap();
            }
            assert_eq!(
                restore(&dir.backup(), &target).unwrap_err().kind(),
                io::ErrorKind::AlreadyExists
            );
            if nonempty {
                assert_eq!(fs::read(target.join("user")).unwrap(), b"keep");
            }
        }
        let target = dir.0.join("failed-backup");
        let count = fs::read_dir(&dir.0).unwrap().count();
        assert!(
            wal.backup(
                &target,
                [Ok(batch()), Err(io::Error::other("export failure"))].into_iter()
            )
            .await
            .is_err()
        );
        assert!(!target.exists());
        assert_eq!(fs::read_dir(&dir.0).unwrap().count(), count);
        assert!(
            wal.backup(
                &dir.0.join("missing-parent").join("backup"),
                std::iter::empty()
            )
            .await
            .is_err()
        );
        wal.append(&batch()).await.unwrap();
    }

    #[test]
    fn publication_race_cannot_replace_even_an_empty_directory() {
        let dir = TempDir::new();
        for nonempty in [false, true] {
            let target = dir.0.join(if nonempty { "nonempty" } else { "empty" });
            let mut scratch = Scratch::directory(&target).unwrap();
            let scratch_path = scratch.path().to_path_buf();
            scratch
                .create_member("wal.ffkv")
                .unwrap()
                .write_all(b"owned")
                .unwrap();
            fs::create_dir(&target).unwrap();
            if nonempty {
                fs::write(target.join("user"), b"untouched").unwrap();
            }
            assert!(scratch.publish(&target).is_err());
            assert!(target.is_dir());
            assert!(!scratch_path.exists());
            if nonempty {
                assert_eq!(fs::read(target.join("user")).unwrap(), b"untouched");
            }
        }
    }

    #[test]
    fn scratch_cleanup_never_removes_unowned_members() {
        let dir = TempDir::new();
        let target = dir.0.join("target");
        let mut scratch = Scratch::directory(&target).unwrap();
        let staging = scratch.path().to_path_buf();
        scratch
            .create_member("wal.ffkv")
            .unwrap()
            .write_all(b"owned")
            .unwrap();
        // Simulate an unowned name appearing after exclusive directory creation.
        fs::write(staging.join("manifest.ffkv"), b"user evidence").unwrap();
        assert_eq!(
            scratch.create_member("manifest.ffkv").unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        drop(scratch);
        assert!(!staging.join("wal.ffkv").exists());
        assert_eq!(
            fs::read(staging.join("manifest.ffkv")).unwrap(),
            b"user evidence"
        );
        assert!(!target.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_sources_members_and_destinations_are_refused() {
        use std::os::unix::fs::symlink;
        let dir = TempDir::new();
        create_backup(&dir).await;
        let alias = dir.0.join("alias");
        symlink(dir.backup(), &alias).unwrap();
        assert!(restore(&alias, &dir.restored()).is_err());
        symlink(dir.0.join("nonexistent"), dir.restored()).unwrap();
        assert_eq!(
            restore(&dir.backup(), &dir.restored()).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        fs::remove_file(dir.restored()).unwrap();
        let wal_path = dir.backup().join("wal.ffkv");
        let original = dir.0.join("original.ffkv");
        fs::rename(&wal_path, &original).unwrap();
        let before = fs::read(&original).unwrap();
        symlink(&original, &wal_path).unwrap();
        assert!(restore(&dir.backup(), &dir.restored()).is_err());
        assert_eq!(fs::read(&original).unwrap(), before);
        assert!(!dir.restored().exists());
    }
}
