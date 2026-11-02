//! Opt-in raw snapshot export. Export ownership follows the response body, and
//! audit completion survives request cancellation and streaming disconnects.
use super::*;
use axum::{Extension, body::Body};
use futures_util::stream;
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::io::AsyncReadExt;

fn text_field<'a>(value: &'a Resp, name: &str) -> Result<&'a str> {
    dto::field(value, name)
        .and_then(dto::text)
        .and_then(|v| std::str::from_utf8(v).ok())
        .ok_or_else(|| ApiError::unavailable("invalid replication bus response"))
}
fn number_field(value: &Resp, name: &str) -> Result<u64> {
    let value = dto::field(value, name)
        .ok_or_else(|| ApiError::unavailable("invalid replication revision"))?;
    match value {
        Resp::Integer(n) => u64::try_from(*n).ok(),
        _ => dto::text(value)
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse().ok()),
    }
    .ok_or_else(|| ApiError::unavailable("invalid replication revision"))
}
fn enabled(app: &App, p: &Principal) -> Result<()> {
    admin(p)?;
    if !app.config.replication_enabled {
        return Err(ApiError::forbidden());
    }
    Ok(())
}
pub(super) async fn status(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
) -> Result<Response> {
    enabled(&app, &p)?;
    let stats = app.call(&["FFKV.STATS"]).await?;
    let server = dto::field(&stats, "server")
        .ok_or_else(|| ApiError::unavailable("replication statistics unavailable"))?;
    // Capture all fields from one coordinator response, never separate INFO calls.
    // This machine protocol preserves u64 JSON numbers, unlike browser DTOs.
    Ok(axum::Json(json!({"instance_id":text_field(server,"instance_id")?,"revision":number_field(server,"revision")?,"role":text_field(server,"role")?,"format":1})).into_response())
}
struct Export {
    path: PathBuf,
}
impl Drop for Export {
    fn drop(&mut self) {
        for name in ["manifest.ffkv", "wal.ffkv"] {
            if let Err(error) = fs::remove_file(self.path.join(name))
                && error.kind() != io::ErrorKind::NotFound
            {
                eprintln!("replication export member cleanup failed: {error}");
            }
        }
        if let Err(error) = fs::remove_dir(&self.path)
            && error.kind() != io::ErrorKind::NotFound
        {
            eprintln!("replication export directory cleanup failed: {error}");
        }
    }
}
fn member(path: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::other("nonregular export member"));
    }
    Ok(file)
}
fn header(name: &str, size: u64) -> io::Result<Bytes> {
    // The POSIX octal size field supports 8 GiB. Refuse rather than silently emit
    // base-256/PAX extensions the deliberately narrow follower does not accept.
    if size > 0o77777777777 {
        return Err(io::Error::other("snapshot member exceeds TAR format limit"));
    }
    let mut h = [0u8; 512];
    h[..name.len()].copy_from_slice(name.as_bytes());
    h[100..108].copy_from_slice(b"0000600\0");
    h[108..116].copy_from_slice(b"0000000\0");
    h[116..124].copy_from_slice(b"0000000\0");
    h[124..136].copy_from_slice(format!("{size:011o}\0").as_bytes());
    h[136..148].copy_from_slice(b"00000000000\0");
    h[148..156].fill(b' ');
    h[156] = b'0';
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    let checksum: u64 = h.iter().map(|b| *b as u64).sum();
    h[148..156].copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
    Ok(Bytes::copy_from_slice(&h))
}
async fn send(sender: &mpsc::Sender<io::Result<Bytes>>, bytes: Bytes) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(30), sender.send(Ok(bytes)))
        .await
        .map_err(|_| io::Error::other("snapshot consumer timed out"))?
        .map_err(|_| io::Error::other("snapshot consumer disconnected"))
}
async fn stream_member(
    sender: &mpsc::Sender<io::Result<Bytes>>,
    name: &str,
    file: File,
) -> io::Result<()> {
    let size = file.metadata()?.len();
    send(sender, header(name, size)?).await?;
    let mut file = tokio::fs::File::from_std(file);
    let mut remaining = size;
    while remaining != 0 {
        let mut chunk = vec![0; remaining.min(64 * 1024) as usize];
        let n = file.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::other("snapshot file truncated"));
        }
        chunk.truncate(n);
        remaining -= n as u64;
        send(sender, Bytes::from(chunk)).await?;
    }
    let mut extra = [0];
    if file.read(&mut extra).await? != 0 {
        return Err(io::Error::other("snapshot file grew"));
    }
    let pad = ((512 - size % 512) % 512) as usize;
    if pad != 0 {
        send(sender, Bytes::from(vec![0; pad])).await?;
    }
    Ok(())
}
pub(super) async fn snapshot(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
) -> Result<Response> {
    enabled(&app, &p)?;
    let permit = app
        .mutations
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::unavailable("audited operation limit reached"))?;
    let (reply, receive) = oneshot::channel();
    // This task is the sole owner from durable intent through backup creation to
    // body completion. Dropping the HTTP handler cannot strand a created export.
    tokio::spawn(async move {
        let _permit = permit;
        let row = app.audit.row(&p, "replication-export", None);
        if let Err(error) = app.audit.append(row.clone()).await {
            let _ = reply.send(Err(error));
            return;
        }
        let name = format!("replication-export-{:032x}", rand::random::<u128>());
        let prepare = async {
            let stats = app.call(&["FFKV.STATS"]).await?;
            let server = dto::field(&stats, "server")
                .ok_or_else(|| ApiError::unavailable("source role unavailable"))?;
            if text_field(server, "role")? != "master" {
                return Err(ApiError::forbidden());
            }
            let (answer, result) = oneshot::channel();
            // No bus timeout here: after dispatch we must consume the answer and
            // clean our own export even if the HTTP request is already gone.
            app.tx
                .send(BusRequest::Command(CommandRequest::internal(
                    vec![
                        Bytes::from_static(b"FFKV.BACKUP"),
                        Bytes::from(name.clone()),
                    ],
                    answer,
                )))
                .await
                .map_err(|_| ApiError::unavailable("server unavailable"))?;
            let response = result
                .await
                .map_err(|_| ApiError::unavailable("snapshot export unavailable"))?;
            if matches!(response, Resp::Error(_)) {
                return Err(ApiError::unavailable("snapshot export rejected"));
            }
            let path = PathBuf::from(text_field(&response, "path")?);
            if text_field(&response, "name")? != name
                || path.file_name().and_then(|n| n.to_str()) != Some(name.as_str())
            {
                return Err(ApiError::unavailable("invalid export ownership"));
            }
            let owned = Export { path };
            let identity = text_field(&response, "instance_id")?.to_owned();
            let revision = number_field(&response, "revision")?;
            let wal_bytes = number_field(&response, "wal_bytes")?;
            let _frames = number_field(&response, "data_frames")?;
            let hash = text_field(&response, "sha256")?;
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(ApiError::unavailable("invalid export digest"));
            }
            let manifest = member(&owned.path.join("manifest.ffkv"))
                .map_err(|_| ApiError::unavailable("export manifest unavailable"))?;
            let wal = member(&owned.path.join("wal.ffkv"))
                .map_err(|_| ApiError::unavailable("export WAL unavailable"))?;
            if manifest
                .metadata()
                .map_err(|_| ApiError::unavailable("export metadata unavailable"))?
                .len()
                != 88
                || wal
                    .metadata()
                    .map_err(|_| ApiError::unavailable("export metadata unavailable"))?
                    .len()
                    != wal_bytes
            {
                return Err(ApiError::unavailable("export size mismatch"));
            }
            header("manifest.ffkv", 88)
                .map_err(|_| ApiError::unavailable("export format limit exceeded"))?;
            header("wal.ffkv", wal_bytes)
                .map_err(|_| ApiError::unavailable("export format limit exceeded"))?;
            let identity = axum::http::HeaderValue::from_str(&identity)
                .map_err(|_| ApiError::unavailable("invalid source identity"))?;
            let revision = axum::http::HeaderValue::from_str(&revision.to_string())
                .map_err(|_| ApiError::unavailable("invalid source revision"))?;
            Ok((owned, manifest, wal, identity, revision))
        }
        .await;
        let (owned, manifest, wal, identity, revision) = match prepare {
            Ok(value) => value,
            Err(error) => {
                let mut outcome = row;
                outcome.timestamp_ms = crate::metrics::now_ms();
                outcome.outcome = "failed".into();
                let audited = app.audit.append(outcome).await;
                let _ = reply.send(Err(audited.err().unwrap_or(error)));
                return;
            }
        };
        let (sender, receiver) = mpsc::channel::<io::Result<Bytes>>(2);
        let complete = Arc::new(AtomicBool::new(false));
        let gate = complete.clone();
        let body = Body::from_stream(stream::unfold(
            (receiver, gate, false),
            |(mut receiver, gate, ended)| async move {
                if ended {
                    return None;
                }
                match receiver.recv().await {
                    Some(item) => Some((item, (receiver, gate, false))),
                    None if gate.load(Ordering::Acquire) => None,
                    None => Some((
                        Err(io::Error::other("snapshot stream or outcome audit failed")),
                        (receiver, gate, true),
                    )),
                }
            },
        ));
        let mut response = body.into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/x-tar"),
        );
        response.headers_mut().insert(
            axum::http::HeaderName::from_static("x-ffkv-instance-id"),
            identity,
        );
        response.headers_mut().insert(
            axum::http::HeaderName::from_static("x-ffkv-revision"),
            revision,
        );
        let delivered = reply.send(Ok(response)).is_ok();
        let result = if delivered {
            tokio::time::timeout(Duration::from_secs(3600), async {
                stream_member(&sender, "manifest.ffkv", manifest).await?;
                stream_member(&sender, "wal.ffkv", wal).await?;
                send(&sender, Bytes::from_static(&[0; 1024])).await
            })
            .await
            .unwrap_or_else(|_| Err(io::Error::other("snapshot stream deadline exceeded")))
        } else {
            Err(io::Error::other("snapshot requester disconnected"))
        };
        drop(owned);
        let mut outcome = row;
        outcome.timestamp_ms = crate::metrics::now_ms();
        outcome.outcome = if result.is_ok() {
            "streamed"
        } else {
            "disconnected/error"
        }
        .into();
        let audit = app.audit.append(outcome).await;
        complete.store(result.is_ok() && audit.is_ok(), Ordering::Release);
        // EOF is emitted only after durable outcome audit. The client therefore
        // cannot ACK installation on an export whose final audit append failed.
    });
    receive
        .await
        .map_err(|_| ApiError::unavailable("snapshot export interrupted"))?
}
