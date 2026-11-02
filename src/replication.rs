//! Authenticated full-snapshot polling for a development read-only replica.
//! Not PSYNC, election, automatic failover, or a freshness guarantee. Lag is a
//! committed-revision distance, not milliseconds. Absolute TTLs require aligned clocks.
use crate::{
    metrics::Metrics,
    protocol::Resp,
    request::{CommandRequest, Request},
};
use bytes::Bytes;
use serde::Deserialize;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

pub struct Config {
    pub source_url: String,
    pub token_file: PathBuf,
    pub ca_file: Option<PathBuf>,
    pub interval: Duration,
    pub timeout: Duration,
    pub max_snapshot_bytes: u64,
    pub directory: PathBuf,
    pub allow_loopback_http: bool,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Point {
    instance_id: String,
    revision: u64,
    role: String,
    format: u32,
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn stopped() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "replication coordinator unavailable",
    )
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
}
fn field<'a>(value: &'a Resp, name: &str) -> Option<&'a Resp> {
    match value {
        Resp::Map(items) => items
            .iter()
            .find(|(key, _)| text(key) == Some(name.as_bytes()))
            .map(|(_, value)| value),
        Resp::Array(Some(items)) => items
            .as_chunks::<2>()
            .0
            .iter()
            .find(|pair| text(&pair[0]) == Some(name.as_bytes()))
            .map(|pair| &pair[1]),
        _ => None,
    }
}
fn text(value: &Resp) -> Option<&[u8]> {
    match value {
        Resp::Simple(s) => Some(s.as_bytes()),
        Resp::Status(b) | Resp::Bulk(Some(b)) => Some(b),
        _ => None,
    }
}
async fn local_id(tx: &mpsc::Sender<Request>, timeout: Duration) -> io::Result<String> {
    let (reply, receive) = oneshot::channel();
    tokio::time::timeout(timeout, async {
        tx.send(Request::Command(CommandRequest::internal(
            vec![Bytes::from_static(b"FFKV.STATS")],
            reply,
        )))
        .await
        .map_err(|_| stopped())?;
        let stats = receive.await.map_err(|_| stopped())?;
        let id = field(&stats, "server")
            .and_then(|s| field(s, "instance_id"))
            .and_then(text)
            .and_then(|v| std::str::from_utf8(v).ok())
            .filter(|s| valid_id(s))
            .ok_or_else(|| invalid("missing local instance identity"))?;
        Ok(id.to_owned())
    })
    .await
    .map_err(|_| invalid("local instance query timed out"))?
}
fn token(path: &Path) -> io::Result<String> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file()
        || meta.len() > 16385
        || meta.permissions().mode() & 0o077 != 0
        || meta.uid() != unsafe { libc::geteuid() }
    {
        return Err(invalid(
            "replication token must be an owned private bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(16386)
        .read_to_end(&mut bytes)?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    let value = String::from_utf8(bytes).map_err(|_| invalid("invalid replication token file"))?;
    if value.is_empty() || value.len() > 16384 || !value.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(invalid("invalid replication token file"));
    }
    Ok(value)
}
fn client(
    config: &Config,
) -> io::Result<(reqwest::Client, reqwest::Url, reqwest::header::HeaderValue)> {
    if config.interval < Duration::from_millis(50)
        || config.interval > Duration::from_secs(3600)
        || config.timeout.is_zero()
        || config.timeout > Duration::from_secs(3600)
        || config.max_snapshot_bytes < 2560
        || config.max_snapshot_bytes > u64::MAX / 3
    {
        return Err(invalid("invalid replication bounds"));
    }
    let mut url = reqwest::Url::parse(&config.source_url)
        .map_err(|_| invalid("invalid replication source URL"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || url.host_str().is_none()
    {
        return Err(invalid(
            "source must be an origin without credentials, query or path",
        ));
    }
    let loopback = url.host_str().is_some_and(|host| {
        host.trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    if url.scheme() != "https"
        && !(url.scheme() == "http" && loopback && config.allow_loopback_http)
    {
        return Err(invalid(
            "replication requires HTTPS; HTTP test allowance requires literal loopback",
        ));
    }
    url.set_path("/");
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(config.timeout)
        .connect_timeout(config.timeout.min(Duration::from_secs(10)))
        .pool_max_idle_per_host(1);
    if let Some(path) = &config.ca_file {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() > 1024 * 1024 {
            return Err(invalid("invalid replication CA file"));
        }
        let mut bytes = Vec::new();
        Read::by_ref(&mut file)
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 1024 * 1024 {
            return Err(invalid("oversized replication CA file"));
        }
        let certificates = reqwest::Certificate::from_pem_bundle(&bytes)
            .map_err(|_| invalid("invalid replication CA certificates"))?;
        if certificates.is_empty() {
            return Err(invalid("replication CA file has no certificates"));
        }
        for cert in certificates {
            builder = builder.add_root_certificate(cert);
        }
    }
    let mut authorization =
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token(&config.token_file)?))
            .map_err(|_| invalid("invalid replication bearer token"))?;
    authorization.set_sensitive(true);
    Ok((
        builder
            .build()
            .map_err(|_| invalid("replication TLS client initialization failed"))?,
        url,
        authorization,
    ))
}
async fn get(
    client: &reqwest::Client,
    base: &reqwest::Url,
    auth: &reqwest::header::HeaderValue,
    path: &str,
) -> io::Result<reqwest::Response> {
    let response = client
        .get(
            base.join(path)
                .map_err(|_| invalid("invalid replication endpoint"))?,
        )
        .header(reqwest::header::AUTHORIZATION, auth.clone())
        .send()
        .await
        .map_err(|_| invalid("replication source request failed"))?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(invalid("replication source rejected request"));
    }
    if response
        .headers()
        .contains_key(reqwest::header::CONTENT_ENCODING)
    {
        return Err(invalid("encoded replication response is not supported"));
    }
    Ok(response)
}
async fn status(
    client: &reqwest::Client,
    base: &reqwest::Url,
    auth: &reqwest::header::HeaderValue,
    own: &str,
) -> io::Result<Point> {
    let mut response = get(client, base, auth, "api/replication/status").await?;
    if response.content_length().is_some_and(|n| n > 4096) {
        return Err(invalid("oversized replication status"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| invalid("replication status read failed"))?
    {
        if body.len().saturating_add(chunk.len()) > 4096 {
            return Err(invalid("oversized replication status"));
        }
        body.extend_from_slice(&chunk);
    }
    let point: Point =
        serde_json::from_slice(&body).map_err(|_| invalid("invalid replication status"))?;
    if point.format != 1
        || point.role != "master"
        || !valid_id(&point.instance_id)
        || point.instance_id == own
    {
        return Err(invalid(
            "unsupported, self, or non-master replication source",
        ));
    }
    Ok(point)
}
/// Owned staging removes only fixed members created by this module, never operator backups.
struct Staging {
    root: PathBuf,
}
impl Staging {
    fn new(parent: &Path) -> io::Result<Self> {
        for _ in 0..128 {
            let root = parent.join(format!(".ffkv-replica-{:032x}", rand::random::<u128>()));
            match fs::DirBuilder::new().mode(0o700).create(&root) {
                Ok(()) => return Ok(Self { root }),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(invalid("replication staging name collisions"))
    }
}
use std::os::unix::fs::DirBuilderExt;
impl Drop for Staging {
    fn drop(&mut self) {
        for directory in ["archive", "restored"] {
            for name in ["manifest.ffkv", "wal.ffkv", "LOCK"] {
                let _ = fs::remove_file(self.root.join(directory).join(name));
            }
            let _ = fs::remove_dir(self.root.join(directory));
        }
        let _ = fs::remove_file(self.root.join("snapshot.tar"));
        let _ = fs::remove_dir(&self.root);
    }
}
fn octal(bytes: &[u8]) -> io::Result<u64> {
    let bytes = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    let s = std::str::from_utf8(bytes)
        .map_err(|_| invalid("invalid TAR numeric field"))?
        .trim_matches([' ', '\0']);
    if s.is_empty() || !s.bytes().all(|b| matches!(b, b'0'..=b'7')) {
        return Err(invalid("invalid TAR numeric field"));
    }
    u64::from_str_radix(s, 8).map_err(|_| invalid("oversized TAR numeric field"))
}
fn unpack(stage: &Staging, max: u64) -> io::Result<()> {
    let mut file = File::open(stage.root.join("snapshot.tar"))?;
    let directory = stage.root.join("archive");
    fs::create_dir(&directory)?;
    let mut seen = 0u8;
    for _ in 0..2 {
        let mut header = [0; 512];
        file.read_exact(&mut header)?;
        let checksum = octal(&header[148..156])?;
        let actual: u64 = header
            .iter()
            .enumerate()
            .map(|(i, b)| {
                if (148..156).contains(&i) {
                    32
                } else {
                    *b as u64
                }
            })
            .sum();
        let end = header[..100]
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| invalid("invalid TAR member name"))?;
        let (name, bit) = match &header[..end] {
            b"manifest.ffkv" => ("manifest.ffkv", 1),
            b"wal.ffkv" => ("wal.ffkv", 2),
            _ => return Err(invalid("unexpected TAR member")),
        };
        if checksum != actual
            || header[156] != b'0'
            || header[end..100].iter().any(|b| *b != 0)
            || header[157..257].iter().any(|b| *b != 0)
            || &header[257..263] != b"ustar\0"
            || &header[263..265] != b"00"
            || header[345..512].iter().any(|b| *b != 0)
            || seen & bit != 0
        {
            return Err(invalid(
                "noncanonical, linked, duplicate or corrupt TAR member",
            ));
        }
        let size = octal(&header[124..136])?;
        if size > max || (bit == 1 && size != 88) {
            return Err(invalid("TAR member exceeds size bound"));
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join(name))?;
        let copied = io::copy(&mut Read::by_ref(&mut file).take(size), &mut output)?;
        if copied != size {
            return Err(invalid("truncated TAR payload"));
        }
        output.sync_all()?;
        let pad = ((512 - size % 512) % 512) as usize;
        let mut padding = [0; 512];
        file.read_exact(&mut padding[..pad])?;
        if padding[..pad].iter().any(|b| *b != 0) {
            return Err(invalid("nonzero TAR padding"));
        }
        seen |= bit;
    }
    let mut end = [0; 1024];
    file.read_exact(&mut end)?;
    let mut extra = [0];
    if seen != 3 || end.iter().any(|b| *b != 0) || file.read(&mut extra)? != 0 {
        return Err(invalid("TAR has missing, extra or trailing members"));
    }
    // Immutable restore authenticates manifest, length, SHA256 and every complete
    // physical/logical WAL group. It never repairs the source.
    crate::backup::restore(&directory, &stage.root.join("restored"))?;
    Ok(())
}
async fn install(
    config: &Config,
    client: &reqwest::Client,
    base: &reqwest::Url,
    auth: &reqwest::header::HeaderValue,
    own: &str,
    expected: &Point,
    tx: &mpsc::Sender<Request>,
) -> io::Result<Point> {
    let mut response = get(client, base, auth, "api/replication/snapshot").await?;
    let id = response
        .headers()
        .get("x-ffkv-instance-id")
        .and_then(|v| v.to_str().ok())
        .filter(|id| valid_id(id))
        .ok_or_else(|| invalid("snapshot identity missing"))?
        .to_owned();
    let revision = response
        .headers()
        .get("x-ffkv-revision")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| invalid("snapshot revision missing"))?;
    if id == own || id != expected.instance_id || revision < expected.revision {
        return Err(invalid("snapshot source changed or revision regressed"));
    }
    if response
        .content_length()
        .is_some_and(|n| n > config.max_snapshot_bytes)
    {
        return Err(invalid("snapshot exceeds configured bound"));
    }
    let stage = Staging::new(&config.directory)?;
    let output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(stage.root.join("snapshot.tar"))?;
    let mut output = tokio::fs::File::from_std(output);
    let mut bytes = 0u64;
    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| invalid("snapshot download failed"))?
    {
        bytes = bytes
            .checked_add(chunk.len() as u64)
            .filter(|n| *n <= config.max_snapshot_bytes)
            .ok_or_else(|| invalid("snapshot exceeds configured bound"))?;
        output.write_all(&chunk).await?;
    }
    output.sync_all().await?;
    drop(output);
    let max = config.max_snapshot_bytes;
    let stage = tokio::task::spawn_blocking(move || {
        unpack(&stage, max)?;
        Ok::<_, io::Error>(stage)
    })
    .await
    .map_err(|_| invalid("snapshot verification task failed"))??;
    let tx = tx.clone();
    // Once dispatched, retain staging through the coordinator ACK even if run is
    // cancelled for shutdown. Timing out then deleting a queued install is unsafe.
    tokio::spawn(async move {
        let (reply, receive) = oneshot::channel();
        tx.send(Request::InstallSnapshot {
            directory: stage.root.join("restored"),
            reply,
        })
        .await
        .map_err(|_| stopped())?;
        let result = receive.await.map_err(|_| stopped())?;
        drop(stage);
        result
    })
    .await
    .map_err(|_| invalid("snapshot installation task failed"))??;
    Ok(Point {
        instance_id: id,
        revision,
        role: "master".into(),
        format: 1,
    })
}

pub async fn run(
    config: Config,
    tx: mpsc::Sender<Request>,
    metrics: Arc<Metrics>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    match fs::DirBuilder::new().mode(0o700).create(&config.directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let directory = fs::symlink_metadata(&config.directory)?;
    if !directory.is_dir()
        || directory.permissions().mode() & 0o077 != 0
        || directory.uid() != unsafe { libc::geteuid() }
    {
        return Err(invalid(
            "replication staging directory must be private and owned",
        ));
    }
    let (client, base, auth) = client(&config)?;
    let own = local_id(&tx, config.timeout).await?;
    let mut applied: Option<Point> = None;
    let mut failures = 0u32;
    metrics.set_replication("slave", "loading", None);
    loop {
        if *shutdown.borrow() {
            break;
        }
        let attempt = async {
            let observed = status(&client, &base, &auth, &own).await?;
            let lag = applied
                .as_ref()
                .filter(|p| p.instance_id == observed.instance_id)
                .map(|p| observed.revision.saturating_sub(p.revision));
            if applied.as_ref().is_some_and(|p| {
                p.instance_id == observed.instance_id && p.revision == observed.revision
            }) {
                metrics.set_replication("slave", "connected", Some(0));
                return Ok::<_, io::Error>(None);
            }
            if applied.as_ref().is_some_and(|p| {
                p.instance_id == observed.instance_id && p.revision > observed.revision
            }) {
                return Err(invalid("source revision regressed"));
            }
            metrics.set_replication("slave", "syncing", lag);
            let point = install(&config, &client, &base, &auth, &own, &observed, &tx).await?;
            Ok(Some(point))
        };
        let result = tokio::select! { result = attempt => result, _ = shutdown.changed() => break };
        match result {
            Ok(point) => {
                if let Some(point) = point {
                    applied = Some(point);
                }
                failures = 0;
                metrics.set_replication("slave", "connected", Some(0));
            }
            Err(error) => {
                failures = failures.saturating_add(1);
                metrics.set_replication(
                    "slave",
                    if applied.is_some() {
                        "disconnected"
                    } else {
                        "loading/disconnected"
                    },
                    None,
                );
                eprintln!("replication poll failed: {error}");
            }
        }
        let delay = if failures == 0 {
            config.interval
        } else {
            config
                .interval
                .max(Duration::from_millis(250))
                .saturating_mul(1u32 << failures.min(7))
                .min(Duration::from_secs(30))
        };
        tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = shutdown.changed() => break }
    }
    metrics.set_replication("slave", "stopped", None);
    Ok(())
}
