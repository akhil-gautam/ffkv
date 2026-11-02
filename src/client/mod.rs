//! Shared wire client. Connections never reconnect or replay commands.
mod decoder;

use bytes::Bytes;
use std::{
    fs::OpenOptions,
    io::{self, Cursor, IoSlice, Read},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::{TlsConnector, client::TlsStream};

pub struct Config {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: u32,
    pub protocol: u8,
    pub tls: Option<TlsConfig>,
    pub connect_timeout: Duration,
}

pub struct TlsConfig {
    pub ca_cert: Option<PathBuf>,
    pub server_name: Option<String>,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
}

#[derive(Debug, PartialEq)]
pub enum Frame {
    Simple(Bytes),
    Error(Bytes),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
    Map(Vec<(Frame, Frame)>),
    Set(Vec<Frame>),
    Double(f64),
    Boolean(bool),
    BigNumber(Bytes),
    Verbatim { format: [u8; 3], data: Bytes },
    Push(Vec<Frame>),
    Attribute(Vec<(Frame, Frame)>, Box<Frame>),
}

impl Frame {
    pub fn is_push(&self) -> bool {
        match self {
            Self::Push(_) => true,
            Self::Attribute(_, value) => value.is_push(),
            _ => false,
        }
    }

    pub fn has_error(&self) -> bool {
        match self {
            Self::Error(_) => true,
            Self::Array(values) | Self::Set(values) | Self::Push(values) => {
                values.iter().any(Self::has_error)
            }
            Self::Map(values) => values.iter().any(|(k, v)| k.has_error() || v.has_error()),
            Self::Attribute(values, value) => {
                value.has_error() || values.iter().any(|(k, v)| k.has_error() || v.has_error())
            }
            _ => false,
        }
    }
}

enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_write(cx, buf),
        }
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_write_vectored(cx, bufs),
        }
    }
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(&mut **stream).poll_shutdown(cx),
        }
    }
}

pub struct Client {
    stream: Stream,
    decoder: decoder::Decoder,
    usable: bool,
}

/// Independently readable half of a negotiated connection. Decoder state survives
/// cancellation, including bytes read ahead while authenticating or selecting a DB.
pub struct ClientReader {
    stream: tokio::io::ReadHalf<Stream>,
    decoder: decoder::Decoder,
    usable: bool,
}

/// Independently writable half. An interrupted write permanently poisons it.
pub struct ClientWriter {
    stream: tokio::io::WriteHalf<Stream>,
    usable: bool,
}

impl Client {
    pub async fn connect(config: Config) -> io::Result<Self> {
        if !matches!(config.protocol, 2 | 3) {
            return Err(invalid("protocol must be 2 or 3"));
        }
        if config.username.is_some() && config.password.is_none() {
            return Err(invalid("a username requires a password"));
        }
        let deadline = config.connect_timeout;
        timeout(deadline, Self::connect_inner(config))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "connection initialization timed out",
                )
            })?
    }

    async fn connect_inner(config: Config) -> io::Result<Self> {
        let stream = TcpStream::connect((config.host.as_str(), config.port)).await?;
        stream.set_nodelay(true)?;
        let stream = match &config.tls {
            None => Stream::Plain(stream),
            Some(tls) => {
                let name = tls
                    .server_name
                    .as_deref()
                    .unwrap_or(&config.host)
                    .to_owned();
                let name = rustls::pki_types::ServerName::try_from(name)
                    .map_err(|_| invalid("invalid TLS server name"))?;
                let connector = tls_connector(tls)?;
                let stream = connector.connect(name, stream).await.map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "TLS handshake or certificate verification failed",
                    )
                })?;
                Stream::Tls(Box::new(stream))
            }
        };
        let mut client = Self {
            stream,
            decoder: decoder::Decoder::new(),
            usable: true,
        };
        if config.protocol == 3 {
            let mut hello = vec![Bytes::from_static(b"HELLO"), Bytes::from_static(b"3")];
            if let Some(password) = config.password {
                hello.extend([
                    Bytes::from_static(b"AUTH"),
                    Bytes::from(config.username.unwrap_or_else(|| "default".into())),
                    Bytes::from(password),
                ]);
            }
            client.write_command(&hello).await?;
            let reply = client.read_frame().await?;
            if reply.has_error() {
                return Err(invalid(
                    "server rejected HELLO 3 (protocol negotiation or authentication failed)",
                ));
            }
            if !matches!(without_attributes(&reply), Frame::Map(_)) {
                return Err(invalid("unexpected HELLO 3 response"));
            }
        } else if let Some(password) = config.password {
            let mut auth = vec![Bytes::from_static(b"AUTH")];
            if let Some(username) = config.username {
                auth.push(Bytes::from(username));
            }
            auth.push(Bytes::from(password));
            client.write_command(&auth).await?;
            require_ok(
                client.read_frame().await?,
                "authentication failed (server rejected AUTH)",
            )?;
        }
        if config.database != 0 {
            let select = [
                Bytes::from_static(b"SELECT"),
                Bytes::from(config.database.to_string()),
            ];
            client.write_command(&select).await?;
            require_ok(
                client.read_frame().await?,
                "database selection failed (server rejected SELECT)",
            )?;
        }
        Ok(client)
    }

    /// Split after negotiation without losing decoder read-ahead. Both halves
    /// must be dropped to close the connection; there is no reconnect path.
    pub fn into_split(self) -> (ClientReader, ClientWriter) {
        let (reader, writer) = tokio::io::split(self.stream);
        (
            ClientReader {
                stream: reader,
                decoder: self.decoder,
                usable: self.usable,
            },
            ClientWriter {
                stream: writer,
                usable: self.usable,
            },
        )
    }

    pub async fn write_command(&mut self, args: &[Bytes]) -> io::Result<()> {
        write_command(&mut self.stream, &mut self.usable, args).await
    }

    pub async fn read_frame(&mut self) -> io::Result<Frame> {
        read_frame(&mut self.stream, &mut self.decoder, &mut self.usable).await
    }
}

impl ClientReader {
    pub async fn read_frame(&mut self) -> io::Result<Frame> {
        read_frame(&mut self.stream, &mut self.decoder, &mut self.usable).await
    }

    /// Raw mode may start at a RESP frame boundary, never in a partly decoded
    /// frame. Empty bytes mean EOF, not a timeout.
    pub async fn read_raw(&mut self, max_bytes: usize) -> io::Result<Bytes> {
        check_usable(self.usable)?;
        if max_bytes == 0 {
            return Err(invalid("raw read size must be positive"));
        }
        let buffered = self.decoder.take_raw(max_bytes)?;
        if !buffered.is_empty() {
            return Ok(buffered);
        }
        let mut bytes = [0u8; 8192];
        let limit = max_bytes.min(bytes.len());
        match self.stream.read(&mut bytes[..limit]).await {
            Ok(n) => {
                if n == 0 {
                    self.usable = false;
                }
                Ok(Bytes::copy_from_slice(&bytes[..n]))
            }
            Err(error) => {
                self.usable = false;
                Err(error)
            }
        }
    }
}

impl ClientWriter {
    pub async fn write_command(&mut self, args: &[Bytes]) -> io::Result<()> {
        write_command(&mut self.stream, &mut self.usable, args).await
    }

    pub async fn write_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        check_usable(self.usable)?;
        self.usable = false;
        self.stream.write_all(bytes).await?;
        self.stream.flush().await?;
        self.usable = true;
        Ok(())
    }
}

fn check_usable(usable: bool) -> io::Result<()> {
    if usable {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "connection is unusable after an interrupted write or transport/protocol error",
        ))
    }
}

async fn write_command<W: AsyncWrite + Unpin>(
    stream: &mut W,
    usable: &mut bool,
    args: &[Bytes],
) -> io::Result<()> {
    check_usable(*usable)?;
    validate_command(args)?;
    let mut headers = Vec::with_capacity(args.len() + 1);
    headers.push(Header::new(b'*', args.len()));
    headers.extend(args.iter().map(|arg| Header::new(b'$', arg.len())));
    let mut slices = Vec::with_capacity(args.len() * 3 + 1);
    slices.push(IoSlice::new(headers[0].bytes()));
    for (arg, header) in args.iter().zip(&headers[1..]) {
        slices.push(IoSlice::new(header.bytes()));
        slices.push(IoSlice::new(arg));
        slices.push(IoSlice::new(b"\r\n"));
    }
    // A dropped write future must not allow the next call to append to a
    // partially transmitted command.
    *usable = false;
    let mut remaining = slices.as_mut_slice();
    while !remaining.is_empty() {
        let n = stream.write_vectored(remaining).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "connection stopped accepting command bytes",
            ));
        }
        IoSlice::advance_slices(&mut remaining, n);
    }
    stream.flush().await?;
    *usable = true;
    Ok(())
}

async fn read_frame<R: AsyncRead + Unpin>(
    stream: &mut R,
    decoder: &mut decoder::Decoder,
    usable: &mut bool,
) -> io::Result<Frame> {
    check_usable(*usable)?;
    loop {
        match decoder.decode() {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => (),
            Err(error) => {
                *usable = false;
                return Err(error);
            }
        }
        let limit = match decoder.read_limit() {
            Ok(limit) => limit,
            Err(error) => {
                *usable = false;
                return Err(error);
            }
        };
        // Incremental state belongs to the reader, not this future.
        let mut bytes = [0u8; 8192];
        match stream.read(&mut bytes[..limit]).await {
            Ok(0) => {
                *usable = false;
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before a complete RESP reply",
                ));
            }
            Ok(n) => decoder.feed(&bytes[..n]),
            Err(error) => {
                *usable = false;
                return Err(error);
            }
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn require_ok(frame: Frame, message: &'static str) -> io::Result<()> {
    match without_attributes(&frame) {
        Frame::Simple(value) if value.as_ref() == b"OK" => Ok(()),
        _ => Err(invalid(message)),
    }
}

fn without_attributes(mut frame: &Frame) -> &Frame {
    while let Frame::Attribute(_, value) = frame {
        frame = value;
    }
    frame
}

struct Header {
    data: [u8; 24],
    start: usize,
}
impl Header {
    fn new(marker: u8, mut n: usize) -> Self {
        let mut data = [0; 24];
        data[22..].copy_from_slice(b"\r\n");
        let mut start = 22;
        loop {
            start -= 1;
            data[start] = b'0' + (n % 10) as u8;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        start -= 1;
        data[start] = marker;
        Self { data, start }
    }
    fn bytes(&self) -> &[u8] {
        &self.data[self.start..]
    }
}

pub fn validate_command(args: &[Bytes]) -> io::Result<()> {
    if args.is_empty() || args.len() > 1024 {
        return Err(invalid("command must contain 1..1024 arguments"));
    }
    let mut total = Header::new(b'*', args.len()).bytes().len();
    for arg in args {
        total = total
            .checked_add(Header::new(b'$', arg.len()).bytes().len())
            .and_then(|n| n.checked_add(arg.len()))
            .and_then(|n| n.checked_add(2))
            .filter(|n| *n <= 8 * 1024 * 1024)
            .ok_or_else(|| invalid("command exceeds 8 MiB including framing"))?;
    }
    Ok(())
}

fn pem_file(path: &Path) -> io::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .map_err(|_| invalid("cannot open TLS material"))?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid("cannot inspect TLS material"))?;
    if !metadata.is_file() || metadata.len() > 1024 * 1024 {
        return Err(invalid(
            "TLS material must be a regular file no larger than 1 MiB",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| invalid("cannot read TLS material"))?;
    if bytes.len() > 1024 * 1024 {
        return Err(invalid("TLS material exceeds 1 MiB"));
    }
    Ok(bytes)
}

fn tls_connector(tls: &TlsConfig) -> io::Result<TlsConnector> {
    if tls.client_cert.is_some() != tls.client_key.is_some() {
        return Err(invalid(
            "TLS client certificate and key must be provided together",
        ));
    }
    let mut roots = rustls::RootCertStore::empty();
    if let Some(path) = &tls.ca_cert {
        let bytes = pem_file(path)?;
        let certificates = rustls_pemfile::certs(&mut Cursor::new(bytes))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("invalid TLS CA PEM"))?;
        if certificates.is_empty() {
            return Err(invalid("TLS CA file contains no certificates"));
        }
        for certificate in certificates {
            roots
                .add(certificate)
                .map_err(|_| invalid("invalid TLS CA certificate"))?;
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| invalid("TLS protocol configuration failed"))?
    .with_root_certificates(roots);
    let config = if let (Some(cert), Some(key)) = (&tls.client_cert, &tls.client_key) {
        let certificates = rustls_pemfile::certs(&mut Cursor::new(pem_file(cert)?))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| invalid("invalid TLS client certificate PEM"))?;
        if certificates.is_empty() {
            return Err(invalid("TLS client certificate chain is empty"));
        }
        let mut bytes = pem_file(key)?;
        let key = rustls_pemfile::private_key(&mut Cursor::new(bytes.as_slice()));
        bytes.fill(0);
        let key = key
            .map_err(|_| invalid("invalid TLS private key PEM"))?
            .ok_or_else(|| invalid("TLS private key is missing"))?;
        builder
            .with_client_auth_cert(certificates, key)
            .map_err(|_| invalid("TLS client certificate/private key validation failed"))?
    } else {
        builder.with_no_client_auth()
    };
    Ok(TlsConnector::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn duplex_pipeline_drains_before_request_write_can_finish() {
        // The peer refuses to read the request until its large reply is sent.
        // A write-entire-batch-then-read implementation deadlocks at 64 bytes.
        let (client, mut peer) = tokio::io::duplex(64);
        let (mut reader, mut writer) = tokio::io::split(client);
        let payload = Bytes::from(vec![b'x'; 4096]);
        let args = [Bytes::from_static(b"ECHO"), payload];
        let server = async {
            peer.write_all(b"$4096\r\n").await.unwrap();
            peer.write_all(&vec![b'y'; 4096]).await.unwrap();
            peer.write_all(b"\r\n").await.unwrap();
            let mut request = vec![0; 4119];
            peer.read_exact(&mut request).await.unwrap();
            assert!(request.starts_with(b"*2\r\n$4\r\nECHO\r\n$4096\r\n"));
        };
        let client = async {
            let mut readable = true;
            let mut writable = true;
            let mut decoder = decoder::Decoder::new();
            let (_, reply) = tokio::try_join!(
                write_command(&mut writer, &mut writable, &args),
                read_frame(&mut reader, &mut decoder, &mut readable),
            )
            .unwrap();
            assert_eq!(reply, Frame::Bulk(Bytes::from(vec![b'y'; 4096])));
        };
        timeout(Duration::from_secs(2), async {
            tokio::join!(server, client);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn interrupted_write_poison_prevents_appending_another_command() {
        let (mut stream, _peer) = tokio::io::duplex(8);
        let mut usable = true;
        let args = [Bytes::from_static(b"ECHO"), Bytes::from_static(b"payload")];
        assert!(
            timeout(
                Duration::ZERO,
                write_command(&mut stream, &mut usable, &args)
            )
            .await
            .is_err()
        );
        assert!(!usable);
        assert_eq!(
            write_command(&mut stream, &mut usable, &args)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotConnected
        );
    }

    #[tokio::test]
    async fn interrupted_read_keeps_incremental_decoder_state() {
        let (mut stream, mut peer) = tokio::io::duplex(64);
        peer.write_all(b"*2\r\n$3\r\nab").await.unwrap();
        let mut decoder = decoder::Decoder::new();
        let mut usable = true;
        assert!(
            timeout(
                Duration::ZERO,
                read_frame(&mut stream, &mut decoder, &mut usable)
            )
            .await
            .is_err()
        );
        assert!(usable);
        peer.write_all(b"c\r\n:42\r\n").await.unwrap();
        assert_eq!(
            read_frame(&mut stream, &mut decoder, &mut usable)
                .await
                .unwrap(),
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"abc")),
                Frame::Integer(42)
            ])
        );
    }
}
