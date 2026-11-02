//! RESP transport; TLS negotiation never falls back to plaintext.
use std::{
    fs::OpenOptions,
    io::{self, Cursor, Read},
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

const MAX_PEM_BYTES: u64 = 1024 * 1024;

pub enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Stream {
    pub async fn accept(stream: TcpStream, acceptor: Option<TlsAcceptor>) -> io::Result<Self> {
        match acceptor {
            None => Ok(Self::Plain(stream)),
            Some(acceptor) => timeout(Duration::from_secs(5), acceptor.accept(stream))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))?
                .map(|stream| Self::Tls(Box::new(stream))),
        }
    }

    fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            Self::Tls(stream) => stream.get_ref().0,
        }
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.tcp().peer_addr()
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp().local_addr()
    }
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.tcp().set_nodelay(nodelay)
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write_vectored(cx, bufs),
        }
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn pem_file(path: &Path) -> io::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Open the final component atomically without following a symlink. A
        // malicious FIFO must not block before the regular-file check below.
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    }
    #[cfg(not(unix))]
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(invalid("TLS material must not be a symlink"));
    }
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_PEM_BYTES {
        return Err(invalid("TLS material must be a bounded regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o022 != 0 {
            return Err(invalid("TLS material must not be group/world writable"));
        }
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_PEM_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_PEM_BYTES {
        return Err(invalid("TLS material exceeds size limit"));
    }
    Ok(bytes)
}

pub fn server_config(cert: &Path, key: &Path) -> io::Result<TlsAcceptor> {
    let certificate_bytes = pem_file(cert)?;
    let mut key_bytes = pem_file(key)?;
    let certificates = rustls_pemfile::certs(&mut Cursor::new(certificate_bytes))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| invalid("invalid TLS certificate PEM"))?;
    if certificates.is_empty() {
        return Err(invalid("TLS certificate chain is empty"));
    }
    // Parse every chain member, not just the end-entity certificate used for
    // signing-key matching. This is validation, not a client trust override.
    let mut parsed = rustls::RootCertStore::empty();
    for certificate in &certificates {
        parsed
            .add(certificate.clone())
            .map_err(|_| invalid("invalid TLS certificate DER"))?;
    }
    let key = rustls_pemfile::private_key(&mut Cursor::new(key_bytes.as_slice()));
    key_bytes.fill(0);
    let key = key
        .map_err(|_| invalid("invalid TLS private key PEM"))?
        .ok_or_else(|| invalid("TLS private key is missing"))?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| invalid("TLS protocol configuration failed"))?
    .with_no_client_auth()
    .with_single_cert(certificates, key)
    .map_err(|_| invalid("TLS certificate/private key validation failed"))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}
