use super::TlsConfig;
use axum::Router;
use axum_server::accept::{Accept, DefaultAcceptor};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
};
#[derive(Clone)]
struct Limited<A> {
    inner: A,
    connections: Arc<Semaphore>,
}
struct Stream<I> {
    inner: I,
    _permit: OwnedSemaphorePermit,
    idle: Pin<Box<tokio::time::Sleep>>,
    lifetime: Pin<Box<tokio::time::Sleep>>,
}
impl<I> Stream<I> {
    fn touch(&mut self) {
        self.idle
            .as_mut()
            .reset(tokio::time::Instant::now() + Duration::from_secs(15));
    }
    fn timeout(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.idle.as_mut().poll(cx).is_ready() || self.lifetime.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "management connection expired",
            ))
        } else {
            Ok(())
        }
    }
}
impl<I: AsyncRead + Unpin> AsyncRead for Stream<I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if let Err(e) = this.timeout(cx) {
            return Poll::Ready(Err(e));
        }
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            this.touch();
        }
        result
    }
}
impl<I: AsyncWrite + Unpin> AsyncWrite for Stream<I> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if let Err(e) = this.timeout(cx) {
            return Poll::Ready(Err(e));
        }
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        if matches!(result,Poll::Ready(Ok(n))if n>0) {
            this.touch();
        }
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
impl<A, S> Accept<TcpStream, S> for Limited<A>
where
    A: Accept<TcpStream, S> + Clone + Send + 'static,
    A::Future: Send,
    A::Stream: Send + 'static,
    A::Service: Send + 'static,
    S: Send + 'static,
{
    type Stream = Stream<A::Stream>;
    type Service = A::Service;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;
    fn accept(&self, stream: TcpStream, service: S) -> Self::Future {
        let permit = self.connections.clone().try_acquire_owned();
        let inner = self.inner.clone();
        Box::pin(async move {
            let permit = permit.map_err(|_| {
                io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "management connection limit",
                )
            })?;
            stream.set_nodelay(true)?;
            let (stream, service) =
                tokio::time::timeout(Duration::from_secs(5), inner.accept(stream, service))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "TLS handshake deadline")
                    })??;
            Ok((
                Stream {
                    inner: stream,
                    _permit: permit,
                    idle: Box::pin(tokio::time::sleep(Duration::from_secs(15))),
                    lifetime: Box::pin(tokio::time::sleep(Duration::from_secs(300))),
                },
                service,
            ))
        })
    }
}
pub(super) async fn run(
    listener: TcpListener,
    tls: Option<TlsConfig>,
    router: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    let listener = listener.into_std()?;
    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();
    let watcher = tokio::spawn(async move {
        shutdown.await;
        shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
    });
    let limit = Arc::new(Semaphore::new(128));
    let result = if let Some(tls) = tls {
        let config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(tls.cert_file, tls.key_file)
                .await?;
        let mut server = axum_server::from_tcp(listener)
            .acceptor(Limited {
                inner: axum_server::tls_rustls::RustlsAcceptor::new(config),
                connections: limit,
            })
            .handle(handle);
        configure(&mut server);
        server.serve(router.into_make_service()).await
    } else {
        let mut server = axum_server::from_tcp(listener)
            .acceptor(Limited {
                inner: DefaultAcceptor::new(),
                connections: limit,
            })
            .handle(handle);
        configure(&mut server);
        server.serve(router.into_make_service()).await
    };
    watcher.abort();
    result
}
fn configure<A>(server: &mut axum_server::Server<A>) {
    server
        .http_builder()
        .http1()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(Duration::from_secs(5))
        .max_buf_size(32768)
        .max_headers(64);
    server
        .http_builder()
        .http2()
        .timer(hyper_util::rt::TokioTimer::new())
        .max_concurrent_streams(16)
        .max_header_list_size(32768);
}
