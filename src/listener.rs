//! The node's HTTP listener: HTTP/1.1 and cleartext HTTP/2 (h2c, prior
//! knowledge) on one port.
//!
//! This is what `axum::serve` does, with one difference: it lets us set the
//! HTTP/2 flow-control windows and stream limits that node-to-node traffic
//! needs (see [`peer_h2`]). Browsers and CouchDB clients speak HTTP/1.1 and
//! see no change; the connection builder tells the two apart by the HTTP/2
//! connection preface.

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// HTTP/2 settings shared by the peer client and the listener.
///
/// * Stream window 1 MiB, connection window 8 MiB. The protocol default of
///   64 KiB caps a stream at 64 KiB per round trip, which starves a large
///   `_bulk_docs` or `_changes` body. A stream can hold at most an eighth of
///   the connection window, so it takes eight concurrently streaming large
///   bodies to fill the window that small requests share; a receiver that
///   reads promptly returns window as it goes.
/// * Frames up to 64 KiB (default 16 KiB): fewer frames and syscalls for big
///   bodies, while frames of different streams are still interleaved, so a
///   small request waits behind at most one frame.
/// * Keep-alive ping every 30 s, a dead peer declared after 10 s without an
///   answer, also while idle. A pooled connection to a machine that vanished
///   is noticed in about 40 s instead of at the next request's 60 s timeout.
pub(crate) mod peer_h2 {
    use std::time::Duration;

    pub(crate) const STREAM_WINDOW: u32 = 1024 * 1024;
    pub(crate) const CONNECTION_WINDOW: u32 = 8 * 1024 * 1024;
    pub(crate) const MAX_FRAME: u32 = 64 * 1024;
    pub(crate) const PING_INTERVAL: Duration = Duration::from_secs(30);
    pub(crate) const PING_TIMEOUT: Duration = Duration::from_secs(10);
    /// Concurrent streams the listener allows per connection. A coordinator
    /// multiplexes every request in flight to one peer over a single
    /// connection, so this bounds a peer's fan-out; the node's own admission
    /// limits (`--max-in-flight`) are what actually cap the load.
    pub(crate) const MAX_STREAMS: u32 = 256;
}

/// Serve `app` on `listener` until `shutdown` is cancelled, then stop
/// accepting and wait for the open connections to finish their requests.
pub(crate) async fn serve(listener: TcpListener, app: Router, shutdown: CancellationToken) {
    let mut builder = Builder::new(TokioExecutor::new());
    builder
        .http2()
        .timer(TokioTimer::new())
        .initial_stream_window_size(peer_h2::STREAM_WINDOW)
        .initial_connection_window_size(peer_h2::CONNECTION_WINDOW)
        .max_frame_size(peer_h2::MAX_FRAME)
        .max_concurrent_streams(peer_h2::MAX_STREAMS)
        .keep_alive_interval(peer_h2::PING_INTERVAL)
        .keep_alive_timeout(peer_h2::PING_TIMEOUT);
    let graceful = GracefulShutdown::new();
    loop {
        let stream = tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(error) => {
                    // Out of file descriptors and the like: back off instead
                    // of spinning on the same error.
                    tracing::warn!("accept failed: {error}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        // Streamed responses are written in several pieces. With Nagle's
        // algorithm a later piece waits for the client's delayed ACK of the
        // first, adding about 40 ms to every such response.
        if let Err(error) = stream.set_nodelay(true) {
            tracing::debug!("could not set TCP_NODELAY: {error}");
        }
        let connection = builder
            .serve_connection_with_upgrades(
                TokioIo::new(stream),
                TowerToHyperService::new(app.clone()),
            )
            .into_owned();
        let connection = graceful.watch(connection);
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::trace!("connection ended: {error}");
            }
        });
    }
    drop(listener);
    graceful.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post};

    /// A listener on a free port serving: the protocol version a request
    /// arrived with, and the size of a posted body.
    async fn start() -> (String, CancellationToken, tokio::task::JoinHandle<()>) {
        let app =
            Router::new()
                .route(
                    "/version",
                    get(|request: axum::extract::Request| async move {
                        format!("{:?}", request.version())
                    }),
                )
                .route(
                    "/size",
                    post(|body: axum::body::Body| async move {
                        let bytes = axum::body::to_bytes(body, usize::MAX).await;
                        bytes.map(|bytes| bytes.len()).unwrap_or(0).to_string()
                    }),
                );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(serve(listener, app, shutdown.clone()));
        (base, shutdown, task)
    }

    #[tokio::test]
    async fn one_port_serves_http1_and_h2c() {
        let (base, shutdown, task) = start().await;
        let h1 = crate::cluster_http::peer_client(false, false).unwrap();
        let h2 = crate::cluster_http::peer_client(true, false).unwrap();
        let reply = h1.get(format!("{base}/version")).send().await.unwrap();
        assert_eq!(reply.version(), reqwest::Version::HTTP_11);
        assert_eq!(reply.text().await.unwrap(), "HTTP/1.1");
        let reply = h2.get(format!("{base}/version")).send().await.unwrap();
        assert_eq!(reply.version(), reqwest::Version::HTTP_2);
        assert_eq!(reply.text().await.unwrap(), "HTTP/2.0");
        shutdown.cancel();
        // The listener drains and returns once the clients are idle.
        drop((h1, h2));
        task.await.unwrap();
    }

    /// Large bodies and small requests share one HTTP/2 connection and all
    /// complete (the windows are big enough to pass 12 MiB streams).
    #[tokio::test]
    async fn large_and_small_requests_share_one_h2_connection() {
        let (base, shutdown, task) = start().await;
        let client = crate::cluster_http::peer_client(true, false).unwrap();
        let big = vec![b'x'; 12 * 1024 * 1024];
        let mut pending = Vec::new();
        for _ in 0..3 {
            let (client, url, body) = (client.clone(), format!("{base}/size"), big.clone());
            pending.push(tokio::spawn(async move {
                client.post(url).body(body).send().await?.text().await
            }));
        }
        for _ in 0..20 {
            let reply = client.get(format!("{base}/version")).send().await.unwrap();
            assert_eq!(reply.version(), reqwest::Version::HTTP_2);
        }
        for pending in pending {
            assert_eq!(pending.await.unwrap().unwrap(), big.len().to_string());
        }
        shutdown.cancel();
        drop(client);
        task.await.unwrap();
    }
}
