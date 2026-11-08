//! Plumbing for node-to-node HTTP: URL encoding, the forwarded marker and
//! the cluster secret, and credential forwarding.

use crate::auth::Auth;
use crate::cluster::Cluster;
use axum::http::{header, HeaderMap};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// What a node needs to talk to its peers: the placement, an HTTP client and
/// the credentials of this node. Core cluster modules depend on this instead
/// of the HTTP layer's application state.
pub struct ClusterCtx {
    pub cluster: Cluster,
    pub client: reqwest::Client,
    pub auth: Option<Auth>,
    /// Which peers discovery last found unreachable.
    pub health: NodeHealth,
}

/// What the last discovery pass found out about each peer. Reads use it to
/// send a down node's shards to a replica in the first round instead of
/// failing against the node first. It is a hint: a stale "down" costs a
/// detour, never a wrong answer, and the next pass that reaches the node
/// clears it.
#[derive(Default)]
pub struct NodeHealth {
    down: Mutex<Vec<bool>>,
}

impl NodeHealth {
    pub fn set_down(&self, node: usize, down: bool) {
        let mut flags = self.down.lock().unwrap_or_else(|p| p.into_inner());
        if flags.len() <= node {
            flags.resize(node + 1, false);
        }
        flags[node] = down;
    }

    pub fn is_down(&self, node: usize) -> bool {
        self.down
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(node)
            .copied()
            .unwrap_or(false)
    }
}

/// Internal cluster calls must keep the caller's credentials. Basic auth and
/// the `_session` cookie both arrive as plain headers; the peer is trusted, so
/// forward whichever one the client used.
pub(crate) fn forward_client_auth(
    request: reqwest::RequestBuilder,
    headers: &HeaderMap,
) -> reqwest::RequestBuilder {
    let request = match headers.get(header::AUTHORIZATION) {
        Some(value) => request.header(header::AUTHORIZATION, value),
        None => request,
    };
    match headers.get(header::COOKIE) {
        Some(value) => request.header(header::COOKIE, value),
        None => request,
    }
}

/// A peer URL with every path segment percent-encoded. Database names may hold
/// `/` (CouchDB clients send `a%2Fb`) and document ids may hold `?`, `#` or
/// `%`, so a segment is never interpolated raw: the receiving node decodes
/// each one exactly once.
pub(crate) fn peer_url(peer: &str, segments: &[&str]) -> String {
    let mut url = peer.to_string();
    for segment in segments {
        url.push('/');
        url.push_str(&percent_encode(segment));
    }
    url
}

/// An internal cluster request: encoded URL plus the forwarded marker and the
/// cluster secret that make the peer serve it from its local shards.
pub(crate) fn internal_request_raw(
    client: &reqwest::Client,
    secret: &str,
    method: reqwest::Method,
    peer: &str,
    segments: &[&str],
) -> reqwest::RequestBuilder {
    client
        .request(method, peer_url(peer, segments))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", secret)
}

/// `internal_request_raw` plus credentials: the calling client's own when the request
/// was made on its behalf (`Some(headers)`), otherwise this node's.
pub(crate) fn internal_request(
    ctx: &ClusterCtx,
    method: reqwest::Method,
    peer: &str,
    segments: &[&str],
    client_headers: Option<&HeaderMap>,
) -> reqwest::RequestBuilder {
    let request = internal_request_raw(&ctx.client, &ctx.cluster.secret, method, peer, segments);
    match (client_headers, &ctx.auth) {
        (Some(headers), _) => forward_client_auth(request, headers),
        (None, Some(auth)) => request.basic_auth(&auth.user, Some(&auth.pass)),
        (None, None) => request,
    }
}

// ---- Peer round-trip accounting -------------------------------------------
//
// Every client request that reaches this node runs inside a `PeerStats` scope
// (a task-local installed by the request-logging middleware). Peer calls made
// on its behalf go through `send_peer`, which records them there.
//
// Definitions, used by the metrics, the tracing fields and the tests:
//
// * client request vs nested request: a request that carries the cluster
//   marker or secret headers was sent by another node. Its own peer calls
//   (a forwarded write fanning out to validators, say) are reported under
//   the `nested_*` counters, so the cost of a client request is its
//   coordinator's `peer_*` plus the `nested_*` of the nodes it reached.
// * peer call: one HTTP request sent to another node with `send_peer`. Retries
//   and fallback requests each count. Background work (anti-entropy, discovery)
//   runs outside any request scope and is not counted.
// * round: a maximal period during which at least one peer call of the request
//   is in flight. A group of calls issued concurrently and awaited together is
//   one round; a call issued after every earlier call finished starts a new
//   one. Detached tasks that keep running after the response head is decided
//   (slow replica writes) still belong to the request and extend the round
//   they are part of.
// * peer wait: total time during which a round was open. It runs from issuing
//   the request until the response head arrives; reading the body is not
//   included, so it slightly understates the network share for large replies.
//   Local work is the request duration minus the peer wait.

tokio::task_local! {
    static PEER_SCOPE: Arc<PeerStats>;
}

static OPEN_SCOPES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Requests whose peer accounting has not finished yet, including detached
/// replica tasks still running on their behalf. Tests wait for zero before
/// reading the per-route counters.
pub(crate) fn open_scopes() -> u64 {
    OPEN_SCOPES.load(std::sync::atomic::Ordering::Relaxed)
}

#[derive(Default)]
struct PeerTally {
    calls: u64,
    rounds: u64,
    in_flight: u64,
    round_started: Option<Instant>,
    wait: Duration,
}

/// Peer-call accounting for one client request. When the last holder (the
/// request itself or a detached task it spawned) drops it, the totals are
/// added to the per-route metrics.
pub(crate) struct PeerStats {
    route: String,
    /// The request came from another node (a forwarded or scattered call), so
    /// its peer calls are nested inside some coordinator's request.
    internal: bool,
    tally: Mutex<PeerTally>,
}

/// Totals so far: `(calls, rounds, wait)`.
pub(crate) type PeerTotals = (u64, u64, Duration);

impl PeerStats {
    pub(crate) fn new(route: String, internal: bool) -> Arc<PeerStats> {
        OPEN_SCOPES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Arc::new(PeerStats {
            route,
            internal,
            tally: Mutex::new(PeerTally::default()),
        })
    }

    fn tally(&self) -> std::sync::MutexGuard<'_, PeerTally> {
        self.tally
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub(crate) fn totals(&self) -> PeerTotals {
        let tally = self.tally();
        let open = tally
            .round_started
            .map(|at| at.elapsed())
            .unwrap_or_default();
        (tally.calls, tally.rounds, tally.wait + open)
    }

    fn begin(self: &Arc<Self>) -> PeerCall {
        let mut tally = self.tally();
        tally.calls += 1;
        if tally.in_flight == 0 {
            tally.rounds += 1;
            tally.round_started = Some(Instant::now());
        }
        tally.in_flight += 1;
        PeerCall(self.clone())
    }
}

impl Drop for PeerStats {
    fn drop(&mut self) {
        let (calls, rounds, wait) = self.totals();
        if calls > 0 {
            let wait_ms = wait.as_secs_f64() * 1000.0;
            crate::telemetry::record_peer(&self.route, self.internal, calls, rounds, wait_ms);
        }
        OPEN_SCOPES.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// One peer call in flight; ends on drop, so a cancelled call is closed too.
struct PeerCall(Arc<PeerStats>);

impl Drop for PeerCall {
    fn drop(&mut self) {
        let mut tally = self.0.tally();
        tally.in_flight = tally.in_flight.saturating_sub(1);
        if tally.in_flight == 0 {
            if let Some(started) = tally.round_started.take() {
                tally.wait += started.elapsed();
            }
        }
    }
}

/// Run `future` with `stats` as the current request's peer accounting.
pub(crate) async fn with_stats<F: Future>(stats: Arc<PeerStats>, future: F) -> F::Output {
    PEER_SCOPE.scope(stats, future).await
}

/// Keep counting peer calls while a streamed response body is produced: the
/// body's work (a scatter behind `_all_docs`, a `_changes` page) runs when the
/// body is polled, after the handler returned. The accounting also stays open
/// until the body ends. A body of known size is complete already and is
/// returned untouched.
pub(crate) fn scope_body(stats: Arc<PeerStats>, body: axum::body::Body) -> axum::body::Body {
    use axum::body::HttpBody;
    use futures::StreamExt;
    if body.size_hint().exact().is_some() {
        return body;
    }
    let mut stream = Box::pin(body.into_data_stream());
    axum::body::Body::from_stream(futures::stream::poll_fn(move |cx| {
        PEER_SCOPE.sync_scope(stats.clone(), || stream.poll_next_unpin(cx))
    }))
}

/// Carry the current request's peer accounting into a spawned task. A task
/// started with `tokio::spawn` does not inherit task-locals.
pub(crate) fn in_request_scope<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let stats = PEER_SCOPE.try_with(Arc::clone).ok();
    async move {
        match stats {
            Some(stats) => PEER_SCOPE.scope(stats, future).await,
            None => future.await,
        }
    }
}

/// Send a request to a peer, counting it against the current client request.
/// Every node-to-node request made on behalf of a client goes through here.
pub(crate) async fn send_peer(
    request: reqwest::RequestBuilder,
) -> reqwest::Result<reqwest::Response> {
    let _call = PEER_SCOPE.try_with(|stats| stats.begin()).ok();
    request.send().await
}

pub(crate) fn percent_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn overlapping_peer_calls_are_one_round() {
        let stats = PeerStats::new("/{db}/doc".to_string(), false);
        // Two calls in flight together, then one after both finished.
        let first = stats.begin();
        let second = stats.begin();
        assert_eq!(stats.totals().0, 2);
        assert_eq!(stats.totals().1, 1);
        drop(first);
        // Still inside the round while the second is outstanding.
        let third = stats.begin();
        assert_eq!(stats.totals().1, 1);
        drop(second);
        drop(third);
        let fourth = stats.begin();
        drop(fourth);
        let (calls, rounds, _) = stats.totals();
        assert_eq!((calls, rounds), (4, 2));
    }

    #[tokio::test]
    async fn calls_outside_a_request_scope_need_no_scope() {
        let request = reqwest::Client::new().get("http://127.0.0.1:1/");
        let result = send_peer(request).await;
        // Nothing listens there; the point is that it does not need a scope.
        assert!(result.is_err());
    }

    #[test]
    fn peer_url_encodes_every_segment() {
        assert_eq!(
            peer_url("http://n1:5984", &["a/b", "x?y#z%", "_design", "p+q"]),
            "http://n1:5984/a%2Fb/x%3Fy%23z%25/_design/p%2Bq"
        );
        assert_eq!(peer_url("http://n1:5984", &[]), "http://n1:5984");
    }

    #[test]
    fn internal_requests_carry_the_marker_and_secret() {
        let client = reqwest::Client::new();
        let request = internal_request_raw(
            &client,
            "s3cret",
            reqwest::Method::GET,
            "http://n1",
            &["a/b", "doc"],
        )
        .build()
        .unwrap();
        assert_eq!(request.url().path(), "/a%2Fb/doc");
        assert_eq!(request.headers()["x-douchdb-forwarded"], "1");
        assert_eq!(request.headers()["x-douchdb-secret"], "s3cret");
    }

    #[test]
    fn forward_client_auth_copies_credentials_and_nothing_else() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic Ym9iOnB3"),
        );
        headers.insert(header::COOKIE, HeaderValue::from_static("AuthSession=abc"));
        headers.insert("x-douchdb-secret", HeaderValue::from_static("forged"));
        let request = forward_client_auth(reqwest::Client::new().get("http://peer/db"), &headers)
            .build()
            .unwrap();
        assert_eq!(request.headers()["authorization"], "Basic Ym9iOnB3");
        assert_eq!(request.headers()["cookie"], "AuthSession=abc");
        assert!(request.headers().get("x-douchdb-secret").is_none());

        let bare = forward_client_auth(
            reqwest::Client::new().get("http://peer/db"),
            &HeaderMap::new(),
        )
        .build()
        .unwrap();
        assert!(bare.headers().is_empty());
    }
}
