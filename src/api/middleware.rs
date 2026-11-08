//! Request middleware: request ids, HEAD handling, security headers,
//! logging, admission control and CORS.

use super::*;

/// Request log, enabled for every node. Cheap, and indispensable when a client
/// disagrees with us about the protocol.
/// Baseline response hardening. TLS is an operator concern; these headers still
/// stop MIME sniffing and framing when the console is served directly.
pub async fn request_id(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let id = crate::util::uuid();
    let mut response = next.run(req).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert("x-couch-request-id", value);
    }
    response
}

pub async fn head_only(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let head = req.method() == Method::HEAD;
    let mut response = next.run(req).await;
    if head {
        *response.body_mut() = Body::empty();
    }
    response
}

pub async fn security_headers(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers
        .entry(header::X_CONTENT_TYPE_OPTIONS)
        .or_insert(HeaderValue::from_static("nosniff"));
    headers
        .entry(header::X_FRAME_OPTIONS)
        .or_insert(HeaderValue::from_static("DENY"));
    headers
        .entry(header::REFERRER_POLICY)
        .or_insert(HeaderValue::from_static("no-referrer"));
    response
}

pub async fn log_requests(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    use tracing::Instrument;

    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path().to_string();
    let started = std::time::Instant::now();
    // Held across the handler so a dropped request (client disconnect) still
    // decrements the in-flight count.
    let _in_flight = crate::metrics::request_started();
    let span = tracing::info_span!(
        "http.request",
        "http.method" = %method,
        "http.url" = %uri,
        "peer.calls" = tracing::field::Empty,
        "peer.rounds" = tracing::field::Empty,
        "peer.wait_ms" = tracing::field::Empty,
    );
    // Peer calls made for this request are counted here and added to the route
    // metrics when the request, and any replica task it left running, is done.
    let internal = req.headers().contains_key("x-douchdb-forwarded")
        || req.headers().contains_key("x-douchdb-secret");
    if internal {
        crate::metrics::record_internal_request(req.version());
    }
    let peer_stats = crate::metrics::peer_route(&path)
        .map(|route| crate::cluster_http::PeerStats::new(route, internal));
    let handler = next.run(req).instrument(span.clone());
    let response = match &peer_stats {
        Some(stats) => crate::cluster_http::with_stats(stats.clone(), handler).await,
        None => handler.await,
    };
    let response = match peer_stats {
        Some(stats) => {
            // Streamed bodies do part of their peer work after this point, so
            // the span only carries what the handler itself used.
            let (calls, rounds, wait) = stats.totals();
            if calls > 0 {
                span.record("peer.calls", calls);
                span.record("peer.rounds", rounds);
                span.record("peer.wait_ms", wait.as_secs_f64() * 1000.0);
            }
            let (parts, body) = response.into_parts();
            Response::from_parts(parts, crate::cluster_http::scope_body(stats, body))
        }
        None => response,
    };
    let status = response.status().as_u16();
    let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
    crate::metrics::record_http(method.as_str(), status, duration_ms, &path);
    crate::telemetry::record_http(method.as_str(), status, duration_ms);
    tracing::info!("{} {} -> {}", method, uri, status);
    response
}

/// Per-principal admission. This is the fairness layer: no single user can fill
/// the global in-flight pool, and view/search work uses separate heavy permits.
pub async fn admit(
    State(st): State<Arc<AppState>>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if crate::admission::Admission::exempt(req.uri().path()) {
        return next.run(req).await;
    }
    // The one place a request is authenticated; handlers read it back through
    // the `Caller` extractor.
    let principal = st.principal(req.headers());
    req.extensions_mut().insert(principal.clone());
    let lane = crate::admission::Admission::classify(req.method(), req.uri().path());
    match st.admission.try_admit(&principal, lane) {
        Ok(permit) => hold_for_body(permit, next.run(req).await),
        Err(denied) => {
            let mut response = json(
                StatusCode::TOO_MANY_REQUESTS,
                json!({
                    "error": "too_many_requests",
                    "reason": denied.reason(),
                }),
            );
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
            response
        }
    }
}

/// A response whose body is still being produced (a streamed feed) keeps its
/// admission permit until the body ends or the client disconnects and the body
/// is dropped; releasing it when the head returns would let any number of
/// feeds escape the limits. A body of known size is complete already, so its
/// permit goes at once and the response is untouched.
///
/// Long-lived feeds count against the normal per-user and global limits, with
/// no lane of their own: see docs/multiuser.md.
fn hold_for_body(permit: crate::admission::Permit, response: Response) -> Response {
    use axum::body::HttpBody;
    let (parts, body) = response.into_parts();
    if body.size_hint().exact().is_some() {
        return Response::from_parts(parts, body);
    }
    let held = async_stream::stream! {
        let _permit = permit;
        let mut data = body.into_data_stream();
        while let Some(chunk) = data.next().await {
            yield chunk;
        }
    };
    Response::from_parts(parts, Body::from_stream(held))
}

/// Opt-in CORS, matching CouchDB's `[cors]` configuration and its
/// `chttpd_cors` module (apache/couchdb src/chttpd/src/chttpd_cors.erl). The
/// embedded console is served from the same origin and needs none of this; it
/// exists for browser apps (PouchDB, Fauxton, a separate frontend) on another
/// origin.
///
/// Like CouchDB, only a request that passes every preflight check gets the
/// preflight answer (204). One that does not (unknown method, a requested
/// header outside the allowed list, no `Access-Control-Request-Method`) is not
/// treated as a preflight and is handled as the plain request it is.
pub async fn cors(
    State(st): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if st.config.cors_origins.is_empty() {
        return next.run(req).await;
    }
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
        .filter(|origin| cors_origin_allowed(&st.config.cors_origins, origin));
    let Some(origin) = origin else {
        return next.run(req).await;
    };

    if req.method() == Method::OPTIONS {
        if let Some(mut response) = preflight_response(&st.config, req.headers()) {
            let out = response.headers_mut();
            put_cors_header(out, header::ACCESS_CONTROL_ALLOW_ORIGIN, &origin);
            if st.config.cors_credentials && origin != "*" {
                put_cors_header(out, header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
            }
            return response;
        }
    }

    let mut response = next.run(req).await;
    add_cors_headers(response.headers_mut(), &st.config, &origin);
    response
}

fn cors_origin_allowed(rules: &[String], origin: &str) -> bool {
    rules.iter().any(|rule| {
        if rule == "*" {
            return true;
        }
        let Some((rule_scheme, rule_host)) = rule.split_once("://") else {
            return false;
        };
        let Some((origin_scheme, origin_host)) = origin.split_once("://") else {
            return false;
        };
        if !rule_scheme.eq_ignore_ascii_case(origin_scheme) {
            return false;
        }
        match rule_host.strip_prefix("*.") {
            // CouchDB-style subdomain wildcard: https://*.example.com
            Some(suffix) => {
                let host = origin_host.to_ascii_lowercase();
                let suffix = suffix.to_ascii_lowercase();
                host.len() > suffix.len() + 1 && host.ends_with(&format!(".{suffix}"))
            }
            None => rule_host.eq_ignore_ascii_case(origin_host),
        }
    })
}

/// CouchDB's `?SUPPORTED_HEADERS` (chttpd_cors.hrl): the request headers a
/// preflight may ask for unless `[cors] headers` (`--cors-headers`) replaces
/// the list.
const CORS_DEFAULT_HEADERS: &[&str] = &[
    "accept",
    "accept-language",
    "authorization",
    "content-length",
    "content-range",
    "content-type",
    "destination",
    "expires",
    "if-match",
    "last-modified",
    "origin",
    "pragma",
    "x-couch-full-commit",
    "x-couch-id",
    "x-couch-persist",
    "x-couchdb-www-authenticate",
    "x-http-method-override",
    "x-requested-with",
    "x-couchdb-vhost-path",
];

/// CouchDB's `?SUPPORTED_METHODS`, the default of `[cors] methods`.
const CORS_METHODS: &str = "CONNECT, COPY, DELETE, GET, HEAD, OPTIONS, POST, PUT, TRACE";

/// CouchDB's `?CORS_DEFAULT_MAX_AGE`.
const CORS_MAX_AGE: &str = "600";

/// CouchDB's `?SIMPLE_HEADERS` other than `content-type`, which is always
/// listed: response headers worth naming in `Access-Control-Expose-Headers`.
const CORS_SIMPLE_HEADERS: &[&str] = &[
    "cache-control",
    "content-language",
    "expires",
    "last-modified",
    "pragma",
];

/// CouchDB's `?COUCH_HEADERS`, always exposed.
const CORS_COUCH_HEADERS: &str =
    "accept-ranges, etag, server, x-couch-request-id, x-couch-update-newrev, x-couchdb-body-time";

/// Whether a preflight may ask for this (lowercase) header: CouchDB's list, or
/// exactly `--cors-headers` when set (`[cors] headers` replaces the default).
fn cors_header_allowed(config: &Config, name: &str) -> bool {
    if config.cors_headers.is_empty() {
        CORS_DEFAULT_HEADERS.contains(&name)
    } else {
        config.cors_headers.iter().any(|allowed| allowed == name)
    }
}

/// `chttpd_cors:handle_preflight_request/3`: the 204 answer, or `None` when the
/// request is not an acceptable preflight (no request method, a method outside
/// the supported list, or an unsupported requested header). The origin headers
/// are added by the caller.
fn preflight_response(config: &Config, headers: &HeaderMap) -> Option<Response> {
    let method = headers
        .get(header::ACCESS_CONTROL_REQUEST_METHOD)?
        .to_str()
        .ok()?;
    if !CORS_METHODS
        .split(", ")
        .any(|supported| supported == method)
    {
        return None;
    }
    let requested = match headers.get(header::ACCESS_CONTROL_REQUEST_HEADERS) {
        Some(value) => value.to_str().ok()?,
        None => "",
    };
    // `re:split(H, ",\\s*", [trim])`: split on commas, drop trailing empties.
    let mut names: Vec<&str> = requested.split(',').map(str::trim_start).collect();
    while names.last().is_some_and(|name| name.is_empty()) {
        names.pop();
    }
    if !names
        .iter()
        .all(|name| cors_header_allowed(config, &name.to_ascii_lowercase()))
    {
        return None;
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    let out = response.headers_mut();
    put_cors_header(out, header::ACCESS_CONTROL_MAX_AGE, CORS_MAX_AGE);
    put_cors_header(out, header::ACCESS_CONTROL_ALLOW_METHODS, CORS_METHODS);
    put_cors_header(out, header::ACCESS_CONTROL_ALLOW_HEADERS, requested);
    Some(response)
}

fn put_cors_header(headers: &mut HeaderMap, name: header::HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

/// `chttpd_cors:headers/4` for a non-preflight response: the request origin
/// (also for `*`; CouchDB echoes it), credentials if configured, and the expose
/// list: `content-type`, the simple response headers present, then CouchDB's
/// own headers.
fn add_cors_headers(headers: &mut HeaderMap, config: &Config, origin: &str) {
    let mut exposed = vec!["content-type".to_string()];
    for name in headers.keys() {
        let name = name.as_str().to_ascii_lowercase();
        if CORS_SIMPLE_HEADERS.contains(&name.as_str()) && !exposed.contains(&name) {
            exposed.push(name);
        }
    }
    exposed.push(CORS_COUCH_HEADERS.to_string());
    put_cors_header(headers, header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    if config.cors_credentials && origin != "*" {
        put_cors_header(headers, header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
    }
    put_cors_header(
        headers,
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        &exposed.join(", "),
    );
}
