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
        "http.url" = %uri
    );
    let response = next.run(req).instrument(span).await;
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
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let path = req.uri().path();
    if crate::admission::Admission::exempt(path) {
        return next.run(req).await;
    }
    let principal = st.principal(req.headers());
    let lane = crate::admission::Admission::classify(req.method(), path);
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

/// Opt-in CORS, matching CouchDB's `[cors]` configuration. The embedded
/// console is served from the same origin and needs none of this; it exists for
/// browser apps (PouchDB, Fauxton, a separate frontend) on another origin.
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
        .map(str::to_string);
    let allowed = origin
        .as_deref()
        .map(|origin| cors_origin_allowed(&st.config.cors_origins, origin))
        .unwrap_or(false);

    if req.method() == Method::OPTIONS {
        if !allowed {
            return fail(ApiError::new(
                403,
                "forbidden",
                "CORS origin is not allowed",
            ));
        }
        if let Some(refused) = preflight_header_refused(&st.config, req.headers()) {
            return fail(ApiError::new(
                403,
                "forbidden",
                format!("CORS header `{refused}` is not allowed"),
            ));
        }
        let mut response = StatusCode::NO_CONTENT.into_response();
        add_cors_headers(
            response.headers_mut(),
            &st.config,
            origin.as_deref().unwrap_or_default(),
            Some(req.headers()),
        );
        return response;
    }

    let mut response = next.run(req).await;
    if allowed {
        add_cors_headers(
            response.headers_mut(),
            &st.config,
            origin.as_deref().unwrap_or_default(),
            None,
        );
    }
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

/// Request headers a preflight may ask for: CouchDB's supported set plus
/// `authorization` and the browser-set `referer`, `x-csrf-token`, extended by
/// `--cors-headers` (CouchDB `[cors] headers`).
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
    "referer",
    "x-csrf-token",
    "x-http-method-override",
    "x-requested-with",
];

fn cors_allowed_headers(config: &Config) -> Vec<&str> {
    CORS_DEFAULT_HEADERS
        .iter()
        .copied()
        .chain(config.cors_headers.iter().map(String::as_str))
        .collect()
}

/// The first header a preflight requests that is not allowed, if any.
fn preflight_header_refused(config: &Config, headers: &HeaderMap) -> Option<String> {
    let allowed = cors_allowed_headers(config);
    headers
        .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(',')
        .map(|name| name.trim().to_ascii_lowercase())
        .find(|name| !name.is_empty() && !allowed.contains(&name.as_str()))
}

fn add_cors_headers(
    headers: &mut HeaderMap,
    config: &Config,
    origin: &str,
    preflight: Option<&HeaderMap>,
) {
    let put = |headers: &mut HeaderMap, name, value: &str| {
        if let Ok(value) = HeaderValue::from_str(value) {
            headers.insert(name, value);
        }
    };
    // Echo the origin rather than using `*`, so cookie credentials work even
    // when the rule is `*`.
    put(headers, header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    // CouchDB's `[cors] credentials` defaults to false.
    if config.cors_credentials {
        put(headers, header::ACCESS_CONTROL_ALLOW_CREDENTIALS, "true");
    }
    put(
        headers,
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        "ETag, Location, Server, X-Couch-Request-ID, Content-Type, Content-Length",
    );
    headers.append(header::VARY, HeaderValue::from_static("Origin"));

    if preflight.is_some() {
        put(
            headers,
            header::ACCESS_CONTROL_ALLOW_METHODS,
            "GET, PUT, POST, HEAD, DELETE, OPTIONS",
        );
        put(
            headers,
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            &cors_allowed_headers(config).join(", "),
        );
        put(headers, header::ACCESS_CONTROL_MAX_AGE, "3600");
        headers.append(
            header::VARY,
            HeaderValue::from_static("Access-Control-Request-Method"),
        );
        headers.append(
            header::VARY,
            HeaderValue::from_static("Access-Control-Request-Headers"),
        );
    }
}
