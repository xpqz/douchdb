//! Embedded CouchDB-style web console.
//!
//! The assets are compiled into the binary and served under `/_utils/`, so the
//! console works with no build step and no asset directory next to the binary.
//! It talks only to the public CouchDB HTTP API.

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};

const INDEX: &str = include_str!("../ui/index.html");
const APP: &str = include_str!("../ui/app.js");
const CSS: &str = include_str!("../ui/style.css");
const VUE: &str = include_str!("../ui/vendor/vue.global.prod.js");
const CODEMIRROR_JS: &str = include_str!("../ui/vendor/codemirror/codemirror.js");
const CODEMIRROR_CSS: &str = include_str!("../ui/vendor/codemirror/codemirror.css");
const CODEMIRROR_JS_MODE: &str =
    include_str!("../ui/vendor/codemirror/mode/javascript/javascript.js");
const FAVICON: &str = include_str!("../ui/favicon.svg");

/// Policy for the console. The vendored Vue build compiles the in-DOM
/// templates at runtime (`Function(...)`), which needs `'unsafe-eval'`; moving
/// to precompiled templates would let it go. `connect-src` is open to
/// http(s) because the console's server URL can point at another node.
const CSP: &str = "default-src 'none'; script-src 'self' 'unsafe-eval'; \
    style-src 'self' 'unsafe-inline'; img-src 'self' data:; \
    connect-src 'self' http: https:; form-action 'self'; \
    base-uri 'none'; object-src 'none'; frame-ancestors 'none'";

pub async fn redirect() -> Redirect {
    Redirect::permanent("/_utils/")
}

pub async fn index() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::CONTENT_SECURITY_POLICY, CSP),
        ],
        INDEX,
    )
        .into_response()
}

pub async fn favicon() -> Response {
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "image/svg+xml"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        FAVICON,
    )
        .into_response()
}

pub async fn asset(Path(path): Path<String>) -> Response {
    let (content_type, body) = match path.as_str() {
        "app.js" => ("text/javascript; charset=utf-8", APP),
        "style.css" => ("text/css; charset=utf-8", CSS),
        "favicon.svg" => ("image/svg+xml", FAVICON),
        "vendor/vue.global.prod.js" => ("text/javascript; charset=utf-8", VUE),
        "vendor/codemirror/codemirror.js" => ("text/javascript; charset=utf-8", CODEMIRROR_JS),
        "vendor/codemirror/codemirror.css" => ("text/css; charset=utf-8", CODEMIRROR_CSS),
        "vendor/codemirror/mode/javascript/javascript.js" => {
            ("text/javascript; charset=utf-8", CODEMIRROR_JS_MODE)
        }
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    let cache = if path.starts_with("vendor/") {
        "public, max-age=86400"
    } else {
        "no-cache"
    };
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, cache),
            (header::CONTENT_SECURITY_POLICY, CSP),
        ],
        body,
    )
        .into_response()
}
