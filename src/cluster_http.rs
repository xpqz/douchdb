//! Plumbing for node-to-node HTTP: URL encoding, the forwarded marker and
//! the cluster secret, and credential forwarding.

use axum::http::{header, HeaderMap};

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
