//! Per-endpoint wire conformance tests.
//!
//! Each test starts a real douchdb process on an ephemeral port and talks to it
//! over HTTP, exactly as PouchDB does. The shapes asserted here are CouchDB's,
//! not ours: status codes, `{"error","reason"}` bodies, ETags, `_all_docs`
//! offsets, `_bulk_get` envelopes and attachment digests all have clients that
//! sniff them.

use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Server {
    child: Child,
    base: String,
    dir: PathBuf,
    client: Client,
    port: u16,
    extra: Vec<String>,
    admin: bool,
}

impl Server {
    fn start(admin: bool) -> Server {
        Server::start_with(&[], admin)
    }

    /// Start a node with extra CLI arguments (`--q`, `--nodes`, `--me`, ...).
    fn start_with(extra: &[String], admin: bool) -> Server {
        for attempt in 0..20 {
            if let Some(server) = Server::spawn_on(free_port(), extra, admin, attempt) {
                return server;
            }
        }
        panic!("douchdb did not start");
    }

    fn spawn_on(port: u16, extra: &[String], admin: bool, attempt: usize) -> Option<Server> {
        let dir = std::env::temp_dir().join(format!(
            "douchdb-test-{}-{}-{}",
            std::process::id(),
            port,
            attempt
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_douchdb"));
        cmd.arg("--listen")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--data")
            .arg(&dir)
            .arg("--sync-secs")
            .arg("3600")
            .args(extra)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if admin {
            cmd.arg("--admin").arg("admin:douchdb");
        }
        let mut child = cmd.spawn().expect("spawn douchdb");
        let base = format!("http://127.0.0.1:{port}");
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        // Wait for the listener, and notice a child that died (e.g. port
        // collision) instead of sleeping for the whole budget.
        let mut up = false;
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                up = true;
                break;
            }
            if let Ok(Some(_)) = child.try_wait() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        if up {
            // A colliding process can win the port after `free_port` checked
            // it; the child then exits while our connect succeeds against the
            // other listener. Give the bind a moment and confirm the child is
            // still the one serving.
            std::thread::sleep(Duration::from_millis(30));
            if let Ok(Some(_)) = child.try_wait() {
                let _ = child.wait();
                let _ = std::fs::remove_dir_all(&dir);
                return None;
            }
            return Some(Server {
                child,
                base,
                dir,
                client,
                port,
                extra: extra.to_vec(),
                admin,
            });
        }
        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&dir);
        None
    }

    /// Restart this node on the same port and data directory.
    fn restart(&mut self) {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_douchdb"));
        cmd.arg("--listen")
            .arg(format!("127.0.0.1:{}", self.port))
            .arg("--data")
            .arg(&self.dir)
            .arg("--sync-secs")
            .arg("3600")
            .args(&self.extra)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if self.admin {
            cmd.arg("--admin").arg("admin:douchdb");
        }
        self.child = cmd.spawn().expect("restart douchdb");
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                std::thread::sleep(Duration::from_millis(50));
                return;
            }
            if let Ok(Some(_)) = self.child.try_wait() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("douchdb did not restart on port {}", self.port);
    }

    async fn req(&self, method: Method, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        let mut request = self
            .client
            .request(method, format!("{}{}", self.base, path));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("request");
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let value = serde_json::from_str(&text).unwrap_or(Value::Null);
        (status, value)
    }

    /// Request returning response headers as well as status and JSON.
    async fn req_headers(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        headers: &[(&str, String)],
    ) -> (StatusCode, HashMap<String, String>, Value) {
        let mut request = self
            .client
            .request(method, format!("{}{}", self.base, path));
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("request");
        let status = response.status();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_ascii_lowercase(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let text = response.text().await.unwrap_or_default();
        let value = serde_json::from_str(&text).unwrap_or(Value::Null);
        (status, headers, value)
    }

    /// Request with extra headers (auth cookies, internal routing markers).
    async fn req_with(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        headers: &[(&str, String)],
    ) -> (StatusCode, Value) {
        let mut request = self
            .client
            .request(method, format!("{}{}", self.base, path));
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.expect("request");
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let value = serde_json::from_str(&text).unwrap_or(Value::Null);
        (status, value)
    }

    async fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.req(Method::PUT, path, Some(body)).await
    }
    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.req(Method::POST, path, Some(body)).await
    }
    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.req(Method::GET, path, None).await
    }
    async fn delete(&self, path: &str) -> (StatusCode, Value) {
        self.req(Method::DELETE, path, None).await
    }

    /// Stop the process as if the machine had gone away. The HTTP port is
    /// released; the data directory stays until `Drop`.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    async fn fresh_db(&self, name: &str) {
        let _ = self.delete(&format!("/{name}")).await;
        let (status, _) = self.put(&format!("/{name}"), json!({})).await;
        assert_eq!(status, StatusCode::CREATED, "create {name}");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    // Tests run in parallel threads. The kernel's ephemeral-port pool can hand
    // the same released port to two tests, so assign ports from a process-wide
    // counter and confirm each one is free.
    static NEXT_PORT: AtomicUsize = AtomicUsize::new(0);
    loop {
        let n = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        let candidate = 31000 + ((std::process::id() as usize + n * 37) % 20000);
        if std::net::TcpListener::bind(("127.0.0.1", candidate as u16)).is_ok() {
            return candidate as u16;
        }
    }
}

/// Start `n` nodes in one cluster with `q=4`. Anti-entropy is effectively off
/// (`--sync-secs 3600`), so tests exercise request routing rather than
/// background repair: a document reachable only through its owner proves the
/// proxy path works.
fn start_cluster(n: usize) -> Vec<Server> {
    start_cluster_with(n, "3600", 1)
}

fn start_cluster_with(n: usize, sync_secs: &str, replication: usize) -> Vec<Server> {
    start_cluster_full(n, sync_secs, replication, false)
}

fn start_cluster_with_auth(n: usize, sync_secs: &str, replication: usize) -> Vec<Server> {
    start_cluster_full(n, sync_secs, replication, true)
}

fn start_cluster_full(n: usize, sync_secs: &str, replication: usize, admin: bool) -> Vec<Server> {
    for attempt in 0..5 {
        let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let nodes = ports
            .iter()
            .map(|p| format!("http://127.0.0.1:{p}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut servers = Vec::new();
        let mut failed = false;
        for i in 0..n {
            let extra = vec![
                "--q".to_string(),
                "4".to_string(),
                "--n".to_string(),
                replication.to_string(),
                "--nodes".to_string(),
                nodes.clone(),
                "--me".to_string(),
                i.to_string(),
                "--sync-secs".to_string(),
                sync_secs.to_string(),
                "--cluster-secret".to_string(),
                "test-cluster-secret".to_string(),
            ];
            match Server::spawn_on(ports[i], &extra, admin, attempt) {
                Some(server) => servers.push(server),
                None => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            return servers;
        }
        drop(servers);
    }
    panic!("douchdb cluster did not start");
}

#[tokio::test]
async fn welcome_and_database_lifecycle() {
    let s = Server::start(false);
    let response = s.client.get(format!("{}/", s.base)).send().await.unwrap();
    assert_eq!(
        response
            .headers()
            .get("x-content-type-options")
            .and_then(|v| v.to_str().ok()),
        Some("nosniff")
    );
    assert_eq!(
        response
            .headers()
            .get("x-frame-options")
            .and_then(|v| v.to_str().ok()),
        Some("DENY")
    );
    let (status, body) = s.get("/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["couchdb"], "Welcome");
    assert!(body["version"].is_string());
    assert_eq!(body["vendor"]["name"], "douchdb");

    s.fresh_db("lifecycle").await;
    let (status, body) = s.get("/lifecycle").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["db_name"], "lifecycle");
    assert_eq!(body["doc_count"], 0);
    assert!(body["update_seq"].is_number());

    let (status, body) = s.put("/lifecycle", json!({})).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    assert_eq!(body["error"], "file_exists");

    let (status, _) = s.delete("/lifecycle").await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = s.get("/lifecycle").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found");

    let (status, _) = s.put("/UPPER", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn cors_is_opt_in_and_answers_preflight() {
    let options = |server: &Server, origin: &str| {
        server
            .client
            .request(Method::OPTIONS, format!("{}/demo/doc", server.base))
            .header("Origin", origin)
            .header("Access-Control-Request-Method", "PUT")
            .header(
                "Access-Control-Request-Headers",
                "content-type, authorization",
            )
            .send()
    };

    let plain = Server::start(false);
    let response = options(&plain, "http://example.test").await.unwrap();
    assert_ne!(response.status(), StatusCode::NO_CONTENT);
    assert!(response
        .headers()
        .get("access-control-allow-origin")
        .is_none());

    let s = Server::start_with(
        &[
            "--cors-origins".to_string(),
            "http://example.test,https://*.trusted.test".to_string(),
        ],
        false,
    );

    let response = options(&s, "http://example.test").await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("http://example.test")
    );
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-credentials")
            .and_then(|v| v.to_str().ok()),
        Some("true")
    );
    let methods = response
        .headers()
        .get("access-control-allow-methods")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(methods.contains("PUT") && methods.contains("DELETE"));
    let allowed_headers = response
        .headers()
        .get("access-control-allow-headers")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(allowed_headers.contains("authorization"));

    // Wildcard subdomains match and are echoed for credential support.
    let response = options(&s, "https://app.trusted.test").await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("https://app.trusted.test")
    );

    // Actual responses, including errors, get the header too.
    let response = s
        .client
        .get(format!("{}/_all_dbs", s.base))
        .header("Origin", "http://example.test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|v| v.to_str().ok()),
        Some("http://example.test")
    );

    let response = options(&s, "http://evil.test").await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(response
        .headers()
        .get("access-control-allow-origin")
        .is_none());
}

#[tokio::test]
async fn console_is_served_from_the_couchdb_api_origin() {
    let s = Server::start(false);

    let response = s
        .client
        .get(format!("{}/_utils/", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .starts_with("text/html"));
    let html = response.text().await.unwrap();
    assert!(html.contains("id=\"app\""), "Vue mount point missing");
    assert!(html.contains("vendor/vue.global.prod.js"));
    assert!(html.contains("data-testid=\"open-cluster\""));

    let response = s
        .client
        .get(format!("{}/_utils/app.js", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .starts_with("text/javascript"));
    let app = response.text().await.unwrap();
    assert!(app.contains("createApp"), "not a Vue application");
    assert!(app.contains("/_node/_local/_cluster"));
    assert!(app.contains("/_node/_local/_replication"));
    // The console may only use the public CouchDB API, never a private route.
    assert!(!app.contains("/api/"), "console uses a private API path");

    let response = s
        .client
        .get(format!("{}/_utils/vendor/vue.global.prod.js", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().len() > 100_000);

    let response = s
        .client
        .get(format!("{}/_utils/style.css", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().contains("--accent"));

    let response = s
        .client
        .get(format!("{}/_utils/vendor/codemirror/codemirror.js", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().len() > 10_000);

    let response = s
        .client
        .get(format!(
            "{}/_utils/vendor/codemirror/mode/javascript/javascript.js",
            s.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let no_redirect = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let response = no_redirect
        .get(format!("{}/_utils", s.base))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_redirection());
    assert_eq!(
        response
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok()),
        Some("/_utils/")
    );
}

#[tokio::test]
async fn document_crud_matches_couchdb_rev_ids() {
    let s = Server::start(false);
    s.fresh_db("crud").await;

    // The rev id for `{"a":1}` is CouchDB's, byte for byte.
    let (status, body) = s.put("/crud/doc1", json!({"a": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["ok"], true);
    assert_eq!(body["id"], "doc1");
    assert_eq!(body["rev"], "1-23202479633c2b380f79507a776743d5");
    let rev = body["rev"].as_str().unwrap().to_string();

    let response = s
        .client
        .get(format!("{}/crud/doc1", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let expected_etag = format!("\"{rev}\"");
    assert_eq!(
        response.headers().get("etag").and_then(|v| v.to_str().ok()),
        Some(expected_etag.as_str())
    );
    let body: Value = response.json().await.unwrap();
    let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(&keys[..2], &["_id", "_rev"], "CouchDB emits _id/_rev first");

    let (status, body) = s.put("/crud/doc1", json!({"a": 2, "_rev": &rev})).await;
    assert_eq!(status, StatusCode::CREATED);
    let rev2 = body["rev"].as_str().unwrap().to_string();
    assert_ne!(rev2, rev);

    // Stale edit: exactly CouchDB's conflict body, with no id.
    let (status, body) = s.put("/crud/doc1", json!({"a": 3, "_rev": &rev})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body,
        json!({"error": "conflict", "reason": "Document update conflict."})
    );

    let (status, _) = s.delete("/crud/nope").await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "DELETE without rev, missing doc"
    );

    let (status, _) = s.delete(&format!("/crud/doc1?rev={rev2}")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = s.get("/crud/doc1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["reason"], "deleted");
    let (status, body) = s.delete("/crud/doc1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["reason"], "deleted");

    // A PUT that lands as a tombstone is 200, not 201.
    let (status, body) = s.put("/crud/tomb", json!({"_deleted": true})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);
}

#[tokio::test]
async fn encoded_slashes_in_document_ids_are_not_path_separators() {
    let s = Server::start(false);
    s.fresh_db("slashy").await;

    // CouchDB ids may contain slashes; clients encode them as %2F.
    let (status, body) = s.put("/slashy/a%2Fb", json!({"x": 1})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["id"], "a/b");

    let (status, doc) = s.get("/slashy/a%2Fb").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["_id"], "a/b");
    assert_eq!(doc["x"], 1);

    let (status, _) = s.put("/slashy/_design%2Ffoo", json!({"views": {}})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, doc) = s.get("/slashy/_design%2Ffoo").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["_id"], "_design/foo");
}

#[tokio::test]
async fn bulk_docs_and_all_docs() {
    let s = Server::start(false);
    s.fresh_db("all").await;
    let (status, body) = s
        .post(
            "/all/_bulk_docs",
            json!({"docs": [{"_id": "b"}, {"_id": "a"}, {"_id": "c"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body.as_array().unwrap().len(), 3);
    assert_eq!(body[1]["id"], "a", "results keep request order");

    let (status, body) = s.get("/all/_all_docs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 3);
    assert_eq!(body["offset"], 0);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);

    let (status, body) = s
        .get("/all/_all_docs?include_docs=true&startkey=%22b%22&skip=1")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["offset"], 2, "offset is index position, not skip");
    assert_eq!(body["rows"][0]["id"], "c");
    assert!(
        body["rows"][0]["doc"].get("_revisions").is_none(),
        "include_docs does not add _revisions"
    );

    let (status, body) = s
        .post("/all/_all_docs", json!({"keys": ["a", "zzz"]}))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["offset"].is_null(),
        "keys queries report a null offset"
    );
    assert_eq!(body["rows"][0]["id"], "a");
    assert_eq!(body["rows"][1]["error"], "not_found");

    // Deleted docs appear in a keys query, flagged deleted, and are not
    // counted in total_rows.
    let (_, existing) = s.get("/all/a").await;
    let rev = existing["_rev"].as_str().unwrap();
    s.delete(&format!("/all/a?rev={rev}")).await;
    let (_, body) = s.post("/all/_all_docs", json!({"keys": ["a", "b"]})).await;
    assert_eq!(body["total_rows"], 2);
    assert_eq!(body["rows"][0]["value"]["deleted"], true);
}

#[tokio::test]
async fn all_docs_ranges_and_changes_limits_after_deletes() {
    let s = Server::start(false);
    s.fresh_db("ranges").await;
    let (status, _) = s
        .post(
            "/ranges/_bulk_docs",
            json!({"docs": [
                {"_id": "a"}, {"_id": "b"}, {"_id": "c"},
                {"_id": "d"}, {"_id": "e"}, {"_id": "f"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    for id in ["b", "d"] {
        let (_, doc) = s.get(&format!("/ranges/{id}")).await;
        let rev = doc["_rev"].as_str().unwrap();
        let (status, _) = s.delete(&format!("/ranges/{id}?rev={rev}")).await;
        assert_eq!(status, StatusCode::OK);
    }

    // Live docs are a, c, e, f. Deleted docs do not count or appear in ranges.
    let (_, body) = s.get("/ranges/_all_docs").await;
    assert_eq!(body["total_rows"], 4);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["a", "c", "e", "f"]);

    let (_, body) = s.get("/ranges/_all_docs?limit=2&skip=1").await;
    assert_eq!(body["offset"], 1, "offset includes the skipped row");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["c", "e"]);

    let (_, body) = s
        .get("/ranges/_all_docs?startkey=%22c%22&limit=1&skip=1")
        .await;
    assert_eq!(body["offset"], 2, "one live doc precedes c, plus skip");
    assert_eq!(body["rows"][0]["id"], "e");

    let (_, body) = s
        .get("/ranges/_all_docs?descending=true&startkey=%22e%22&limit=1")
        .await;
    assert_eq!(body["offset"], 1, "descending counts docs after startkey");
    assert_eq!(body["rows"][0]["id"], "e");

    let (_, body) = s
        .get("/ranges/_all_docs?descending=true&skip=1&limit=2")
        .await;
    assert_eq!(body["offset"], 1);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["e", "c"]);

    let (_, body) = s
        .get("/ranges/_all_docs?startkey=%22c%22&endkey=%22e%22")
        .await;
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["c", "e"]);

    // Keys queries still surface deleted docs but keep total_rows live-only.
    let (_, body) = s
        .post("/ranges/_all_docs", json!({"keys": ["b", "e"]}))
        .await;
    assert_eq!(body["total_rows"], 4);
    assert_eq!(body["offset"], Value::Null);
    assert_eq!(body["rows"][0]["value"]["deleted"], true);
    assert_eq!(body["rows"][1]["id"], "e");

    // A limited changes feed returns only the requested window, and its
    // last_seq resumes exactly where it stopped.
    let (_, first) = s.get("/ranges/_changes?since=0&limit=3").await;
    let first_rows = first["results"].as_array().unwrap();
    assert_eq!(first_rows.len(), 3);
    let last_seq = first["last_seq"].as_u64().unwrap();
    assert_eq!(first_rows[2]["seq"].as_u64().unwrap(), last_seq);

    let (_, second) = s
        .get(&format!("/ranges/_changes?since={last_seq}&limit=3"))
        .await;
    let second_rows = second["results"].as_array().unwrap();
    assert_eq!(second_rows.len(), 3);
    assert!(
        second_rows
            .iter()
            .all(|row| row["seq"].as_u64().unwrap() > last_seq),
        "resumed feed starts after last_seq"
    );

    // Descending still returns the newest rows, not the first ones scanned.
    let (_, body) = s.get("/ranges/_changes?descending=true&limit=2").await;
    let ids: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["d", "b"]);

    // The live-document map is maintained after it has been built.
    let (_, doc) = s.get("/ranges/c").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    let (status, _) = s.delete(&format!("/ranges/c?rev={rev}")).await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = s.get("/ranges/_all_docs?startkey=%22d%22&limit=2").await;
    assert_eq!(body["total_rows"], 3);
    assert_eq!(body["offset"], 1);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["e", "f"]);
}

#[tokio::test]
async fn request_body_limit_is_enforced() {
    let s = Server::start_with(&["--max-request-mb".to_string(), "1".to_string()], false);
    s.fresh_db("limits").await;
    let blob = "x".repeat(2 * 1024 * 1024);
    let (status, _) = s
        .post(
            "/limits/_bulk_docs",
            json!({"docs": [{"_id": "too-big", "blob": blob}]}),
        )
        .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn bulk_docs_accepts_requests_larger_than_two_megabytes() {
    let s = Server::start(false);
    s.fresh_db("bigbulk").await;

    // Axum's default body limit is 2 MiB; a CouchDB bulk request routinely
    // exceeds that. Three 1.5 MB documents is ~4.5 MB on the wire.
    let blob = "x".repeat(1_500_000);
    let docs: Vec<Value> = (0..3)
        .map(|i| json!({"_id": format!("big{i}"), "blob": blob}))
        .collect();
    let (status, body) = s.post("/bigbulk/_bulk_docs", json!({"docs": docs})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body.as_array().unwrap().len(), 3);

    let (status, doc) = s.get("/bigbulk/big2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["blob"].as_str().unwrap().len(), 1_500_000);
}

#[tokio::test]
async fn continuous_changes_streams_newline_json_and_heartbeats() {
    let s = Server::start(false);
    s.fresh_db("stream").await;
    let (status, _) = s.put("/stream/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);

    let response = s
        .client
        .get(format!(
            "{}/stream/_changes?feed=continuous&since=0&timeout=250&heartbeat=40",
            s.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = response.text().await.unwrap();
    let changes: Vec<Value> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("change line is JSON"))
        .collect();
    assert!(changes.iter().any(|row| row["id"] == "doc"), "{text}");

    // With since=now and no new writes, heartbeat newlines keep the stream
    // alive until the timeout closes it.
    let response = s
        .client
        .get(format!(
            "{}/stream/_changes?feed=continuous&since=now&timeout=200&heartbeat=30",
            s.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let heartbeat = response.text().await.unwrap();
    assert!(
        heartbeat.contains('\n'),
        "no heartbeat bytes: {heartbeat:?}"
    );
}

#[tokio::test]
async fn changes_supports_sse_eventsource_feed() {
    let s = Server::start(false);
    s.fresh_db("sse").await;
    let (status, _) = s.put("/sse/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);

    let response = s
        .client
        .get(format!(
            "{}/sse/_changes?feed=eventsource&since=0&timeout=250&heartbeat=40",
            s.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    let body = response.text().await.unwrap();
    let data: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect();
    assert!(!data.is_empty(), "{body:?}");
    let event: Value = serde_json::from_str(data[0]).unwrap();
    assert_eq!(event["id"], "doc");
    assert!(body.contains(": heartbeat"), "{body:?}");
}

#[tokio::test]
async fn changes_revs_diff_and_bulk_get() {
    let s = Server::start(false);
    s.fresh_db("rep").await;
    let (_, a) = s.put("/rep/a", json!({"x": 1})).await;
    let (_, b) = s.put("/rep/b", json!({"x": 2})).await;

    let (status, body) = s.get("/rep/_changes").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"].as_array().unwrap().len(), 2);
    assert_eq!(body["results"][0]["id"], "a");
    assert_eq!(body["results"][0]["changes"][0]["rev"], a["rev"]);
    assert_eq!(body["pending"], 0);

    let (_, body) = s.get("/rep/_changes?since=1&include_docs=true").await;
    assert_eq!(body["results"].as_array().unwrap().len(), 1);
    assert_eq!(body["results"][0]["doc"]["x"], 2);
    assert!(body["results"][0]["doc"].get("_revisions").is_none());

    let (status, body) = s
        .post(
            "/rep/_revs_diff",
            json!({"a": [a["rev"], "3-00000000000000000000000000000000"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["a"]["missing"],
        json!(["3-00000000000000000000000000000000"])
    );
    assert_eq!(body["a"]["possible_ancestors"], json!([a["rev"]]));

    // A completely unknown document gets no possible_ancestors field.
    let (_, body) = s
        .post(
            "/rep/_revs_diff",
            json!({"nope": ["1-11111111111111111111111111111111"]}),
        )
        .await;
    assert!(body["nope"].get("possible_ancestors").is_none());

    let (status, body) = s
        .post(
            "/rep/_bulk_get",
            json!({"docs": [{"id": "a", "rev": a["rev"]}, {"id": "nope"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["results"][0]["docs"][0]["ok"]
        .get("_revisions")
        .is_none());
    assert_eq!(body["results"][1]["docs"][0]["error"]["error"], "not_found");
    assert_eq!(body["results"][1]["docs"][0]["error"]["rev"], "undefined");

    let (_, body) = s
        .post(
            "/rep/_bulk_get?revs=true",
            json!({"docs": [{"id": "b", "rev": b["rev"]}]}),
        )
        .await;
    assert_eq!(
        body["results"][0]["docs"][0]["ok"]["_revisions"]["start"],
        1
    );
}

#[tokio::test]
async fn otel_endpoint_is_optional_and_does_not_block_startup() {
    let s = Server::start_with(
        &[
            "--otel-endpoint".to_string(),
            "http://127.0.0.1:9".to_string(),
        ],
        false,
    );
    let (status, body) = s.get("/_up").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn all_dbs_pagination_and_dbs_info() {
    let s = Server::start(false);
    for name in ["aaa", "bbb", "ccc"] {
        s.fresh_db(name).await;
    }
    let (status, body) = s.get("/_all_dbs?startkey=%22bbb%22&endkey=%22ccc%22").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!(["bbb", "ccc"]));

    let (_, body) = s.get("/_all_dbs?descending=true&limit=2").await;
    assert_eq!(body, json!(["ccc", "bbb"]));
    let (_, body) = s.get("/_all_dbs?skip=1&limit=1").await;
    assert_eq!(body.as_array().unwrap().len(), 1);

    let (status, body) = s
        .get("/_dbs_info?keys=%5B%22aaa%22%2C%22missing%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK);
    let rows = body.as_array().unwrap();
    assert_eq!(rows[0]["key"], "aaa");
    assert_eq!(rows[0]["info"]["db_name"], "aaa");
    assert_eq!(rows[1]["key"], "missing");
    assert_eq!(rows[1]["error"], "not_found");

    let (status, body) = s.post("/_dbs_info", json!({"keys": ["ccc"]})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body[0]["info"]["db_name"], "ccc");
}

#[tokio::test]
async fn db_updates_reports_lifecycle_and_write_events() {
    let s = Server::start(false);
    let (_, baseline) = s.get("/_db_updates?since=0").await;
    let mut since = baseline["last_seq"].as_str().unwrap().to_string();

    let (status, _) = s.put("/events", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get(&format!("/_db_updates?since={since}")).await;
    assert!(body["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["db_name"] == "events" && event["type"] == "created"));
    since = body["last_seq"].as_str().unwrap().to_string();

    let (status, _) = s.put("/events/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get(&format!("/_db_updates?since={since}")).await;
    assert!(body["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["db_name"] == "events" && event["type"] == "updated"));
    since = body["last_seq"].as_str().unwrap().to_string();

    let (status, _) = s.delete("/events").await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = s.get(&format!("/_db_updates?since={since}")).await;
    assert!(body["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["db_name"] == "events" && event["type"] == "deleted"));

    // Longpoll waits for the next event.
    let (_, baseline) = s.get("/_db_updates?since=0").await;
    let since = baseline["last_seq"].as_str().unwrap().to_string();
    let base = s.base.clone();
    let client = s.client.clone();
    let waiting = tokio::spawn(async move {
        client
            .get(format!(
                "{base}/_db_updates?feed=longpoll&since={since}&timeout=2000"
            ))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, _) = s.put("/lpdb", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    let body = waiting.await.unwrap();
    assert!(body["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["db_name"] == "lpdb"));
}

#[tokio::test]
async fn http_conditional_copy_head_and_request_id() {
    let s = Server::start(false);
    s.fresh_db("http").await;
    let (status, body) = s.put("/http/src", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let rev = body["rev"].as_str().unwrap().to_string();

    // If-None-Match returns 304 with the ETag and no body.
    let response = s
        .client
        .get(format!("{}/http/src", s.base))
        .header("if-none-match", format!("\"{rev}\""))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok()),
        Some(format!("\"{rev}\"").as_str())
    );
    assert!(response.text().await.unwrap().is_empty());

    // A stale If-Match is a precondition failure.
    let (status, _) = s
        .req_with(
            Method::PUT,
            "/http/src",
            Some(json!({"v": 2})),
            &[(
                "if-match",
                "\"1-deadbeefdeadbeefdeadbeefdeadbeef\"".to_string(),
            )],
        )
        .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);
    // The matching If-Match succeeds.
    let (status, _) = s
        .req_with(
            Method::PUT,
            "/http/src",
            Some(json!({"v": 2, "_rev": &rev})),
            &[("if-match", format!("\"{rev}\""))],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // COPY uses Destination and creates a new document.
    let response = s
        .client
        .request(
            Method::from_bytes(b"COPY").unwrap(),
            format!("{}/http/src", s.base),
        )
        .header("destination", "dst")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let (status, doc) = s.get("/http/dst").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(doc["v"], 2);

    // HEAD has the normal status/headers but no body.
    let response = s
        .client
        .head(format!("{}/http/src", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().is_empty());

    assert!(s
        .client
        .get(format!("{}/", s.base))
        .send()
        .await
        .unwrap()
        .headers()
        .get("x-couch-request-id")
        .is_some());
}

#[tokio::test]
async fn responses_are_gzipped_when_requested() {
    let s = Server::start(false);
    s.fresh_db("gzip").await;
    let blob = "x".repeat(8 * 1024);
    let (status, _) = s.put("/gzip/doc", json!({"blob": blob})).await;
    assert_eq!(status, StatusCode::CREATED);

    let response = s
        .client
        .get(format!("{}/gzip/doc", s.base))
        .header("accept-encoding", "gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-encoding")
            .and_then(|value| value.to_str().ok()),
        Some("gzip")
    );
}

#[tokio::test]
async fn bulk_get_preserves_order_for_a_large_batch() {
    let s = Server::start(false);
    s.fresh_db("bulkbig").await;
    let docs: Vec<Value> = (0..250)
        .map(|i| json!({"_id": format!("b{i:03}"), "n": i}))
        .collect();
    let (status, _) = s.post("/bulkbig/_bulk_docs", json!({"docs": docs})).await;
    assert_eq!(status, StatusCode::CREATED);

    let reqs: Vec<Value> = (0..250)
        .rev()
        .map(|i| json!({"id": format!("b{i:03}")}))
        .collect();
    let (status, body) = s.post("/bulkbig/_bulk_get", json!({"docs": reqs})).await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 250);
    for (index, row) in results.iter().enumerate() {
        let expected = 249 - index;
        assert_eq!(row["id"], format!("b{expected:03}"));
        assert_eq!(row["docs"][0]["ok"]["n"], expected);
    }
}

#[tokio::test]
async fn new_edits_false_merges_paths_and_keeps_conflicts() {
    let s = Server::start(false);
    s.fresh_db("revs").await;
    let root = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let c = "cccccccccccccccccccccccccccccccc";
    let (status, body) = s
        .post(
            "/revs/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "a", "_rev": format!("2-{b}"),
                 "_revisions": {"start": 2, "ids": [b, root]}, "v": "b"},
                {"_id": "a", "_rev": format!("2-{c}"),
                 "_revisions": {"start": 2, "ids": [c, root]}, "v": "c"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body, json!([]), "new_edits=false returns only errors");

    let (_, body) = s.get("/revs/a?conflicts=true").await;
    assert_eq!(
        body["_rev"],
        format!("2-{c}"),
        "greater rev id wins the tie"
    );
    assert_eq!(body["_conflicts"], json!([format!("2-{b}")]));

    let (_, body) = s.get("/revs/a?open_revs=all").await;
    let mut revs: Vec<String> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["ok"]["_rev"].as_str().unwrap().to_string())
        .collect();
    revs.sort();
    assert_eq!(revs, vec![format!("2-{b}"), format!("2-{c}")]);

    let (_, body) = s.get(&format!("/revs/a?rev=2-{b}&revs=true")).await;
    assert_eq!(body["_revisions"]["start"], 2);
    assert_eq!(body["_revisions"]["ids"], json!([b, root]));
}

#[tokio::test]
async fn local_docs_and_revs_limit() {
    let s = Server::start(false);
    s.fresh_db("local").await;
    let (status, body) = s.put("/local/_local/checkpoint", json!({"seq": 7})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["rev"], "0-1");
    let (_, body) = s.get("/local/_local/checkpoint").await;
    assert_eq!(body["seq"], 7);

    let (status, body) = s
        .put("/local/_local/checkpoint", json!({"seq": 8, "_rev": "0-1"}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["rev"], "0-2");
    let (status, _) = s.delete("/local/_local/checkpoint").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.get("/local/_local/checkpoint").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = s.get("/local/_revs_limit").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!(1000));
    let (status, _) = s.put("/local/_revs_limit", json!(50)).await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = s.get("/local/_revs_limit").await;
    assert_eq!(body, json!(50));
}

#[tokio::test]
async fn attachments_are_rejected() {
    let s = Server::start(false);
    s.fresh_db("att").await;
    let data = base64_encode(b"hi there");

    let (status, body) = s
        .put(
            "/att/doc",
            json!({"_attachments": {
                "a.txt": {"content_type": "text/plain", "data": data}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "attachments_not_supported");

    let (status, body) = s
        .post(
            "/att/_bulk_docs",
            json!({"docs": [{"_id": "bulk", "_attachments": {"a": {"data": data}}}]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "attachments_not_supported");

    let (status, body) = s
        .req_with(
            Method::PUT,
            "/att/doc/a.txt",
            Some(Value::String("ignored".into())),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "attachments_not_supported");
}

#[tokio::test]
async fn purge_removes_only_the_requested_revisions() {
    let s = Server::start(false);
    s.fresh_db("purge").await;
    let (_, r1) = s.put("/purge/a", json!({"v": 1})).await;
    let rev1 = r1["rev"].as_str().unwrap().to_string();
    let (_, r2) = s.put("/purge/a", json!({"v": 2, "_rev": rev1})).await;
    let rev2 = r2["rev"].as_str().unwrap().to_string();
    let (_, r3) = s.put("/purge/a", json!({"v": 3, "_rev": rev2})).await;
    let rev3 = r3["rev"].as_str().unwrap().to_string();

    let (status, body) = s.post("/purge/_purge", json!({"a": [rev2]})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["purged"]["a"], json!([rev2]));
    let (status, _) = s.get(&format!("/purge/a?rev={rev2}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = s.get("/purge/a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_rev"], rev3);

    let (_, body) = s.post("/purge/_purge", json!({"a": [rev1, rev3]})).await;
    assert_eq!(body["purged"]["a"].as_array().unwrap().len(), 2);
    let (status, _) = s.get("/purge/a").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn compact_and_ensure_full_commit() {
    let s = Server::start(false);
    s.fresh_db("maint").await;
    let (status, body) = s.post("/maint/_compact", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, json!({"ok": true}));

    let (status, body) = s.post("/maint/_ensure_full_commit", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["ok"], true);
    assert!(body["instance_start_time"].is_string());
}

#[tokio::test]
async fn rejects_unknown_special_members_like_couchdb() {
    let s = Server::start(false);
    s.fresh_db("valid").await;
    let (status, body) = s.put("/valid/a", json!({"_x": 1})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "doc_validation");
    assert_eq!(body["reason"], "Bad special document member: _x");

    // `_bulk_docs` rejects the whole request, not just the bad document.
    let (status, body) = s
        .post(
            "/valid/_bulk_docs",
            json!({"docs": [{"_id": "b"}, {"_id": "c", "_oops": true}]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "doc_validation");
    let (status, _) = s.get("/valid/b").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "no partial bulk write");

    // Meta members clients echo back are allowed.
    let (status, _) = s
        .put(
            "/valid/d",
            json!({"_id": "d", "v": 1, "_conflicts": [], "_revs_info": []}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn auth_basic_session_cookie_and_unauthorized_shape() {
    let s = Server::start(true);
    let (status, body) = s.get("/_all_dbs").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"], "unauthorized");

    let (status, _) = s.get("/_membership").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let response = s
        .client
        .get(format!("{}/_all_dbs", s.base))
        .basic_auth("admin", Some("douchdb"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = s
        .client
        .get(format!("{}/_membership", s.base))
        .basic_auth("admin", Some("douchdb"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = s
        .client
        .post(format!("{}/_session", s.base))
        .json(&json!({"name": "admin", "password": "douchdb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response
        .headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .expect("AuthSession cookie")
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert!(cookie.starts_with("AuthSession="));

    let response = s
        .client
        .get(format!("{}/_all_dbs", s.base))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let (status, _) = s
        .post("/_session", json!({"name": "admin", "password": "wrong"}))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn cluster_routes_writes_and_merges_all_docs() {
    let nodes = start_cluster(2);
    for node in &nodes {
        node.fresh_db("cluster").await;
    }

    let docs: Vec<Value> = (0..8)
        .map(|i| json!({"_id": format!("d{i}"), "n": i}))
        .collect();
    let (status, body) = nodes[0]
        .post("/cluster/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body.as_array().map(Vec::len), Some(8));
    assert!(body.as_array().unwrap().iter().all(|row| row["ok"] == true));

    // A routed write lives on exactly one node until anti-entropy runs, so a
    // read through either node only succeeds if the proxy reaches the owner.
    for i in 0..8 {
        for node in &nodes {
            let (status, body) = node.get(&format!("/cluster/d{i}")).await;
            assert_eq!(status, StatusCode::OK, "d{i} via {}", node.base);
            assert_eq!(body["n"], i);
        }
    }

    // _all_docs is fanned out and merged by id, not one node's local view.
    for node in &nodes {
        let (status, body) = node.get("/cluster/_all_docs?include_docs=true").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["total_rows"], 8);
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 8);
        assert_eq!(rows[0]["id"], "d0");
        assert_eq!(rows[7]["id"], "d7");
        assert!(
            rows.iter().all(|row| row["doc"]["_id"] == row["id"]),
            "include_docs must survive the fan-out"
        );
    }

    // startkey/skip/limit are applied after the merge, so the offset counts the
    // full cluster index and not a single node's slice.
    let (status, body) = nodes[0]
        .get("/cluster/_all_docs?startkey=%22d3%22&limit=2&skip=1")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["offset"], 4);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["d4", "d5"]);

    // Delete on the owner, then ask for the key: keys queries must still show
    // the tombstone even though range scans omit it.
    let (_, doc) = nodes[0].get("/cluster/d0").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    let (status, _) = nodes[0]
        .req(Method::DELETE, &format!("/cluster/d0?rev={rev}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = nodes[1]
        .post("/cluster/_all_docs", json!({"keys": ["d0", "d1", "zzz"]}))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 7);
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows[0]["id"], "d0");
    assert_eq!(rows[0]["value"]["deleted"], true);
    assert_eq!(rows[1]["id"], "d1");
    assert_eq!(rows[2]["key"], "zzz");
    assert_eq!(rows[2]["error"], "not_found");

    // Repeated keys preserve request order and repeat the row.
    let (_, body) = nodes[0]
        .post("/cluster/_all_docs", json!({"keys": ["d1", "d1"]}))
        .await;
    assert_eq!(body["rows"][0]["id"], "d1");
    assert_eq!(body["rows"][1]["id"], "d1");

    let (_, body) = nodes[0].get("/cluster/_all_docs").await;
    assert_eq!(body["total_rows"], 7);
}

#[tokio::test]
async fn cluster_changes_use_an_opaque_version_vector() {
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("clchanges").await;
    }

    let docs: Vec<Value> = (0..6).map(|i| json!({"_id": format!("v{i}")})).collect();
    let (status, _) = nodes[0]
        .post("/clchanges/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = nodes[0].get("/clchanges/_changes").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"].as_array().unwrap().len(), 6);
    // Both replicas report the same documents; the feed must dedupe by id.
    let ids: std::collections::HashSet<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 6);
    assert!(
        body["last_seq"].is_string(),
        "cluster since is opaque: {:?}",
        body["last_seq"]
    );
    let since = body["last_seq"].as_str().unwrap().to_string();

    // Feeding the vector back resumes exactly where the fan-out stopped.
    let (_, body) = nodes[0]
        .get(&format!("/clchanges/_changes?since={since}"))
        .await;
    assert!(body["results"].as_array().unwrap().is_empty());
    assert_eq!(body["last_seq"], since);

    // GET /db reports the same vector, so update_seq works as a since token.
    let (_, info) = nodes[0].get("/clchanges").await;
    let update_seq = info["update_seq"].as_str().expect("vector update_seq");
    let (_, body) = nodes[0]
        .get(&format!("/clchanges/_changes?since={update_seq}"))
        .await;
    assert!(body["results"].as_array().unwrap().is_empty());

    // since=now is an empty feed that still hands back a usable vector.
    let (_, body) = nodes[0].get("/clchanges/_changes?since=now").await;
    assert!(body["results"].as_array().unwrap().is_empty());
    assert!(body["last_seq"].is_string());

    // _revs_diff and _bulk_get route per document as well.
    let (_, doc) = nodes[0].get("/clchanges/v0").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    let missing = "1-00000000000000000000000000000000";
    let (status, body) = nodes[0]
        .post(
            "/clchanges/_revs_diff",
            json!({"v0": [rev], "v1": [missing]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("v0").is_none());
    assert_eq!(body["v1"]["missing"][0], missing);

    let reqs: Vec<Value> = (0..6).map(|i| json!({"id": format!("v{i}")})).collect();
    let (status, body) = nodes[0]
        .post("/clchanges/_bulk_get", json!({"docs": reqs}))
        .await;
    assert_eq!(status, StatusCode::OK);
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 6);
    for (i, row) in results.iter().enumerate() {
        assert_eq!(row["id"], format!("v{i}"));
        assert!(row["docs"][0]["ok"].is_object());
    }

    // A deletion after the checkpoint is the only row the next poll sees.
    let (_, doc) = nodes[0].get("/clchanges/v1").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    let (status, _) = nodes[0]
        .req(Method::DELETE, &format!("/clchanges/v1?rev={rev}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);

    let (_, body) = nodes[0]
        .get(&format!("/clchanges/_changes?since={since}"))
        .await;
    let rows = body["results"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "v1");
    assert_eq!(rows[0]["deleted"], true);
}

#[tokio::test]
async fn cluster_changes_limit_covers_all_nodes() {
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("clchlimit").await;
    }
    let docs: Vec<Value> = (0..9).map(|i| json!({"_id": format!("lim{i}")})).collect();
    let (status, _) = nodes[0]
        .post("/clchlimit/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let mut since = "0".to_string();
    let mut ids = Vec::new();
    for _ in 0..5 {
        let (status, body) = nodes[0]
            .get(&format!("/clchlimit/_changes?since={since}&limit=4"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let results = body["results"].as_array().unwrap();
        assert!(results.len() <= 4);
        if results.is_empty() {
            break;
        }
        for row in results {
            ids.push(row["id"].as_str().unwrap().to_string());
        }
        since = body["last_seq"].as_str().unwrap().to_string();
    }
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        9,
        "limited feed must eventually cover every node"
    );
}

fn all_shards_on(node: usize) -> Value {
    json!({
        "q": 4,
        "node_count": 3,
        "replicas": vec![vec![node]; 4],
    })
}

#[tokio::test]
async fn cluster_shard_map_moves_data_between_nodes() {
    let mut nodes = start_cluster_with(3, "1", 1);
    for node in &nodes {
        node.fresh_db("movedb").await;
    }

    // Pin every shard to node0, write a document, then move every shard to
    // node1. Anti-entropy must pull the document under the new placement.
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/movedb", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, map) = nodes[1].get("/_node/_local/_dbs/movedb").await;
    assert_eq!(map["replicas"][0][0], 0, "metadata replicated to peers");

    let (status, _) = nodes[0]
        .put("/movedb/move", json!({"value": "moved"}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/movedb", all_shards_on(1))
        .await;
    assert_eq!(status, StatusCode::OK);

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = nodes[0].get("/movedb/move").await;
        if status == StatusCode::OK {
            assert_eq!(body["value"], "moved");
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("shard move did not replicate: {status} {body}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (status, body) = nodes[1].get("/movedb/move").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], "moved");

    // The old replica may still hold bytes, but reads route to the new owner.
    nodes[0].kill();
    let (status, body) = nodes[1].get("/movedb/move").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["value"], "moved");
}

#[tokio::test]
async fn cluster_shard_map_versions_reconcile_after_peer_returns() {
    let mut nodes = start_cluster_with(3, "1", 1);
    for node in &nodes {
        node.fresh_db("meta").await;
    }

    let (status, first) = nodes[0]
        .put("/_node/_local/_dbs/meta", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["version"], 1);
    assert_eq!(first["pending"], false);

    let (status, second) = nodes[0]
        .put("/_node/_local/_dbs/meta", all_shards_on(1))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["version"], 2);
    assert_eq!(second["pending"], false);

    // Lose metadata quorum, then revive one peer. The pending versioned map
    // must be pushed by anti-entropy and the pending marker cleared.
    nodes[1].kill();
    nodes[2].kill();
    let (status, pending) = nodes[0]
        .put("/_node/_local/_dbs/meta", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pending["version"], 3);
    assert_eq!(pending["pending"], true);
    assert!(nodes[0]
        .dir
        .join("meta")
        .join("pending-shard-map.json")
        .is_file());

    nodes[1].restart();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, map) = nodes[1].get("/_node/_local/_dbs/meta").await;
        if status == StatusCode::OK && map["version"] == 3 {
            assert_eq!(map["replicas"][0][0], 0);
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("map did not reconcile on restarted peer: {status} {map}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        !nodes[0]
            .dir
            .join("meta")
            .join("pending-shard-map.json")
            .exists(),
        "pending map was not cleared after quorum acknowledgement"
    );
}

#[tokio::test]
async fn cluster_rebalance_converges_and_cleans_old_replicas() {
    let nodes = start_cluster_with(3, "1", 1);
    for node in &nodes {
        node.fresh_db("reb").await;
    }
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/reb", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);

    let docs: Vec<Value> = (0..9)
        .map(|i| json!({"_id": format!("reb{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/reb/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // The planner spreads the four shards across the three nodes.
    let (status, map) = nodes[0]
        .post("/_node/_local/_dbs/reb/rebalance", json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "{map}");
    let owners: std::collections::HashSet<u64> = map["replicas"]
        .as_array()
        .unwrap()
        .iter()
        .map(|replicas| replicas[0].as_u64().unwrap())
        .collect();
    assert!(owners.len() > 1, "planner must use multiple nodes: {map}");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = nodes[0].get("/reb/_all_docs").await;
        if status == StatusCode::OK && body["total_rows"] == 9 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("rebalance did not converge: {status} {body}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Move everything to node1, wait for convergence, then clean node0's
    // non-owned local shards.
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/reb", all_shards_on(1))
        .await;
    assert_eq!(status, StatusCode::OK);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = nodes[1].get("/reb/_all_docs").await;
        if status == StatusCode::OK && body["total_rows"] == 9 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("shard move did not converge on node1: {status} {body}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (status, cleared) = nodes[0]
        .post("/_node/_local/_dbs/reb/cleanup", json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "{cleared}");
    assert_eq!(cleared["cleared"].as_array().unwrap().len(), 4);

    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let (_, info) = nodes[0]
        .req_with(Method::GET, "/reb", None, &internal)
        .await;
    assert_eq!(info["doc_count"], 0, "old replica was not cleared");
    let (_, info) = nodes[1]
        .req_with(Method::GET, "/reb", None, &internal)
        .await;
    assert_eq!(info["doc_count"], 9, "new replica lost data");

    let (status, body) = nodes[0].get("/reb/_all_docs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 9);
}

#[tokio::test]
async fn cluster_rebalance_auto_cleans_old_replicas() {
    let nodes = start_cluster_with(3, "1", 1);
    for node in &nodes {
        node.fresh_db("autoreb").await;
    }
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/autoreb", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);

    let docs: Vec<Value> = (0..9)
        .map(|i| json!({"_id": format!("auto{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/autoreb/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = nodes[0]
        .post(
            "/_node/_local/_dbs/autoreb/rebalance?timeout_ms=20000",
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["pending"], false);
    assert_eq!(body["cleaned"].as_array().unwrap().len(), 3);
    let map = body.as_object().unwrap();
    let replicas = map["replicas"].as_array().unwrap();

    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let mut total = 0u64;
    for (index, node) in nodes.iter().enumerate() {
        let (status, counts) = node
            .req_with(
                Method::GET,
                "/_node/_local/_dbs/autoreb/shards",
                None,
                &internal,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{counts}");
        let shards = counts["shards"].as_array().unwrap();
        for (shard, row) in shards.iter().enumerate() {
            let docs = row["docs"].as_u64().unwrap();
            total += docs;
            let owned = replicas[shard]
                .as_array()
                .unwrap()
                .iter()
                .any(|owner| owner.as_u64().unwrap() as usize == index);
            if !owned {
                assert_eq!(docs, 0, "node {index} kept shard {shard} after cleanup");
            }
        }
    }
    assert_eq!(total, 9, "cleanup must not duplicate or lose documents");

    let (status, body) = nodes[0].get("/autoreb/_all_docs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 9);
}

#[tokio::test]
async fn cluster_rebalance_timeout_does_not_clean_old_replicas() {
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("timeoutreb").await;
    }
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/timeoutreb", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);
    let docs: Vec<Value> = (0..9)
        .map(|i| json!({"_id": format!("to{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/timeoutreb/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = nodes[0]
        .post(
            "/_node/_local/_dbs/timeoutreb/rebalance?timeout_ms=500",
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{body}");

    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let (status, counts) = nodes[0]
        .req_with(
            Method::GET,
            "/_node/_local/_dbs/timeoutreb/shards",
            None,
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{counts}");
    let total: u64 = counts["shards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["docs"].as_u64().unwrap())
        .sum();
    assert_eq!(total, 9, "timeout must not clean the old replica");
}

#[tokio::test]
async fn cluster_changes_survive_node_loss_and_resume() {
    let mut nodes = start_cluster_with(3, "1", 2);
    for node in &nodes {
        node.fresh_db("cfail").await;
    }
    let docs: Vec<Value> = (0..9)
        .map(|i| json!({"_id": format!("cf{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/cfail/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, baseline) = nodes[0].get("/cfail/_changes?since=0").await;
    assert_eq!(status, StatusCode::OK);
    let ids: std::collections::HashSet<&str> = baseline["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 9);
    let since = baseline["last_seq"].as_str().unwrap().to_string();

    nodes[2].kill();
    let (status, _) = nodes[0].put("/cfail/after", json!({"n": 100})).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
        "write after node loss: {status}"
    );

    let (status, body) = nodes[0]
        .get(&format!("/cfail/_changes?since={since}"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(
        ids.contains(&"after"),
        "resumed feed missed new change: {ids:?}"
    );
    assert!(body["last_seq"].is_string());
}

#[tokio::test]
async fn cluster_all_docs_survives_node_loss_via_replicas() {
    let mut nodes = start_cluster_with(3, "3600", 2);
    for node in &nodes {
        node.fresh_db("alldocsfail").await;
    }
    let docs: Vec<Value> = (0..9)
        .map(|i| json!({"_id": format!("al{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/alldocsfail/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = nodes[0].get("/alldocsfail/_all_docs").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 9);

    nodes[2].kill();

    let (status, body) = nodes[0].get("/alldocsfail/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 9);
    assert_eq!(body["rows"].as_array().unwrap().len(), 9);

    let (status, body) = nodes[0]
        .get("/alldocsfail/_all_docs?startkey=%22al3%22&limit=3&skip=1")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 9);
    assert_eq!(body["offset"], 4);

    let (status, body) = nodes[0]
        .post("/alldocsfail/_all_docs", json!({"keys": ["al0", "al8"]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 9);
    assert_eq!(body["rows"][0]["id"], "al0");
    assert_eq!(body["rows"][1]["id"], "al8");
}

#[tokio::test]
async fn cluster_shard_mask_reads_only_selected_shards() {
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("shardmask").await;
    }
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/shardmask", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);

    let docs: Vec<Value> = (0..24)
        .map(|i| json!({"_id": format!("sm{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/shardmask/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[0]
        .put(
            "/shardmask/_design/m",
            json!({"views": {"all": {"map": "function(doc){ emit(doc.n, null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let mut all_docs_ids = std::collections::HashSet::new();
    let mut view_ids = std::collections::HashSet::new();
    let mut total = 0u64;
    for shard in 0..4 {
        let (status, body) = nodes[0]
            .req_with(
                Method::GET,
                &format!("/shardmask/_all_docs?shards={shard}"),
                None,
                &internal,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let selected = body["total_rows"].as_u64().unwrap();
        total += selected;
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len() as u64, selected, "shard {shard}: {body}");
        for row in rows {
            assert!(
                all_docs_ids.insert(row["id"].as_str().unwrap().to_string()),
                "document returned by more than one shard mask"
            );
        }

        let (status, body) = nodes[0]
            .req_with(
                Method::GET,
                &format!("/shardmask/_design/m/_view/all?reduce=false&shards={shard}"),
                None,
                &internal,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body["rows"].as_array().unwrap();
        assert!(rows.len() as u64 <= selected, "view shard {shard}: {body}");
        for row in rows {
            assert!(
                view_ids.insert(row["id"].as_str().unwrap().to_string()),
                "view row returned by more than one shard mask"
            );
        }
    }
    assert_eq!(total, 25);
    assert_eq!(all_docs_ids.len(), 25);
    // Design documents are excluded from view indexes, so the view union is
    // every non-design document exactly once.
    all_docs_ids.remove("_design/m");
    assert_eq!(view_ids, all_docs_ids);
}

#[tokio::test]
async fn cluster_full_replica_reads_survive_two_node_loss() {
    let mut nodes = start_cluster_with(3, "3600", 3);
    for node in &nodes {
        node.fresh_db("clfull").await;
    }
    let docs: Vec<Value> = (0..12)
        .map(|i| json!({"_id": format!("f{i:02}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/clfull/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[0]
        .put(
            "/clfull/_design/v",
            json!({
                "views": {
                    "all": {"map": "function(doc){ emit(doc.n, null); }"},
                    "count": {
                        "map": "function(doc){ emit(doc.n, null); }",
                        "reduce": "_count"
                    }
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    nodes[1].kill();
    nodes[2].kill();

    // With n=3 on three nodes, node0 holds every shard. Shard-masked reads
    // must use those local replicas and not require any surviving peer.
    let (status, body) = nodes[0].get("/clfull/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 13);
    let (status, body) = nodes[0]
        .post("/clfull/_all_docs", json!({"keys": ["f00", "f11"]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["id"], "f00");
    assert_eq!(body["rows"][1]["id"], "f11");
    let (status, body) = nodes[0].get("/clfull/_design/v/_view/all").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 12);
    let (status, body) = nodes[0].get("/clfull/_design/v/_view/count").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 12);
    let (status, body) = nodes[0].get("/clfull/_changes").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().unwrap().len(), 13);
    let (status, body) = nodes[0].get("/clfull/_changes?limit=5").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().unwrap().len(), 5);
    let (status, body) = nodes[0]
        .get("/clfull/_changes?include_docs=true&limit=3")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().unwrap().len(), 3);
    assert!(body["last_seq"].is_string(), "{body}");
    for row in body["results"].as_array().unwrap() {
        assert_eq!(row["doc"]["_id"], row["id"]);
        assert!(row["doc"]["_rev"].is_string());
    }

    // Bulk reads also prefer the local replica, so no peer is required.
    let (status, body) = nodes[0]
        .post(
            "/clfull/_bulk_get",
            json!({"docs": [{"id": "f00"}, {"id": "f11"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let results = body["results"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|row| row["docs"][0]["ok"].is_object()));

    let missing = "1-00000000000000000000000000000000";
    let (status, body) = nodes[0]
        .post("/clfull/_revs_diff", json!({"f00": [missing]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["f00"]["missing"][0], missing);
}

#[tokio::test]
async fn cluster_post_creates_the_document_on_its_owner() {
    let nodes = start_cluster(2);
    for node in &nodes {
        node.fresh_db("clpost").await;
    }

    // POST generates the id after the request arrives, so the handler must
    // route the resulting document rather than writing it to the local shards.
    for _ in 0..10 {
        let (status, body) = nodes[0].post("/clpost", json!({"x": 1})).await;
        assert_eq!(status, StatusCode::CREATED);
        let id = body["id"].as_str().unwrap().to_string();
        for node in &nodes {
            let (status, doc) = node.get(&format!("/clpost/{id}")).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "generated id {id} missing via {}",
                node.base
            );
            assert_eq!(doc["x"], 1);
        }
    }
}

#[tokio::test]
async fn cluster_anti_entropy_converges_without_client_reads() {
    let nodes = start_cluster_with(2, "1", 2);
    for node in &nodes {
        node.fresh_db("clsync").await;
    }

    let docs: Vec<Value> = (0..8)
        .map(|i| json!({"_id": format!("s{i}"), "n": i}))
        .collect();
    // Write with the forwarded marker so only node 0 stores the documents:
    // node 1 must then pull them through anti-entropy.
    let response = nodes[0]
        .client
        .post(format!("{}/clsync/_bulk_docs", nodes[0].base))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", "test-cluster-secret")
        .json(&json!({"docs": docs}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // A forwarded `_all_docs` reads only this node's shards, so seeing all
    // eight rows on node 1 means the anti-entropy pull actually copied them
    // (a client read would be proxied to the owner and prove nothing).
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let response = nodes[1]
            .client
            .post(format!("{}/clsync/_all_docs", nodes[1].base))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", "test-cluster-secret")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        if response["total_rows"] == 8 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "anti-entropy did not converge: {response}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn cluster_anti_entropy_drains_a_large_backlog_in_one_pass() {
    let nodes = start_cluster_with(2, "1", 2);
    for node in &nodes {
        node.fresh_db("cldrain").await;
    }

    let total = 1000usize;
    let docs: Vec<Value> = (0..total)
        .map(|i| json!({"_id": format!("b{i:04}"), "n": i}))
        .collect();
    let response = nodes[0]
        .client
        .post(format!("{}/cldrain/_bulk_docs", nodes[0].base))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", "test-cluster-secret")
        .json(&json!({"docs": docs}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // More than the old 200-document-per-interval cap. One anti-entropy pass
    // must drain every page rather than stopping after a single batch.
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    loop {
        let response = nodes[1]
            .client
            .post(format!("{}/cldrain/_all_docs", nodes[1].base))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", "test-cluster-secret")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap();
        if response["total_rows"] == total {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "anti-entropy did not drain the backlog: {response}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn cluster_writes_land_on_the_whole_replica_set() {
    let nodes = start_cluster_with(3, "3600", 2);
    for node in &nodes {
        node.fresh_db("clrep").await;
    }

    let docs: Vec<Value> = (0..12)
        .map(|i| json!({"_id": format!("r{i}"), "n": i}))
        .collect();
    let (status, body) = nodes[0]
        .post("/clrep/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body.as_array().map(Vec::len), Some(12));
    assert!(body.as_array().unwrap().iter().all(|row| row["ok"] == true));

    // Read each node's shards directly (forwarded requests are not routed), so
    // the copies counted here are the ones physically stored on that node.
    let mut copies: HashMap<String, Vec<(usize, String)>> = HashMap::new();
    let mut total = 0usize;
    for (index, node) in nodes.iter().enumerate() {
        let body: Value = node
            .client
            .post(format!("{}/clrep/_all_docs?include_docs=true", node.base))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", "test-cluster-secret")
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        total += body["total_rows"].as_u64().unwrap() as usize;
        for row in body["rows"].as_array().unwrap() {
            copies
                .entry(row["id"].as_str().unwrap().to_string())
                .or_default()
                .push((index, row["doc"]["_rev"].as_str().unwrap().to_string()));
        }
    }
    assert_eq!(total, 24, "12 documents x replication factor 2");
    assert_eq!(copies.len(), 12);
    for (id, held) in &copies {
        assert_eq!(held.len(), 2, "{id} replicas {held:?}");
        assert_eq!(held[0].1, held[1].1, "{id} diverged across replicas");
    }

    // Routed reads reach a replica from every node.
    for i in 0..12 {
        for node in &nodes {
            let (status, body) = node.get(&format!("/clrep/r{i}")).await;
            assert_eq!(status, StatusCode::OK, "r{i} via {}", node.base);
            assert_eq!(body["n"], i);
        }
    }
}

#[tokio::test]
async fn cluster_survives_killing_a_node_mid_replication() {
    let mut nodes = start_cluster_with(3, "1", 2);
    for node in &nodes {
        node.fresh_db("clkill").await;
    }

    let first: Vec<Value> = (0..12)
        .map(|i| json!({"_id": format!("k{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/clkill/_bulk_docs", json!({"docs": first}))
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
        "initial write: {status}"
    );

    // Kill a node while anti-entropy is still moving shards around.
    nodes[2].kill();

    // _all_docs falls back to the surviving replicas.
    let (status, body) = nodes[0].get("/clkill/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 12);
    // The changes feed falls back to the surviving replicas too.
    let (status, body) = nodes[0].get("/clkill/_changes").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["last_seq"].is_string());

    let second: Vec<Value> = (100..106)
        .map(|i| json!({"_id": format!("k{i}"), "n": i}))
        .collect();
    let (status, body) = nodes[0]
        .post("/clkill/_bulk_docs", json!({"docs": second}))
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
        "write after kill: {status} {body}"
    );
    assert!(body.as_array().unwrap().iter().all(|row| row["ok"] == true));

    // Every document remains readable through either survivor: reads fall back
    // to the next replica when the primary is the dead node.
    let survivors = &nodes[..2];
    for i in (0..12).chain(100..106) {
        for node in survivors {
            let (status, _) = node.get(&format!("/clkill/k{i}")).await;
            assert_eq!(status, StatusCode::OK, "k{i} via {}", node.base);
        }
    }

    // Anti-entropy converges the survivors to a full copy of every document
    // that has a replica among them.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut seen = std::collections::HashSet::new();
        for node in survivors {
            let body: Value = node
                .client
                .post(format!("{}/clkill/_all_docs", node.base))
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", "test-cluster-secret")
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            for row in body["rows"].as_array().unwrap() {
                seen.insert(row["id"].as_str().unwrap().to_string());
            }
        }
        if seen.len() == 18 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "survivors did not converge: {} of 18 docs",
            seen.len()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn cluster_honours_w_and_r_quorum_parameters() {
    let mut nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("clquorum").await;
    }

    // Default is a write quorum; ?w=2 asks for both replicas explicitly.
    let (status, _) = nodes[0]
        .post("/clquorum/_bulk_docs?w=2", json!({"docs": [{"_id": "q1"}]}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, doc) = nodes[0].get("/clquorum/q1?r=2").await;
    assert_eq!(status, StatusCode::OK);
    assert!(doc["_rev"].is_string());

    // With one of two replicas down, w=2 cannot be met and CouchDB answers
    // 202; w=1 is still satisfiable and answers 201.
    nodes[1].kill();
    let (status, body) = nodes[0]
        .post("/clquorum/_bulk_docs?w=2", json!({"docs": [{"_id": "q2"}]}))
        .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "w=2 with one live replica: {body}"
    );
    let (status, body) = nodes[0]
        .post("/clquorum/_bulk_docs?w=1", json!({"docs": [{"_id": "q3"}]}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "w=1: {body}");

    // r=2 tolerates the dead replica as long as one answers.
    let (status, _) = nodes[0].get("/clquorum/q2?r=2").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn view_index_updates_incrementally_and_persists() {
    let s = Server::start(false);
    s.fresh_db("incview").await;

    let (status, _) = s
        .post(
            "/incview/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "city": "Berlin"},
                {"_id": "b", "city": "Berlin"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/incview/_design/v",
            json!({"views": {"by_city": {"map": "function(doc){ emit(doc.city, 1); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Build the index.
    let (_, body) = s.get("/incview/_design/v/_view/by_city").await;
    assert_eq!(body["total_rows"], 2);
    let index_dir = s.dir.join("incview").join("_view_indexes");
    assert!(index_dir.is_dir(), "view index directory was not created");

    // A new document is picked up through _changes, without a rebuild.
    let (status, _) = s.put("/incview/c", json!({"city": "Paris"})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get("/incview/_design/v/_view/by_city").await;
    assert_eq!(body["total_rows"], 3);
    assert_eq!(body["rows"][2]["id"], "c");

    // Moving a document between keys removes its old row.
    let (_, doc) = s.get("/incview/b").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    s.put("/incview/b", json!({"_rev": rev, "city": "Paris"}))
        .await;
    let (_, body) = s
        .get("/incview/_design/v/_view/by_city?key=%22Berlin%22")
        .await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["id"], "a");
    let (_, body) = s
        .get("/incview/_design/v/_view/by_city?key=%22Paris%22")
        .await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);

    // Deleting a document removes its rows.
    let (_, doc) = s.get("/incview/a").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    let (status, _) = s
        .req(Method::DELETE, &format!("/incview/a?rev={rev}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = s.get("/incview/_design/v/_view/by_city").await;
    assert_eq!(body["total_rows"], 2);
}

#[tokio::test]
async fn view_index_is_reused_after_restart() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data = std::env::temp_dir().join(format!(
        "douchdb-view-persist-{}-{stamp}",
        std::process::id()
    ));
    let extra = vec!["--data".to_string(), data.to_string_lossy().into_owned()];

    let mut first = Server::start_with(&extra, false);
    first.fresh_db("persist").await;
    let (status, _) = first
        .post(
            "/persist/_bulk_docs",
            json!({"docs": [{"_id": "a", "city": "Berlin"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = first
        .put(
            "/persist/_design/v",
            json!({"views": {"rnd": {"map": "function(doc){ emit(doc.city, Math.random()); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = first.get("/persist/_design/v/_view/rnd").await;
    assert_eq!(status, StatusCode::OK);
    let value = body["rows"][0]["value"].clone();
    assert!(value.is_number());
    assert!(data.join("persist").join("_view_indexes").is_dir());

    first.kill();
    drop(first);

    // A fresh process must load the persisted index rather than re-running the
    // map function: Math.random would produce a different value on a rebuild.
    let mut second = Server::start_with(&extra, false);
    let (status, body) = second.get("/persist/_design/v/_view/rnd").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["value"], value);
    second.kill();
    drop(second);
    let _ = std::fs::remove_dir_all(data);
}

#[tokio::test]
async fn cluster_views_merge_rows_from_every_replica() {
    let nodes = start_cluster_with(3, "3600", 2);
    for node in &nodes {
        node.fresh_db("clview").await;
    }

    let docs: Vec<Value> = (0..12)
        .map(|i| {
            json!({
                "_id": format!("v{i}"),
                "city": if i % 2 == 0 { "Berlin" } else { "Paris" },
                "n": i,
            })
        })
        .collect();
    let (status, _) = nodes[0]
        .post("/clview/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let ddoc = json!({"views": {
        "by_city": {"map": "function(doc){ emit(doc.city, doc.n); }"},
        "count": {
            "map": "function(doc){ emit(doc.city, null); }",
            "reduce": "_count"
        },
        "sum": {
            "map": "function(doc){ emit(doc.city, doc.n); }",
            "reduce": "_sum"
        },
        "custom_sum": {
            "map": "function(doc){ emit(doc.city, doc.n); }",
            "reduce": "function(keys, values, rereduce){ return sum(values); }"
        }
    }});
    let (status, body) = nodes[0].put("/clview/_design/v", ddoc).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Each node stores only its replicas; every node's view query must still
    // see all documents after the coordinator merges the map rows.
    for node in &nodes {
        let (status, body) = node.get("/clview/_design/v/_view/by_city").await;
        assert_eq!(status, StatusCode::OK, "view via {}", node.base);
        assert_eq!(body["total_rows"], 12, "view via {}", node.base);
        let mut ids: Vec<&str> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 12, "duplicate rows via {}", node.base);

        let (status, body) = node.get("/clview/_design/v/_view/count?group=true").await;
        assert_eq!(status, StatusCode::OK);
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        for row in rows {
            assert_eq!(row["value"], 6);
        }
    }

    // Global built-in reduces are computed per owner and combined.
    let (_, body) = nodes[0].get("/clview/_design/v/_view/count").await;
    assert_eq!(body["rows"][0]["value"], 12);
    let (_, body) = nodes[0].get("/clview/_design/v/_view/sum").await;
    assert_eq!(body["rows"][0]["value"], 66);
    let (_, body) = nodes[0].get("/clview/_design/v/_view/sum?group=true").await;
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["key"], "Berlin");
    assert_eq!(rows[0]["value"], 30);
    assert_eq!(rows[1]["key"], "Paris");
    assert_eq!(rows[1]["value"], 36);

    // Custom reducers are recombined across owners with rereduce=true.
    let (_, body) = nodes[0].get("/clview/_design/v/_view/custom_sum").await;
    assert_eq!(body["rows"][0]["value"], 66);
    let (_, body) = nodes[0]
        .get("/clview/_design/v/_view/custom_sum?group=true")
        .await;
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows[0]["value"], 30);
    assert_eq!(rows[1]["value"], 36);

    // Map queries push the window down to the owner nodes but still report the
    // whole-view total and the correct offset.
    let (_, body) = nodes[0]
        .get("/clview/_design/v/_view/by_city?skip=2&limit=5")
        .await;
    assert_eq!(body["total_rows"], 12);
    assert_eq!(body["offset"], 2);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["v2", "v4", "v6", "v8", "v1"]);

    let (_, body) = nodes[0]
        .get("/clview/_design/v/_view/by_city?descending=true&limit=3")
        .await;
    assert_eq!(body["total_rows"], 12);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["v9", "v7", "v5"]);

    // include_docs travels with the pushed-down window.
    let (_, body) = nodes[0]
        .get("/clview/_design/v/_view/by_city?include_docs=true&limit=3")
        .await;
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row["doc"]["_id"] == row["id"]));
}

#[tokio::test]
async fn purge_invalidates_view_indexes() {
    let s = Server::start(false);
    s.fresh_db("purgeview").await;

    let (status, _) = s.put("/purgeview/a", json!({"city": "Berlin"})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get("/purgeview/a").await;
    let rev = body["_rev"].as_str().unwrap().to_string();
    let (status, _) = s
        .put(
            "/purgeview/_design/v",
            json!({"views": {"cities": {"map": "function(doc){ emit(doc.city, 1); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (_, body) = s.get("/purgeview/_design/v/_view/cities").await;
    assert_eq!(body["total_rows"], 1);

    let (status, _) = s.post("/purgeview/_purge", json!({"a": [rev]})).await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = s.get("/purgeview/_design/v/_view/cities").await;
    assert_eq!(body["total_rows"], 0);
}

#[test]
fn clustered_startup_requires_a_cluster_secret() {
    let output = Command::new(env!("CARGO_BIN_EXE_douchdb"))
        .env_remove("DOUCHDB_CLUSTER_SECRET")
        .args([
            "--nodes",
            "http://127.0.0.1:1,http://127.0.0.1:2",
            "--me",
            "0",
            "--data",
            "/tmp/douchdb-should-not-be-created",
        ])
        .output()
        .expect("run douchdb");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--cluster-secret"), "{stderr}");
}

#[tokio::test]
async fn forged_internal_headers_are_ignored() {
    let nodes = start_cluster_with(2, "3600", 1);
    for node in &nodes {
        node.fresh_db("forged").await;
    }
    let docs: Vec<Value> = (0..8).map(|i| json!({"_id": format!("f{i}")})).collect();
    let (status, _) = nodes[0]
        .post("/forged/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // A client cannot force local-only behaviour by inventing the internal
    // marker: without the cluster secret this must still fan out.
    let forged: Value = nodes[0]
        .client
        .get(format!("{}/forged/_all_docs", nodes[0].base))
        .header("x-douchdb-forwarded", "1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(forged["total_rows"], 8, "forged header skipped peers");

    // The real header from a peer is honoured and returns only local rows.
    // Sum local totals across nodes: a node that happens to own every shard is
    // allowed to return all 8, but the two nodes together must not double-count.
    let mut local_total = 0u64;
    for node in &nodes {
        let local: Value = node
            .client
            .get(format!("{}/forged/_all_docs", node.base))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", "test-cluster-secret")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        local_total += local["total_rows"].as_u64().unwrap();
    }
    assert_eq!(local_total, 8, "local reads must partition the database");

    // Same rule for the database-lifecycle marker: a forged one must not
    // suppress the broadcast.
    let (status, _) = nodes[0]
        .req_with(
            Method::PUT,
            "/forged-local",
            Some(json!({})),
            &[("x-douchdb-local-db", "1".to_string())],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[1].get("/forged-local").await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = nodes[0]
        .req_with(
            Method::PUT,
            "/forged-real",
            Some(json!({})),
            &[
                ("x-douchdb-local-db", "1".to_string()),
                ("x-douchdb-secret", "test-cluster-secret".to_string()),
            ],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[1].get("/forged-real").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cluster_views_span_shards_with_replication_factor_one() {
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("clview1").await;
    }

    let docs: Vec<Value> = (0..9)
        .map(|i| json!({"_id": format!("s{i}"), "city": if i % 2 == 0 { "A" } else { "B" }}))
        .collect();
    let (status, _) = nodes[0]
        .post("/clview1/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[0]
        .put(
            "/clview1/_design/v",
            json!({"views": {"by_city": {"map": "function(doc){ emit(doc.city, 1); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // One replica per document means no single node has every document. The
    // design doc is on every node so each can map its own shards, and the
    // coordinator merges all of them.
    for node in &nodes {
        let (status, body) = node.get("/clview1/_design/v/_view/by_city").await;
        assert_eq!(status, StatusCode::OK, "view via {}", node.base);
        assert_eq!(body["total_rows"], 9, "view via {}", node.base);
    }
}

#[tokio::test]
async fn cluster_view_query_survives_a_node_loss_via_replicas() {
    let mut nodes = start_cluster_with(3, "3600", 2);
    for node in &nodes {
        node.fresh_db("clviewfail").await;
    }
    let (status, _) = nodes[0]
        .post(
            "/clviewfail/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "city": "Berlin"},
                {"_id": "b", "city": "Paris"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[0]
        .put(
            "/clviewfail/_design/v",
            json!({"views": {
                "by_city": {"map": "function(doc){ emit(doc.city, 1); }"},
                "count": {
                    "map": "function(doc){ emit(doc.city, null); }",
                    "reduce": "_count"
                }
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = nodes[0].get("/clviewfail/_design/v/_view/by_city").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 2);

    // A dead node is covered by its replicas. The fallback reads all local
    // replicas from the surviving nodes and deduplicates rows.
    nodes[2].kill();
    let (status, body) = nodes[0].get("/clviewfail/_design/v/_view/by_city").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 2);
    let mut ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["a", "b"]);

    // Reduce partials fall back to the dead owner's replicas too.
    let (status, body) = nodes[0].get("/clviewfail/_design/v/_view/count").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 2);
    let (status, body) = nodes[0]
        .get("/clviewfail/_design/v/_view/count?group=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut groups: Vec<(String, u64)> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["key"].as_str().unwrap().to_string(),
                row["value"].as_u64().unwrap(),
            )
        })
        .collect();
    groups.sort();
    assert_eq!(
        groups,
        [("Berlin".to_string(), 1), ("Paris".to_string(), 1)]
    );
}

#[tokio::test]
async fn authenticated_cluster_anti_entropy_uses_node_credentials() {
    let nodes = start_cluster_with_auth(2, "1", 2);
    let basic = format!("Basic {}", base64_encode(b"admin:douchdb"));
    let auth = [("Authorization", basic.clone())];

    let (status, _) = nodes[0]
        .req_with(Method::PUT, "/authsync", Some(json!({})), &auth)
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[1]
        .req_with(Method::GET, "/authsync", None, &auth)
        .await;
    assert_eq!(status, StatusCode::OK, "database create was authenticated");

    // Write only on node 0; node 1 must pull the documents with the node's
    // own credentials, not the client's (there is no client here).
    let docs: Vec<Value> = (0..6)
        .map(|i| json!({"_id": format!("as{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .req_with(
            Method::POST,
            "/authsync/_bulk_docs",
            Some(json!({"docs": docs})),
            &[
                ("Authorization", basic.clone()),
                ("x-douchdb-forwarded", "1".to_string()),
                ("x-douchdb-secret", "test-cluster-secret".to_string()),
            ],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let (status, body) = nodes[1]
            .req_with(
                Method::POST,
                "/authsync/_all_docs",
                Some(json!({})),
                &[
                    ("Authorization", basic.clone()),
                    ("x-douchdb-forwarded", "1".to_string()),
                    ("x-douchdb-secret", "test-cluster-secret".to_string()),
                ],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        if body["total_rows"] == 6 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "authenticated anti-entropy did not converge: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn cluster_forwards_session_cookies_to_peers() {
    let nodes = start_cluster_with_auth(2, "3600", 2);

    let response = nodes[0]
        .client
        .post(format!("{}/_session", nodes[0].base))
        .json(&json!({"name": "admin", "password": "douchdb"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .find_map(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .expect("session cookie")
        .to_string();
    let auth = [("Cookie", cookie)];

    // Database create is broadcast, so the peer must receive the cookie.
    let (status, _) = nodes[0]
        .req_with(Method::PUT, "/cookiecl", Some(json!({})), &auth)
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[1]
        .req_with(Method::GET, "/cookiecl", None, &auth)
        .await;
    assert_eq!(status, StatusCode::OK, "cookie reached the peer's create");

    let (status, _) = nodes[0]
        .req_with(Method::PUT, "/cookiecl/cdoc", Some(json!({"v": 1})), &auth)
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Both fan-outs call peers: a 401 here would fail the request.
    let (status, body) = nodes[0]
        .req_with(
            Method::POST,
            "/cookiecl/_all_docs",
            Some(json!({"include_docs": true})),
            &auth,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["doc"]["v"], 1);

    let (status, body) = nodes[0]
        .req_with(Method::GET, "/cookiecl/_changes", None, &auth)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"].as_array().unwrap().len(), 1);

    // Delete is broadcast as well.
    let (status, _) = nodes[0]
        .req_with(Method::DELETE, "/cookiecl", None, &auth)
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = nodes[1]
        .req_with(Method::GET, "/cookiecl", None, &auth)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cluster_database_creation_and_drop_are_broadcast() {
    let nodes = start_cluster(2);
    let _ = nodes[0].delete("/clife").await;
    let (status, _) = nodes[0].put("/clife", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);

    // A database created through one node must exist on every node, and
    // _all_dbs must show the union.
    for node in &nodes {
        let (status, _) = node.get("/clife").await;
        assert_eq!(status, StatusCode::OK, "missing db via {}", node.base);
        let (_, dbs) = node.get("/_all_dbs").await;
        assert!(dbs.as_array().unwrap().contains(&json!("clife")));
    }

    let (status, _) = nodes[1].delete("/clife").await;
    assert_eq!(status, StatusCode::OK);
    for node in &nodes {
        let (status, _) = node.get("/clife").await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "db survived on {}",
            node.base
        );
    }
}

#[tokio::test]
async fn views_map_reduce_collation_and_query_options() {
    let s = Server::start(false);
    s.fresh_db("shop").await;

    let docs = json!({"docs": [
        {"_id": "a", "city": "Berlin", "type": "book", "price": 10},
        {"_id": "b", "city": "Berlin", "type": "book", "price": 20},
        {"_id": "c", "city": "Paris",  "type": "music", "price": 5},
        {"_id": "d", "city": "Paris",  "type": "book", "price": 15},
    ]});
    let (status, _) = s.post("/shop/_bulk_docs", docs).await;
    assert_eq!(status, StatusCode::CREATED);

    let map = "function(doc){ if(doc.city) emit(doc.city, doc.price); }";
    let ddoc = json!({"language": "javascript", "views": {
        "by_city": {"map": map},
        "count": {"map": "function(doc){ if(doc.city) emit(doc.city, null); }", "reduce": "_count"},
        "sum": {"map": map, "reduce": "_sum"},
        "stats": {"map": map, "reduce": "_stats"},
        "custom": {"map": map, "reduce": "function(keys, values, rereduce){ return sum(values) * 2; }"},
        "by_type_city": {"map": "function(doc){ emit([doc.type, doc.city], 1); }", "reduce": "_count"},
        "meta": {"map": "function(doc){ emit(doc._id, doc._rev); }"},
        "bad": {"map": "function(doc){ emit(; }"}
    }});
    let (status, body) = s.put("/shop/_design/shop", ddoc).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let ddoc_rev = body["rev"].as_str().unwrap().to_string();

    // Map rows are sorted by key then document id.
    let (status, body) = s.get("/shop/_design/shop/_view/by_city").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total_rows"], 4);
    assert_eq!(body["offset"], 0);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["a", "b", "c", "d"]);
    assert_eq!(body["rows"][0]["key"], "Berlin");
    assert_eq!(body["rows"][0]["value"], 10);

    // Map functions see `_id` and `_rev` even though bodies are stored without
    // them; the index build injects them without a JSON parse/encode round trip.
    let (_, body) = s.get("/shop/_design/shop/_view/meta?reduce=false").await;
    assert_eq!(body["total_rows"], 4);
    assert_eq!(body["rows"][0]["key"], "a");
    assert!(body["rows"][0]["value"].as_str().unwrap().starts_with("1-"));

    // Descending, limits and offsets follow CouchDB.
    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?descending=true&limit=2")
        .await;
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["d", "c"]);

    let (_, body) = s.get("/shop/_design/shop/_view/by_city?skip=2").await;
    assert_eq!(body["offset"], 2);
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);

    // startkey_docid breaks ties within a key.
    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?startkey=%22Berlin%22&startkey_docid=b")
        .await;
    assert_eq!(body["offset"], 1);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["b", "c", "d"]);

    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?key=%22Paris%22")
        .await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);

    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?startkey=%22Berlin%22&endkey=%22Paris%22&include_docs=true")
        .await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 4);
    assert_eq!(body["rows"][0]["doc"]["_id"], "a");

    // POST keys preserves the requested key order.
    let (_, body) = s
        .post(
            "/shop/_design/shop/_view/by_city",
            json!({"keys": ["Paris", "Berlin"]}),
        )
        .await;
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["c", "d", "a", "b"]);

    // Built-in reduces.
    let (_, body) = s.get("/shop/_design/shop/_view/count").await;
    assert_eq!(body["rows"][0]["key"], Value::Null);
    assert_eq!(body["rows"][0]["value"], 4);

    let (_, body) = s.get("/shop/_design/shop/_view/sum").await;
    assert_eq!(body["rows"][0]["value"], 50);

    let (_, body) = s.get("/shop/_design/shop/_view/stats").await;
    assert_eq!(body["rows"][0]["value"]["count"], 4);
    assert_eq!(body["rows"][0]["value"]["min"], 5);
    assert_eq!(body["rows"][0]["value"]["max"], 20);
    assert_eq!(body["rows"][0]["value"]["sum"], 50);
    assert_eq!(body["rows"][0]["value"]["sumsqr"], 750);

    let (_, body) = s.get("/shop/_design/shop/_view/custom").await;
    assert_eq!(body["rows"][0]["value"], 100);

    let (_, body) = s.get("/shop/_design/shop/_view/count?group=true").await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    assert_eq!(body["rows"][0]["key"], "Berlin");
    assert_eq!(body["rows"][0]["value"], 2);

    let (_, body) = s
        .get("/shop/_design/shop/_view/by_type_city?group_level=1")
        .await;
    assert_eq!(body["rows"][0]["key"], json!(["book"]));
    assert_eq!(body["rows"][0]["value"], 3);
    assert_eq!(body["rows"][1]["key"], json!(["music"]));

    // reduce=false and update_seq.
    let (_, body) = s
        .get("/shop/_design/shop/_view/count?reduce=false&limit=1")
        .await;
    assert_eq!(body["total_rows"], 4);
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["value"], Value::Null);

    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?update_seq=true&limit=0")
        .await;
    assert!(body["update_seq"].is_number() || body["update_seq"].is_string());

    // A map-only view rejects reduce=true, and a broken map is a view error.
    let (status, body) = s.get("/shop/_design/shop/_view/by_city?reduce=true").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "query_parse_error");

    let (status, body) = s.get("/shop/_design/shop/_view/bad").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"], "view_error");

    // Design document changes take effect immediately and deleted documents
    // leave the view.
    let ddoc = json!({
        "_rev": ddoc_rev,
        "views": {
            "by_city": {"map": "function(doc){ if(doc.city && doc.price > 10) emit(doc.city, doc.price); }"}
        }
    });
    let (status, _) = s.put("/shop/_design/shop", ddoc).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get("/shop/_design/shop/_view/by_city").await;
    assert_eq!(body["total_rows"], 2);

    let (_, doc) = s.get("/shop/d").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    let (status, _) = s
        .req(Method::DELETE, &format!("/shop/d?rev={rev}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = s.get("/shop/_design/shop/_view/by_city").await;
    assert_eq!(body["total_rows"], 1);

    // Only by_city still exists in the design document; cleanup removes the
    // index directories for the views that were deleted.
    let indexes = s.dir.join("shop").join("_view_indexes");
    let before = std::fs::read_dir(&indexes)
        .map(|entries| entries.flatten().count())
        .unwrap_or(0);
    assert!(before > 1, "expected several indexes, found {before}");
    let (status, _) = s.post("/shop/_view_cleanup", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let after = std::fs::read_dir(&indexes)
        .map(|entries| entries.flatten().count())
        .unwrap_or(0);
    assert_eq!(after, 1, "stale view indexes were not removed");
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[tokio::test]
async fn partitioned_database_documents_info_and_views() {
    let node = &start_cluster(1)[0];
    let (status, _) = node.put("/parted-test?partitioned=true", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, info) = node.get("/parted-test").await;
    assert_eq!(status, StatusCode::OK, "{info}");
    assert_eq!(info["props"]["partitioned"], true);

    let (status, _) = node.put("/parted-test/a:1", json!({"n": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = node.put("/parted-test/a:2", json!({"n": 2})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = node.put("/parted-test/b:1", json!({"n": 10})).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = node.put("/parted-test/no-colon", json!({"n": 3})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "illegal_docid");
    let (status, body) = node.put("/parted-test/:bad", json!({"n": 3})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "illegal_partition");
    let (status, body) = node.put("/parted-test/_bad:1", json!({"n": 3})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "illegal_partition");

    // Design documents are global and exempt from the partition:id rule.
    let (status, _) = node
        .put(
            "/parted-test/_design/p",
            json!({
                "views": {
                    "by_n": {"map": "function(doc){ emit(doc.n, null); }"},
                    "count": {
                        "map": "function(doc){ emit(doc.n, null); }",
                        "reduce": "_count"
                    }
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // POST cannot generate an id in a partitioned database; an explicit id works.
    let (status, body) = node.post("/parted-test", json!({"n": 4})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "illegal_docid");
    let (status, _) = node
        .post("/parted-test", json!({"_id": "c:1", "n": 20}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = node
        .post(
            "/parted-test/_bulk_docs",
            json!({"docs": [
                {"_id": "a:3", "n": 3},
                {"_id": "missing-partition", "n": 9}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let rows = body.as_array().unwrap();
    assert!(rows[0].get("ok").is_some(), "{body}");
    assert_eq!(rows[1]["error"], "illegal_docid", "{body}");

    let (status, info) = node.get("/parted-test/_partition/a").await;
    assert_eq!(status, StatusCode::OK, "{info}");
    assert_eq!(info["partition"], "a");
    assert_eq!(info["doc_count"], 3);
    assert_eq!(info["doc_del_count"], 0);

    // Delete one document; partition info reports the tombstone separately.
    let (_, doc) = node.get("/parted-test/a:2").await;
    let rev = doc["_rev"].as_str().unwrap();
    let (status, _) = node.delete(&format!("/parted-test/a:2?rev={rev}")).await;
    assert_eq!(status, StatusCode::OK);
    let (_, info) = node.get("/parted-test/_partition/a").await;
    assert_eq!(info["doc_count"], 2);
    assert_eq!(info["doc_del_count"], 1);

    let (status, body) = node.get("/parted-test/_partition/a/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 2);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["a:1", "a:3"]);

    let (status, body) = node
        .post(
            "/parted-test/_partition/a/_all_docs",
            json!({"keys": ["a:1", "b:1", "a:3"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["id"], "a:1");
    assert_eq!(body["rows"][1]["error"], "not_found");
    assert_eq!(body["rows"][2]["id"], "a:3");

    // A partitioned design doc is queried through the partition path only.
    let (status, body) = node
        .get("/parted-test/_partition/a/_design/p/_view/by_n")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 2);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["a:1", "a:3"]);

    let (status, body) = node
        .get("/parted-test/_partition/a/_design/p/_view/count")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 2);

    let (status, body) = node
        .get("/parted-test/_partition/a/_design/p/_view/by_n?include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["doc"]["n"], 1);

    let (status, body) = node.get("/parted-test/_design/p/_view/by_n").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["reason"].as_str().unwrap().contains("partition"),
        "{body}"
    );

    // A global design doc works across partitions and rejects partition paths.
    let (status, _) = node
        .put(
            "/parted-test/_design/g",
            json!({
                "options": {"partitioned": false},
                "views": {"all": {"map": "function(doc){ emit(doc.n, null); }"}}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = node.get("/parted-test/_design/g/_view/all").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4);
    let (status, body) = node
        .get("/parted-test/_partition/a/_design/g/_view/all")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["reason"].as_str().unwrap().contains("partition"),
        "{body}"
    );
}

#[tokio::test]
async fn cluster_partitioned_database_propagates_and_isolates_partitions() {
    let mut nodes = start_cluster_with(3, "3600", 3);
    let (status, _) = nodes[0].put("/clpart?partitioned=true", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);

    // Creation propagates the partitioned flag to every peer.
    for node in &nodes {
        let (status, info) = node.get("/clpart").await;
        assert_eq!(status, StatusCode::OK, "{info}");
        assert_eq!(info["props"]["partitioned"], true, "{info}");
    }

    let docs: Vec<Value> = (0..24)
        .map(|i| {
            let partition = if i % 2 == 0 { "even" } else { "odd" };
            json!({"_id": format!("{partition}:doc{i}"), "n": i, "partition": partition})
        })
        .collect();
    let (status, _) = nodes[0]
        .post("/clpart/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    for node in &nodes {
        let (status, body) = node.get("/clpart/_partition/even/_all_docs").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 12);
        assert!(body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["id"].as_str().unwrap().starts_with("even:")));
        let (status, info) = node.get("/clpart/_partition/even").await;
        assert_eq!(status, StatusCode::OK, "{info}");
        assert_eq!(info["doc_count"], 12);
    }

    // All documents in a partition hash to one shard. Query each shard mask
    // directly and verify partition documents appear in exactly one mask.
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let mut even_shards = std::collections::HashSet::new();
    for shard in 0..4 {
        let (status, body) = nodes[0]
            .req_with(
                Method::GET,
                &format!("/clpart/_all_docs?shards={shard}"),
                None,
                &internal,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        for row in body["rows"].as_array().unwrap() {
            if row["id"].as_str().unwrap().starts_with("even:") {
                even_shards.insert(shard);
            }
        }
    }
    assert_eq!(
        even_shards.len(),
        1,
        "partition spans shards: {even_shards:?}"
    );

    // Partition views are complete from every coordinator.
    let (status, _) = nodes[1]
        .put(
            "/clpart/_design/p",
            json!({
                "views": {
                    "by_n": {"map": "function(doc){ if (doc.n % 2 === 0) emit(doc.n, null); }"},
                    "count": {
                        "map": "function(doc){ if (doc.n % 2 === 0) emit(doc.n, null); }",
                        "reduce": "_count"
                    }
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = nodes[2]
        .get("/clpart/_partition/even/_design/p/_view/by_n")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 12);
    let (status, body) = nodes[2]
        .get("/clpart/_partition/even/_design/p/_view/count")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 12);
    let (status, body) = nodes[2].get("/clpart/_design/p/_view/by_n").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // Partitioned reduce and map queries fall back to replicas after node loss.
    nodes[2].kill();
    let (status, body) = nodes[0]
        .get("/clpart/_partition/even/_design/p/_view/count")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 12);
    let (status, body) = nodes[0]
        .get("/clpart/_partition/even/_design/p/_view/by_n")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 12);
}

#[tokio::test]
async fn cluster_partitioned_reads_route_to_the_owner_with_one_replica() {
    let nodes = start_cluster_with(3, "3600", 1);
    let (status, _) = nodes[0].put("/clpart1?partitioned=true", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    let docs: Vec<Value> = (0..12)
        .map(|i| json!({"_id": format!("p:doc{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[1]
        .post("/clpart1/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[1]
        .put(
            "/clpart1/_design/p",
            json!({
                "views": {"by_n": {"map": "function(doc){ emit(doc.n, null); }"}}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Each coordinator may or may not own p's shard. Partition metadata and
    // reads must be forwarded to a replica when it does not.
    for node in &nodes {
        let (status, body) = node.get("/clpart1/_partition/p").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["doc_count"], 12, "{body}");
        let (status, body) = node.get("/clpart1/_partition/p/_all_docs").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 12);
        assert_eq!(body["rows"].as_array().unwrap().len(), 12);
        let (status, body) = node.get("/clpart1/_partition/p/_design/p/_view/by_n").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 12);
    }
}

#[tokio::test]
async fn partitioned_all_docs_ranges_keys_and_nested_ids() {
    let node = &start_cluster(1)[0];
    let (status, _) = node.put("/prange?partitioned=true", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    let docs: Vec<Value> = ["p:a", "p:b", "p:c", "p:d:e", "q:x"]
        .iter()
        .map(|id| json!({"_id": id}))
        .collect();
    let (status, _) = node.post("/prange/_bulk_docs", json!({"docs": docs})).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = node.get("/prange/_partition/p/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["p:a", "p:b", "p:c", "p:d:e"]);

    let (status, body) = node
        .get("/prange/_partition/p/_all_docs?startkey=%22p:b%22")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4);
    assert_eq!(body["offset"], 1);
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);

    let (status, body) = node
        .get("/prange/_partition/p/_all_docs?endkey=%22p:c%22")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);

    let (status, body) = node
        .get("/prange/_partition/p/_all_docs?descending=true&limit=2")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["p:d:e", "p:c"]);

    let (status, body) = node
        .post(
            "/prange/_partition/p/_all_docs",
            json!({"keys": ["p:a", "q:x", "p:d:e"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4);
    assert_eq!(body["rows"][0]["id"], "p:a");
    assert_eq!(body["rows"][1]["error"], "not_found");
    assert_eq!(body["rows"][2]["id"], "p:d:e");

    // Global _all_docs remains available in a partitioned database.
    let (status, body) = node.get("/prange/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 5);

    // Non-partitioned databases reject partition endpoints and omit props.
    let (status, _) = node.put("/plain", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, info) = node.get("/plain").await;
    assert_eq!(status, StatusCode::OK);
    assert!(info.get("props").is_none(), "{info}");
    let (status, body) = node.get("/plain/_partition/p/_all_docs").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["reason"].as_str().unwrap().contains("partition"),
        "{body}"
    );
}

#[tokio::test]
async fn changes_style_all_docs_includes_conflict_leaves() {
    let s = Server::start(false);
    s.fresh_db("changesconf").await;
    let root = "00000000000000000000000000000000";
    let (status, _) = s
        .post(
            "/changesconf/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "x", "_rev": "2-b", "_revisions": {"start": 2, "ids": ["b", root]}, "v": "b"},
                {"_id": "x", "_rev": "2-c", "_revisions": {"start": 2, "ids": ["c", root]}, "v": "c"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s.get("/changesconf/_changes").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let default_changes = body["results"][0]["changes"].as_array().unwrap();
    assert_eq!(default_changes.len(), 1, "default style is main_only");
    assert_eq!(default_changes[0]["rev"], "2-c");

    let (status, body) = s.get("/changesconf/_changes?style=all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let leaves: Vec<&str> = body["results"][0]["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|change| change["rev"].as_str().unwrap())
        .collect();
    assert_eq!(leaves, ["2-b", "2-c"]);

    let (status, body) = s
        .get("/changesconf/_changes?style=all_docs&include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["doc"]["v"], "c");
}

// ---------------------------------------------------------------------------
// Ports from Apache CouchDB's Elixir integration suite.

#[tokio::test]
async fn couchdb_basics_ports() {
    let s = Server::start(false);
    let (status, body) = s.get("/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["couchdb"], "Welcome");

    let name = "couchbasics";
    let (status, headers, _) = s
        .req_headers(Method::PUT, &format!("/{name}"), Some(json!({})), &[])
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(headers
        .get("location")
        .is_some_and(|value| value.ends_with(name)));
    let (status, _) = s.put(&format!("/{name}"), json!({})).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED);

    let (status, info) = s.get(&format!("/{name}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(info["db_name"], name);
    assert_eq!(info["doc_count"], 0);

    let (status, headers, body) = s
        .req_headers(
            Method::PUT,
            &format!("/{name}/doc0"),
            Some(json!({"a": 1})),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let etag = headers.get("etag").cloned().expect("etag on creation");
    assert_eq!(etag, format!("\"{}\"", body["rev"].as_str().unwrap()));

    let (status, headers, _) = s
        .req_headers(
            Method::GET,
            &format!("/{name}/doc0"),
            None,
            &[("if-none-match", etag.clone())],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert_eq!(headers.get("etag"), Some(&etag));

    let (status, _, body) = s
        .req_headers(Method::HEAD, &format!("/{name}/doc0"), None, &[])
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers.get("etag"), Some(&etag));
    assert_eq!(body, Value::Null);

    let (status, body) = s
        .get(&format!("/{name}/doc0?revs_info=true&local_seq=true"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_revs_info"][0]["status"], "available");
    assert_eq!(body["_local_seq"], 1);

    let (_, dbs) = s.get("/_all_dbs?start_key=%22couchbasics%22&limit=1").await;
    assert!(dbs.as_array().unwrap().iter().any(|db| db == name));
    let (_, dbs) = s.get("/_all_dbs?limit=0").await;
    assert_eq!(dbs, json!([]));
}

#[tokio::test]
async fn couchdb_all_docs_ports() {
    let s = Server::start(false);
    s.fresh_db("couchalldocs").await;
    let docs: Vec<Value> = (0..4)
        .map(|i| json!({"_id": i.to_string(), "a": i + 1, "b": (i + 1) * (i + 1)}))
        .collect();
    let (status, _) = s
        .post("/couchalldocs/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s.get("/couchalldocs/_all_docs?key=%221%22").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["id"], "1");

    let (status, body) = s
        .get("/couchalldocs/_all_docs?keys=%5B%220%22,%222%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);

    let (status, body) = s.post("/couchalldocs/_all_docs", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"].as_array().unwrap().len(), 4);

    let (status, body) = s
        .post(
            "/couchalldocs/_all_docs",
            json!({"keys": ["1", "2"], "limit": 1}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);

    let (status, body) = s
        .post(
            "/couchalldocs/_all_docs?limit=1",
            json!({"keys": ["1", "2"], "limit": 2}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);

    let (status, body) = s
        .post(
            "/couchalldocs/_all_docs?include_docs=true",
            json!({"keys": ["1"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["doc"]["a"], 2);

    let (status, body) = s.get("/couchalldocs/_all_docs?start_key=%221%22").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 1);
}

#[tokio::test]
async fn couchdb_invalid_docid_and_bulk_error_ports() {
    let s = Server::start(false);
    s.fresh_db("couchinvalid").await;

    for path in ["/_other", "/_tmp_view"] {
        let (status, body) = s.put(&format!("/couchinvalid{path}"), json!({})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["error"], "illegal_docid");
    }

    let (status, body) = s.post("/couchinvalid", json!({"_id": 1})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "illegal_docid");
    assert_eq!(body["reason"], "Document id must be a string");

    let (status, body) = s.post("/couchinvalid", json!({"_id": "_invalid"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "illegal_docid");

    let (status, body) = s
        .post(
            "/couchinvalid/_bulk_docs",
            json!({"docs": [{"_id": "_invalid"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "illegal_docid");

    let (status, body) = s
        .post(
            "/couchinvalid/_bulk_docs",
            json!({"docs": [], "new_edits": 0}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "bad_request");

    let (status, body) = s
        .post(
            "/couchinvalid/_bulk_docs",
            json!({"docs": [{"_id": "ok"}], "all_or_nothing": true}),
        )
        .await;
    assert_eq!(status, StatusCode::EXPECTATION_FAILED, "{body}");
    assert_eq!(body[0]["error"], "not_implemented");
    assert_eq!(body[0]["reason"], "all_or_nothing is not supported");
}

#[tokio::test]
async fn couchdb_bulk_docs_ports() {
    let s = Server::start(false);
    s.fresh_db("couchbulk").await;
    let docs: Vec<Value> = (1..=5)
        .map(|i| json!({"_id": i.to_string(), "string": format!("doc{i}")}))
        .collect();
    let (status, rows) = s.post("/couchbulk/_bulk_docs", json!({"docs": docs})).await;
    assert_eq!(status, StatusCode::CREATED);
    let rows = rows.as_array().unwrap();
    assert!(rows
        .iter()
        .all(|row| row["rev"].as_str().unwrap().starts_with("1-")));

    let mut updated = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        let id = (index + 1).to_string();
        let (_, doc) = s.get(&format!("/couchbulk/{id}")).await;
        doc.as_object()
            .unwrap()
            .clone()
            .into_iter()
            .for_each(|_| {});
        updated.push(json!({
            "_id": id,
            "_rev": row["rev"],
            "string": format!("doc{}.00", index + 1)
        }));
    }
    let (status, rows) = s
        .post("/couchbulk/_bulk_docs", json!({"docs": updated}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(rows
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["rev"].as_str().unwrap().starts_with("2-")));

    let (_, one) = s.get("/couchbulk/1").await;
    assert_eq!(one["string"], "doc1.00");

    // Create a conflict on doc 1, then bulk-delete everything using old revs.
    let (status, _) = s
        .put(
            "/couchbulk/1",
            json!({"_id": "1", "_rev": one["_rev"], "string": "newer"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let mut deletes = Vec::new();
    for row in rows.as_array().unwrap() {
        deletes.push(json!({
            "_id": row["id"],
            "_rev": row["rev"],
            "_deleted": true
        }));
    }
    let (status, rows) = s
        .post("/couchbulk/_bulk_docs", json!({"docs": deletes}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let rows = rows.as_array().unwrap();
    assert!(rows.iter().any(|row| row["error"] == "conflict"));
    assert!(rows.iter().any(|row| row["ok"] == true));

    let (status, body) = s.get("/couchbulk/2").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["reason"], "deleted");
}

#[tokio::test]
async fn couchdb_changes_ports() {
    let s = Server::start(false);
    s.fresh_db("couchchanges").await;
    let (status, _) = s
        .post(
            "/couchchanges/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "n": 1},
                {"_id": "b", "n": 2},
                {"_id": "c", "n": 3}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .post(
            "/couchchanges/_changes?filter=_doc_ids",
            json!({"doc_ids": ["a", "c"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"a"));
    assert!(ids.contains(&"c"));

    let (status, body) = s.get("/couchchanges/_changes?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["results"].as_array().unwrap().len(), 1);

    let (status, body) = s
        .get("/couchchanges/_changes?since=0&include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["results"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| row["doc"].is_object()));
}

#[tokio::test]
async fn couchdb_view_pagination_and_alias_ports() {
    let s = Server::start(false);
    s.fresh_db("couchviewpage").await;
    let docs: Vec<Value> = (0..100)
        .map(|i| json!({"_id": i.to_string(), "integer": i}))
        .collect();
    let (status, _) = s
        .post("/couchviewpage/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/couchviewpage/_design/test",
            json!({"views": {"by_int": {"map": "function(doc){ emit(doc.integer, null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .get("/couchviewpage/_design/test/_view/by_int?startkey=30&startkey_docid=30&limit=10")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 100);
    assert_eq!(body["offset"], 30);
    assert_eq!(body["rows"].as_array().unwrap().len(), 10);
    assert_eq!(body["rows"][0]["key"], 30);

    let (status, body) = s
        .get("/couchviewpage/_design/test/_view/by_int?start_key=30&start_key_docid=30&limit=10")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 30);
    assert_eq!(body["rows"][0]["key"], 30);

    let (status, body) = s
        .get("/couchviewpage/_design/test/_view/by_int?startkey=70&startkey_docid=70&limit=10&descending=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 29);
    assert_eq!(body["rows"][0]["key"], 70);
}

#[tokio::test]
async fn couchdb_builtin_reduce_ports() {
    let s = Server::start(false);
    s.fresh_db("couchreduce").await;
    let docs: Vec<Value> = (1..=4)
        .map(|i| json!({"_id": i.to_string(), "value": i}))
        .collect();
    let (status, _) = s
        .post("/couchreduce/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/couchreduce/_design/r",
            json!({"views": {
                "count": {"map": "function(doc){ emit(null, doc.value); }", "reduce": "_count"},
                "sum": {"map": "function(doc){ emit(null, doc.value); }", "reduce": "_sum"},
                "stats": {"map": "function(doc){ emit(null, doc.value); }", "reduce": "_stats"},
                "distinct": {"map": "function(doc){ emit(null, doc.value); }", "reduce": "_approx_count_distinct"}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (_, body) = s.get("/couchreduce/_design/r/_view/count").await;
    assert_eq!(body["rows"][0]["value"], 4);
    let (_, body) = s.get("/couchreduce/_design/r/_view/sum").await;
    assert_eq!(body["rows"][0]["value"], 10);
    let (_, body) = s.get("/couchreduce/_design/r/_view/stats").await;
    assert_eq!(body["rows"][0]["value"]["count"], 4);
    assert_eq!(body["rows"][0]["value"]["sum"], 10);
    let (_, body) = s.get("/couchreduce/_design/r/_view/distinct").await;
    assert_eq!(body["rows"][0]["value"], 4);
}

#[tokio::test]
async fn couchdb_view_include_docs_and_utf8_ports() {
    let s = Server::start(false);
    s.fresh_db("couchviewdocs").await;
    let (status, _) = s
        .post(
            "/couchviewdocs/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "city": "Zürich", "price": 10},
                {"_id": "b", "city": "Paris", "price": 20}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/couchviewdocs/_design/v",
            json!({"views": {"by_city": {"map": "function(doc){ emit(doc.city, doc.price); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .get("/couchviewdocs/_design/v/_view/by_city?include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["doc"]["city"], "Paris");
    assert_eq!(rows[1]["doc"]["city"], "Zürich");

    let (status, body) = s
        .get("/couchviewdocs/_design/v/_view/by_city?reduce=false")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn couchdb_view_collation_ports() {
    let s = Server::start(false);
    s.fresh_db("couchcollation").await;
    let values = vec![
        Value::Null,
        json!(false),
        json!(true),
        json!(1),
        json!(2),
        json!(3.0),
        json!(4),
        json!("a"),
        json!("A"),
        json!("aa"),
        json!("b"),
        json!("B"),
        json!("ba"),
        json!("bb"),
        json!(["a"]),
        json!(["b"]),
        json!(["b", "c"]),
        json!(["b", "c", "a"]),
        json!(["b", "d"]),
        json!(["b", "d", "e"]),
        json!({"a": 1}),
        json!({"a": 2}),
        json!({"b": 1}),
        json!({"b": 2}),
        json!({"b": 2, "a": 1}),
        json!({"b": 2, "c": 2}),
    ];
    let docs: Vec<Value> = values
        .iter()
        .enumerate()
        .map(|(i, value)| json!({"_id": i.to_string(), "foo": value}))
        .collect();
    let (status, _) = s
        .post("/couchcollation/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/couchcollation/_design/foo",
            json!({"views": {"foo": {"map": "function(doc){ emit(doc.foo, null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s.get("/couchcollation/_design/foo/_view/foo").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let keys: Vec<Value> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["key"].clone())
        .collect();
    assert_eq!(keys, values);

    let (status, body) = s
        .get("/couchcollation/_design/foo/_view/foo?descending=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut reversed = values.clone();
    reversed.reverse();
    let keys: Vec<Value> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["key"].clone())
        .collect();
    assert_eq!(keys, reversed);

    for (query, expected) in [
        ("key=null", Value::Null),
        ("key=%22b%22", json!("b")),
        ("key=%5B%22b%22,%22c%22%5D", json!(["b", "c"])),
    ] {
        let (status, body) = s
            .get(&format!("/couchcollation/_design/foo/_view/foo?{query}"))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["rows"].as_array().unwrap().len(), 1, "{body}");
        assert_eq!(body["rows"][0]["key"], expected);
    }

    let (status, body) = s
        .get("/couchcollation/_design/foo/_view/foo?endkey=%22b%22&inclusive_end=false")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"].as_array().unwrap().last().unwrap()["key"],
        "aa"
    );

    let (status, body) = s
        .get("/couchcollation/_design/foo/_view/foo?endkey=%22b%22&inclusive_end=false&descending=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().last().unwrap()["key"], "B");
}

fn search_ddoc() -> Value {
    json!({
        "indexes": {
            "fruits": {
                "analyzer": {"name": "standard"},
                "index": "function(doc) {\n  index(\"item\", doc.item, {facet: true});\n  index(\"place\", doc.place, {facet: true});\n  index(\"state\", doc.state, {facet: true});\n  index(\"price\", doc.price, {facet: true});\n}"
            }
        }
    })
}

async fn seed_search_docs(s: &Server, db: &str) {
    s.fresh_db(db).await;
    let (status, _) = s
        .post(
            &format!("/{db}/_bulk_docs"),
            json!({"docs": [
                {"_id": "apple", "item": "apple", "place": "kitchen", "state": "new", "price": 0.99},
                {"_id": "banana", "item": "banana", "place": "kitchen", "state": "new", "price": 1.49},
                {"_id": "carrot", "item": "carrot", "place": "kitchen", "state": "old", "price": 0.75},
                {"_id": "date", "item": "date", "place": "lobby", "state": "unknown", "price": 1.25}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(&format!("/{db}/_design/inventory"), search_ddoc())
        .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn couchdb_search_ports() {
    let s = Server::start(false);
    seed_search_docs(&s, "couchsearch").await;

    let (status, body) = s
        .get("/couchsearch/_design/inventory/_search/fruits?q=*:*&include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut items: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["doc"]["item"].as_str().unwrap())
        .collect();
    items.sort_unstable();
    assert_eq!(items, ["apple", "banana", "carrot", "date"]);

    let (status, body) = s
        .get("/couchsearch/_design/inventory/_search/fruits?q=item:apple&include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["doc"]["item"], "apple");

    let (status, body) = s
        .post(
            "/couchsearch/_design/inventory/_search/fruits",
            json!({"q": "*:*", "drilldown": ["place", "kitchen"], "include_docs": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut items: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["doc"]["item"].as_str().unwrap())
        .collect();
    items.sort_unstable();
    assert_eq!(items, ["apple", "banana", "carrot"]);

    let (status, body) = s
        .post(
            "/couchsearch/_design/inventory/_search/fruits",
            json!({"q": "*:*", "limit": 0, "counts": ["place"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["counts"]["place"]["kitchen"], 3);
    assert_eq!(body["counts"]["place"]["lobby"], 1);

    let (status, body) = s
        .post(
            "/couchsearch/_design/inventory/_search/fruits",
            json!({"q": "*:*", "limit": 0, "ranges": {"price": {
                "cheap": "[0 TO 0.99]",
                "expensive": "[1.00 TO Infinity]"
            }}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["ranges"]["price"]["cheap"], 2);
    assert_eq!(body["ranges"]["price"]["expensive"], 2);

    let (status, body) = s
        .post(
            "/couchsearch/_design/inventory/_search/fruits",
            json!({"q": "*:*", "group_field": "state"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 3);
    for group in groups {
        let expected = match group["by"].as_str().unwrap() {
            "new" => 2,
            "old" | "unknown" => 1,
            other => panic!("unexpected group {other}"),
        };
        assert_eq!(group["total_rows"], expected);
    }

    let (status, body) = s
        .get("/couchsearch/_design/inventory/_search_info/fruits")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["search_index"]["doc_count"], 4);
    assert!(!body["search_index"]["signature"]
        .as_str()
        .unwrap()
        .is_empty());

    let (status, body) = s
        .post(
            "/couchsearch/_design/inventory/_search/fruits",
            json!({"q": "*:*", "drilldown": [["state", "old"], ["item", "apple"]], "include_docs": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["rows"].as_array().unwrap().is_empty());

    let (status, body) = s
        .post(
            "/couchsearch/_design/inventory/_search/fruits",
            json!({"q": "*:*", "drilldown": [["state", "old", "new"], ["item", "apple"]], "include_docs": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["doc"]["item"], "apple");

    let (status, body) = s
        .get("/couchsearch/_design/inventory/_search/fruits?q=*:*&drilldown=%5B%22state%22,%22old%22%5D&drilldown=%5B%22item%22,%22apple%22%5D&include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["rows"].as_array().unwrap().is_empty());

    let (_, apple) = s.get("/couchsearch/apple").await;
    let (status, _) = s
        .post(
            "/couchsearch/_bulk_docs",
            json!({"docs": [{"_id": "apple", "_rev": apple["_rev"], "item": "apple2", "place": "kitchen", "state": "new", "price": 0.99}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s
        .get("/couchsearch/_design/inventory/_search/fruits?q=item:apple2&include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["doc"]["item"], "apple2");

    let (status, body) = s
        .post(
            "/couchsearch/_design/inventory/_search/fruits",
            json!({"q": "item:tomato", "limit": 0, "counts": ["place"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["counts"]["place"], json!({}));

    let (status, body) = s
        .post(
            "/couchsearch/_design/inventory/_search/fruits",
            json!({"q": "*:*", "limit": 0, "ranges": {"price": {}}}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["ranges"]["price"], json!({}));

    // Duplicate JSON members: the last drilldown wins, matching PouchDB.
    let raw = r#"{"include_docs":true,"q":"*:*","drilldown":["state","old"],"drilldown":["item","banana"]}"#;
    let response = s
        .client
        .post(format!(
            "{}/couchsearch/_design/inventory/_search/fruits",
            s.base
        ))
        .header("content-type", "application/json")
        .body(raw)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["rows"][0]["doc"]["item"], "banana");

    let (status, body) = s
        .get("/couchsearch/_design/inventory/_search_info/fruits")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let pending = body["search_index"]["updates_pending"].as_object().unwrap();
    let mut keys: Vec<&str> = pending.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "copies",
            "copies_expected",
            "maximum",
            "minimum",
            "preferred",
            "total"
        ]
    );

    let (status, _) = s.post("/couchsearch/_search_cleanup", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
}

#[tokio::test]
async fn couchdb_partition_search_ports() {
    let s = Server::start(false);
    let (status, _) = s.put("/couchpsearch?partitioned=true", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    let docs: Vec<Value> = (1..=10)
        .map(|i| {
            let partition = if i % 2 == 0 { "foo" } else { "bar" };
            json!({"_id": format!("{partition}:{i}"), "some": "field", "value": i})
        })
        .collect();
    let (status, _) = s
        .post("/couchpsearch/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/couchpsearch/_design/library",
            json!({"indexes": {"books": {
                "analyzer": {"name": "standard"},
                "index": "function(doc) { if (doc.some) { index('some', doc.some); } }"
            }}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .get("/couchpsearch/_partition/foo/_design/library/_search/books?q=some:field")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["foo:10", "foo:2", "foo:4", "foo:6", "foo:8"]);

    let (status, body) = s
        .get("/couchpsearch/_partition/foo/_design/library/_search/books?q=some:field&limit=3")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let bookmark = body["bookmark"].as_str().unwrap().to_string();
    let (status, body) = s
        .get(&format!(
            "/couchpsearch/_partition/foo/_design/library/_search/books?q=some:field&limit=3&bookmark={bookmark}"
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["foo:6", "foo:8"]);

    let (status, _) = s
        .get("/couchpsearch/_partition/foo/_design/library/_search/books?q=some:field&limit=2001")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn pouchdb_local_docs_revs_diff_and_bulk_get_ports() {
    let s = Server::start(false);
    s.fresh_db("pouchports").await;

    let (status, body) = s.put("/pouchports/_local/foo", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["rev"], "0-1");
    let (status, body) = s.get("/pouchports/_local/foo").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_id"], "_local/foo");
    assert_eq!(body["_rev"], "0-1");
    assert!(body.get("_revisions").is_none());

    let (_, body) = s.get("/pouchports/_all_docs").await;
    assert_eq!(body["total_rows"], 0);
    let (_, body) = s.get("/pouchports/_changes").await;
    assert!(body["results"].as_array().unwrap().is_empty());

    let (status, body) = s.delete("/pouchports/_local/foo?rev=0-1").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rev"], "0-0");
    let (status, _) = s.get("/pouchports/_local/foo").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = s.put("/pouchports/_local/foo", json!({"v": 2})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["rev"], "0-1");

    // revs_diff: known leaves are not missing; unknown revisions are.
    let (status, first) = s.put("/pouchports/somestuff", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let rev1 = first["rev"].as_str().unwrap().to_string();
    let (status, second) = s
        .put("/pouchports/somestuff", json!({"_rev": rev1, "v": 2}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let rev2 = second["rev"].as_str().unwrap().to_string();
    let (status, body) = s
        .post("/pouchports/_revs_diff", json!({"somestuff": [rev1, rev2]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("somestuff").is_none());
    let missing = "2-00000000000000000000000000000000";
    let (status, body) = s
        .post(
            "/pouchports/_revs_diff",
            json!({"somestuff": [rev1, rev2, missing], "ghost": ["1-a", "2-a"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["somestuff"]["missing"][0], missing);
    assert_eq!(body["ghost"]["missing"], json!(["1-a", "2-a"]));

    // bulk_get: revisions are opt-in.
    let (status, body) = s
        .post(
            "/pouchports/_bulk_get",
            json!({"docs": [{"id": "somestuff"}, {"id": "ghost"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["docs"][0]["ok"]["_id"], "somestuff");
    assert!(body["results"][0]["docs"][0]["ok"]
        .get("_revisions")
        .is_none());
    assert!(body["results"][1]["docs"][0].get("error").is_some());

    let (status, body) = s
        .post(
            "/pouchports/_bulk_get?revs=true",
            json!({"docs": [{"id": "somestuff", "rev": rev2}]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["results"][0]["docs"][0]["ok"]["_revisions"]["ids"][0],
        rev2.split_once('-').unwrap().1
    );
}

#[tokio::test]
async fn pouchdb_changes_and_all_docs_ports() {
    let s = Server::start(false);
    s.fresh_db("pouchchanges").await;

    let (status, body) = s.get("/pouchchanges/_changes?since=now").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["results"].as_array().unwrap().is_empty());
    let since = body["last_seq"].to_string();

    let (status, _) = s.put("/pouchchanges/one", json!({"n": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s
        .get(&format!(
            "/pouchchanges/_changes?since={since}&include_docs=true"
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().unwrap().len(), 1);
    assert_eq!(body["results"][0]["doc"]["n"], 1);

    let (status, body) = s.get("/pouchchanges/_changes?limit=0").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["results"].as_array().unwrap().is_empty());

    let (status, _) = s.put("/pouchchanges/a", json!({"n": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s.put("/pouchchanges/c", json!({"n": 3})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s.get("/pouchchanges/_all_docs?descending=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["one", "c", "a"]);

    let (status, body) = s
        .get("/pouchchanges/_all_docs?include_docs=true&skip=1&limit=1")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["id"], "c");
    assert_eq!(body["rows"][0]["doc"]["n"], 3);
}

#[tokio::test]
async fn pouchdb_mapreduce_conflicts_and_uuids_ports() {
    let s = Server::start(false);
    s.fresh_db("pouchviews").await;
    let (status, _) = s
        .post(
            "/pouchviews/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "key": "a", "value": 1},
                {"_id": "b", "key": "b", "value": 2},
                {"_id": "c", "key": "c", "value": 3}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/pouchviews/_design/v",
            json!({"views": {"by_key": {"map": "function(doc){ emit(doc.key); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .get("/pouchviews/_design/v/_view/by_key?start_key=%22b%22&end_key=%22c%22")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["key"], "b");
    assert_eq!(rows[0]["value"], Value::Null);

    // PouchDB conflict/leaf behavior.
    let root = "00000000000000000000000000000000";
    let (status, _) = s
        .post(
            "/pouchviews/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "x", "_rev": "2-b", "_revisions": {"start": 2, "ids": ["b", root]}},
                {"_id": "x", "_rev": "2-c", "_revisions": {"start": 2, "ids": ["c", root]}}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s.get("/pouchviews/x?conflicts=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let conflicts = body["_conflicts"].as_array().unwrap();
    assert_eq!(conflicts.len(), 1);
    let (status, body) = s.get("/pouchviews/_changes?style=all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let row = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "x")
        .unwrap();
    assert_eq!(row["changes"].as_array().unwrap().len(), 2);

    let (status, body) = s.get("/_uuids?count=4").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let uuids = body["uuids"].as_array().unwrap();
    assert_eq!(uuids.len(), 4);
    let unique: std::collections::HashSet<&str> =
        uuids.iter().map(|uuid| uuid.as_str().unwrap()).collect();
    assert_eq!(unique.len(), 4);
}

#[tokio::test]
async fn pouchdb_reserved_ids_ports() {
    let s = Server::start(false);
    s.fresh_db("pouchreserved").await;
    let (status, _) = s
        .post(
            "/pouchreserved/_bulk_docs",
            json!({"docs": [
                {"_id": "constructor"},
                {"_id": "toString"},
                {"_id": "valueOf"},
                {"_id": "_design/all", "views": {"all": {
                    "map": "function(doc) { emit(doc._id); }"
                }}}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .get("/pouchreserved/_all_docs?key=%22constructor%22")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    let (status, body) = s
        .get("/pouchreserved/_all_docs?keys=%5B%22constructor%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    let (status, body) = s.get("/pouchreserved/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4);

    let (status, body) = s
        .get("/pouchreserved/_design/all/_view/all?key=%22constructor%22")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    let (status, body) = s
        .get("/pouchreserved/_design/all/_view/all?keys=%5B%22constructor%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn pouchdb_get_and_error_ports() {
    let s = Server::start(false);
    s.fresh_db("pouchget").await;
    let (status, info) = s.get("/pouchget").await;
    assert_eq!(status, StatusCode::OK);
    assert!(info["uuid"].as_str().is_some());
    let (status, created) = s.put("/pouchget/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s.get("/pouchget/missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], "not_found");
    assert_eq!(body["reason"], "missing");

    let rev = created["rev"].as_str().unwrap().to_string();
    let (status, _) = s.delete(&format!("/pouchget/doc?rev={rev}")).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = s.get("/pouchget/doc").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["reason"], "deleted");
    let (status, body) = s.get(&format!("/pouchget/doc?rev={rev}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_rev"], rev);

    let (status, created) = s.put("/pouchget/doc2", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let bad_rev = format!("{}broken", created["rev"].as_str().unwrap());
    let (status, _) = s
        .put("/pouchget/doc2", json!({"_rev": bad_rev, "v": 2}))
        .await;
    assert!(
        status == StatusCode::CONFLICT || status == StatusCode::BAD_REQUEST,
        "invalid revision must be rejected: {status}"
    );
}

#[tokio::test]
async fn couchdb_conflict_request_ports() {
    let s = Server::start(false);
    s.fresh_db("couchconflict").await;
    let (status, created) = s.put("/couchconflict/doc-1", json!({"a": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let rev = created["rev"].as_str().unwrap().to_string();

    let (status, body) = s
        .put(
            &format!("/couchconflict/doc-1?rev=1-foobar"),
            json!({"_rev": rev, "a": 2}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body["reason"]
        .as_str()
        .unwrap()
        .contains("body and query string"));

    let (status, _, body) = s
        .req_headers(
            Method::PUT,
            "/couchconflict/doc-1",
            Some(json!({"_rev": rev, "a": 2})),
            &[("if-match", "1-foobar".to_string())],
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body["reason"]
        .as_str()
        .unwrap()
        .contains("Document rev and etag"));

    // Same revision twice: the second write conflicts.
    let (status, _) = s
        .put("/couchconflict/doc-1", json!({"_rev": rev, "a": 2}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s
        .put("/couchconflict/doc-1", json!({"_rev": rev, "a": 3}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "conflict");
}

#[tokio::test]
async fn pouchdb_all_docs_keys_ports() {
    let s = Server::start(false);
    s.fresh_db("pouchkeys").await;
    let docs = json!({"docs": [
        {"_id": "0", "a": 1, "b": 1},
        {"_id": "3", "a": 4, "b": 16},
        {"_id": "1", "a": 2, "b": 4},
        {"_id": "2", "a": 3, "b": 9}
    ]});
    let (status, _) = s.post("/pouchkeys/_bulk_docs", docs).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .post(
            "/pouchkeys/_all_docs",
            json!({"keys": ["2", "0", "1000"], "descending": true}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows[0]["key"], "1000");
    assert_eq!(rows[0]["error"], "not_found");
    assert_eq!(rows[1]["key"], "0");
    assert_eq!(rows[2]["key"], "2");

    let (status, body) = s
        .post(
            "/pouchkeys/_all_docs",
            json!({"keys": ["3", "1"], "skip": 1}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4);
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["id"], "1");

    let (status, body) = s
        .post(
            "/pouchkeys/_all_docs",
            json!({"keys": ["0", "2"], "limit": 3}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);

    let (status, body) = s.post("/pouchkeys/_all_docs", json!({"keys": 1234})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body) = s
        .post(
            "/pouchkeys/_all_docs?startkey=%22a%22",
            json!({"keys": ["0"]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (_, doc) = s.get("/pouchkeys/2").await;
    let (status, _) = s
        .delete(&format!(
            "/pouchkeys/2?rev={}",
            doc["_rev"].as_str().unwrap()
        ))
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = s
        .post(
            "/pouchkeys/_all_docs?include_docs=true",
            json!({"keys": ["2"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"]["deleted"], true);
    assert_eq!(body["rows"][0]["doc"], Value::Null);
}

#[tokio::test]
async fn pouchdb_bulk_docs_and_design_docs_ports() {
    let s = Server::start(false);
    s.fresh_db("pouchbulk").await;

    let (status, rows) = s
        .post(
            "/pouchbulk/_bulk_docs",
            json!({"docs": [
                {"_id": "d1", "v": 1},
                {"_id": "d2", "v": 2}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{rows}");
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row["ok"] == true));

    let (status, rows) = s
        .post(
            "/pouchbulk/_bulk_docs",
            json!({"docs": [
                {"_id": "d1", "_rev": rows[0]["rev"], "v": 10},
                {"_id": "d2", "_rev": rows[1]["rev"], "_deleted": true}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{rows}");
    assert!(rows.as_array().unwrap().iter().all(|row| row["ok"] == true));
    let (status, doc) = s.get("/pouchbulk/d2").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{doc}");
    assert_eq!(doc["reason"], "deleted");

    // Duplicate ids in one bulk request: one succeeds, one conflicts.
    let (status, rows) = s
        .post(
            "/pouchbulk/_bulk_docs",
            json!({"docs": [
                {"_id": "dup", "v": 1},
                {"_id": "dup", "v": 2}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let rows = rows.as_array().unwrap();
    assert!(rows.iter().any(|row| row["ok"] == true));
    assert!(rows.iter().any(|row| row["error"] == "conflict"));

    // new_edits=false accepts a replication revision.
    let (status, body) = s
        .post(
            "/pouchbulk/_bulk_docs",
            json!({"new_edits": false, "docs": [{
                "_id": "repdoc",
                "_rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "_revisions": {"start": 1, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]},
                "v": 1
            }]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body, json!([]));
    let (status, doc) = s.get("/pouchbulk/repdoc").await;
    assert_eq!(status, StatusCode::OK, "{doc}");
    assert_eq!(doc["_rev"], "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    // Design docs are stored, retrievable, indexed and visible in _all_docs.
    let (status, _) = s
        .put(
            "/pouchbulk/_design/foo",
            json!({
                "views": {
                    "scores": {
                        "map": "function(doc){ if (typeof doc.score === 'number') emit(null, doc.score); }",
                        "reduce": "function(keys, values, rereduce){ return sum(values); }"
                    }
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, design) = s.get("/pouchbulk/_design/foo").await;
    assert_eq!(status, StatusCode::OK, "{design}");
    assert_eq!(design["_id"], "_design/foo");
    let (status, _) = s
        .post(
            "/pouchbulk/_bulk_docs",
            json!({"docs": [
                {"_id": "dale", "score": 3},
                {"_id": "mikeal", "score": 5},
                {"_id": "max", "score": 4},
                {"_id": "nuno", "score": 3}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s
        .get("/pouchbulk/_design/foo/_view/scores?reduce=false")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 4);
    let (status, body) = s.get("/pouchbulk/_design/foo/_view/scores").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 15);
    let (status, body) = s.get("/pouchbulk/_all_docs?key=%22_design%2Ffoo%22").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn couchdb_search_sort_highlight_timeout_and_index_info_ports() {
    let s = Server::start(false);
    seed_search_docs(&s, "couchsearchopts").await;

    let (status, body) = s
        .get("/couchsearchopts/_design/inventory/_search/fruits?q=*:*&sort=%5B%22price%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["carrot", "apple", "date", "banana"]);
    assert_eq!(body["rows"][0]["order"], json!([0.75]));

    let (status, body) = s
        .get("/couchsearchopts/_design/inventory/_search/fruits?q=*:*&sort=%5B%22-price%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["banana", "date", "apple", "carrot"]);

    let (status, body) = s
        .post(
            "/couchsearchopts/_design/inventory/_search/fruits",
            json!({"q": "*:*", "sort": ["-price"], "limit": 2}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["id"], "banana");
    assert_eq!(body["rows"][1]["id"], "date");

    let (status, body) = s
        .get("/couchsearchopts/_design/inventory/_search/fruits?q=item:apple&highlight_fields=%5B%22item%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"][0]["highlights"]["item"],
        json!(["<em>apple</em>"])
    );

    let (status, body) = s
        .post(
            "/couchsearchopts/_design/inventory/_search/fruits",
            json!({
                "q": "item:apple",
                "highlight_fields": ["item"],
                "highlight_pre_tag": "[",
                "highlight_post_tag": "]"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["highlights"]["item"], json!(["[apple]"]));

    let (status, body) = s
        .get("/couchsearchopts/_design/inventory/_search/fruits?q=*:*&timeout=0")
        .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "timeout");
    assert_eq!(
        body["reason"],
        "The request could not be processed in a reasonable amount of time."
    );

    let (status, body) = s.get("/couchsearchopts/_index_info?type=search").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["view_indexes"], json!([]));
    assert_eq!(body["nouveau_indexes"], json!([]));
    let search = body["search_indexes"].as_array().unwrap();
    assert_eq!(search.len(), 1);
    assert_eq!(search[0]["ddoc"], "_design/inventory");
    assert_eq!(search[0]["name"], "fruits");
    assert_eq!(search[0]["ok"], true);
    let mut keys: Vec<&str> = search[0]["info"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["doc_count", "signature", "updates_pending"]);

    let (status, info) = s
        .get("/couchsearchopts/_design/inventory/_search_info/fruits")
        .await;
    assert_eq!(status, StatusCode::OK, "{info}");
    assert_eq!(search[0]["info"], info["search_index"]);
}

#[tokio::test]
async fn search_fans_out_across_cluster_shards() {
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("csearchfan").await;
    }
    let docs: Vec<Value> = (0..9)
        .map(|i| {
            json!({
                "_id": format!("fruit{i}"),
                "item": format!("item{i}"),
                "place": "kitchen",
                "state": if i % 2 == 0 { "new" } else { "old" },
                "price": i
            })
        })
        .collect();
    let (status, _) = nodes[0]
        .post("/csearchfan/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[0]
        .put("/csearchfan/_design/inventory", search_ddoc())
        .await;
    assert_eq!(status, StatusCode::CREATED);

    for node in &nodes {
        let (status, body) = node
            .get("/csearchfan/_design/inventory/_search/fruits?q=*:*")
            .await;
        assert_eq!(status, StatusCode::OK, "search via {}", node.base);
        assert_eq!(body["total_rows"], 9, "search via {}", node.base);
        assert_eq!(body["rows"].as_array().unwrap().len(), 9);
    }

    // Sorting and faceting are applied after the per-shard merge.
    let (status, body) = nodes[1]
        .post(
            "/csearchfan/_design/inventory/_search/fruits",
            json!({"q": "*:*", "sort": ["-price"], "limit": 3, "counts": ["state"]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["fruit8", "fruit7", "fruit6"]);
    assert_eq!(body["counts"]["state"]["new"], 5);
    assert_eq!(body["counts"]["state"]["old"], 4);

    let (status, body) = nodes[2]
        .get("/csearchfan/_design/inventory/_search/fruits?q=*:*&limit=2&bookmark=search%3A2&sort=%5B%22price%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["fruit2", "fruit3"]);
}

#[tokio::test]
async fn cluster_management_reports_discovery_and_runs_replication() {
    let nodes = start_cluster_with(2, "3600", 1);
    for node in &nodes {
        node.fresh_db("cadmin").await;
    }
    let docs: Vec<Value> = (0..8)
        .map(|i| json!({"_id": format!("m{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/cadmin/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Discovery probes the configured peers and follows their membership
    // lists. Wait for the startup passes to see both nodes.
    for node in &nodes {
        let mut snapshot = Value::Null;
        for _ in 0..80 {
            let (status, body) = node.get("/_node/_local/_cluster").await;
            assert_eq!(status, StatusCode::OK, "{body}");
            let reachable = body["discovery"]["nodes"]
                .as_array()
                .map(|nodes| {
                    nodes.len() >= 2
                        && nodes.iter().all(|node| node["reachable"] == true)
                        && nodes
                            .iter()
                            .all(|node| node["membership"]["all_nodes"].as_array().is_some())
                })
                .unwrap_or(false);
            if reachable {
                snapshot = body;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(!snapshot.is_null(), "discovery via {}", node.base);
        assert_eq!(
            snapshot["membership"]["cluster_nodes"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(snapshot["local"]["clustered"], true);
        assert!(snapshot["databases"]
            .as_array()
            .unwrap()
            .iter()
            .any(|db| db["name"] == "cadmin"));
    }

    // Let the startup pass settle before testing the explicit controls.
    for _ in 0..100 {
        let (status, body) = nodes[1].get("/_node/_local/_replication").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["running"] == false {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Background replication can be paused and resumed from the interface.
    let (status, body) = nodes[1]
        .post("/_node/_local/_replication/pause", json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["replication"]["paused"], true);
    let (status, body) = nodes[1]
        .post("/_node/_local/_replication/resume", json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["replication"]["paused"], false);

    // "Sync now" runs the same background anti-entropy pass on demand.
    let (status, body) = nodes[1]
        .post("/_node/_local/_replication/run", json!({}))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");

    let mut entry = Value::Null;
    for _ in 0..100 {
        let (status, body) = nodes[1].get("/_node/_local/_replication").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["running"] == false {
            if let Some(found) = body["entries"]
                .as_array()
                .and_then(|entries| entries.iter().find(|entry| entry["db"] == "cadmin"))
            {
                entry = found.clone();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!entry.is_null(), "replication did not run");
    assert_eq!(entry["state"], "ok", "{entry}");
    assert!(!entry["checkpoint"].is_null(), "{entry}");

    let (status, body) = nodes[0].get("/_node/_local/_cluster").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["replication"]["runs"].as_u64().unwrap_or(0) >= 1);
}

#[tokio::test]
async fn discovery_follows_seed_membership() {
    let nodes = start_cluster_with(2, "3600", 1);
    let seed = nodes[0].base.clone();
    let other = nodes[1].base.clone();

    let port = free_port();
    let self_url = format!("http://127.0.0.1:{port}");
    let extra = vec![
        "--nodes".to_string(),
        self_url.clone(),
        "--me".to_string(),
        "0".to_string(),
        "--seed-nodes".to_string(),
        seed.clone(),
        "--cluster-secret".to_string(),
        "test-cluster-secret".to_string(),
    ];
    let probe = Server::spawn_on(port, &extra, false, 0).expect("probe server");

    let mut body = Value::Null;
    let mut last = Value::Null;
    for _ in 0..100 {
        let (status, current) = probe.get("/_node/_local/_cluster").await;
        assert_eq!(status, StatusCode::OK, "{current}");
        last = current.clone();
        let found = current["discovery"]["nodes"]
            .as_array()
            .map(|nodes| {
                nodes.len() >= 3
                    && nodes
                        .iter()
                        .any(|node| node["url"] == seed && node["seed"] == true)
                    && nodes
                        .iter()
                        .any(|node| node["url"] == other && node["discovered"] == true)
            })
            .unwrap_or(false);
        if found {
            body = current;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!body.is_null(), "seed discovery did not complete: {last}");
    assert_eq!(body["local"]["clustered"], false);
    let nodes = body["discovery"]["nodes"].as_array().unwrap();
    assert!(nodes
        .iter()
        .any(|node| node["url"] == self_url && node["in_membership"] == true));
    assert!(nodes
        .iter()
        .any(|node| node["url"] == seed && node["seed"] == true && node["in_membership"] == false));
    assert!(nodes.iter().any(|node| node["url"] == other
        && node["discovered"] == true
        && node["membership"]["all_nodes"].as_array().unwrap().len() == 2));
}

async fn local_shard_docs(node: &Server, db: &str, shard: u32) -> u64 {
    let (_, body) = node.get(&format!("/_node/_local/_dbs/{db}/shards")).await;
    body["shards"]
        .as_array()
        .and_then(|shards| shards.iter().find(|row| row["shard"] == shard))
        .and_then(|row| row["docs"].as_u64())
        .unwrap_or(0)
}

#[tokio::test]
async fn background_replication_repairs_a_missed_shard_without_a_client() {
    use md5::{Digest, Md5};

    // Full replication makes the target shard deterministic: both nodes hold
    // it, so a local-only write on node 0 leaves node 1 to be repaired.
    let nodes = start_cluster_with(2, "1", 2);
    for node in &nodes {
        node.fresh_db("rfan").await;
    }
    let q = 4u32;
    let (status, map) = nodes[0].get("/_node/_local/_dbs/rfan").await;
    assert_eq!(status, StatusCode::OK, "{map}");
    let target = (0..q)
        .find(|shard| {
            map["replicas"][*shard as usize]
                .as_array()
                .map(|replicas| replicas.contains(&json!(1)))
                .unwrap_or(false)
        })
        .expect("node 1 replicates a shard");
    let mut id = String::new();
    for n in 0..10_000 {
        let candidate = format!("miss{n}");
        let digest = Md5::digest(candidate.as_bytes());
        let shard = u32::from_be_bytes(digest[..4].try_into().unwrap()) % q;
        if shard == target {
            id = candidate;
            break;
        }
    }
    assert!(!id.is_empty());
    let before = local_shard_docs(&nodes[1], "rfan", target).await;

    // Write directly to node 0, which does not own this shard. A normal write
    // would be routed to node 1; the internal forwarded form leaves the copy
    // only on node 0, exactly like a write whose replica fan-out failed.
    let rev = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let (status, _, body) = nodes[0]
        .req_headers(
            Method::POST,
            "/rfan/_bulk_docs",
            Some(json!({
                "new_edits": false,
                "docs": [{
                    "_id": id,
                    "_rev": format!("1-{rev}"),
                    "_revisions": {"start": 1, "ids": [rev]},
                    "v": 1
                }]
            })),
            &[
                ("x-douchdb-forwarded", "1".to_string()),
                ("x-douchdb-secret", "test-cluster-secret".to_string()),
            ],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // No client request touches node 1 for this document. The one-second
    // anti-entropy timer must pull it into node 1's copy of the shard.
    let mut after = before;
    for _ in 0..150 {
        after = local_shard_docs(&nodes[1], "rfan", target).await;
        if after > before {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        after > before,
        "anti-entropy did not repair shard {target}: {before} -> {after}"
    );

    let (status, body) = nodes[1].get(&format!("/rfan/{id}?r=1")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["v"], 1);
}

#[tokio::test]
async fn quickjs_view_sandbox_ports() {
    let s = Server::start(false);
    s.fresh_db("jsandbox").await;
    let (status, _) = s
        .put(
            "/jsandbox/doc",
            json!({"integer": 1, "string": "1", "array": [1, 2, 3]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jsandbox/_design/v",
            json!({"views": {
                "mutate": {"map": "function(doc){ doc.integer = 2; doc.array[0] = 0; emit(null, doc); }"},
                "gc": {"map": "function(doc){ gc(); emit(null, doc); }"},
                "internals": {"map": "function(doc){ map_funs.push(1); emit(null, doc); }"},
                "escape": {"map": "function(doc){ var glob = emit.constructor('return this')(); emit(doc._id, null); }"}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Mutation is local to the JS value; the stored document and include_docs
    // still see the original fields.
    let (status, body) = s
        .get("/jsandbox/_design/v/_view/mutate?include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"]["integer"], 2);
    assert_eq!(body["rows"][0]["value"]["array"][0], 0);
    assert_eq!(body["rows"][0]["doc"]["integer"], 1);
    assert_eq!(body["rows"][0]["doc"]["array"][0], 1);

    // Interpreter internals are not part of the view environment.
    for view in ["gc", "internals"] {
        let (status, body) = s.get(&format!("/jsandbox/_design/v/_view/{view}")).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{view}: {body}");
        assert_eq!(body["error"], "view_error", "{view}: {body}");
    }

    // Function-constructor access reaches the isolated QuickJS global, not the
    // Rust host.
    let (status, body) = s.get("/jsandbox/_design/v/_view/escape").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["id"], "doc");
}

#[tokio::test]
async fn quickjs_view_deadline_stops_an_infinite_loop() {
    let args = vec!["--view-timeout-ms".to_string(), "150".to_string()];
    let s = Server::start_with(&args, false);
    s.fresh_db("jsloop").await;
    let (status, _) = s.put("/jsloop/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jsloop/_design/v",
            json!({"views": {"loop": {"map": "function(doc){ while (true) {} }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let started = std::time::Instant::now();
    let (status, body) = s.get("/jsloop/_design/v/_view/loop").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "view_error");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "infinite loop was not interrupted in time: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn quickjs_process_sandbox_runs_views_reduce_and_search() {
    let args = vec!["--js-sandbox".to_string(), "process".to_string()];
    let s = Server::start_with(&args, false);
    s.fresh_db("jsprocess").await;
    let (status, _) = s
        .post(
            "/jsprocess/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "city": "Berlin", "price": 10},
                {"_id": "b", "city": "Paris", "price": 20}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jsprocess/_design/v",
            json!({
                "views": {
                    "by_city": {"map": "function(doc){ emit(doc.city, doc.price); }"},
                    "computed": {"map": "function(doc){ var suffix = '-x'; emit(doc.city + suffix, doc.price); }"},
                    "total": {
                        "map": "function(doc){ emit(null, doc.price); }",
                        "reduce": "function(keys, values, rereduce){ return sum(values); }"
                    }
                },
                "indexes": {
                    "cities": {"index": "function(doc){ index('city', doc.city); }"}
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .get("/jsprocess/_design/v/_view/by_city?reduce=false")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    let (status, body) = s
        .get("/jsprocess/_design/v/_view/computed?reduce=false")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["key"], "Berlin-x");
    let (status, body) = s.get("/jsprocess/_design/v/_view/total").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 30);

    let (status, body) = s
        .get("/jsprocess/_design/v/_search/cities?q=city:Berlin&include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["id"], "a");
}

#[tokio::test]
async fn quickjs_process_sandbox_enforces_the_deadline() {
    let args = vec![
        "--js-sandbox".to_string(),
        "process".to_string(),
        "--view-timeout-ms".to_string(),
        "150".to_string(),
    ];
    let s = Server::start_with(&args, false);
    s.fresh_db("jsploop").await;
    let (status, _) = s.put("/jsploop/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jsploop/_design/v",
            json!({"views": {"loop": {"map": "function(doc){ while (true) {} }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let started = std::time::Instant::now();
    let (status, body) = s.get("/jsploop/_design/v/_view/loop").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "view_error");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "worker deadline was not enforced: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn quickjs_process_pool_handles_concurrent_queries() {
    let args = vec![
        "--js-sandbox".to_string(),
        "process".to_string(),
        "--js-workers".to_string(),
        "2".to_string(),
    ];
    let s = Server::start_with(&args, false);
    s.fresh_db("jspool").await;
    let (status, _) = s
        .post(
            "/jspool/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "city": "Berlin"},
                {"_id": "b", "city": "Paris"},
                {"_id": "c", "city": "Berlin"},
                {"_id": "d", "city": "Rome"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jspool/_design/v",
            json!({"views": {"by_city": {"map": "function(doc){ emit(doc.city, null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let requests = (0..8).map(|_| s.get("/jspool/_design/v/_view/by_city?reduce=false"));
    let results = futures::future::join_all(requests).await;
    for (status, body) in results {
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 4, "{body}");
    }
}

#[tokio::test]
async fn quickjs_wasm_sandbox_runs_views_reduce_and_search() {
    let args = vec!["--js-sandbox".to_string(), "wasm".to_string()];
    let s = Server::start_with(&args, false);
    s.fresh_db("jswasm").await;
    let (status, _) = s
        .post(
            "/jswasm/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "city": "Berlin", "price": 10},
                {"_id": "b", "city": "Paris", "price": 20}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jswasm/_design/v",
            json!({
                "views": {
                    "by_city": {"map": "function(doc){ emit(doc.city, doc.price); }"},
                    "computed": {"map": "function(doc){ var suffix = '-x'; emit(doc.city + suffix, doc.price); }"},
                    "total": {
                        "map": "function(doc){ emit(null, doc.price); }",
                        "reduce": "function(keys, values, rereduce){ return sum(values); }"
                    }
                },
                "indexes": {
                    "cities": {"index": "function(doc){ index('city', doc.city); }"}
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s.get("/jswasm/_design/v/_view/by_city?reduce=false").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    let (status, body) = s.get("/jswasm/_design/v/_view/computed?reduce=false").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["key"], "Berlin-x");
    let (status, body) = s.get("/jswasm/_design/v/_view/total").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 30);

    let (status, body) = s
        .get("/jswasm/_design/v/_search/cities?q=city:Berlin&include_docs=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 1);
    assert_eq!(body["rows"][0]["id"], "a");
}

#[tokio::test]
async fn quickjs_wasm_sandbox_enforces_the_deadline() {
    let args = vec![
        "--js-sandbox".to_string(),
        "wasm".to_string(),
        "--view-timeout-ms".to_string(),
        "150".to_string(),
    ];
    let s = Server::start_with(&args, false);
    s.fresh_db("jswloop").await;
    let (status, _) = s.put("/jswloop/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jswloop/_design/v",
            json!({"views": {"loop": {"map": "function(doc){ while (true) {} }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let started = std::time::Instant::now();
    let (status, body) = s.get("/jswloop/_design/v/_view/loop").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "view_error");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "wasm fuel deadline was not enforced: {:?}",
        started.elapsed()
    );
}
