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

/// Wait until `port` accepts connections. Returns false as soon as the child
/// exits, which is how a port collision shows up. A live child is never given
/// up on early: with `--wasm-runtime wasmtime` it compiles the runner before
/// it listens, which can take tens of seconds in a debug build on a loaded CI
/// runner, and killing it to retry would only pay that cost again.
fn wait_for_listener(child: &mut Child, port: u16) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        if let Ok(Some(_)) = child.try_wait() {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// Whether to exercise the given `--js-sandbox` mode. Set
/// `DOUCHDB_TEST_JS_SANDBOXES` to a comma-separated list (for example
/// `native`) to restrict the sandbox tests; unset, every mode runs. CI runs
/// only `native`, so run the full set locally before changing the process or
/// WASM sandboxes. Tests for an excluded mode return early and report as
/// passed.
fn sandbox_tested(mode: &str) -> bool {
    if mode == "wasm" && cfg!(not(feature = "wasm")) {
        return false;
    }
    match std::env::var("DOUCHDB_TEST_JS_SANDBOXES") {
        Ok(modes) => modes.split(',').any(|listed| listed.trim() == mode),
        Err(_) => true,
    }
}

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
        for attempt in 0..100 {
            if let Some(server) = Server::spawn_on(free_port(), extra, admin, attempt) {
                return server;
            }
            std::thread::sleep(Duration::from_millis(20));
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
        let mut cmd = server_command();
        cmd.arg("--listen")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--data")
            .arg(&dir)
            .arg("--sync-secs")
            .arg("3600")
            .args(DEFAULT_SERVER_ARGS)
            .args(extra)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if admin {
            cmd.arg("--admin").arg("admin:douchdb");
        }
        // Built first: nothing may panic while the bare `Child` is unowned.
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        let mut child = spawn_locked(&mut cmd).expect("spawn douchdb");
        let base = format!("http://127.0.0.1:{port}");
        if wait_for_listener(&mut child, port) {
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
    /// Stop this node (if it is still running) and start a fresh process on
    /// the same port and data dir, with a new client so no pooled connection
    /// to the old process is reused.
    fn restart(&mut self) {
        self.kill();
        self.client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .expect("client");
        let mut cmd = server_command();
        cmd.arg("--listen")
            .arg(format!("127.0.0.1:{}", self.port))
            .arg("--data")
            .arg(&self.dir)
            .arg("--sync-secs")
            .arg("3600")
            .args(DEFAULT_SERVER_ARGS)
            .args(&self.extra)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if self.admin {
            cmd.arg("--admin").arg("admin:douchdb");
        }
        self.child = spawn_locked(&mut cmd).expect("restart douchdb");
        if wait_for_listener(&mut self.child, self.port) {
            std::thread::sleep(Duration::from_millis(50));
            return;
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

    /// PUT a body whose exact bytes matter, such as duplicate keys or a large
    /// integer that `json!` would have rounded.
    async fn put_raw(&self, path: &str, body: &str) -> (StatusCode, Value) {
        let response = self
            .client
            .put(format!("{}{path}", self.base))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_owned())
            .send()
            .await
            .unwrap();
        let status = response.status();
        let value = response.json().await.unwrap_or(Value::Null);
        (status, value)
    }
    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.req(Method::POST, path, Some(body)).await
    }

    async fn post_raw(&self, path: &str, body: &str) -> (StatusCode, Value) {
        let response = self
            .client
            .post(format!("{}{path}", self.base))
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_owned())
            .send()
            .await
            .unwrap();
        let status = response.status();
        let value = response.json().await.unwrap_or(Value::Null);
        (status, value)
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

/// Every test server gets a view deadline far beyond what a loaded machine
/// needs, so a slow JavaScript run cannot fail a test that is not about the
/// deadline. A test about the deadline passes its own `--view-timeout-ms`,
/// which comes later and so wins.
const DEFAULT_SERVER_ARGS: [&str; 2] = ["--view-timeout-ms", "120000"];

/// Held while `free_port` has a probe socket open and while a server is
/// spawned. On macOS a new socket is made close-on-exec only after it is
/// created, so a server spawned by another test thread in that window inherits
/// the probe listener and keeps the port. The real server then fails to bind,
/// and the test talks to the stray socket ("Connection reset by peer").
static SPAWN_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A douchdb server command that exits when this process goes away. Dropping
/// a `Server` kills its child, but a run killed by a timeout or Ctrl-C never
/// drops anything. The child holds the read end of a pipe whose write end is
/// `Child::stdin`, so the kernel closes it with this process on Linux and
/// macOS alike, and the server exits on EOF (`DOUCHDB_EXIT_ON_STDIN_EOF`).
fn server_command() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_douchdb"));
    cmd.env("DOUCHDB_EXIT_ON_STDIN_EOF", "1")
        .stdin(std::process::Stdio::piped());
    cmd
}

fn spawn_locked(cmd: &mut Command) -> std::io::Result<Child> {
    let _guard = SPAWN_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    cmd.spawn()
}

fn free_port() -> u16 {
    // Tests run in parallel threads. The kernel's ephemeral-port pool can hand
    // the same released port to two tests, so assign ports from a process-wide
    // counter and confirm each one is free.
    static NEXT_PORT: AtomicUsize = AtomicUsize::new(0);
    loop {
        let n = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        let candidate = 31000 + ((std::process::id() as usize + n * 37) % 20000);
        let _guard = SPAWN_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if std::net::TcpListener::bind(("127.0.0.1", candidate as u16)).is_ok() {
            return candidate as u16;
        }
    }
}

/// A `(reader, victim)` pair for `db`: `victim` serves `reader`'s reads of a
/// shard that holds one of `ids` and that `reader` has no replica of. Killing
/// `victim` forces reads coordinated by `reader` onto the degraded-read
/// fallback for that document. Placement depends on the nodes' (random)
/// ports, so the pair is looked up rather than fixed.
async fn fallback_pair(nodes: &[Server], db: &str, ids: &[String]) -> (usize, usize) {
    let (status, map) = nodes[0].get(&format!("/_node/_local/_dbs/{db}")).await;
    assert_eq!(status, StatusCode::OK, "{map}");
    let q = map["q"].as_u64().unwrap() as u32;
    for (shard, replicas) in map["replicas"].as_array().unwrap().iter().enumerate() {
        if !ids.iter().any(|id| douchdb::shard_index(id, q) == shard) {
            continue;
        }
        let replicas: Vec<usize> = replicas
            .as_array()
            .unwrap()
            .iter()
            .map(|node| node.as_u64().unwrap() as usize)
            .collect();
        if let Some(reader) = (0..nodes.len()).find(|node| !replicas.contains(node)) {
            return (reader, replicas[0]);
        }
    }
    panic!("every node holds every shard with a test document in {db}: {map}");
}

/// Start `n` nodes in one cluster with `q=4`. Anti-entropy is effectively off
/// (`--sync-secs 3600`), so tests exercise request routing rather than
/// background repair: a document reachable only through its owner proves the
/// proxy path works.
fn start_cluster(n: usize) -> Vec<Server> {
    start_cluster_with(n, "3600", 1)
}

fn start_cluster_with(n: usize, sync_secs: &str, replication: usize) -> Vec<Server> {
    start_cluster_full(n, 4, sync_secs, replication, false)
}

fn start_cluster_with_auth(n: usize, sync_secs: &str, replication: usize) -> Vec<Server> {
    start_cluster_full(n, 4, sync_secs, replication, true)
}

/// A cluster with `q` shards per database and `replication` copies of each.
fn start_cluster_sharded(n: usize, q: u32, replication: usize) -> Vec<Server> {
    start_cluster_full(n, q, "3600", replication, false)
}

fn start_cluster_full(
    n: usize,
    q: u32,
    sync_secs: &str,
    replication: usize,
    admin: bool,
) -> Vec<Server> {
    for attempt in 0..5 {
        let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let nodes = ports
            .iter()
            .map(|p| format!("http://127.0.0.1:{p}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut servers = Vec::new();
        let mut failed = false;
        for (i, &port) in ports.iter().enumerate() {
            let extra = vec![
                "--q".to_string(),
                q.to_string(),
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
                // Cookies only verify across nodes with a shared session key.
                "--session-secret".to_string(),
                "test-session-secret".to_string(),
            ];
            match Server::spawn_on(port, &extra, admin, attempt) {
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

/// Status of `GET /{db}` answered from `node`'s own copy only, as a peer
/// would: the coordinator's cluster-wide `update_seq` would also need every
/// other node to have the database.
async fn local_db_status(node: &Server, db: &str) -> StatusCode {
    let headers = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    node.req_with(Method::GET, &format!("/{db}"), None, &headers)
        .await
        .0
}

/// Concurrent creates of one name: CouchDB answers exactly one `201` and
/// `412 file_exists` to the rest (couch_server serialises creation per name).
#[tokio::test]
async fn concurrent_create_of_one_database_gives_one_201() {
    let s = Server::start(false);
    let results = futures::future::join_all((0..16).map(|_| s.put("/racy", json!({})))).await;
    let created = results
        .iter()
        .filter(|(status, _)| *status == StatusCode::CREATED)
        .count();
    let exists = results
        .iter()
        .filter(|(status, body)| {
            *status == StatusCode::PRECONDITION_FAILED && body["error"] == "file_exists"
        })
        .count();
    assert_eq!((created, exists), (1, 15), "{results:?}");
    let (status, _) = s.get("/racy").await;
    assert_eq!(status, StatusCode::OK);
}

/// A database that exists on disk but is not open yet is opened once, however
/// many first requests arrive together; a second open would hit the LevelDB
/// LOCK and answer 500.
#[tokio::test]
async fn concurrent_first_requests_share_one_open() {
    let mut s = Server::start(false);
    s.fresh_db("cold").await;
    s.restart();
    let results = futures::future::join_all((0..32).map(|_| s.get("/cold"))).await;
    let statuses: Vec<StatusCode> = results.iter().map(|(status, _)| *status).collect();
    assert_eq!(statuses, vec![StatusCode::OK; 32], "{results:?}");
}

/// A request that raced a DELETE must not reopen the database and recreate it
/// on disk: after the delete the database is gone for good.
#[tokio::test]
async fn delete_is_not_undone_by_concurrent_requests() {
    let mut s = Server::start(false);
    for round in 0..5 {
        let name = format!("gone{round}");
        s.fresh_db(&name).await;
        s.restart();
        let path = format!("/{name}");
        let doc = format!("/{name}/d");
        let mut calls = Vec::new();
        for i in 0..24 {
            let (method, target) = match i {
                12 => (Method::DELETE, &path),
                _ if i % 2 == 0 => (Method::GET, &path),
                _ => (Method::GET, &doc),
            };
            calls.push(s.req(method, target, None));
        }
        let results = futures::future::join_all(calls).await;
        assert_eq!(results[12].0, StatusCode::OK, "{results:?}");
        let (status, _) = s.get(&path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{name} was resurrected");
        assert!(!s.dir.join(&name).exists(), "{name} is back on disk");
    }
}

/// CouchDB's fabric_db_create answers 202 when fewer than a quorum of nodes
/// recorded the database, and a retry does not stop at the existing database:
/// the node that missed it is brought up to date (CouchDB heals it by
/// replicating the shard map).
#[tokio::test]
async fn create_reports_missing_peers_and_a_retry_completes_them() {
    let mut nodes = start_cluster_with(2, "3600", 1);
    nodes[1].kill();
    let (status, body) = nodes[0].put("/partial", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["ok"], true);
    nodes[1].restart();
    let status = local_db_status(&nodes[1], "partial").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = nodes[0].put("/partial", json!({})).await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    let status = local_db_status(&nodes[1], "partial").await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// With a quorum of nodes up (2 of 3) the create is a plain 201.
#[tokio::test]
async fn create_with_a_quorum_of_nodes_answers_201() {
    let mut nodes = start_cluster_with(3, "3600", 1);
    nodes[2].kill();
    let (status, body) = nodes[0].put("/quorum", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let status = local_db_status(&nodes[1], "quorum").await;
    assert_eq!(status, StatusCode::OK);
}

/// CouchDB's fabric_db_delete answers 202, not 200, when a node did not
/// confirm the delete; a later delete removes the copy that was missed.
#[tokio::test]
async fn delete_does_not_report_ok_when_a_peer_failed() {
    let mut nodes = start_cluster_with(2, "3600", 1);
    let (status, body) = nodes[0].put("/shared", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    nodes[1].kill();
    let (status, body) = nodes[0].delete("/shared").await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let status = local_db_status(&nodes[0], "shared").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    nodes[1].restart();
    let status = local_db_status(&nodes[1], "shared").await;
    assert_eq!(status, StatusCode::OK, "the missed peer still has it");
    let (status, body) = nodes[0].delete("/shared").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let status = local_db_status(&nodes[1], "shared").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
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
            "--cors-credentials".to_string(),
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

    // CouchDB answers an unlisted origin as the plain request it is.
    let response = options(&s, "http://evil.test").await.unwrap();
    assert_ne!(response.status(), StatusCode::NO_CONTENT);
    assert_ne!(response.status(), StatusCode::FORBIDDEN);
    assert!(response
        .headers()
        .get("access-control-allow-origin")
        .is_none());
}

/// CouchDB's `[cors]` defaults: `credentials = false`, and a preflight is only
/// answered when every requested header is in the allowed list.
#[tokio::test]
async fn cors_credentials_and_headers_follow_couchdb_defaults() {
    let preflight = |server: &Server, requested: &'static str| {
        server
            .client
            .request(Method::OPTIONS, format!("{}/demo/doc", server.base))
            .header("Origin", "http://example.test")
            .header("Access-Control-Request-Method", "PUT")
            .header("Access-Control-Request-Headers", requested)
            .send()
    };
    let header = |response: &reqwest::Response, name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };

    // Default: no credentials, and a header outside the allowed list is not a
    // preflight (see cors_preflight_matches_chttpd_cors for the details).
    let s = Server::start_with(
        &[
            "--cors-origins".to_string(),
            "http://example.test".to_string(),
        ],
        false,
    );
    let response = preflight(&s, "content-type, authorization").await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(header(&response, "access-control-allow-credentials"), None);
    let allowed = header(&response, "access-control-allow-headers").unwrap_or_default();
    assert_eq!(allowed, "content-type, authorization", "echoed");
    let response = preflight(&s, "x-evil-header").await.unwrap();
    assert_ne!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(header(&response, "access-control-allow-headers"), None);
    let response = s
        .client
        .get(format!("{}/_all_dbs", s.base))
        .header("Origin", "http://example.test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        header(&response, "access-control-allow-origin").as_deref(),
        Some("http://example.test")
    );
    assert_eq!(header(&response, "access-control-allow-credentials"), None);

    // Opt in to credentials and to an extra header.
    let s = Server::start_with(
        &[
            "--cors-origins".to_string(),
            "http://example.test".to_string(),
            "--cors-credentials".to_string(),
            "--cors-headers".to_string(),
            "X-Custom-Header,Content-Type".to_string(),
        ],
        false,
    );
    let response = preflight(&s, "x-custom-header, content-type")
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header(&response, "access-control-allow-credentials").as_deref(),
        Some("true")
    );
    let response = s
        .client
        .get(format!("{}/_all_dbs", s.base))
        .header("Origin", "http://example.test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        header(&response, "access-control-allow-credentials").as_deref(),
        Some("true")
    );
}

/// Preflights exactly as CouchDB's `chttpd_cors` (3.5.3) answers them: a
/// preflight that passes gets 204 with the request origin, `Max-Age: 600`, the
/// full method list and the requested headers echoed back; anything else
/// (header not in `[cors] headers`, unknown method, no request method) is not
/// treated as a preflight at all, so it gets no preflight headers.
#[tokio::test]
async fn cors_preflight_matches_chttpd_cors() {
    const METHODS: &str = "CONNECT, COPY, DELETE, GET, HEAD, OPTIONS, POST, PUT, TRACE";
    let preflight = |server: &Server,
                     origin: &'static str,
                     method: Option<&'static str>,
                     requested: Option<&'static str>| {
        let mut request = server
            .client
            .request(Method::OPTIONS, format!("{}/demo/doc", server.base))
            .header("Origin", origin);
        if let Some(method) = method {
            request = request.header("Access-Control-Request-Method", method);
        }
        if let Some(requested) = requested {
            request = request.header("Access-Control-Request-Headers", requested);
        }
        request.send()
    };
    let header = |response: &reqwest::Response, name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };

    // Any origin, credentials off (CouchDB's default).
    let any = Server::start_with(&["--cors-origins".to_string(), "*".to_string()], false);
    // The preflights browser PouchDB 9 sends for replication, captured from
    // Chromium (clients/pouchdb/browser.js): _changes, _revs_diff, _bulk_get,
    // _bulk_docs, _local checkpoints and the database itself all ask for
    // exactly these, with Basic auth in `authorization`.
    for (method, requested) in [
        ("GET", "authorization"),
        ("GET", "authorization,content-type"),
        ("POST", "authorization,content-type"),
        ("PUT", "authorization,content-type"),
    ] {
        let response = preflight(&any, "http://app.test", Some(method), Some(requested))
            .await
            .unwrap();
        let context = format!("{method} {requested}");
        assert_eq!(response.status(), StatusCode::NO_CONTENT, "{context}");
        assert_eq!(
            header(&response, "access-control-allow-origin").as_deref(),
            Some("http://app.test"),
            "{context}"
        );
        assert_eq!(
            header(&response, "access-control-allow-headers").as_deref(),
            Some(requested),
            "{context}: the requested headers are echoed verbatim"
        );
        assert_eq!(
            header(&response, "access-control-allow-methods").as_deref(),
            Some(METHODS),
            "{context}"
        );
        assert_eq!(
            header(&response, "access-control-max-age").as_deref(),
            Some("600"),
            "{context}"
        );
        assert_eq!(
            header(&response, "access-control-allow-credentials"),
            None,
            "{context}"
        );
        assert_eq!(header(&response, "access-control-expose-headers"), None);
        assert!(!header(&response, "vary")
            .unwrap_or_default()
            .to_lowercase()
            .contains("origin"));
    }
    // No requested headers: Allow-Headers is present and empty.
    let response = preflight(&any, "http://app.test", Some("PUT"), None)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header(&response, "access-control-allow-headers").as_deref(),
        Some("")
    );

    // Not a preflight: a header outside the supported list, an unknown method,
    // no request method. No preflight headers and no 403; the request is
    // handled as the plain OPTIONS it is.
    for (method, requested) in [
        (Some("PUT"), Some("x-evil")),
        (Some("PUT"), Some("content-type, x-evil")),
        (Some("PUT"), Some("referer")),
        (Some("FOO"), Some("content-type")),
        (None, Some("content-type")),
    ] {
        let response = preflight(&any, "http://app.test", method, requested)
            .await
            .unwrap();
        let context = format!("{method:?} {requested:?}");
        assert_ne!(response.status(), StatusCode::NO_CONTENT, "{context}");
        assert_ne!(response.status(), StatusCode::FORBIDDEN, "{context}");
        for name in [
            "access-control-allow-headers",
            "access-control-allow-methods",
            "access-control-max-age",
        ] {
            assert_eq!(header(&response, name), None, "{context}: {name}");
        }
    }

    // A specific origin with credentials on: credentials are advertised; an
    // origin that is not listed gets no CORS headers at all.
    let credentialed = Server::start_with(
        &[
            "--cors-origins".to_string(),
            "http://app.test".to_string(),
            "--cors-credentials".to_string(),
        ],
        false,
    );
    let response = preflight(
        &credentialed,
        "http://app.test",
        Some("PUT"),
        Some("authorization,content-type"),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        header(&response, "access-control-allow-credentials").as_deref(),
        Some("true")
    );
    let response = preflight(
        &credentialed,
        "http://other.test",
        Some("PUT"),
        Some("content-type"),
    )
    .await
    .unwrap();
    assert_ne!(response.status(), StatusCode::NO_CONTENT);
    assert_ne!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(header(&response, "access-control-allow-origin"), None);

    // `--cors-headers` is CouchDB's `[cors] headers`: it replaces the default
    // list rather than extending it.
    let custom = Server::start_with(
        &[
            "--cors-origins".to_string(),
            "http://app.test".to_string(),
            "--cors-headers".to_string(),
            "X-Foo".to_string(),
        ],
        false,
    );
    let response = preflight(&custom, "http://app.test", Some("PUT"), Some("x-foo"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let response = preflight(
        &custom,
        "http://app.test",
        Some("PUT"),
        Some("content-type"),
    )
    .await
    .unwrap();
    assert_ne!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(header(&response, "access-control-allow-headers"), None);

    // Actual responses: the origin (also for `*`), and an expose list that
    // starts with content-type and ends with CouchDB's own headers.
    let response = any
        .client
        .get(format!("{}/", any.base))
        .header("Origin", "http://app.test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        header(&response, "access-control-allow-origin").as_deref(),
        Some("http://app.test")
    );
    assert_eq!(header(&response, "access-control-allow-credentials"), None);
    assert!(!header(&response, "vary")
        .unwrap_or_default()
        .to_lowercase()
        .contains("origin"));
    let exposed = header(&response, "access-control-expose-headers").unwrap_or_default();
    assert!(exposed.starts_with("content-type"), "{exposed}");
    assert!(
        exposed.ends_with(
            "accept-ranges, etag, server, x-couch-request-id, x-couch-update-newrev, x-couchdb-body-time"
        ),
        "{exposed}"
    );
}

/// The console is the one HTML surface, so it carries a Content-Security-Policy.
/// The Vue build compiles in-DOM templates at runtime, which needs
/// `'unsafe-eval'`; everything else is locked down to the same origin.
#[tokio::test]
async fn console_responses_carry_a_content_security_policy() {
    let s = Server::start(false);
    for path in ["/_utils/", "/_utils/app.js", "/_utils/style.css"] {
        let response = s
            .client
            .get(format!("{}{path}", s.base))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let csp = response
            .headers()
            .get("content-security-policy")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_else(|| panic!("{path}: no Content-Security-Policy"));
        for directive in [
            "default-src 'none'",
            "script-src 'self' 'unsafe-eval'",
            "frame-ancestors 'none'",
            "object-src 'none'",
            "base-uri 'none'",
        ] {
            assert!(csp.contains(directive), "{path}: {csp}");
        }
        assert!(!csp.contains("script-src 'self' 'unsafe-inline'"), "{csp}");
    }
    // API responses are JSON and get none.
    let response = s
        .client
        .get(format!("{}/_all_dbs", s.base))
        .send()
        .await
        .unwrap();
    assert!(response.headers().get("content-security-policy").is_none());
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
    assert!(html.contains("data-testid=\"open-metrics\""));
    assert!(html.contains("data-testid=\"metrics-routes\""));
    // Fauxton-style document browsing: list, open, edit, save and delete.
    assert!(html.contains("data-testid=\"doc-list\""));
    assert!(html.contains("data-testid=\"doc-body\""));
    assert!(html.contains("data-testid=\"save-doc\""));
    assert!(html.contains("data-testid=\"delete-doc\""));
    assert!(html.contains("data-testid=\"open-access\""));
    assert!(html.contains("data-testid=\"access-table\""));
    assert!(html.contains("data-testid=\"security-modal\""));

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
    assert!(app.contains("/_node/_local/_metrics"));
    assert!(app.contains("loadMetrics"));
    assert!(app.contains("consoleProcess"));
    assert!(app.contains("dbsError"));
    assert!(app.contains("/_security"));
    assert!(app.contains("loadAccess"));
    // The request log must not retain login credentials.
    assert!(app.contains("[redacted credentials]"));
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

    // Integers above i64::MAX stay unsigned in the ETF body hash. Casting
    // them to i64 would encode a negative number and change the rev id.
    let (status, body) = s
        .put("/crud/big-u64", json!({"value": 9223372036854775808u64}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["rev"], "1-60e7deb48fe40a85d83333847552f100");
    let (status, body) = s.put("/crud/max-u64", json!({"value": u64::MAX})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["rev"], "1-b093d5afc8e54320086999e1b55c8ec6");

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
async fn internal_consistency_after_concurrent_writes() {
    use futures::future::join_all;

    let s = Server::start(false);
    s.fresh_db("consistency").await;
    let base = s.base.clone();
    let client = s.client.clone();

    let mut writers = Vec::new();
    for worker in 0..8u32 {
        let base = base.clone();
        let client = client.clone();
        writers.push(tokio::spawn(async move {
            for index in 0..10u32 {
                let id = format!("doc-{worker}-{index}");
                let body = json!({"n": worker * 10 + index, "s": format!("s{}", worker)});
                let response = client
                    .put(format!("{base}/consistency/{id}"))
                    .json(&body)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::CREATED);
            }
        }));
    }
    for result in join_all(writers).await {
        result.unwrap();
    }

    let bulk_docs: Vec<Value> = (0..20)
        .map(|index| json!({"_id": format!("bulk-{index}"), "n": index, "s": "bulk"}))
        .collect();
    let (status, body) = s
        .post("/consistency/_bulk_docs", json!({"docs": bulk_docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let bulk_revs: Vec<(String, String)> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["id"].as_str().unwrap().to_string(),
                row["rev"].as_str().unwrap().to_string(),
            )
        })
        .collect();

    let expected = 8 * 10 + bulk_revs.len();
    let (status, changes) = s
        .get("/consistency/_changes?since=0&include_docs=true&style=all_docs&limit=1000")
        .await;
    assert_eq!(status, StatusCode::OK, "{changes}");
    let mut latest: HashMap<String, Value> = HashMap::new();
    for row in changes["results"].as_array().unwrap() {
        latest.insert(row["id"].as_str().unwrap().to_string(), row.clone());
    }
    assert_eq!(latest.len(), expected, "one change row per document");

    let (_, all_docs) = s
        .get("/consistency/_all_docs?include_docs=true&limit=1000")
        .await;
    assert_eq!(all_docs["total_rows"], expected);
    for row in all_docs["rows"].as_array().unwrap() {
        let id = row["id"].as_str().unwrap();
        let (status, doc) = s.get(&format!("/consistency/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(row["value"]["rev"], doc["_rev"], "{id}");
        assert_eq!(row["doc"], doc, "{id}: _all_docs include_docs");

        let change = latest
            .get(id)
            .unwrap_or_else(|| panic!("{id}: missing change"));
        assert_eq!(
            change["changes"][0]["rev"], doc["_rev"],
            "{id}: changes rev"
        );
        assert_eq!(change["doc"], doc, "{id}: changes include_docs");
    }

    for (id, rev) in &bulk_revs {
        let (status, doc) = s.get(&format!("/consistency/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(doc["_rev"], *rev, "{id}: bulk response rev");
    }

    let (status, _) = s
        .put(
            "/consistency/_design/check",
            json!({"views": {
                "by_s": {"map": "function(doc){ if (doc.s !== undefined) emit(doc.s, doc.n); }"}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, view) = s
        .get("/consistency/_design/check/_view/by_s?reduce=false&limit=200")
        .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["total_rows"], expected);
    for row in view["rows"].as_array().unwrap() {
        let id = row["id"].as_str().unwrap();
        let (_, doc) = s.get(&format!("/consistency/{id}")).await;
        assert_eq!(row["key"], doc["s"], "{id}: view key");
        assert_eq!(row["value"], doc["n"], "{id}: view value");
        assert_eq!(row["id"], doc["_id"], "{id}: view id");
    }
}

#[tokio::test]
async fn changes_returns_latest_per_document_and_pending() {
    let s = Server::start(false);
    s.fresh_db("latest").await;

    let (_, first) = s.put("/latest/a", json!({"v": 1})).await;
    let mut rev = first["rev"].as_str().unwrap().to_string();
    for value in [2, 3, 4] {
        let (status, body) = s.put("/latest/a", json!({"v": value, "_rev": rev})).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        rev = body["rev"].as_str().unwrap().to_string();
    }
    let (_, b) = s.put("/latest/b", json!({"v": 1})).await;
    let b_rev = b["rev"].as_str().unwrap().to_string();

    let (status, body) = s.get("/latest/_changes?since=0&limit=100").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().unwrap().len(), 2);
    let a_row = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "a")
        .unwrap();
    assert_eq!(a_row["changes"][0]["rev"], rev);
    assert_eq!(body["pending"], 0);

    let (_, page) = s.get("/latest/_changes?since=0&limit=1").await;
    assert_eq!(page["results"].as_array().unwrap().len(), 1);
    assert_eq!(page["pending"], 1);
    // Resuming from a page's last_seq starts after that row, not at it.
    let (_, next) = s
        .get(&format!(
            "/latest/_changes?since={}&limit=1",
            page["last_seq"]
        ))
        .await;
    assert_eq!(next["results"].as_array().unwrap().len(), 1, "{next}");
    assert_ne!(next["results"][0]["id"], page["results"][0]["id"], "{next}");
    assert_eq!(next["pending"], 0);

    let (_, descending) = s
        .get("/latest/_changes?since=0&limit=1&descending=true")
        .await;
    assert_eq!(descending["results"].as_array().unwrap().len(), 1);
    assert_eq!(descending["results"][0]["id"], "b");
    assert_eq!(descending["pending"], 1);

    let (_, all_docs) = s
        .get("/latest/_changes?since=0&style=all_docs&include_docs=true")
        .await;
    let b_row = all_docs["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "b")
        .unwrap();
    assert_eq!(b_row["changes"][0]["rev"], b_rev);
    assert_eq!(b_row["doc"]["_rev"], b_rev);
}

#[tokio::test]
async fn changes_style_all_docs_lists_the_winner_first() {
    let s = Server::start(false);
    s.fresh_db("chstyle").await;
    let base = "af8ad5700570192f918c22b2bb800165";
    let mid = "b705850634addeeb5a78b55d88fd3977";
    let live = "cccccccccccccccccccccccccccccccc";
    let deleted = "99999999999999999999999999999999";
    let winner = "ffffffffffffffffffffffffffffffff";
    let docs = [
        json!({"_id": "foo", "_rev": format!("1-{base}"),
               "_revisions": {"start": 1, "ids": [base]}, "v": 0}),
        json!({"_id": "foo", "_rev": format!("3-{live}"),
               "_revisions": {"start": 3, "ids": [live, mid, base]}, "v": 3}),
        json!({"_id": "foo", "_rev": format!("2-{deleted}"), "_deleted": true,
               "_revisions": {"start": 2, "ids": [deleted, base]}, "v": 2}),
        json!({"_id": "foo", "_rev": format!("4-{winner}"),
               "_revisions": {"start": 4, "ids": [winner, mid, live, base]}, "v": 4}),
    ];
    for doc in docs {
        let (status, body) = s
            .post(
                "/chstyle/_bulk_docs",
                json!({"docs": [doc], "new_edits": false}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    let (status, body) = s
        .get("/chstyle/_changes?style=all_docs&include_docs=true&conflicts=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let revs: Vec<String> = body["results"][0]["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|change| change["rev"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        revs,
        vec![
            format!("4-{winner}"),
            format!("3-{live}"),
            format!("2-{deleted}")
        ],
        "winner first, then leaves by descending position"
    );
    assert_eq!(
        body["results"][0]["doc"]["_conflicts"],
        json!([format!("3-{live}")])
    );
}

#[tokio::test]
async fn changes_filters_support_design_named_view_and_shorthand() {
    let s = Server::start(false);
    s.fresh_db("chfilter").await;
    let docs = json!({"docs": [
        {"_id": "0", "integer": 0},
        {"_id": "1", "integer": 1},
        {"_id": "2", "integer": 2},
        {"_id": "_design/foo", "integer": 4,
         "filters": {"even": "function (doc) { return doc.integer % 2 === 1; }"},
         "views": {"even": {"map": "function (doc) { if (doc.integer % 2 === 1) { emit(doc._id, null); } }"}}},
        {"_id": "_design/even", "filters": {"even": "function (doc) { return doc.integer % 2 === 0; }"}}
    ]});
    let (status, body) = s.post("/chfilter/_bulk_docs", docs).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body) = s.get("/chfilter/_changes?filter=_design").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut ids: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["_design/even", "_design/foo"]);

    let (status, body) = s.get("/chfilter/_changes?filter=foo/even").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["1"]);

    let (status, body) = s.get("/chfilter/_changes?filter=_view&view=foo/even").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["1"]);

    let (status, body) = s.get("/chfilter/_changes?filter=even").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["0", "2", "_design/foo"]);

    let (status, _) = s.get("/chfilter/_changes?filter=missing/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = s.get("/chfilter/_changes?filter=_view").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn filtered_changes_with_a_limit_finds_sparse_matches_beyond_one_page() {
    // A filter that rarely matches forces the scan to refill past several
    // internal pages before the limit is met.
    let s = Server::start(false);
    s.fresh_db("chsparse").await;
    let mut docs: Vec<Value> = (0..6000)
        .map(|i| json!({"_id": format!("d{i:05}"), "hit": i % 1500 == 1499}))
        .collect();
    docs.push(json!({"_id": "_design/f",
        "filters": {"hit": "function (doc) { return doc.hit === true; }"}}));
    let (status, body) = s
        .post("/chsparse/_bulk_docs", json!({ "docs": docs }))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (_, all) = s.get("/chsparse/_changes").await;
    let all = all["results"].as_array().unwrap();
    let expected: Vec<(&Value, &str)> = all
        .iter()
        .filter(|row| {
            row["id"]
                .as_str()
                .map(|id| {
                    id.starts_with('d')
                        && id[1..].parse::<u32>().map(|i| i % 1500 == 1499) == Ok(true)
                })
                .unwrap_or(false)
        })
        .map(|row| (&row["seq"], row["id"].as_str().unwrap()))
        .collect();
    assert_eq!(expected.len(), 4);

    for limit in [1usize, 2, 3, 10] {
        let (status, body) = s
            .get(&format!("/chsparse/_changes?filter=f/hit&limit={limit}"))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body["results"].as_array().unwrap();
        let want = &expected[..limit.min(expected.len())];
        let got: Vec<(&Value, &str)> = rows
            .iter()
            .map(|row| (&row["seq"], row["id"].as_str().unwrap()))
            .collect();
        assert_eq!(got, want, "limit {limit}");
        assert_eq!(
            body["last_seq"],
            **got.last().map(|(seq, _)| seq).unwrap(),
            "limit {limit}"
        );
    }

    // Starting after the second match leaves the other two.
    let since = expected[1].0.clone();
    let (_, body) = s
        .get(&format!(
            "/chsparse/_changes?filter=f/hit&limit=1&since={since}"
        ))
        .await;
    assert_eq!(body["results"][0]["id"], json!(expected[2].1));
}

#[tokio::test]
async fn bulk_docs_handles_local_docs_and_rejects_replicated_docs_without_rev() {
    let s = Server::start(false);
    s.fresh_db("bulkedge").await;

    let (status, body) = s
        .post(
            "/bulkedge/_bulk_docs",
            json!({"docs": [{"_id": "_local/bar"}, {"_id": "baz"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body[0]["ok"], true);
    assert_eq!(body[0]["id"], "_local/bar");
    assert_eq!(body[1]["ok"], true);

    let (status, body) = s.get("/bulkedge/_local/bar").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rev = body["_rev"].as_str().unwrap().to_string();
    let (status, body) = s
        .post(
            "/bulkedge/_bulk_docs",
            json!({"docs": [{"_id": "_local/bar", "_rev": rev, "_deleted": true}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, _) = s.get("/bulkedge/_local/bar").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = s
        .post(
            "/bulkedge/_bulk_docs",
            json!({"docs": [{"_id": "no-rev"}], "new_edits": false}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "bad_request");
}

#[tokio::test]
async fn cluster_validation_functions_follow_design_docs_across_shards() {
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("clvalidate").await;
    }
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    // Only node 0 stores the design document, as if node 1 missed the write
    // and anti-entropy (off here) has not repaired it. Node 1 must still apply
    // the validator when it coordinates a write.
    let (status, body) = nodes[0]
        .req_with(
            Method::POST,
            "/clvalidate/_bulk_docs",
            Some(json!({"docs": [{
                "_id": "_design/validate",
                "validate_doc_update": "function (doc) { \
                    if (doc.ok !== true) { throw {forbidden: 'need ok'}; } \
                }"
            }]})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, _) = nodes[1]
        .req_with(Method::GET, "/clvalidate/_design/validate", None, &internal)
        .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "node 1 must not hold the design doc"
    );

    let (status, body) = nodes[1]
        .put("/clvalidate/accepted", json!({"ok": true}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = nodes[1]
        .put("/clvalidate/rejected", json!({"ok": false}))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["reason"], "need ok");
    let (status, _) = nodes[0].get("/clvalidate/rejected").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cluster_bulk_docs_use_the_cluster_merged_validators() {
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("clvalbulk").await;
    }
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    // Only node 0 stores the design document, as if node 1 missed the write.
    let (status, body) = nodes[0]
        .req_with(
            Method::POST,
            "/clvalbulk/_bulk_docs",
            Some(json!({"docs": [{
                "_id": "_design/validate",
                "validate_doc_update": "function (doc) { \
                    if (doc.ok !== true) { throw {forbidden: 'need ok'}; } \
                }"
            }]})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // A batch coordinated by node 1 must see the same validators as a
    // single-document write does.
    let (status, body) = nodes[1]
        .post(
            "/clvalbulk/_bulk_docs",
            json!({"docs": [
                {"_id": "accepted", "ok": true},
                {"_id": "rejected", "ok": false}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body[0]["ok"], true, "{body}");
    assert_eq!(body[1]["error"], "forbidden", "{body}");
    assert_eq!(body[1]["reason"], "need ok", "{body}");
    let (status, _) = nodes[0].get("/clvalbulk/rejected").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cluster_validation_still_applies_with_a_peer_down() {
    let mut nodes = start_cluster_with(3, "3600", 3);
    for node in &nodes {
        node.fresh_db("clvaldown").await;
    }
    let (status, body) = nodes[0]
        .put(
            "/clvaldown/_design/validate",
            json!({"validate_doc_update": "function (doc) { \
                if (doc.ok !== true) { throw {forbidden: 'need ok'}; } \
            }"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    nodes[2].kill();

    // The local validators apply whether or not every peer answers, to
    // single-document and batch writes alike.
    let (status, body) = nodes[1]
        .put("/clvaldown/rejected", json!({"ok": false}))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, body) = nodes[1]
        .post(
            "/clvaldown/_bulk_docs",
            json!({"docs": [{"_id": "rejected2", "ok": false}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body[0]["error"], "forbidden", "{body}");
}

#[tokio::test]
async fn cluster_validators_are_cached_until_a_design_doc_changes() {
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("clvalcache").await;
    }
    let validator = |reason: &str| {
        format!("function (doc) {{ if (doc.ok !== true) {{ throw {{forbidden: '{reason}'}}; }} }}")
    };
    // Node 1 coordinates a write with no validators, so it caches none.
    let (status, body) = nodes[1]
        .put("/clvalcache/before", json!({"ok": false}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // A design-document write reaches node 1 too and drops its cache at once.
    let (status, body) = nodes[0]
        .put(
            "/clvalcache/_design/validate",
            json!({"validate_doc_update": validator("first")}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let rev = body["rev"].as_str().unwrap().to_string();
    let (status, body) = nodes[1]
        .put("/clvalcache/rejected", json!({"ok": false}))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["reason"], "first");

    // A change only node 0 receives, as if node 1 missed it, still reaches
    // node 1's writes once the cached set expires.
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let (status, body) = nodes[0]
        .req_with(
            Method::POST,
            "/clvalcache/_bulk_docs",
            Some(json!({"docs": [{
                "_id": "_design/validate",
                "_rev": rev,
                "validate_doc_update": validator("second"),
            }]})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    // Node 1 keeps its cached set until the cache expires, so it still says
    // "first" for a while. A rejected write changes nothing, so retry it
    // until the new validator shows.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = nodes[1]
            .put("/clvalcache/rejected", json!({"ok": false}))
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        if body["reason"] == "second" {
            break;
        }
        assert_eq!(body["reason"], "first", "{body}");
        assert!(
            std::time::Instant::now() < deadline,
            "node 1 never dropped its cached validator: {body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn cluster_validation_uses_the_newest_design_doc_revision() {
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("clvalnew").await;
    }
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let validator = |reason: &str| {
        format!("function (doc) {{ if (doc.ok !== true) {{ throw {{forbidden: '{reason}'}}; }} }}")
    };
    let one = "11111111111111111111111111111111";
    let two = "22222222222222222222222222222222";
    // Node 1 still has revision 1; node 0 has moved on to revision 2.
    for (node, revs, reason) in [(1, vec![one], "old"), (0, vec![two, one], "new")] {
        let (status, body) = nodes[node]
            .req_with(
                Method::POST,
                "/clvalnew/_bulk_docs",
                Some(json!({"new_edits": false, "docs": [{
                    "_id": "_design/validate",
                    "_rev": format!("{}-{}", revs.len(), revs[0]),
                    "_revisions": {"start": revs.len(), "ids": revs},
                    "validate_doc_update": validator(reason),
                }]})),
                &internal,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }
    for node in &nodes {
        let (status, body) = node.put("/clvalnew/rejected", json!({"ok": false})).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["reason"], "new", "{body}");
    }
}

#[tokio::test]
async fn validate_doc_update_skips_local_docs_in_bulk_docs() {
    // CouchDB runs no validate_doc_update for _local documents, however they
    // are written.
    let s = Server::start(false);
    s.fresh_db("validlocal").await;
    let (status, body) = s
        .put(
            "/validlocal/_design/deny",
            json!({"validate_doc_update": "function () { throw {forbidden: 'no'}; }"}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .post(
            "/validlocal/_bulk_docs",
            json!({"docs": [{"_id": "_local/a", "x": 1}, {"_id": "b"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body[0]["ok"], true, "{body}");
    assert_eq!(body[1]["error"], "forbidden", "{body}");
}

#[tokio::test]
async fn validate_doc_update_rejects_writes_like_couchdb() {
    let s = Server::start(false);
    s.fresh_db("validation").await;
    let (status, body) = s
        .put(
            "/validation/_design/validate",
            json!({
                "validate_doc_update": "function (newDoc) { \
                    if (newDoc.foo === undefined) { throw {unauthorized: 'Document must have a foo.'}; } \
                    if (newDoc.foo === 'bad') { throw {forbidden: 'bad foo'}; } \
                }"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body) = s.put("/validation/no-foo", json!({"bar": 1})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["error"], "unauthorized");
    assert_eq!(body["reason"], "Document must have a foo.");
    let (status, _) = s.get("/validation/no-foo").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = s.put("/validation/bad", json!({"foo": "bad"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"], "forbidden");
    assert_eq!(body["reason"], "bad foo");

    let (status, body) = s.put("/validation/ok", json!({"foo": "good"})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // A batch applies valid documents and reports rejected ones in order.
    let (status, body) = s
        .post(
            "/validation/_bulk_docs",
            json!({"docs": [
                {"_id": "ok2", "foo": "good"},
                {"_id": "missing", "bar": 1},
                {"_id": "bad", "foo": "bad"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body.as_array().unwrap().len(), 3);
    assert_eq!(body[0]["ok"], true);
    assert_eq!(body[1]["error"], "unauthorized");
    assert_eq!(body[2]["error"], "forbidden");
    let (status, _) = s.get("/validation/ok2").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.get("/validation/missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The replicated-write path is validated too.
    let (status, body) = s
        .post(
            "/validation/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "replicated", "_rev": "1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                 "_revisions": {"start": 1, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body[0]["error"], "unauthorized");
    let (status, _) = s.get("/validation/replicated").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Removing the required field from an update is rejected without changing
    // the stored revision.
    let (_, current) = s.get("/validation/ok").await;
    let rev = current["_rev"].as_str().unwrap();
    let (status, body) = s
        .put("/validation/ok", json!({"_rev": rev, "bar": 2}))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    let (_, current) = s.get("/validation/ok").await;
    assert_eq!(current["_rev"], rev);
}

#[tokio::test]
async fn conflict_leaves_are_listed_by_descending_position() {
    let s = Server::start(false);
    s.fresh_db("corder").await;
    let (_, first) = s.put("/corder/doc", json!({"v": 0})).await;
    let root = first["rev"].as_str().unwrap().to_string();
    let lower = "2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let winner = "2-ffffffffffffffffffffffffffffffff";
    let (status, body) = s
        .post(
            "/corder/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "doc", "_rev": lower, "v": 1},
                {"_id": "doc", "_rev": winner, "v": 2}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s.get("/corder/doc?conflicts=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_rev"], winner);
    assert_eq!(
        body["_conflicts"],
        json!([lower, root]),
        "higher revision positions come first"
    );
}

#[tokio::test]
async fn latest_resolves_interior_revisions_to_the_branch_tip() {
    let s = Server::start(false);
    s.fresh_db("latestrev").await;
    let (_, first) = s.put("/latestrev/doc", json!({"version": "first"})).await;
    let first_rev = first["rev"].as_str().unwrap().to_string();
    let (_, second) = s
        .put(
            "/latestrev/doc",
            json!({"_rev": first_rev, "version": "second"}),
        )
        .await;
    let second_rev = second["rev"].as_str().unwrap().to_string();

    let (status, body) = s
        .get(&format!("/latestrev/doc?rev={first_rev}&latest=true"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_rev"], second_rev);
    assert_eq!(body["version"], "second");

    let open = format!(
        "/latestrev/doc?open_revs={}&latest=true",
        urlencoding(&json!([first_rev, second_rev]))
    );
    let (status, body) = s.get(&open).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body.as_array().unwrap().len(),
        1,
        "both requested revisions resolve to one branch tip: {body}"
    );
    assert_eq!(body[0]["ok"]["_rev"], second_rev);

    let (status, body) = s
        .get(&format!(
            "/latestrev/doc?open_revs={}",
            urlencoding(&json!(["1-almost", "not-a-rev"]))
        ))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

#[tokio::test]
async fn latest_returns_every_leaf_below_the_requested_revision() {
    let s = Server::start(false);
    s.fresh_db("latestfork").await;
    let (_, root) = s.put("/latestfork/doc", json!({"v": "root"})).await;
    let root_rev = root["rev"].as_str().unwrap().to_string();
    let (_, left) = s
        .put("/latestfork/doc", json!({"_rev": root_rev, "v": "left"}))
        .await;
    let left_rev = left["rev"].as_str().unwrap().to_string();
    // A second branch off the same parent, written as a replicated edit.
    let right_rev = format!("2-{}", "b".repeat(32));
    let (status, _) = s
        .post(
            "/latestfork/_bulk_docs",
            json!({"new_edits": false, "docs": [{
                "_id": "doc", "_rev": right_rev, "v": "right",
                "_revisions": {"start": 2, "ids": ["b".repeat(32), root_rev[2..]]}
            }]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let open = format!(
        "/latestfork/doc?open_revs={}&latest=true",
        urlencoding(&json!([root_rev]))
    );
    let (status, body) = s.get(&open).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut revs: Vec<String> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["ok"]["_rev"].as_str().unwrap().to_string())
        .collect();
    revs.sort();
    let mut expected = vec![left_rev, right_rev];
    expected.sort();
    assert_eq!(revs, expected, "every leaf below the revision: {body}");

    // A single `rev` resolves to the winning leaf among the descendants.
    let winner = expected.iter().max().cloned().unwrap();
    let (status, body) = s
        .get(&format!("/latestfork/doc?rev={root_rev}&latest=true"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_rev"], winner);
}

#[tokio::test]
async fn upper_case_hex_revisions_match_and_respond_in_lower_case() {
    let s = Server::start(false);
    s.fresh_db("revcase").await;
    let (_, put) = s.put("/revcase/doc", json!({"v": 1})).await;
    let rev = put["rev"].as_str().unwrap().to_string();
    let upper = rev.to_uppercase();
    assert_ne!(rev, upper);

    let (status, body) = s.get(&format!("/revcase/doc?rev={upper}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_rev"], rev);

    let open = format!("/revcase/doc?open_revs={}", urlencoding(&json!([upper])));
    let (status, body) = s.get(&open).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body[0]["ok"]["_rev"], rev);

    let (status, body) = s.put("/revcase/doc", json!({"_rev": upper, "v": 2})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

/// `_design_docs` and `_local_docs` are `_all_docs` over the `_design/` and
/// `_local/` ranges. Shapes below were taken from CouchDB 3.5.3: `_design_docs`
/// counts only design documents in `total_rows` (the offset still counts
/// every document before the range); `_local_docs` reports null `total_rows`
/// and `offset`, revision `0-1`, and a `not_found` row for a missing local key.
#[tokio::test]
async fn design_docs_and_local_docs_list_their_ranges() {
    let s = Server::start(false);
    s.fresh_db("dd").await;
    for (path, doc) in [
        ("/dd/_design/x", json!({"views": {}})),
        ("/dd/_design/y", json!({"views": {}})),
        ("/dd/d1", json!({"a": 1})),
        ("/dd/zz", json!({"b": 2})),
        ("/dd/_local/l1", json!({"a": 1})),
        ("/dd/_local/l2", json!({"b": 2})),
    ] {
        let (status, body) = s.put(path, doc).await;
        assert_eq!(status, StatusCode::CREATED, "{path}: {body}");
    }
    let ids = |body: &Value| -> Vec<String> {
        body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap_or("-").to_string())
            .collect()
    };

    let (status, body) = s.get("/dd/_design_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body), ["_design/x", "_design/y"]);
    assert_eq!(body["total_rows"], 2);
    assert_eq!(body["offset"], 0);
    assert_eq!(body["rows"][0]["key"], "_design/x");
    assert!(body["rows"][0]["value"]["rev"].is_string());

    let (_, body) = s.get("/dd/_design_docs?include_docs=true&limit=1").await;
    assert_eq!(body["rows"][0]["doc"]["_id"], "_design/x");
    assert_eq!(ids(&body), ["_design/x"]);
    let (_, body) = s.get("/dd/_design_docs?descending=true").await;
    assert_eq!(ids(&body), ["_design/y", "_design/x"]);
    assert_eq!(body["offset"], 2);
    let (_, body) = s.get("/dd/_design_docs?skip=1").await;
    assert_eq!(ids(&body), ["_design/y"]);
    assert_eq!(body["offset"], 1);
    let (_, body) = s
        .get("/dd/_design_docs?startkey=%22_design/x%22&endkey=%22_design/y%22&inclusive_end=false")
        .await;
    assert_eq!(ids(&body), ["_design/x"]);
    let (_, body) = s.get("/dd/_design_docs?key=%22_design/y%22").await;
    assert_eq!(ids(&body), ["_design/y"]);
    assert_eq!(body["offset"], 1);
    let (_, body) = s.get("/dd/_design_docs?startkey=%22a%22").await;
    assert_eq!(ids(&body), Vec::<String>::new());
    assert_eq!(body["offset"], 2);
    let (_, body) = s
        .post(
            "/dd/_design_docs",
            json!({"keys": ["_design/y", "d1", "nope", 1, "_design/x"]}),
        )
        .await;
    assert_eq!(ids(&body), ["_design/y", "_design/x"]);
    assert_eq!(body["total_rows"], 2);
    assert_eq!(body["offset"], Value::Null);
    let (status, body) = s.get("/dd/_design_docs?descending=maybe").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "query_parse_error");
    let (status, body) = s.put("/dd/_design_docs", json!({})).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{body}");
    assert_eq!(body["reason"], "Only GET,HEAD,POST allowed");

    let (status, body) = s.get("/dd/_local_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(ids(&body), ["_local/l1", "_local/l2"]);
    assert_eq!(body["total_rows"], Value::Null);
    assert_eq!(body["offset"], Value::Null);
    assert_eq!(body["rows"][0]["value"], json!({"rev": "0-1"}));
    let (_, body) = s.get("/dd/_local_docs?include_docs=true&limit=1").await;
    assert_eq!(
        body["rows"][0]["doc"],
        json!({"_id": "_local/l1", "_rev": "0-1", "a": 1})
    );
    let (_, body) = s
        .get("/dd/_local_docs?descending=true&limit=1&skip=1")
        .await;
    assert_eq!(ids(&body), ["_local/l1"]);
    let (_, body) = s
        .get("/dd/_local_docs?endkey=%22_local/l2%22&inclusive_end=false")
        .await;
    assert_eq!(ids(&body), ["_local/l1"]);
    let (_, body) = s.get("/dd/_local_docs?startkey=%22_local/l2%22").await;
    assert_eq!(ids(&body), ["_local/l2"]);
    let (_, body) = s
        .post(
            "/dd/_local_docs",
            json!({"keys": ["_local/l1", "d1", "_local/zz"]}),
        )
        .await;
    assert_eq!(body["rows"][0]["id"], "_local/l1");
    assert_eq!(
        body["rows"][1],
        json!({"key": "_local/zz", "error": "not_found"})
    );
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(2));
    let (_, body) = s.get("/dd/_local_docs?update_seq=true").await;
    assert_eq!(body["update_seq"], Value::Null);
    let (status, body) = s.get("/dd/_local_docs?key=abc").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, _) = s.get("/nodb/_design_docs").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn cluster_design_docs_come_from_every_shard() {
    let nodes = start_cluster_with(2, "3600", 1);
    for node in &nodes {
        node.fresh_db("cdd").await;
    }
    let (status, body) = nodes[0]
        .post(
            "/cdd/_bulk_docs",
            json!({"docs": [
                {"_id": "_design/a", "views": {}},
                {"_id": "_design/b", "views": {}},
                {"_id": "_design/c", "views": {}},
                {"_id": "a1"}, {"_id": "z1"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for node in &nodes {
        let (status, body) = node.get("/cdd/_design_docs").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let ids: Vec<&str> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids,
            ["_design/a", "_design/b", "_design/c"],
            "{}",
            node.base
        );
        assert_eq!(body["total_rows"], 3, "{}", node.base);
        let (_, body) = node
            .get("/cdd/_design_docs?descending=true&limit=2&include_docs=true")
            .await;
        assert_eq!(body["rows"][0]["id"], "_design/c", "{}", node.base);
        assert_eq!(body["rows"][1]["doc"]["_id"], "_design/b", "{}", node.base);
        assert_eq!(body["total_rows"], 3, "{}", node.base);
        let (_, body) = node
            .post(
                "/cdd/_design_docs",
                json!({"keys": ["_design/b", "a1", "_design/q"]}),
            )
            .await;
        assert_eq!(body["rows"][0]["id"], "_design/b", "{}", node.base);
        assert_eq!(body["rows"][1]["error"], "not_found", "{}", node.base);
        assert_eq!(
            body["rows"].as_array().map(Vec::len),
            Some(2),
            "{}",
            node.base
        );
    }
}

/// CouchDB 3.5.3 answers a non-string entry in `keys` with a `not_found` row
/// that echoes the key; douchdb used to drop it, shifting every later row.
#[tokio::test]
async fn all_docs_keys_with_non_string_entries_get_not_found_rows() {
    let s = Server::start(false);
    s.fresh_db("nonstr").await;
    let (status, _) = s.put("/nonstr/d1", json!({"a": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let keys = json!([1, "d1", null, {"a": 1}, "zz", true]);
    let expected = |rows: &Value| {
        assert_eq!(rows[0], json!({"key": 1, "error": "not_found"}));
        assert_eq!(rows[1]["id"], "d1");
        assert_eq!(rows[2], json!({"key": null, "error": "not_found"}));
        assert_eq!(rows[3], json!({"key": {"a": 1}, "error": "not_found"}));
        assert_eq!(rows[4], json!({"key": "zz", "error": "not_found"}));
        assert_eq!(rows[5], json!({"key": true, "error": "not_found"}));
        assert_eq!(rows.as_array().map(Vec::len), Some(6));
    };
    let (status, body) = s
        .post("/nonstr/_all_docs", json!({ "keys": keys.clone() }))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    expected(&body["rows"]);
    let encoded = keys.to_string().replace('"', "%22");
    let (status, body) = s.get(&format!("/nonstr/_all_docs?keys={encoded}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    expected(&body["rows"]);

    // `skip` and `limit` count the error rows too.
    let (status, body) = s
        .post(
            "/nonstr/_all_docs?skip=1&limit=2",
            json!({"keys": [1, "d1", null]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["id"], "d1");
    assert_eq!(body["rows"][1], json!({"key": null, "error": "not_found"}));
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(2));
}

#[tokio::test]
async fn all_docs_key_excludes_deleted_and_reports_update_seq() {
    let s = Server::start(false);
    s.fresh_db("allkey").await;
    let (_, body) = s
        .post(
            "/allkey/_bulk_docs",
            json!({"docs": [{"_id": "0"}, {"_id": "1"}]}),
        )
        .await;
    let rev = body[1]["rev"].as_str().unwrap().to_string();
    let (status, _) = s.delete(&format!("/allkey/1?rev={rev}")).await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = s.get("/allkey/_all_docs?key=%221%22").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 0);

    let (status, body) = s.get("/allkey/_all_docs?key=%220%22").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["id"], "0");

    let (status, body) = s.get("/allkey/_all_docs?update_seq=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["update_seq"].as_u64().unwrap() >= 2);
}

fn urlencoding(value: &Value) -> String {
    value
        .to_string()
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}

#[tokio::test]
async fn percent_encoded_slash_in_database_names() {
    let s = Server::start(false);
    let (status, body) = s.put("/encoded%2Fdb", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s.get("/encoded%2Fdb").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["db_name"], "encoded/db");

    let (status, body) = s.put("/encoded%2Fdb/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s.get("/encoded%2Fdb/doc").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["v"], 1);

    let (status, _) = s.delete("/encoded%2Fdb").await;
    assert_eq!(status, StatusCode::OK);
}

/// CouchDB (mochiweb) decodes '+' as a space in URL paths as well as in query
/// strings, so `a+b` names the document "a b" and only `a%2Bb` names "a+b".
/// Verified against couchdb:3.5; a literal plus in a path would diverge.
#[tokio::test]
async fn plus_in_paths_and_query_strings_decodes_as_space() {
    let s = Server::start(false);
    let (status, body) = s.put("/plusdb", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body) = s.put("/plusdb/a+b", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["id"], "a b");
    let (status, body) = s.put("/plusdb/c%2Bd", json!({"v": 2})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["id"], "c+d");

    let (status, body) = s.get("/plusdb/a+b").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_id"], "a b");
    let (status, body) = s.get("/plusdb/a%20b").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s.get("/plusdb/a%2Bb").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, body) = s.get("/plusdb/c%2Bd").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s.get("/plusdb/c+d").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");

    let (status, body) = s.get("/plusdb/_all_docs?key=%22a+b%22").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["id"], "a b");
    let (status, body) = s.get("/plusdb/_all_docs?key=%22a%2Bb%22").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(0));
}

/// The database segment is decoded like any other path segment, so `a+b` is
/// the name "a b", which CouchDB 3.5 refuses on create with
/// `illegal_database_name` and reports as missing everywhere else. Only
/// `a%2Bb` names the legal database "a+b".
#[tokio::test]
async fn plus_in_database_names_decodes_as_space() {
    let s = Server::start(false);
    let reason = |name: &str| {
        format!(
            "Name: '{name}'. Only lowercase characters (a-z), digits (0-9), and any of the \
             characters _, $, (, ), +, -, and / are allowed. Must begin with a letter."
        )
    };
    for (path, name) in [("/pa+b", "pa b"), ("/px%20y", "px y")] {
        let (status, body) = s.put(path, json!({})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(body["error"], "illegal_database_name", "{path}: {body}");
        assert_eq!(body["reason"], reason(name), "{path}: {body}");
    }

    let (status, body) = s.put("/pa%2Bb", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s.get("/pa%2Bb").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["db_name"], "pa+b");
    let (status, body) = s.put("/pa%2Bb/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (_, body) = s.get("/_all_dbs").await;
    let listed: Vec<&str> = body
        .as_array()
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    assert!(listed.contains(&"pa+b"), "{body}");
    assert!(!listed.contains(&"pa b"), "{body}");

    // Reading, writing or deleting the database "pa b" finds nothing.
    for (method, path) in [
        ("GET", "/pa+b"),
        ("DELETE", "/pa+b"),
        ("POST", "/pa+b"),
        ("GET", "/pa+b/doc"),
        ("PUT", "/pa+b/doc"),
        ("GET", "/pa+b/_all_docs"),
    ] {
        let (status, body) = match method {
            "GET" => s.get(path).await,
            "DELETE" => s.delete(path).await,
            "POST" => s.post(path, json!({})).await,
            _ => s.put(path, json!({})).await,
        };
        assert_eq!(status, StatusCode::NOT_FOUND, "{method} {path}: {body}");
        assert_eq!(body["error"], "not_found", "{method} {path}: {body}");
        assert_eq!(
            body["reason"], "Database does not exist.",
            "{method} {path}: {body}"
        );
    }
    let (status, body) = s.get("/pa%2Bb/doc").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s.delete("/pa%2Bb").await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// Stop the server for good, then start it again on the same data directory.
/// `restart` alone leaves the old process running, and the pooled HTTP
/// connections point at it, so a test of what a fresh process reads from disk
/// needs both the kill and a new client.
/// A name containing '/' is a database like any other: it is listed in
/// `_all_dbs` (sorted by name as CouchDB does) and is reopened after a restart.
#[tokio::test]
async fn slash_database_is_listed_and_survives_restart() {
    let mut s = Server::start(false);
    for name in [
        "slash%2Fplain",
        "slash%2Fa%2Fb",
        "slash%2B%24%28%29",
        "slashz",
    ] {
        let (status, body) = s.put(&format!("/{name}"), json!({})).await;
        assert_eq!(status, StatusCode::CREATED, "{name}: {body}");
    }
    let (status, body) = s.put("/slash%2Fa%2Fb/doc", json!({"v": 7})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let listed = |body: &Value| -> Vec<String> {
        body.as_array()
            .expect("array")
            .iter()
            .filter_map(|n| n.as_str())
            .filter(|n| n.starts_with("slash"))
            .map(str::to_string)
            .collect()
    };
    let expected = ["slash+$()", "slash/a/b", "slash/plain", "slashz"];
    let (_, body) = s.get("/_all_dbs").await;
    assert_eq!(listed(&body), expected, "{body}");

    s.restart();
    let (_, body) = s.get("/_all_dbs").await;
    assert_eq!(listed(&body), expected, "after restart: {body}");
    let (status, body) = s.get("/slash%2Fa%2Fb/doc").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["v"], 7);
    let (status, body) = s.get("/slash%2B%24%28%29").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["db_name"], "slash+$()");
}

/// Databases "a" and "a/b" are independent: neither shares a directory with
/// the other, and each can be deleted and recreated without touching the other.
#[tokio::test]
async fn slash_database_and_parent_name_coexist() {
    let mut s = Server::start(false);
    let (status, body) = s.put("/coex%2Fb", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    s.put("/coex%2Fb/doc", json!({"v": "child"})).await;
    let (status, body) = s.put("/coex", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    s.put("/coex/doc", json!({"v": "parent"})).await;

    let (status, _) = s.delete("/coex").await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = s.get("/coex%2Fb/doc").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["v"], "child");
    let (status, _) = s.get("/coex").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = s.put("/coex", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    s.put("/coex/doc", json!({"v": "parent2"})).await;
    let (status, _) = s.delete("/coex%2Fb").await;
    assert_eq!(status, StatusCode::OK);
    s.restart();
    let (status, body) = s.get("/coex/doc").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["v"], "parent2");
    let (status, _) = s.get("/coex%2Fb").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// A data directory written by an older version keeps "a/b" at `a/b/`. On
/// startup it is moved to its flat directory, once, and the parent database
/// "a" that shared the tree is left intact.
#[tokio::test]
async fn nested_slash_database_directories_are_migrated_on_startup() {
    let mut s = Server::start(false);
    // The database name is not stored in the directory, so flat databases can
    // be moved into the old nested layout while the server is stopped.
    for name in ["mig", "migb", "migc"] {
        let (status, body) = s.put(&format!("/{name}"), json!({})).await;
        assert_eq!(status, StatusCode::CREATED, "{name}: {body}");
        s.put(&format!("/{name}/doc"), json!({"v": name})).await;
    }
    s.kill();
    std::fs::rename(s.dir.join("migb"), s.dir.join("mig").join("b")).expect("nest b");
    std::fs::rename(s.dir.join("migc"), s.dir.join("mig/b/c")).expect("nest c");

    s.restart();
    let (_, body) = s.get("/_all_dbs").await;
    let listed: Vec<&str> = body
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|n| n.as_str())
        .filter(|n| n.starts_with("mig"))
        .collect();
    assert_eq!(listed, ["mig", "mig/b", "mig/b/c"], "{body}");
    for (name, value) in [("mig", "mig"), ("mig%2Fb", "migb"), ("mig%2Fb%2Fc", "migc")] {
        let (status, body) = s.get(&format!("/{name}/doc")).await;
        assert_eq!(status, StatusCode::OK, "{name}: {body}");
        assert_eq!(body["v"], value);
    }
    assert!(!s.dir.join("mig/b").exists(), "old nested dir left behind");
    assert!(s.dir.join("mig%2Fb").join("db.json").is_file());

    // A second startup has nothing left to move.
    s.restart();
    let (status, body) = s.get("/mig%2Fb%2Fc/doc").await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// A db.json that does not parse is a server-side fault, not a missing
/// database: the client gets 500 and the file is left for the operator.
#[tokio::test]
async fn corrupt_db_json_is_a_server_error_not_a_404() {
    let mut s = Server::start(false);
    let (status, body) = s.put("/corruptmeta", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    s.kill();
    let db_json = s.dir.join("corruptmeta").join("db.json");
    std::fs::write(&db_json, b"{ not json").expect("corrupt db.json");

    s.restart();
    let (status, body) = s.get("/corruptmeta").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(
        std::fs::read(&db_json).expect("db.json"),
        b"{ not json",
        "the corrupt file must not be overwritten"
    );
}

#[tokio::test]
async fn replication_state_fields_round_trip() {
    let s = Server::start(false);
    s.fresh_db("repstate").await;
    let (status, body) = s
        .post(
            "/repstate",
            json!({
                "_replication_id": "test",
                "_replication_state": "triggered",
                "_replication_state_time": 1,
                "_replication_stats": {}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    // CouchDB hashes these fields as part of the body.
    assert_eq!(body["rev"], "1-dd04afb29523fb7974448551ed4c2a37");
    let id = body["id"].as_str().unwrap();
    let (status, body) = s.get(&format!("/repstate/{id}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_replication_id"], "test");
    assert_eq!(body["_replication_state"], "triggered");
    assert_eq!(body["_replication_state_time"], 1);
    assert_eq!(body["_replication_stats"], json!({}));
}

#[tokio::test]
async fn revs_limit_truncates_each_revisions_path() {
    let s = Server::start(false);
    s.fresh_db("revlimit").await;
    let base = "af8ad5700570192f918c22b2bb800165";
    let b705 = "b705850634addeeb5a78b55d88fd3977";
    let live = "0f619d79d9ac0858051a1e271f9b2128";
    let deleted = "4026bdae07b424575d0a28a582302f38";
    let deep = "4591760a792c43df397caaba9cca6e77";
    let (status, _) = s.put("/revlimit/_revs_limit", json!(2)).await;
    assert_eq!(status, StatusCode::OK);

    // Insert the shallow live branch first so the shared base stays in the
    // tree; CouchDB still truncates the deep leaf's returned history to two.
    for doc in [
        json!({"_id": "doc", "_rev": format!("2-{live}"),
               "_revisions": {"start": 2, "ids": [live, base]}, "v": "live"}),
        json!({"_id": "doc", "_rev": format!("3-{deep}"),
               "_revisions": {"start": 3, "ids": [deep, b705, base]}, "v": "deep"}),
        json!({"_id": "doc", "_rev": format!("2-{deleted}"), "_deleted": true,
               "_revisions": {"start": 2, "ids": [deleted, base]}, "v": "deleted"}),
    ] {
        let (status, body) = s
            .post(
                "/revlimit/_bulk_docs",
                json!({"docs": [doc], "new_edits": false}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    let (status, body) = s.get("/revlimit/doc?open_revs=all&revs=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let deep_doc = body
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["ok"]["_rev"] == format!("3-{deep}"))
        .expect("deep leaf");
    assert_eq!(deep_doc["ok"]["_revisions"]["ids"], json!([deep, b705]));

    let (status, body) = s
        .get(&format!(
            "/revlimit/doc?rev=3-{deep}&revs=true&revs_info=true&latest=true"
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_revisions"]["ids"], json!([deep, b705]));
    assert_eq!(body["_revs_info"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn deleted_leaf_cannot_be_updated_by_rev() {
    let s = Server::start(false);
    s.fresh_db("tombstones").await;

    let (_, body) = s.put("/tombstones/doc", json!({"a": 1})).await;
    let rev1 = body["rev"].as_str().unwrap().to_string();
    let (status, body) = s.delete(&format!("/tombstones/doc?rev={rev1}")).await;
    assert_eq!(status, StatusCode::OK);
    let rev2 = body["rev"].as_str().unwrap().to_string();

    let (status, body) = s
        .put("/tombstones/doc", json!({"_rev": rev2, "a": 2}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "conflict");

    let (status, body) = s
        .post(
            "/tombstones/_bulk_docs",
            json!({"docs": [{"_id": "doc", "_rev": rev2, "_deleted": true}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body[0]["error"], "conflict");

    // _bulk_get returns the tombstone, not a missing-document error.
    let (status, body) = s
        .post("/tombstones/_bulk_get", json!({"docs": [{"id": "doc"}]}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["docs"][0]["ok"]["_deleted"], true);
    assert_eq!(body["results"][0]["docs"][0]["ok"]["_rev"], rev2);

    // Creating a new revision without a rev is how CouchDB recreates a
    // deleted document.
    let (status, body) = s.put("/tombstones/doc", json!({"a": 3})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test]
async fn large_integers_round_trip_and_match_couchdb_rev_ids() {
    let s = Server::start(false);
    s.fresh_db("bigints").await;

    for (id, literal, rev) in [
        (
            "positive",
            "123456789012345678901234567890",
            "1-da1bd830ce681811ea5692e0ec6bd683",
        ),
        (
            "negative",
            "-9223372036854775809",
            "1-d7430b145a784f813a500d355bd26d42",
        ),
        (
            "above-u64",
            "18446744073709551616",
            "1-02621d7913faebde4c9d1efebd8500b4",
        ),
    ] {
        let (status, body) = s
            .put_raw(&format!("/bigints/{id}"), &format!(r#"{{"a":{literal}}}"#))
            .await;
        assert_eq!(status, StatusCode::CREATED, "{id}: {body}");
        assert_eq!(body["rev"], rev, "{id}");

        let (status, body) = s.get(&format!("/bigints/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["a"].to_string(), literal, "{id}");
    }
}

#[tokio::test]
async fn small_integer_arrays_match_couchdb_rev_ids() {
    let s = Server::start(false);
    s.fresh_db("smallarrays").await;

    // Revs recorded from a live Apache CouchDB 3.5. Arrays of integers 0..=255
    // hash as Erlang strings (STRING_EXT); anything else stays a list.
    for (id, literal, rev) in [
        ("bytes", "[1,2,3]", "1-308048961a84beb0c9e99e07facf0f5f"),
        ("edges", "[0,255]", "1-ebdd13a7ecf6387443aa094a0d18b4d6"),
        ("over", "[1,256]", "1-d0259a7a9a59173bfe7d4da469ac3cb6"),
        ("float", "[1,2.0]", "1-3b562cec9ddf9f74ee2f9b8a9de3f536"),
    ] {
        let (status, body) = s
            .put_raw(
                &format!("/smallarrays/{id}"),
                &format!(r#"{{"a":{literal}}}"#),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{id}: {body}");
        assert_eq!(body["rev"], rev, "{id}");
    }
}

#[tokio::test]
async fn duplicate_object_keys_use_last_position_like_couchdb() {
    let s = Server::start(false);
    s.fresh_db("dups").await;

    let (status, body) = s.put_raw("/dups/doc", r#"{"a":1,"b":2,"a":3}"#).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["rev"], "1-cf8195ee4268460f49235c8e98d42c6f");

    let (status, body) = s.get("/dups/doc").await;
    assert_eq!(status, StatusCode::OK);
    let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["_id", "_rev", "b", "a"]);
    assert_eq!(body["a"], 3);

    let (status, body) = s
        .put_raw("/dups/nested", r#"{"x":{"a":1,"b":2,"a":3}}"#)
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["rev"], "1-5fcb279318097164360622d44905a944");

    let (status, body) = s.put_raw("/dups/first", r#"{"a":1,"a":2,"b":3}"#).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["rev"], "1-d76c74eb7f5aa9bbe9f01ebde895887c");
    let (_, body) = s.get("/dups/first").await;
    let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["_id", "_rev", "a", "b"]);
}

#[tokio::test]
async fn bulk_docs_applies_the_same_number_and_duplicate_rules() {
    let s = Server::start(false);
    s.fresh_db("bulkrules").await;

    let (status, body) = s
        .post_raw(
            "/bulkrules/_bulk_docs",
            r#"{"docs":[{"_id":"dup","a":1,"b":2,"a":3},{"_id":"big","n":123456789012345678901234567890}]}"#,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body[0]["rev"], "1-cf8195ee4268460f49235c8e98d42c6f");
    assert_eq!(body[1]["rev"], "1-b3362bb3fea88f4e11bf86a8651536a9");

    let (_, body) = s.get("/bulkrules/dup").await;
    let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["_id", "_rev", "b", "a"]);
    let (_, body) = s.get("/bulkrules/big").await;
    assert_eq!(body["n"].to_string(), "123456789012345678901234567890");
}

#[tokio::test]
async fn float_spellings_are_canonicalised_like_couchdb() {
    let s = Server::start(false);
    s.fresh_db("floats").await;

    for (id, literal, stored, rev) in [
        (
            "exponent",
            "1e2",
            "100.0",
            "1-fcb573f41c731df1c79b9d2a8111e8e7",
        ),
        (
            "trailing",
            "1.00",
            "1.0",
            "1-8a159b695bf2bd957f1a435bfd6e1739",
        ),
        (
            "leading",
            "0.10",
            "0.1",
            "1-5974d01f385519e930e5a28349efba27",
        ),
        (
            "negative-zero",
            "-0",
            "0",
            "1-dcf485487e051d7f9b7c62bd4265a425",
        ),
    ] {
        let (status, body) = s
            .put_raw(&format!("/floats/{id}"), &format!(r#"{{"a":{literal}}}"#))
            .await;
        assert_eq!(status, StatusCode::CREATED, "{id}: {body}");
        assert_eq!(body["rev"], rev, "{id}");

        let (status, body) = s.get(&format!("/floats/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["a"].to_string(), stored, "{id}");
    }
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

/// `_all_docs` bodies are written as they are read, so a large listing never
/// sits in memory whole, and a HEAD does not read the rows at all. The exact
/// bytes (field order, separators) are part of the contract.
#[tokio::test]
async fn all_docs_is_streamed_with_a_stable_body() {
    let s = Server::start(false);
    s.fresh_db("streamed").await;
    let rev = |n: char| format!("1-{}", n.to_string().repeat(32));
    let (status, _) = s
        .post(
            "/streamed/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "b", "_rev": rev('b'), "v": 2},
                {"_id": "a", "_rev": rev('a'), "v": 1},
                {"_id": "c", "_rev": rev('c'), "v": 3},
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let text = |path: &str| {
        let request = s.client.get(format!("{}{path}", s.base));
        async move {
            let response = request.send().await.expect("request");
            let headers = response.headers().clone();
            (headers, response.text().await.expect("body"))
        }
    };

    let (headers, body) = text("/streamed/_all_docs").await;
    assert_eq!(
        body,
        format!(
            "{{\"total_rows\":3,\"offset\":0,\"rows\":[\
             {{\"id\":\"a\",\"key\":\"a\",\"value\":{{\"rev\":\"{a}\"}}}},\
             {{\"id\":\"b\",\"key\":\"b\",\"value\":{{\"rev\":\"{b}\"}}}},\
             {{\"id\":\"c\",\"key\":\"c\",\"value\":{{\"rev\":\"{c}\"}}}}]}}",
            a = rev('a'),
            b = rev('b'),
            c = rev('c')
        )
    );
    assert!(
        !headers.contains_key("content-length"),
        "the body is streamed, not sized up front: {headers:?}"
    );

    let (_, body) = text("/streamed/_all_docs?descending=true&limit=1&update_seq=true").await;
    assert!(
        body.starts_with(&format!(
            "{{\"total_rows\":3,\"offset\":0,\"rows\":[{{\"id\":\"c\",\"key\":\"c\",\"value\":{{\"rev\":\"{}\"}}}}],\"update_seq\":",
            rev('c')
        )),
        "{body}"
    );

    let head = s
        .client
        .head(format!("{}/streamed/_all_docs", s.base))
        .send()
        .await
        .expect("request");
    assert_eq!(head.status(), StatusCode::OK);
    assert!(
        head.headers()
            .get("content-length")
            .is_none_or(|length| length == "0"),
        "HEAD must not build the body to size it: {:?}",
        head.headers()
    );
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

    // Offset is capped at total_rows even when skip overshoots.
    let (_, body) = s.get("/ranges/_all_docs?skip=100").await;
    assert_eq!(body["offset"], 4);
    assert!(body["rows"].as_array().unwrap().is_empty());
    let (_, body) = s.get("/ranges/_all_docs?startkey=%22zzz%22&skip=4").await;
    assert_eq!(body["offset"], 4);

    // inclusive_end=false excludes the end key in both directions.
    let (_, body) = s
        .get("/ranges/_all_docs?startkey=%22a%22&endkey=%22e%22&inclusive_end=false")
        .await;
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["a", "c"]);
    let (_, body) = s
        .get(
            "/ranges/_all_docs?startkey=%22f%22&endkey=%22a%22&inclusive_end=false&descending=true",
        )
        .await;
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["f", "e", "c"]);

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
    // The server answers 413 and closes the connection without reading the
    // rest of the body, so under load the client can see the reset before the
    // response. Either way the request is refused and nothing is stored. A
    // separate client keeps the closed connection out of the server's pool.
    let response = Client::new()
        .post(format!("{}/limits/_bulk_docs", s.base))
        .json(&json!({"docs": [{"_id": "too-big", "blob": blob}]}))
        .send()
        .await;
    if let Ok(response) = response {
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
    let (status, _) = s.get("/limits/too-big").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
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

    // The feed's own timeout is long: the client reads until it has seen what
    // it expects and then hangs up, so a slow machine cannot end the stream
    // before a heartbeat is due. How the timeout closes a feed is covered by
    // the trailer test below.
    let mut response = s
        .client
        .get(format!(
            "{}/stream/_changes?feed=continuous&since=0&timeout=300000&heartbeat=40",
            s.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let text = read_feed_until(&mut response, |text| text.contains("\"id\":\"doc\"")).await;
    let changes: Vec<Value> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("change line is JSON"))
        .collect();
    assert!(changes.iter().any(|row| row["id"] == "doc"), "{text}");
    drop(response);

    // With since=now and no new writes, heartbeat newlines keep the stream
    // alive.
    let mut response = s
        .client
        .get(format!(
            "{}/stream/_changes?feed=continuous&since=now&timeout=300000&heartbeat=30",
            s.base
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let heartbeat = read_feed_until(&mut response, |text| text.contains('\n')).await;
    assert!(
        heartbeat.contains('\n'),
        "no heartbeat bytes: {heartbeat:?}"
    );
}

/// Read a streamed response until `done` accepts what has arrived so far. A
/// hang is bounded by a failsafe, not by the property under test.
async fn read_feed_until(response: &mut reqwest::Response, done: impl Fn(&str) -> bool) -> String {
    let mut text = String::new();
    tokio::time::timeout(Duration::from_secs(60), async {
        while !done(&text) {
            match response.chunk().await.expect("read feed") {
                Some(chunk) => text.push_str(&String::from_utf8_lossy(&chunk)),
                None => break,
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the feed never produced what was expected: {text:?}"));
    text
}

#[tokio::test]
async fn continuous_changes_ends_with_a_last_seq_trailer() {
    let s = Server::start(false);
    s.fresh_db("trailer").await;
    for id in ["a", "b"] {
        let (status, _) = s.put(&format!("/trailer/{id}"), json!({})).await;
        assert_eq!(status, StatusCode::CREATED);
    }
    let lines = |text: &str| -> Vec<Value> {
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("line is JSON"))
            .collect()
    };

    // CouchDB (chttpd_changes:end_sending_changes) closes a continuous feed
    // that times out with {"last_seq":...,"pending":...}.
    let text = s
        .client
        .get(format!(
            "{}/trailer/_changes?feed=continuous&since=0&timeout=200",
            s.base
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let rows = lines(&text);
    assert_eq!(rows.len(), 3, "{text}");
    assert_eq!(rows[2], json!({"last_seq": 2, "pending": 0}), "{text}");

    // The same trailer closes a feed that reached its limit, at once rather
    // than after the next wake-up: with the longest timeout the server
    // allows, only closing at the limit ends this request before the
    // failsafe.
    let text = tokio::time::timeout(Duration::from_secs(60), async {
        s.client
            .get(format!(
                "{}/trailer/_changes?feed=continuous&since=0&limit=1&timeout=300000",
                s.base
            ))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    })
    .await
    .expect("a feed at its limit stayed open");
    let rows = lines(&text);
    assert_eq!(rows.len(), 2, "{text}");
    assert_eq!(rows[1], json!({"last_seq": 1, "pending": 1}), "{text}");
}

#[tokio::test]
async fn changes_supports_sse_eventsource_feed() {
    let s = Server::start(false);
    s.fresh_db("sse").await;
    let (status, _) = s.put("/sse/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);

    let mut response = s
        .client
        .get(format!(
            "{}/sse/_changes?feed=eventsource&since=0&timeout=300000&heartbeat=40",
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
    let body = read_feed_until(&mut response, |text| {
        text.contains("data: ") && text.contains(": heartbeat")
    })
    .await;
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
async fn dbs_info_caps_the_number_of_keys_at_100() {
    // CouchDB chttpd_misc: max_db_number_for_dbs_info_req defaults to 100 and
    // a longer `keys` list is a 400 bad_request.
    let s = Server::start(false);
    let keys = |n: usize| -> Vec<String> { (0..n).map(|i| format!("nodb{i}")).collect() };
    let (status, body) = s.post("/_dbs_info", json!({"keys": keys(100)})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_array().map(Vec::len), Some(100));
    let (status, body) = s.post("/_dbs_info", json!({"keys": keys(101)})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "bad_request");
    assert_eq!(body["reason"], "too_many_keys");
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
                "{base}/_db_updates?feed=longpoll&since={since}&timeout=300000"
            ))
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    });
    // Whether the long-poll is already waiting or starts after this create,
    // it answers from `since`, so the create needs no head start.
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

// CouchDB 3.5 derives the revision of a new_edits=false document from
// `_revisions` when `_rev` is absent, and `_revisions` wins when the two
// disagree (couch_doc:transfer_fields/3). With neither field the request is
// refused with 400.
#[tokio::test]
async fn new_edits_false_derives_rev_from_revisions() {
    let s = Server::start(false);
    s.fresh_db("revsonly").await;
    let (status, body) = s
        .post(
            "/revsonly/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "t", "_revisions": {"start": 2, "ids": ["bbbb", "aaaa"]}},
                {"_id": "d1", "_rev": "3-cccc",
                 "_revisions": {"start": 2, "ids": ["bbbb", "aaaa"]}},
                {"_id": "d2", "_rev": "2-bbbb",
                 "_revisions": {"start": 3, "ids": ["bbbb", "aaaa"]}},
                {"_id": "g", "_deleted": true,
                 "_revisions": {"start": 2, "ids": ["bbbb", "aaaa"]}}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body, json!([]));
    for (id, rev, start) in [("t", "2-bbbb", 2), ("d1", "2-bbbb", 2), ("d2", "3-bbbb", 3)] {
        let (status, body) = s.get(&format!("/revsonly/{id}?revs=true")).await;
        assert_eq!(status, StatusCode::OK, "{id}: {body}");
        assert_eq!(body["_rev"], rev, "{id}");
        assert_eq!(
            body["_revisions"],
            json!({"start": start, "ids": ["bbbb", "aaaa"]}),
            "{id}"
        );
    }
    let (status, _) = s.get("/revsonly/g").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, body) = s
        .put(
            "/revsonly/p?new_edits=false",
            json!({"_revisions": {"start": 2, "ids": ["bbbb", "aaaa"]}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body, json!({"ok": true, "id": "p", "rev": "2-bbbb"}));

    let (status, body) = s.put("/revsonly/q?new_edits=false", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body,
        json!({"error": "bad_request", "reason":
            "When `new_edits: false`, the document needs `_rev` or `_revisions` specified"})
    );
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

    // CouchDB only emits _conflicts inside the embedded doc, never row.value.
    let (_, body) = s.get("/revs/_all_docs?conflicts=true").await;
    assert!(body["rows"][0]["value"].get("_conflicts").is_none());
    let (_, body) = s
        .get("/revs/_all_docs?conflicts=true&include_docs=true")
        .await;
    assert!(body["rows"][0]["value"].get("_conflicts").is_none());
    assert_eq!(
        body["rows"][0]["doc"]["_conflicts"],
        json!([format!("2-{b}")])
    );

    let (status, body) = s
        .post(
            "/revs/_revs_diff",
            json!({"a": [format!("3-{b}"), format!("3-{c}")]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["a"]["possible_ancestors"],
        json!([format!("2-{b}"), format!("2-{c}")]),
        "ancestors are unique"
    );
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
    // Local writes never conflict (CouchDB 3.5): the new revision is the
    // supplied `_rev` number plus one, whatever is stored.
    let (status, body) = s.put("/local/_local/loose", json!({})).await;
    assert_eq!((status, &body["rev"]), (StatusCode::CREATED, &json!("0-1")));
    let (status, body) = s.put("/local/_local/loose", json!({})).await;
    assert_eq!((status, &body["rev"]), (StatusCode::CREATED, &json!("0-1")));
    let (status, body) = s.put("/local/_local/loose", json!({"_rev": "0-1"})).await;
    assert_eq!((status, &body["rev"]), (StatusCode::CREATED, &json!("0-2")));
    let (status, body) = s.put("/local/_local/loose", json!({"_rev": "0-1"})).await;
    assert_eq!((status, &body["rev"]), (StatusCode::CREATED, &json!("0-2")));
    let (status, body) = s.put("/local/_local/loose", json!({"_rev": "abc"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "bad_request");
    assert_eq!(body["reason"], "Invalid rev format");

    let (status, body) = s.delete("/local/_local/loose").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"ok": true, "id": "_local/loose", "rev": "0-0"})
    );
    let (status, body) = s.delete("/local/_local/loose").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["reason"], "missing");
    // _bulk_docs, unlike DELETE, reports a missing local doc as deleted.
    let (status, body) = s
        .post(
            "/local/_bulk_docs",
            json!({"docs": [{"_id": "_local/loose", "_deleted": true}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(
        body,
        json!([{"ok": true, "id": "_local/loose", "rev": "0-0"}])
    );
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
    assert_eq!(body["error"], "bad_request");

    let (status, body) = s
        .post(
            "/att/_bulk_docs",
            json!({"docs": [{"_id": "bulk", "_attachments": {"a": {"data": data}}}]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "bad_request");

    // Reading an attachment finds none, as CouchDB reports for a document
    // that has no such attachment.
    let (status, _) = s.put("/att/plain", json!({"a": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s.get("/att/plain/a.txt").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], "not_found");
    assert_eq!(body["reason"], "Document is missing attachment");
    let (status, body) = s.get("/att/nodoc/a.txt").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["reason"], "missing");

    let (status, body) = s
        .req_with(
            Method::PUT,
            "/att/doc/a.txt",
            Some(Value::String("ignored".into())),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "bad_request");
}

// CouchDB's `_purge` acts on leaf revisions only: a leaf and the ancestors no
// other branch shares go, and a listed non-leaf revision is ignored and not
// reported. Verified against CouchDB 3.5.
#[tokio::test]
async fn purge_ignores_interior_revisions_and_removes_whole_branches() {
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
    assert_eq!(body["purged"]["a"], json!([]), "interior revs are ignored");
    let (status, body) = s.get(&format!("/purge/a?rev={rev2}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s.get("/purge/a").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_rev"], rev3);

    // Only the leaf is reported, but its whole unshared branch goes.
    let (_, body) = s.post("/purge/_purge", json!({"a": [rev1, rev3]})).await;
    assert_eq!(body["purged"]["a"], json!([rev3]));
    let (status, _) = s.get("/purge/a").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = s.get(&format!("/purge/a?rev={rev2}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = s.get(&format!("/purge/a?rev={rev1}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn purging_one_conflict_leaf_keeps_the_shared_ancestor_and_other_leaf() {
    let s = Server::start(false);
    s.fresh_db("purgeconf").await;
    let root = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let mid = "11111111111111111111111111111111";
    let b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let c = "cccccccccccccccccccccccccccccccc";
    let (status, _) = s
        .post(
            "/purgeconf/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "a", "_rev": format!("3-{b}"),
                 "_revisions": {"start": 3, "ids": [b, mid, root]}, "v": "b"},
                {"_id": "a", "_rev": format!("2-{c}"),
                 "_revisions": {"start": 2, "ids": [c, root]}, "v": "c"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // 3-b wins; purging it removes its unshared parent too and reports only
    // the leaf that was named.
    let (_, body) = s
        .post("/purgeconf/_purge", json!({"a": [format!("3-{b}")]}))
        .await;
    assert_eq!(body["purged"]["a"], json!([format!("3-{b}")]));
    let (status, body) = s.get("/purgeconf/a?conflicts=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_rev"], format!("2-{c}"));
    assert!(body.get("_conflicts").is_none(), "{body}");
    let (status, _) = s.get(&format!("/purgeconf/a?rev=2-{mid}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Purging the last leaf removes the document.
    let (_, body) = s
        .post("/purgeconf/_purge", json!({"a": [format!("2-{c}")]}))
        .await;
    assert_eq!(body["purged"]["a"], json!([format!("2-{c}")]));
    let (status, _) = s.get("/purgeconf/a").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// Revision ids that are not hex are stored verbatim and may contain '/'
// (CouchDB 3.5 accepts them under new_edits=false). Compaction must not
// mistake such a leaf body for a stale one.
#[tokio::test]
async fn compaction_keeps_leaf_bodies_whose_revision_contains_a_slash() {
    let s = Server::start(false);
    s.fresh_db("compactslash").await;
    let (status, _) = s
        .post(
            "/compactslash/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "doc", "_rev": "1-x/y", "v": 1},
                {"_id": "p", "_rev": "2-r/s",
                 "_revisions": {"start": 2, "ids": ["r/s", "p/q"]}, "v": 2},
                {"_id": "p", "_rev": "1-p/q", "v": 1}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s.post("/compactslash/_compact", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, body) = s.get("/compactslash/doc").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_rev"], "1-x/y");
    let (status, body) = s.get("/compactslash/p").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_rev"], "2-r/s");
    let (status, _) = s.get("/compactslash/p?rev=1-p/q").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "stale parent is dropped");
}

// CouchDB keeps non-leaf bodies readable by `?rev=` until compaction drops
// them (checked live: 200 before `_compact`, 404 `missing` after, and
// `_revs_info` then reports the old rev as "missing"). Leaves of every kind
// stay.
#[tokio::test]
async fn compaction_drops_non_leaf_bodies_but_keeps_every_leaf() {
    let s = Server::start(false);
    s.fresh_db("compactbodies").await;
    let (_, r1) = s.put("/compactbodies/a", json!({"v": 1})).await;
    let rev1 = r1["rev"].as_str().unwrap().to_string();
    let (_, r2) = s
        .put("/compactbodies/a", json!({"v": 2, "_rev": rev1}))
        .await;
    let rev2 = r2["rev"].as_str().unwrap().to_string();

    // A conflict leaf and a deleted leaf must survive compaction.
    let root = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let (status, _) = s
        .post(
            "/compactbodies/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "c", "_rev": format!("2-{b}"),
                 "_revisions": {"start": 2, "ids": [b, root]}, "v": "b"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, d1) = s.put("/compactbodies/d", json!({"v": 1})).await;
    let drev1 = d1["rev"].as_str().unwrap().to_string();
    let (_, dd) = s.delete(&format!("/compactbodies/d?rev={drev1}")).await;
    let drev2 = dd["rev"].as_str().unwrap().to_string();

    let (status, _) = s.get(&format!("/compactbodies/a?rev={rev1}")).await;
    assert_eq!(status, StatusCode::OK, "old bodies stay until compaction");

    let (status, _) = s.post("/compactbodies/_compact", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, body) = s.get(&format!("/compactbodies/a?rev={rev1}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], "not_found");
    let (status, body) = s.get("/compactbodies/a?revs_info=true").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["_rev"], rev2);
    assert_eq!(body["_revs_info"][0]["status"], "available");
    assert_eq!(body["_revs_info"][1]["status"], "missing");
    let (status, body) = s.get(&format!("/compactbodies/c?rev=2-{b}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s.get(&format!("/compactbodies/d?rev={drev2}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn compact_and_ensure_full_commit() {
    let s = Server::start(false);
    s.fresh_db("maint").await;
    let (status, body) = s.post("/maint/_compact", json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, json!({"ok": true}));
    let (status, info) = s.get("/maint").await;
    assert_eq!(status, StatusCode::OK, "{info}");
    assert_eq!(info["compact_running"], false);

    let (status, body) = s.post("/maint/_ensure_full_commit", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["ok"], true);
    assert!(body["instance_start_time"].is_string());
}

/// CouchDB answers 405 with `Allow` and `{"error":"method_not_allowed",
/// "reason":"Only POST allowed"}` for a wrong method on the POST-only
/// endpoints (checked against CouchDB 3.5.3), instead of running the handler.
#[tokio::test]
async fn special_endpoints_reject_the_wrong_method() {
    let s = Server::start(false);
    s.fresh_db("methods").await;
    let post_only = [
        "_bulk_docs",
        "_bulk_get",
        "_revs_diff",
        "_purge",
        "_compact",
        "_ensure_full_commit",
        "_view_cleanup",
        "_search_cleanup",
    ];
    for endpoint in post_only {
        for method in [Method::GET, Method::PUT, Method::DELETE] {
            let path = format!("/methods/{endpoint}");
            let (status, headers, body) = s.req_headers(method.clone(), &path, None, &[]).await;
            assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{method} {path}");
            assert_eq!(headers.get("allow").map(String::as_str), Some("POST"));
            assert_eq!(
                body,
                json!({"error": "method_not_allowed", "reason": "Only POST allowed"}),
                "{method} {path}"
            );
        }
    }
    let (status, headers, _) = s
        .req_headers(Method::HEAD, "/methods/_compact", None, &[])
        .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(headers.get("allow").map(String::as_str), Some("POST"));

    // A PUT of a `_bulk_docs` body must not write anything.
    let (status, _) = s
        .put("/methods/_bulk_docs", json!({"docs": [{"_id": "x"}]}))
        .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    let (status, _) = s.get("/methods/x").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    for (method, path, allow) in [
        (Method::PUT, "/methods/_changes", "GET,POST,HEAD"),
        (Method::DELETE, "/methods/_all_docs", "GET,HEAD,POST"),
        (Method::DELETE, "/methods/_revs_limit", "PUT,GET"),
        (Method::DELETE, "/methods/_security", "PUT,GET"),
    ] {
        let (status, headers, body) = s.req_headers(method.clone(), path, None, &[]).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{method} {path}");
        assert_eq!(headers.get("allow").map(String::as_str), Some(allow));
        assert_eq!(
            body,
            json!({"error": "method_not_allowed", "reason": format!("Only {allow} allowed")})
        );
    }

    // The methods CouchDB allows keep working.
    let (status, _) = s.get("/methods/_changes").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.post("/methods/_changes", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.get("/methods/_all_docs").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.post("/methods/_all_docs", json!({})).await;
    assert_eq!(status, StatusCode::OK);
}

/// Invalid parameters are 400s, not silently widened to "everything". The
/// statuses and bodies below were taken from CouchDB 3.5.3.
#[tokio::test]
async fn invalid_query_parameters_are_rejected() {
    let s = Server::start(false);
    s.fresh_db("params").await;
    let (status, _) = s
        .put(
            "/params/_design/v",
            json!({"views": {"a": {"map": "function(d){emit(d._id,1)}"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s.put("/params/d1", json!({"a": 1})).await;
    assert_eq!(status, StatusCode::CREATED);

    let int = |raw: &str| json!(format!("Invalid value for integer: \"{raw}\""));
    let positive = |raw: &str| json!(format!("Invalid value for positive integer: \"{raw}\""));
    for base in [
        "/params/_all_docs",
        "/params/_design/v/_view/a",
        "/_all_dbs",
    ] {
        for (query, reason) in [
            ("limit=abc", int("abc")),
            ("limit=-1", positive("-1")),
            ("skip=abc", int("abc")),
            ("skip=-1", positive("-1")),
        ] {
            let (status, body) = s.get(&format!("{base}?{query}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{base}?{query}: {body}");
            assert_eq!(body["error"], "query_parse_error", "{base}?{query}");
            assert_eq!(body["reason"], reason, "{base}?{query}");
        }
        let (status, _) = s.get(&format!("{base}?limit=0")).await;
        assert_eq!(status, StatusCode::OK, "{base}?limit=0");
    }

    // `_changes` uses its own messages.
    let (status, body) = s.get("/params/_changes?limit=abc").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "bad_request");
    assert_eq!(
        body["reason"],
        "The limit parameter should be a non-negative integer"
    );
    let (status, body) = s.get("/params/_changes?limit=-1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["reason"],
        "The limit value should be a non-negative integer"
    );

    // Keys that are not a JSON array must not return the whole database.
    let (status, body) = s.get("/params/_all_docs?keys=abc").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["reason"], "invalid UTF-8 JSON");
    let (status, body) = s.get("/params/_all_docs?keys=%7B%22a%22%3A1%7D").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["reason"], "`keys` parameter must be an array.");
    let (status, body) = s.get("/params/_all_docs?keys=%5B%22d1%22%5D").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().map(Vec::len), Some(1));

    // Bad POST bodies.
    for path in ["/params/_all_docs", "/params/_design/v/_view/a"] {
        let (status, body) = s.post_raw(path, "not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(body["error"], "bad_request");
        assert_eq!(body["reason"], "invalid UTF-8 JSON");
        let (status, body) = s.post_raw(path, "[1]").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(body["reason"], "Request body must be a JSON object");
        let (status, body) = s.post_raw(path, r#"{"limit":-1}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
        assert_eq!(body["error"], "query_parse_error");
        assert_eq!(body["reason"], "Invalid value for positive integer: -1");
        let (status, body) = s.post_raw(path, r#"{"keys":"a"}"#).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {body}");
    }
    let (status, body) = s.post_raw("/params/_all_docs", r#"{"keys":"a"}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["reason"], "`keys` body member must be an array.");

    // `_revs_limit` takes a JSON integer; garbage must not reset it.
    let (status, _) = s.put_raw("/params/_revs_limit", "12").await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = s.put_raw("/params/_revs_limit", "garbage").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["reason"], "invalid UTF-8 JSON");
    let (status, body) = s.put_raw("/params/_revs_limit", "0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let (status, body) = s.get("/params/_revs_limit").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!(12));
}

/// Query-parameter validation taken from CouchDB 3.5.3: `_changes` rejects bad
/// `since`/`feed`/`heartbeat`/`timeout`, view and `_all_docs` queries reject
/// bad booleans and non-JSON keys, and `_uuids` validates `count`.
#[tokio::test]
async fn query_parameters_are_validated_like_couchdb() {
    let s = Server::start(false);
    s.fresh_db("qp").await;
    let (status, _) = s
        .put(
            "/qp/_design/v",
            json!({"views": {"a": {"map": "function(d){emit(d._id,1)}"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s.put("/qp/d1", json!({"a": 1})).await;
    assert_eq!(status, StatusCode::CREATED);

    for (query, reason) in [
        (
            "since=abc",
            "Malformed sequence supplied in 'since' parameter.",
        ),
        (
            "since=-1",
            "Malformed sequence supplied in 'since' parameter.",
        ),
        (
            "since=",
            "Malformed sequence supplied in 'since' parameter.",
        ),
        (
            "feed=bogus",
            "Supported `feed` types: normal, continuous, live, longpoll, eventsource",
        ),
        (
            "heartbeat=abc",
            "Invalid heartbeat value. Expecting a positive integer value (in milliseconds).",
        ),
        (
            "heartbeat=-1",
            "The heartbeat value should be a positive integer (in milliseconds).",
        ),
        (
            "heartbeat=0",
            "The heartbeat value should be a positive integer (in milliseconds).",
        ),
        (
            "timeout=abc",
            "The timeout parameter should be a non-negative integer",
        ),
        (
            "timeout=-1",
            "The timeout value should be a non-negative integer",
        ),
    ] {
        let (status, body) = s.get(&format!("/qp/_changes?{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert_eq!(body["error"], "bad_request", "{query}");
        assert_eq!(body["reason"], reason, "{query}");
    }
    for query in ["since=0", "since=now", "timeout=0", "heartbeat=1000"] {
        let (status, body) = s.get(&format!("/qp/_changes?{query}")).await;
        assert_eq!(status, StatusCode::OK, "{query}: {body}");
    }

    for base in ["/qp/_all_docs", "/qp/_design/v/_view/a"] {
        for name in ["descending", "include_docs", "inclusive_end", "update_seq"] {
            let (status, body) = s.get(&format!("{base}?{name}=maybe")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{base}?{name}: {body}");
            assert_eq!(body["error"], "query_parse_error", "{base}?{name}");
            assert_eq!(
                body["reason"], "Invalid boolean parameter: \"maybe\"",
                "{base}?{name}"
            );
        }
        for name in ["key", "startkey", "endkey"] {
            let (status, body) = s.get(&format!("{base}?{name}=abc")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{base}?{name}: {body}");
            assert_eq!(body["error"], "bad_request", "{base}?{name}");
            assert_eq!(body["reason"], "invalid UTF-8 JSON", "{base}?{name}");
            let (status, body) = s.get(&format!("{base}?{name}=%22abc%22")).await;
            assert_eq!(status, StatusCode::OK, "{base}?{name}: {body}");
        }
        let (status, body) = s.get(&format!("{base}?descending=TRUE")).await;
        assert_eq!(status, StatusCode::OK, "{base}: {body}");
    }

    for (query, reason) in [
        ("count=abc", "count must be a positive integer"),
        ("count=-1", "count must be a positive integer"),
        ("count=", "count must be a positive integer"),
        ("count=1001", "count parameter too large"),
        ("count=100000", "count parameter too large"),
    ] {
        let (status, body) = s.get(&format!("/_uuids?{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert_eq!(body["error"], "bad_request", "{query}");
        assert_eq!(body["reason"], reason, "{query}");
    }
    let (status, body) = s.get("/_uuids?count=0").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({"uuids": []}));
    let (status, body) = s.get("/_uuids?count=1000").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["uuids"].as_array().map(Vec::len), Some(1000));
}

/// `POST /db?batch=ok` answers 202 with no rev (CouchDB 3.5.3); a malformed
/// `_rev` is a 400 `Invalid rev format` for POST and DELETE, not a 409.
#[tokio::test]
async fn doc_writes_map_errors_to_their_status() {
    let s = Server::start(false);
    s.fresh_db("results").await;
    let (status, body) = s.post("/results?batch=ok", json!({"_id": "b1"})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body, json!({"ok": true, "id": "b1"}));

    let (status, body) = s.put("/results/d1", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .post("/results", json!({"_id": "d1", "_rev": "garbage"}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "bad_request");
    assert_eq!(body["reason"], "Invalid rev format");
    let (status, body) = s.delete("/results/d1?rev=garbage").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["reason"], "Invalid rev format");

    // A real conflict stays a 409.
    let (status, body) = s.post("/results", json!({"_id": "d1"})).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (status, body) = s.delete("/results/d1?rev=1-abc").await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
}

/// Document write errors taken from CouchDB 3.5.3: `PUT ?batch=ok` is 202
/// without a rev, a body that is not JSON is `invalid UTF-8 JSON`, a design
/// document with a member of the wrong type is `invalid_design_doc` and a
/// `_deleted` that is not a boolean is `doc_validation`.
#[tokio::test]
async fn document_writes_validate_like_couchdb() {
    let s = Server::start(false);
    s.fresh_db("dw").await;

    let (status, body) = s.put("/dw/b1?batch=ok", json!({"a": 1})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body, json!({"ok": true, "id": "b1"}));
    let (status, body) = s.get("/dw/b1").await;
    assert_eq!(status, StatusCode::OK, "{body}");

    for (method, path) in [(Method::POST, "/dw"), (Method::PUT, "/dw/j1")] {
        for raw in ["not json", ""] {
            let response = s
                .client
                .request(method.clone(), format!("{}{path}", s.base))
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(raw.to_owned())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path} {raw:?}");
            let body: Value = response.json().await.unwrap();
            assert_eq!(body["error"], "bad_request", "{path} {raw:?}");
            assert_eq!(body["reason"], "invalid UTF-8 JSON", "{path} {raw:?}");
        }
    }
    let (status, body) = s.post_raw("/dw/_bulk_docs", "not json").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["reason"], "invalid UTF-8 JSON");

    for (doc, reason) in [
        (json!({"views": 1}), "`views` field must have object type"),
        (
            json!({"views": {"v": 1}}),
            "`views.v` field must have object type",
        ),
        (
            json!({"views": {"v": {}}}),
            "View `v` must contain map function",
        ),
        (
            json!({"views": {"v": {"map": 1}}}),
            "`views.v.map` field must have string type",
        ),
        (
            json!({"views": {"v": {"map": "function(d){}", "reduce": "_nope"}}}),
            "`_nope` is not a supported reduce function.",
        ),
        (
            json!({"language": 5}),
            "`language` field must have string type",
        ),
        (
            json!({"filters": 1}),
            "`filters` field must have object type",
        ),
        (
            json!({"filters": {"f": 1}}),
            "`filters.f` field must have string or object type",
        ),
        (
            json!({"validate_doc_update": 1}),
            "`validate_doc_update` field must have string type",
        ),
        (
            json!({"rewrites": {}}),
            "`rewrites` field must have array or string type",
        ),
    ] {
        let (status, body) = s.put("/dw/_design/bad", doc.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{doc}: {body}");
        assert_eq!(body["error"], "invalid_design_doc", "{doc}");
        assert_eq!(body["reason"], reason, "{doc}");
    }
    let mut doc = json!({"_id": "_design/bad", "views": 1});
    let (status, body) = s.post("/dw", doc.clone()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"], "invalid_design_doc");
    // `_bulk_docs` reports the failure on the row and stores the rest.
    doc["_id"] = json!("_design/bad2");
    let (status, body) = s
        .post("/dw/_bulk_docs", json!({"docs": [doc, {"_id": "ok1"}]}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body[0]["id"], "_design/bad2");
    assert_eq!(body[0]["error"], "invalid_design_doc");
    assert_eq!(body[0]["reason"], "`views` field must have object type");
    assert_eq!(body[1]["ok"], true);
    // A well-formed design document is still accepted.
    let (status, body) = s
        .put(
            "/dw/_design/good",
            json!({"language": "javascript", "views": {"v": {"map": "function(d){}", "reduce": "_count"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    for deleted in [json!("x"), json!(1), Value::Null] {
        let (status, body) = s.put("/dw/del", json!({"_deleted": deleted})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{deleted}: {body}");
        assert_eq!(body["error"], "doc_validation", "{deleted}");
        assert_eq!(body["reason"], "Bad special document member: _deleted");
        let (status, body) = s
            .post("/dw", json!({"_id": "del", "_deleted": deleted}))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{deleted}: {body}");
        assert_eq!(body["error"], "doc_validation", "{deleted}");
    }
    let (status, body) = s.put("/dw/del", json!({"_deleted": false})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
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

/// CouchDB 3.5.3 reports an anonymous session as `"name": null` (JSON null,
/// not the string "null").
#[tokio::test]
async fn anonymous_session_user_name_is_null() {
    let s = Server::start(true);
    let (status, body) = s.get("/_session").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["userCtx"]["name"], Value::Null, "{body}");
    assert_eq!(body["userCtx"]["roles"], json!([]));
}

/// An `AuthSession` cookie value for `name`, in the format the server issues:
/// `base64url(name:exp).base64url(HMAC-SHA256(name:exp))`. The HMAC key is the
/// session secret plus the SHA-256 of the user's credential, here the admin
/// password.
fn forged_cookie(secret: &[u8], password: &str, name: &str) -> String {
    use sha2::Digest;
    let mut key = secret.to_vec();
    key.extend_from_slice(&sha2::Sha256::digest(password.as_bytes()));
    let key = key.as_slice();
    use base64::Engine;
    use hmac::{Hmac, Mac};
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    let payload = format!("{name}:{exp}");
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(key).unwrap();
    mac.update(payload.as_bytes());
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    format!(
        "AuthSession={}.{}",
        b64.encode(payload.as_bytes()),
        b64.encode(mac.finalize().into_bytes())
    )
}

async fn status_with_cookie(s: &Server, cookie: &str) -> StatusCode {
    s.client
        .get(format!("{}/_all_dbs", s.base))
        .header("cookie", cookie)
        .send()
        .await
        .unwrap()
        .status()
}

/// The session key must not be derivable from the admin credentials: anyone
/// holding a cookie could otherwise brute-force the password offline.
#[tokio::test]
async fn session_key_is_not_derived_from_admin_password() {
    let s = Server::start(true);
    let forged = forged_cookie(b"admin:douchdb:douchdb", "douchdb", "admin");
    assert_eq!(
        status_with_cookie(&s, &forged).await,
        StatusCode::UNAUTHORIZED
    );
}

/// `--session-secret` is the session key.
#[tokio::test]
async fn session_secret_flag_signs_cookies() {
    let secret = "an-explicit-session-secret";
    let s = Server::start_with(&["--session-secret".to_string(), secret.to_string()], true);
    let good = forged_cookie(secret.as_bytes(), "douchdb", "admin");
    assert_eq!(status_with_cookie(&s, &good).await, StatusCode::OK);
    let bad = forged_cookie(b"admin:douchdb:douchdb", "douchdb", "admin");
    assert_eq!(status_with_cookie(&s, &bad).await, StatusCode::UNAUTHORIZED);
}

/// One malformed `AuthSession` cookie must not hide a valid one that follows.
#[tokio::test]
async fn malformed_session_cookie_does_not_hide_a_valid_one() {
    let secret = "an-explicit-session-secret";
    let s = Server::start_with(&["--session-secret".to_string(), secret.to_string()], true);
    let good = forged_cookie(secret.as_bytes(), "douchdb", "admin");
    for junk in [
        "AuthSession=!!!.!!!",
        "AuthSession=%%%",
        "AuthSession=Zm9v.!!!",
    ] {
        let both = format!("{junk}; {good}");
        assert_eq!(
            status_with_cookie(&s, &both).await,
            StatusCode::OK,
            "{both}"
        );
    }
    assert_eq!(
        status_with_cookie(&s, "AuthSession=!!!.!!!").await,
        StatusCode::UNAUTHORIZED
    );
}

/// A cookie is signed with the session secret plus the user's credential
/// digest (CouchDB mixes the user salt into its cookie HMAC key), so rotating
/// a token in the users file revokes that user's sessions at once.
#[tokio::test]
async fn rotating_a_user_token_revokes_existing_sessions() {
    let users = UsersFile::new(USERS);
    let mut s = Server::start_with(&users_server_args(&users), true);
    let (status, headers, body) = s
        .req_headers(
            Method::POST,
            "/_session",
            Some(json!({"name": "alice", "password": "alice-token"})),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cookie = headers
        .get("set-cookie")
        .expect("session cookie")
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_eq!(status_with_cookie(&s, &cookie).await, StatusCode::OK);

    // Same name and roles, new token.
    std::fs::write(&users.path, USERS.replace("alice-token", "alice-rotated")).unwrap();
    s.restart();
    assert_eq!(
        status_with_cookie(&s, &cookie).await,
        StatusCode::UNAUTHORIZED,
        "the old cookie must not outlive the old token"
    );
    // The new token logs in and its cookie works.
    let (status, headers, body) = s
        .req_headers(
            Method::POST,
            "/_session",
            Some(json!({"name": "alice", "password": "alice-rotated"})),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let fresh = headers
        .get("set-cookie")
        .expect("session cookie")
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_eq!(status_with_cookie(&s, &fresh).await, StatusCode::OK);
}

/// Without `--session-secret` the server generates a random key once, keeps it
/// in the data directory (owner-only) and reuses it after a restart.
#[tokio::test]
async fn generated_session_secret_persists_across_restarts() {
    let mut s = Server::start(true);
    let response = s
        .client
        .post(format!("{}/_session", s.base))
        .json(&json!({"name": "admin", "password": "douchdb"}))
        .send()
        .await
        .unwrap();
    let cookie = response
        .headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .expect("AuthSession cookie")
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_eq!(status_with_cookie(&s, &cookie).await, StatusCode::OK);
    s.restart();
    assert_eq!(status_with_cookie(&s, &cookie).await, StatusCode::OK);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(s.dir.join(".session-secret")).expect("secret file");
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
    }
}

#[tokio::test]
async fn uuids_are_unique_in_a_tight_loop() {
    let s = Server::start(false);
    let (status, body) = s.get("/_uuids?count=1000").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let uuids = body["uuids"].as_array().unwrap();
    let unique: std::collections::HashSet<&str> =
        uuids.iter().map(|uuid| uuid.as_str().unwrap()).collect();
    assert_eq!(unique.len(), 1000);
    assert!(uuids.iter().all(|u| {
        let u = u.as_str().unwrap();
        u.len() == 32
            && u.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }));
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

    let (status, body) = nodes[0]
        .get("/cluster/_all_docs?descending=true&startkey=%22d5%22&skip=1&limit=2")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["offset"], 3);
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["d4", "d3"]);

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

/// One node's peer-call counters, summed over every route: `(calls its own
/// client requests made, their sequential rounds, calls that requests sent by
/// other nodes made)`, from its local metrics.
async fn peer_counters(node: &Server) -> (u64, u64, u64) {
    let (status, metrics) = node.get("/_node/_local/_metrics").await;
    assert_eq!(status, StatusCode::OK, "{metrics}");
    let mut totals = (0, 0, 0);
    for row in metrics["http"]["routes"].as_array().into_iter().flatten() {
        totals.0 += row["peer_calls"].as_u64().unwrap_or(0);
        totals.1 += row["peer_rounds"].as_u64().unwrap_or(0);
        totals.2 += row["nested_calls"].as_u64().unwrap_or(0);
    }
    totals
}

/// Every node's counters once all of them are idle: no request has peer
/// accounting open, which includes replica writes that outlive the response
/// and streamed bodies still being produced.
async fn peer_snapshot(nodes: &[Server]) -> Vec<(u64, u64, u64)> {
    let mut out = Vec::new();
    for node in nodes {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let (_, metrics) = node.get("/_node/_local/_metrics").await;
            let Some(open) = metrics["http"]["peer_open"].as_u64() else {
                panic!("the metrics report no http.peer_open gauge");
            };
            if open == 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "peer accounting never closed: {open} requests still open"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        out.push(peer_counters(node).await);
    }
    out
}

/// What one client request sent to `nodes[0]` cost in node-to-node traffic:
/// `(peer calls the coordinator made, its sequential rounds, peer calls the
/// nodes it reached made in turn)`. The difference of the counters around the
/// request. `nodes` are the live nodes.
async fn measure_peer_trips(
    nodes: &[Server],
    method: Method,
    path: &str,
    body: Option<Value>,
) -> (u64, u64, u64) {
    let before = peer_snapshot(nodes).await;
    let (status, response) = nodes[0].req(method, path, body).await;
    assert!(status.is_success(), "{path}: {status} {response}");
    let after = peer_snapshot(nodes).await;
    let nested: u64 = (0..nodes.len()).map(|i| after[i].2 - before[i].2).sum();
    (after[0].0 - before[0].0, after[0].1 - before[0].1, nested)
}

/// A database `trips` on a fresh three-node cluster (`q=4`) holding 40
/// documents and a design document with a map view and a reduce view, both
/// already queried so their indexes exist.
async fn peer_trip_cluster(replication: usize) -> Vec<Server> {
    let nodes = start_cluster_with(3, "3600", replication);
    for node in &nodes {
        node.fresh_db("trips").await;
    }
    let docs: Vec<Value> = (0..40)
        .map(|i| json!({"_id": format!("d{i:02}"), "n": i}))
        .collect();
    let (status, body) = nodes[0]
        .post("/trips/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = nodes[0]
        .put(
            "/trips/_design/v",
            json!({
                "views": {
                    "by_n": {"map": "function (doc) { emit(doc.n, 1); }"},
                    "total": {
                        "map": "function (doc) { emit(doc.n, 1); }",
                        "reduce": "_count"
                    }
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for path in [PEER_TRIPS_MAP_VIEW, PEER_TRIPS_REDUCE_VIEW] {
        let (status, body) = nodes[0].get(path).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    nodes
}

const PEER_TRIPS_MAP_VIEW: &str = "/trips/_design/v/_view/by_n";
const PEER_TRIPS_REDUCE_VIEW: &str = "/trips/_design/v/_view/total?group=true";

/// The read operations of the baseline, in a fixed order.
async fn peer_trip_reads(nodes: &[Server]) -> Vec<(&'static str, (u64, u64, u64))> {
    let mut out = Vec::new();
    let reads = [
        ("get_doc", "/trips/d10"),
        ("all_docs_limit_100", "/trips/_all_docs?limit=100"),
        ("map_view", PEER_TRIPS_MAP_VIEW),
        ("reduce_view", PEER_TRIPS_REDUCE_VIEW),
        ("changes_since_now", "/trips/_changes?since=now"),
    ];
    for (label, path) in reads {
        out.push((
            label,
            measure_peer_trips(nodes, Method::GET, path, None).await,
        ));
    }
    out
}

/// Baseline for the epic on node-to-node round trips: how many peer calls and
/// sequential rounds each client request costs on a healthy three-node
/// cluster with `q=4`, `n=3`. These are the numbers later changes lower; when
/// one does, update the expectation in the same commit.
///
/// A round is a period with at least one peer call in flight (see
/// `cluster_http`), so concurrent calls count once. Each result is
/// `(coordinator calls, coordinator rounds, calls made by the nodes it
/// reached)`, for a request sent to node 0.
#[tokio::test]
async fn cluster_peer_round_trip_baseline() {
    let nodes = peer_trip_cluster(3).await;
    // Writes first. A node with no cached validators pays a validator
    // fan-out (one call per peer, one round) before the write itself (one call
    // per other replica, one round); the cache lasts a second, so the second
    // figure is the cached case unless the machine stalled for longer.
    let put = measure_peer_trips(&nodes, Method::PUT, "/trips/new1", Some(json!({"n": 1}))).await;
    assert!(
        put == (2, 1, 0) || put == (4, 2, 0),
        "single-doc PUT: {put:?}"
    );
    // Every node holds a copy of every shard, so a read, a listing or a view
    // is served from local data without a scatter. A document read still asks
    // a second replica (`r=2`) and `since=now` still asks every peer for its
    // sequence.
    assert_eq!(
        peer_trip_reads(&nodes).await,
        vec![
            ("get_doc", (2, 1, 0)),
            ("all_docs_limit_100", (0, 0, 0)),
            ("map_view", (0, 0, 0)),
            ("reduce_view", (0, 0, 0)),
            ("changes_since_now", (2, 1, 0)),
        ]
    );
}

/// With one copy of each shard the coordinator must scatter to the nodes
/// that own shards. Which nodes those are depends on placement, so the
/// assertions are relations that hold for any placement: a listing, a map view
/// and a reduce view cost one scatter round (none if the node owns every
/// shard), and `since=now` adds a round for the sequence vector before its own
/// scatter.
#[tokio::test]
async fn cluster_peer_scatter_baseline_with_one_replica() {
    let nodes = peer_trip_cluster(1).await;
    let reads = peer_trip_reads(&nodes).await;
    let trips = |label: &str| {
        reads
            .iter()
            .find(|(name, _)| *name == label)
            .map(|(_, trips)| *trips)
            .unwrap_or_default()
    };
    let scatter = trips("all_docs_limit_100");
    assert_eq!(scatter.1, u64::from(scatter.0 > 0), "{reads:?}");
    assert_eq!(trips("map_view"), scatter, "{reads:?}");
    assert_eq!(trips("reduce_view"), scatter, "{reads:?}");
    // The vector round asks both peers, then the scatter goes out again.
    assert_eq!(
        trips("changes_since_now"),
        (2 + scatter.0, 1 + scatter.1, 0),
        "{reads:?}"
    );
}

/// With a node down, a write pays the validator fan-out every time (a partial
/// answer is never cached) and reads route around the dead node.
#[tokio::test]
async fn cluster_peer_round_trip_baseline_with_a_node_down() {
    let mut nodes = peer_trip_cluster(3).await;
    nodes[2].kill();
    nodes.truncate(2);
    // Calls to the dead node count too: they are issued and fail. Only the
    // two live nodes' counters can be read, and the dead node makes no nested
    // calls.
    for id in ["down1", "down2"] {
        let put = measure_peer_trips(
            &nodes,
            Method::PUT,
            &format!("/trips/{id}"),
            Some(json!({"n": 1})),
        )
        .await;
        assert_eq!(put, (4, 2, 0), "PUT {id} with a node down");
    }
    assert_eq!(
        peer_trip_reads(&nodes).await,
        vec![
            ("get_doc", (2, 1, 0)),
            ("all_docs_limit_100", (0, 0, 0)),
            ("map_view", (0, 0, 0)),
            ("reduce_view", (0, 0, 0)),
            ("changes_since_now", (2, 1, 0)),
        ]
    );
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
async fn cluster_changes_report_pending_like_a_single_node() {
    // n=1 fans out to every node; n=3 lets the coordinator read every shard
    // locally.
    for replication in [1, 3] {
        let nodes = start_cluster_with(3, "3600", replication);
        for node in &nodes {
            node.fresh_db("clpending").await;
        }
        let docs: Vec<Value> = (0..30)
            .map(|i| json!({"_id": format!("p{i:02}")}))
            .collect();
        let (status, _) = nodes[0]
            .post("/clpending/_bulk_docs", json!({"docs": docs}))
            .await;
        assert_eq!(status, StatusCode::CREATED);

        // With n=3 the write is acknowledged at quorum and the last replica
        // lands asynchronously, so wait until every node holds every
        // document before reading `pending` from it.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        for node in &nodes {
            loop {
                let (status, all) = node.get("/clpending/_changes").await;
                assert_eq!(status, StatusCode::OK, "{all}");
                if all["results"].as_array().map(Vec::len) == Some(30) {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "n={replication}: {} never saw all 30 documents: {all}",
                    node.base
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        for node in &nodes {
            let mut since = "0".to_string();
            let mut seen = 0;
            for expected in [20, 10, 0] {
                let (status, page) = node
                    .get(&format!("/clpending/_changes?limit=10&since={since}"))
                    .await;
                assert_eq!(status, StatusCode::OK, "{page}");
                seen += page["results"].as_array().unwrap().len();
                assert_eq!(
                    page["pending"], expected,
                    "n={replication} via {} after {seen} rows: {page}",
                    node.base
                );
                since = page["last_seq"].as_str().unwrap().to_string();
            }
            assert_eq!(seen, 30);
            let (_, all) = node.get("/clpending/_changes").await;
            assert_eq!(all["pending"], 0, "{all}");
        }
    }
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
    let total_seen = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 9, "limited feed missed a node");
    assert_eq!(total_seen, 9, "limited feed returned duplicate rows");
}

#[tokio::test]
async fn cluster_changes_limit_pages_make_progress_with_a_node_down() {
    let mut nodes = start_cluster_with(3, "3600", 2);
    for node in &nodes {
        node.fresh_db("clchdown").await;
    }
    let ids: Vec<String> = (0..12).map(|i| format!("pd{i}")).collect();
    let docs: Vec<Value> = ids.iter().map(|id| json!({"_id": id})).collect();
    let (status, _) = nodes[0]
        .post("/clchdown/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (reader, victim) = fallback_pair(&nodes, "clchdown", &ids).await;
    nodes[victim].kill();

    let mut since = "0".to_string();
    let mut seen = std::collections::HashSet::new();
    let mut finished = false;
    for _ in 0..30 {
        let (status, body) = nodes[reader]
            .get(&format!("/clchdown/_changes?since={since}&limit=2"))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let results = body["results"].as_array().unwrap();
        if results.is_empty() {
            finished = true;
            break;
        }
        for row in results {
            seen.insert(row["id"].as_str().unwrap().to_string());
        }
        since = body["last_seq"].as_str().unwrap().to_string();
    }
    assert!(finished, "paged feed never reached the end: {seen:?}");
    assert_eq!(
        seen.len(),
        ids.len(),
        "paged feed skipped changes: {seen:?}"
    );
}

#[tokio::test]
async fn cluster_changes_since_now_with_a_node_down_uses_another_replica() {
    let mut nodes = start_cluster_with(3, "3600", 2);
    for node in &nodes {
        node.fresh_db("clnowdown").await;
    }
    let ids: Vec<String> = (0..9).map(|i| format!("nd{i}")).collect();
    let docs: Vec<Value> = ids.iter().map(|id| json!({"_id": id})).collect();
    let (status, _) = nodes[0]
        .post("/clnowdown/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (reader, victim) = fallback_pair(&nodes, "clnowdown", &ids).await;
    nodes[victim].kill();

    let (status, body) = nodes[reader]
        .get("/clnowdown/_changes?since=now&limit=1")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["results"].as_array().unwrap().is_empty(), "{body}");
    let since = body["last_seq"].as_str().unwrap().to_string();
    let (status, info) = nodes[reader].get("/clnowdown").await;
    assert_eq!(status, StatusCode::OK, "{info}");

    let (status, _) = nodes[reader].put("/clnowdown/fresh", json!({})).await;
    assert!(status == StatusCode::CREATED || status == StatusCode::ACCEPTED);
    let (status, body) = nodes[reader]
        .get(&format!("/clnowdown/_changes?since={since}"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(got, vec!["fresh"], "since=now replayed history: {body}");
}

#[tokio::test]
async fn cluster_changes_since_now_fails_when_a_shard_has_no_live_copy() {
    let mut nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("clnowgone").await;
    }
    // Placement is random, so pin every shard to the node that is killed;
    // otherwise it may own nothing and losing it loses no shard range.
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/clnowgone", all_shards_on(2))
        .await;
    assert_eq!(status, StatusCode::OK);
    nodes[2].kill();
    // CouchDB: fabric_db_info answers {error, {nodedown, <<"progress not
    // possible">>}}, which chttpd maps to 412.
    let (status, body) = nodes[0].get("/clnowgone/_changes?since=now").await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_eq!(body["error"], "nodedown", "{body}");
    assert_eq!(body["reason"], "progress not possible", "{body}");
}

#[tokio::test]
async fn cluster_reads_fail_with_nodedown_when_a_shard_has_no_live_copy() {
    let mut nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("clreadgone").await;
    }
    // Pin placement: the design document's shard lives on node 0 (the
    // coordinator), every other shard only on node 2, which is then killed.
    let design = douchdb::shard_index("_design/v", 4);
    let replicas: Vec<Vec<usize>> = (0..4)
        .map(|shard| if shard == design { vec![0] } else { vec![2] })
        .collect();
    let (status, _) = nodes[0]
        .put(
            "/_node/_local/_dbs/clreadgone",
            json!({"q": 4, "node_count": 3, "replicas": replicas}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = nodes[0]
        .put(
            "/clreadgone/_design/v",
            json!({"views": {"all": {"map": "function(doc) { emit(doc._id, 1); }", "reduce": "_count"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let docs: Vec<Value> = (0..8).map(|i| json!({"_id": format!("rg{i}")})).collect();
    let (status, _) = nodes[0]
        .post("/clreadgone/_bulk_docs", json!({"docs": docs}))
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
        "{status}"
    );
    nodes[2].kill();
    // CouchDB: fabric_streams (all_docs, views, reduce) and fabric_view
    // (_changes) answer {error, {nodedown, <<"progress not possible">>}},
    // which chttpd maps to 412.
    let gets = [
        "/clreadgone/_all_docs",
        "/clreadgone/_design/v/_view/all?reduce=false",
        "/clreadgone/_design/v/_view/all",
        "/clreadgone/_changes",
    ];
    for path in gets {
        let (status, body) = nodes[0].get(path).await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{path}: {body}");
        assert_eq!(body["error"], "nodedown", "{path}: {body}");
        assert_eq!(body["reason"], "progress not possible", "{path}: {body}");
    }
    let (status, body) = nodes[0]
        .post("/clreadgone/_all_docs", json!({"keys": ["rg1", "rg2"]}))
        .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");
    assert_eq!(body["error"], "nodedown", "{body}");
    assert_eq!(body["reason"], "progress not possible", "{body}");
}

#[tokio::test]
async fn cluster_changes_forward_filter_query_args_to_every_shard() {
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("clchquery").await;
    }
    // Node 0 holds only the design document's shard and one more; node 1
    // holds every shard. Reads from node 0 therefore fetch the rest from node
    // 1, which must evaluate the filter with the same `req.query`.
    let design = douchdb::shard_index("_design/f", 4);
    let replicas: Vec<Vec<usize>> = (0..4)
        .map(|shard| {
            if shard == design || shard == (design + 1) % 4 {
                vec![0, 1]
            } else {
                vec![1]
            }
        })
        .collect();
    let (status, _) = nodes[0]
        .put(
            "/_node/_local/_dbs/clchquery",
            json!({"q": 4, "node_count": 3, "replicas": replicas}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = nodes[0]
        .put(
            "/clchquery/_design/f",
            json!({"filters": {"by_kind": "function(doc, req) { return doc.kind === req.query.kind; }"}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let docs: Vec<Value> = (0..12)
        .map(|i| json!({"_id": format!("fq{i}"), "kind": if i % 2 == 0 { "a&b%" } else { "c" }}))
        .collect();
    let (status, _) = nodes[0]
        .post("/clchquery/_bulk_docs", json!({"docs": docs}))
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
        "{status}"
    );
    let (status, body) = nodes[0]
        .get("/clchquery/_changes?filter=f/by_kind&kind=a%26b%25")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().unwrap().len(), 6, "{body}");
}

fn all_shards_on(node: usize) -> Value {
    json!({
        "q": 4,
        "node_count": 3,
        "replicas": vec![vec![node]; 4],
    })
}

/// A node to pin a database on so that a rebalance has to move it: node 0
/// unless the planned placement already keeps every shard there. Default
/// placement depends on the random ports.
fn node_to_pin_away_from(planned: &Value) -> usize {
    let all_on_zero = planned["replicas"]
        .as_array()
        .unwrap()
        .iter()
        .all(|replicas| replicas[0] == 0);
    usize::from(all_on_zero)
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

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
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
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
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
    // The peer may adopt the map on its own pass before ours has seen it
    // current, so the marker clears on whichever pass counts the quorum.
    let marker = nodes[0].dir.join("meta").join("pending-shard-map.json");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while marker.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "pending map was not cleared after quorum acknowledgement"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn cluster_rebalance_converges_and_cleans_old_replicas() {
    let nodes = start_cluster_with(3, "1", 1);
    for node in &nodes {
        node.fresh_db("reb").await;
    }
    let (_, planned) = nodes[0].get("/_node/_local/_dbs/reb").await;
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

    // The planner moves the shards to the placement creation gave them.
    let (status, map) = nodes[0]
        .post("/_node/_local/_dbs/reb/rebalance", json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "{map}");
    assert_eq!(map["replicas"], planned["replicas"], "{map}");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
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
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
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
            "/_node/_local/_dbs/autoreb/rebalance?timeout_ms=20000&cleanup=true",
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
            "/_node/_local/_dbs/timeoutreb/rebalance?timeout_ms=500&cleanup=true",
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
    let (status, map) = nodes[0].get("/_node/_local/_dbs/timeoutreb").await;
    assert_eq!(status, StatusCode::OK, "{map}");
    for replicas in map["replicas"].as_array().unwrap() {
        assert_eq!(replicas[0], 0, "timeout must restore the old shard map");
    }

    // The timeout also lifts the write freeze, and reads kept working.
    let (status, body) = nodes[0].get("/timeoutreb/to0").await;
    assert_eq!(status, StatusCode::OK, "read after timeout: {body}");
    let (status, body) = nodes[0].put("/timeoutreb/after", json!({"ok": true})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
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
    assert_eq!(baseline["results"].as_array().unwrap().len(), 9);
    let ids: std::collections::HashSet<&str> = baseline["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 9);
    let since = baseline["last_seq"].as_str().unwrap().to_string();

    let (reader, victim) = fallback_pair(
        &nodes,
        "cfail",
        &(0..9).map(|i| format!("cf{i}")).collect::<Vec<_>>(),
    )
    .await;
    nodes[victim].kill();
    let (status, _) = nodes[reader].put("/cfail/after", json!({"n": 100})).await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
        "write after node loss: {status}"
    );

    let (status, body) = nodes[reader]
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

    // A fresh replay from zero must recover changes that lived only on the
    // dead node; fallback shards cannot reuse the survivor's checkpoint.
    let (status, body) = nodes[reader].get("/cfail/_changes?since=0").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: std::collections::HashSet<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    for i in 0..9 {
        assert!(
            ids.contains(format!("cf{i}").as_str()),
            "missing cf{i}: {ids:?}"
        );
    }
    // While the owner is down, fallback changes are at-least-once and may
    // replay rows already seen before the checkpoint; clients dedupe by
    // revision. The feed must still contain every old document.

    // The `limit` path merges owner and fallback rows separately.
    let (status, body) = nodes[reader].get("/cfail/_changes?since=0&limit=50").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: std::collections::HashSet<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    for i in 0..9 {
        assert!(
            ids.contains(format!("cf{i}").as_str()),
            "limit feed missing cf{i}: {ids:?}"
        );
    }
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

    let (reader, victim) = fallback_pair(
        &nodes,
        "alldocsfail",
        &(0..9).map(|i| format!("al{i}")).collect::<Vec<_>>(),
    )
    .await;
    nodes[victim].kill();

    let (status, body) = nodes[reader].get("/alldocsfail/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 9);
    assert_eq!(body["rows"].as_array().unwrap().len(), 9);

    let (status, body) = nodes[reader]
        .get("/alldocsfail/_all_docs?startkey=%22al3%22&limit=3&skip=1")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 9);
    assert_eq!(body["offset"], 4);

    let (status, body) = nodes[reader]
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
    let (_, metrics) = nodes[0].get("/_node/_local/_metrics").await;
    let degraded_before = metrics["fabric"]["degraded_reads"].as_u64().unwrap();

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

    // Answered from the local replicas: no read had to fall back.
    let (_, metrics) = nodes[0].get("/_node/_local/_metrics").await;
    assert_eq!(
        metrics["fabric"]["degraded_reads"].as_u64().unwrap(),
        degraded_before,
        "reads fell back instead of using the local replica"
    );
}

#[tokio::test]
async fn cluster_post_creates_the_document_on_its_owner() {
    // Placement hashes the node URLs, so some port pairs give node 0 every
    // shard and nothing would be routed. Restart on new ports until node 1
    // owns at least one shard.
    let mut attempt = 0;
    let (nodes, map) = loop {
        let nodes = start_cluster(2);
        for node in &nodes {
            node.fresh_db("clpost").await;
        }
        let (_, map) = nodes[0].get("/_node/_local/_dbs/clpost").await;
        let primaries = map["replicas"].as_array().unwrap();
        if primaries.iter().any(|replicas| replicas[0] == 1) {
            break (nodes, map);
        }
        attempt += 1;
        assert!(attempt < 10, "node 1 never owned a shard: {map}");
    };
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let mut forwarded = false;

    // POST generates the id after the request arrives, so the handler must
    // route the resulting document rather than writing it to the local shards.
    for round in 0..200 {
        if forwarded && round >= 20 {
            break;
        }
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
        // The owner itself stores it, not just a node that can proxy to it.
        let shard = douchdb::shard_index(&id, map["q"].as_u64().unwrap() as u32);
        let owner = map["replicas"][shard][0].as_u64().unwrap() as usize;
        let (status, _) = nodes[owner]
            .req_with(Method::GET, &format!("/clpost/{id}"), None, &internal)
            .await;
        assert_eq!(status, StatusCode::OK, "{id} is not on its owner {owner}");
        forwarded |= owner != 0;
    }
    assert!(
        forwarded,
        "no generated id was owned by node 1; nothing was routed"
    );
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
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
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
async fn replication_source_and_target_agree_after_convergence() {
    let nodes = start_cluster_with(2, "1", 2);
    for node in &nodes {
        node.fresh_db("clagree").await;
    }

    let docs: Vec<Value> = (0..12)
        .map(|i| json!({"_id": format!("r{i:02}"), "n": i, "s": format!("s{}", i % 3)}))
        .collect();
    let response = nodes[0]
        .client
        .post(format!("{}/clagree/_bulk_docs", nodes[0].base))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", "test-cluster-secret")
        .json(&json!({"docs": docs}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut local: Vec<HashMap<String, Value>> = Vec::new();
    loop {
        local.clear();
        for node in &nodes {
            let body = node
                .client
                .post(format!("{}/clagree/_all_docs?include_docs=true", node.base))
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", "test-cluster-secret")
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            let rows: HashMap<String, Value> = body["rows"]
                .as_array()
                .unwrap()
                .iter()
                .map(|row| (row["id"].as_str().unwrap().to_string(), row.clone()))
                .collect();
            local.push(rows);
        }
        if local.iter().all(|rows| rows.len() == 12) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replicas did not converge: {:?}",
            local.iter().map(HashMap::len).collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(local[0], local[1], "local _all_docs differ");

    let mut changes: Vec<HashMap<String, Value>> = Vec::new();
    for node in &nodes {
        let (status, body) = node
            .get("/clagree/_changes?since=0&include_docs=true&style=all_docs&limit=100")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let mut latest = HashMap::new();
        for row in body["results"].as_array().unwrap() {
            latest.insert(row["id"].as_str().unwrap().to_string(), row.clone());
        }
        changes.push(latest);
    }
    assert_eq!(changes[0], changes[1], "cluster _changes differ");

    for id in local[0].keys() {
        let (status0, doc0) = nodes[0].get(&format!("/clagree/{id}")).await;
        let (status1, doc1) = nodes[1].get(&format!("/clagree/{id}")).await;
        assert_eq!(status0, StatusCode::OK);
        assert_eq!(status1, StatusCode::OK);
        assert_eq!(doc0, doc1, "{id}: reads differ across coordinators");
        assert_eq!(doc0["_rev"], local[0][id]["value"]["rev"], "{id}");
    }
}

#[tokio::test]
async fn internal_consistency_survives_node_loss_and_restart() {
    let mut nodes = start_cluster_with(3, "1", 2);
    for node in &nodes {
        node.fresh_db("ccons").await;
    }

    let initial: Vec<Value> = (0..30)
        .map(|i| json!({"_id": format!("i{i:02}"), "n": i, "s": "init"}))
        .collect();
    let (status, _) = nodes[0]
        .post("/ccons/_bulk_docs", json!({"docs": initial}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    nodes[2].kill();

    let base = nodes[0].base.clone();
    let client = nodes[0].client.clone();
    let mut writers = Vec::new();
    for worker in 0..4u32 {
        let base = base.clone();
        let client = client.clone();
        writers.push(tokio::spawn(async move {
            for index in 0..10u32 {
                let id = format!("w{worker}-{index:02}");
                let response = client
                    .put(format!("{base}/ccons/{id}"))
                    .json(&json!({"n": worker * 10 + index, "s": format!("s{worker}")}))
                    .send()
                    .await
                    .unwrap();
                assert!(
                    response.status() == StatusCode::CREATED
                        || response.status() == StatusCode::ACCEPTED,
                    "write {id}: {}",
                    response.status()
                );
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    nodes[2].restart();
    for writer in writers {
        writer.await.unwrap();
    }

    let expected = 30 + 40;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (_, all_docs) = nodes[2].get("/ccons/_all_docs?limit=1000").await;
        if all_docs["total_rows"] == expected {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "restarted node did not converge: {all_docs}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut changes: Vec<HashMap<String, Value>> = Vec::new();
    let mut all_rows: Vec<HashMap<String, Value>> = Vec::new();
    for node in &nodes {
        let (status, body) = node
            .get("/ccons/_changes?since=0&include_docs=true&limit=1000")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let latest: HashMap<String, Value> = body["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                let mut row = row.clone();
                row.as_object_mut().unwrap().remove("seq");
                (row["id"].as_str().unwrap().to_string(), row)
            })
            .collect();
        assert_eq!(latest.len(), expected, "changes row count");
        changes.push(latest);

        let (status, body) = node
            .get("/ccons/_all_docs?include_docs=true&limit=1000")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows: HashMap<String, Value> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| (row["id"].as_str().unwrap().to_string(), row["doc"].clone()))
            .collect();
        assert_eq!(rows.len(), expected, "all_docs row count");
        all_rows.push(rows);
    }
    assert_eq!(changes[0], changes[1], "changes differ after restart");
    assert_eq!(changes[0], changes[2], "changes differ after restart");
    assert_eq!(all_rows[0], all_rows[1], "all_docs differ after restart");
    assert_eq!(all_rows[0], all_rows[2], "all_docs differ after restart");

    let (status, _) = nodes[0]
        .put(
            "/ccons/_design/check",
            json!({"views": {"by_s": {"map": "function(doc){ if(doc.s) emit(doc.s, doc.n); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let mut views = Vec::new();
    for node in &nodes {
        let (_, body) = node
            .get("/ccons/_design/check/_view/by_s?reduce=false&limit=1000")
            .await;
        views.push(body);
    }
    assert_eq!(views[0]["total_rows"], expected);
    assert_eq!(views[0], views[1], "view differs after restart");
    assert_eq!(views[0], views[2], "view differs after restart");

    for id in all_rows[0].keys() {
        let (s0, d0) = nodes[0].get(&format!("/ccons/{id}")).await;
        let (s1, d1) = nodes[1].get(&format!("/ccons/{id}")).await;
        let (s2, d2) = nodes[2].get(&format!("/ccons/{id}")).await;
        assert_eq!(
            (s0, s1, s2),
            (StatusCode::OK, StatusCode::OK, StatusCode::OK)
        );
        assert_eq!(d0, d1, "{id}: read differs across coordinators");
        assert_eq!(d0, d2, "{id}: read differs after restart");
    }
}

#[tokio::test]
async fn cluster_anti_entropy_drains_a_large_backlog_in_one_pass() {
    // Background passes are off; the test triggers exactly one.
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("cldrain").await;
    }

    // Several 1,000-document pull pages.
    let total = 3500usize;
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

    let (_, before) = nodes[1].get("/_node/_local/_replication").await;
    let runs_before = before["runs"].as_u64().unwrap_or(0);
    let (status, _) = nodes[1]
        .post("/_node/_local/_replication/run?db=cldrain", json!({}))
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (_, status) = nodes[1].get("/_node/_local/_replication").await;
        if status["runs"].as_u64().unwrap_or(0) > runs_before && status["running"] == false {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the pass never finished: {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // That single pass must have drained every page.
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
    assert_eq!(response["total_rows"], total, "one pass left a backlog");
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

    // Kill a node that serves some of the reader's shards, while
    // anti-entropy is still moving shards around.
    let ids: Vec<String> = (0..12).map(|i| format!("k{i}")).collect();
    let (reader, victim) = fallback_pair(&nodes, "clkill", &ids).await;
    nodes[victim].kill();

    // _all_docs falls back to the surviving replicas.
    let (status, body) = nodes[reader].get("/clkill/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 12);
    // The changes feed falls back to the surviving replicas too.
    let (status, body) = nodes[reader].get("/clkill/_changes").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["last_seq"].is_string());
    let changed: std::collections::HashSet<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    for id in &ids {
        assert!(changed.contains(id.as_str()), "_changes lost {id}: {body}");
    }

    let second: Vec<Value> = (100..106)
        .map(|i| json!({"_id": format!("k{i}"), "n": i}))
        .collect();
    let (status, body) = nodes[reader]
        .post("/clkill/_bulk_docs", json!({"docs": second}))
        .await;
    assert!(
        status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
        "write after kill: {status} {body}"
    );
    assert!(body.as_array().unwrap().iter().all(|row| row["ok"] == true));

    // The default read quorum is r=2 and one of the two replicas is gone, so
    // a quorum read must fail rather than silently serve a best-effort answer.
    // Availability under node loss explicitly opts into the local fast path.
    let survivors: Vec<&Server> = (0..3).filter(|n| *n != victim).map(|n| &nodes[n]).collect();
    for i in (0..12).chain(100..106) {
        for node in &survivors {
            let (status, _) = node.get(&format!("/clkill/k{i}?r=1")).await;
            assert_eq!(status, StatusCode::OK, "k{i} via {}", node.base);
        }
    }

    // Anti-entropy converges the survivors to a full copy of every document
    // that has a replica among them.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut seen = std::collections::HashSet::new();
        for node in &survivors {
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

    // A document written to one survivor only reaches the other through
    // anti-entropy: no client write path copies it.
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let (_, map) = nodes[reader].get("/_node/_local/_dbs/clkill").await;
    let alive: Vec<usize> = (0..3).filter(|n| *n != victim).collect();
    let (id, owner, writer) = (0..)
        .map(|i| format!("ae{i}"))
        .find_map(|id| {
            let shard = douchdb::shard_index(&id, 4);
            let owners: Vec<usize> = map["replicas"][shard]
                .as_array()
                .unwrap()
                .iter()
                .map(|node| node.as_u64().unwrap() as usize)
                .collect();
            let owner = *alive.iter().find(|n| owners.contains(n))?;
            let writer = *alive.iter().find(|n| **n != owner)?;
            Some((id, owner, writer))
        })
        .unwrap();
    let (status, body) = nodes[writer]
        .req_with(
            Method::POST,
            "/clkill/_bulk_docs",
            Some(json!({"docs": [{"_id": id, "late": true}]})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, _) = nodes[owner]
            .req_with(Method::GET, &format!("/clkill/{id}"), None, &internal)
            .await;
        if status == StatusCode::OK {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "anti-entropy never copied {id} from node {writer} to its owner {owner}"
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

    // r=2 with one replica down is a quorum failure, not a best-effort read.
    let (status, body) = nodes[0].get("/clquorum/q2?r=2").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"], "quorum_not_met");
}

#[tokio::test]
async fn cluster_default_write_quorum_is_a_majority() {
    let mut nodes = start_cluster_with(3, "3600", 3);
    for node in &nodes {
        node.fresh_db("clquorumdefault").await;
    }
    nodes[2].kill();

    let (status, body) = nodes[0]
        .post(
            "/clquorumdefault/_bulk_docs",
            json!({"docs": [{"_id": "majority"}]}),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "default w=2 is still satisfied by two live replicas: {body}"
    );

    let (status, body) = nodes[0]
        .post(
            "/clquorumdefault/_bulk_docs?w=3",
            json!({"docs": [{"_id": "all"}]}),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "w=3 cannot be met with one replica down: {body}"
    );

    // With only one live replica the default (a majority, 2 of 3) is not
    // met either: the write is accepted but not confirmed.
    nodes[1].kill();
    let (status, body) = nodes[0]
        .post(
            "/clquorumdefault/_bulk_docs",
            json!({"docs": [{"_id": "minority"}]}),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "default w=2 cannot be met by one live replica: {body}"
    );
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
            // A random value shows whether an unchanged document was re-mapped.
            json!({"views": {"by_city": {"map": "function(doc){ emit(doc.city, Math.random()); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Build the index.
    let (_, body) = s.get("/incview/_design/v/_view/by_city").await;
    assert_eq!(body["total_rows"], 2);
    let index_dir = s.dir.join("incview").join("_view_indexes");
    assert!(index_dir.is_dir(), "view index directory was not created");

    let value_of = |body: &Value, id: &str| {
        body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == id)
            .map(|row| row["value"].clone())
            .unwrap()
    };
    let a_value = value_of(&body, "a");

    // A new document is picked up through _changes, without a rebuild.
    let (status, _) = s.put("/incview/c", json!({"city": "Paris"})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get("/incview/_design/v/_view/by_city").await;
    assert_eq!(body["total_rows"], 3);
    assert_eq!(body["rows"][2]["id"], "c");
    assert_eq!(
        value_of(&body, "a"),
        a_value,
        "unchanged doc a was re-mapped"
    );

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
async fn view_index_rebuilds_and_catches_up_in_pages() {
    let s = Server::start(false);
    s.fresh_db("pagedview").await;
    let docs = |range: std::ops::Range<u32>| {
        json!({"docs": range
            .map(|n| json!({"_id": format!("d{n:05}"), "n": n}))
            .collect::<Vec<_>>()})
    };
    let (status, _) = s.post("/pagedview/_bulk_docs", docs(0..9000)).await;
    assert_eq!(status, StatusCode::CREATED);
    let design = |map: &str, rev: Option<&str>| {
        let mut body = json!({"views": {"by_n": {"map": map}}});
        if let Some(rev) = rev {
            body["_rev"] = json!(rev);
        }
        body
    };
    let (status, put) = s
        .put(
            "/pagedview/_design/v",
            design("function(doc){ emit(doc.n, 1); }", None),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // The first build writes more rows than one batch holds.
    let (_, body) = s.get("/pagedview/_design/v/_view/by_n?limit=0").await;
    assert_eq!(body["total_rows"], 9000);

    // A new map function replaces every stored row, old and new, in batches.
    let rev = put["rev"].as_str().unwrap().to_string();
    let (status, _) = s
        .put(
            "/pagedview/_design/v",
            design("function(doc){ emit(doc.n + 100000, 1); }", Some(&rev)),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get("/pagedview/_design/v/_view/by_n?limit=1").await;
    assert_eq!(body["total_rows"], 9000);
    assert_eq!(body["rows"][0]["key"], 100000);

    // A catch-up longer than one page of changes: new documents, a changed
    // one and a deleted one.
    let (status, _) = s.post("/pagedview/_bulk_docs", docs(9000..11500)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, doc) = s.get("/pagedview/d00001").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    s.put("/pagedview/d00001", json!({"_rev": rev, "n": 5}))
        .await;
    let (_, doc) = s.get("/pagedview/d00002").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    let (status, _) = s
        .req(
            Method::DELETE,
            &format!("/pagedview/d00002?rev={rev}"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = s.get("/pagedview/_design/v/_view/by_n?limit=0").await;
    assert_eq!(body["total_rows"], 11499);
    let (_, body) = s.get("/pagedview/_design/v/_view/by_n?key=100005").await;
    assert_eq!(body["rows"][0]["id"], "d00001");
    let (_, body) = s.get("/pagedview/_design/v/_view/by_n?key=111499").await;
    assert_eq!(body["rows"][0]["id"], "d11499");
    let (_, body) = s.get("/pagedview/_design/v/_view/by_n?key=100001").await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 0);
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
        assert_eq!(status, StatusCode::OK, "view via {}: {body}", node.base);
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
    let _guard = SPAWN_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
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
        assert_eq!(status, StatusCode::OK, "view via {}: {body}", node.base);
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
    let (reader, victim) = fallback_pair(&nodes, "clviewfail", &["a", "b"].map(String::from)).await;
    nodes[victim].kill();
    let (status, body) = nodes[reader]
        .get("/clviewfail/_design/v/_view/by_city")
        .await;
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
    let (status, body) = nodes[reader].get("/clviewfail/_design/v/_view/count").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 2);
    let (status, body) = nodes[reader]
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
async fn cluster_search_survives_a_node_loss_via_replicas() {
    let mut nodes = start_cluster_with(3, "3600", 2);
    for node in &nodes {
        node.fresh_db("clsearchfail").await;
    }
    let (status, _) = nodes[0]
        .post(
            "/clsearchfail/_bulk_docs",
            json!({"docs": [
                {"_id": "apple", "item": "apple", "place": "kitchen", "state": "new", "price": 0.99},
                {"_id": "banana", "item": "banana", "place": "kitchen", "state": "new", "price": 1.49},
                {"_id": "carrot", "item": "carrot", "place": "kitchen", "state": "old", "price": 0.75},
                {"_id": "date", "item": "date", "place": "lobby", "state": "unknown", "price": 1.25}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = nodes[0]
        .put("/clsearchfail/_design/inventory", search_ddoc())
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let url = "/clsearchfail/_design/inventory/_search/fruits?q=*:*&counts=[\"place\"]";
    let (status, before) = nodes[0].get(url).await;
    assert_eq!(status, StatusCode::OK, "{before}");
    assert_eq!(before["total_rows"], 4);

    // The dead node's shards are answered by their replicas, and results from
    // the surviving owners are kept rather than queried again.
    let (reader, victim) = fallback_pair(
        &nodes,
        "clsearchfail",
        &["apple", "banana", "carrot", "date"].map(String::from),
    )
    .await;
    nodes[victim].kill();
    let (status, body) = nodes[reader].get(url).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4);
    let mut ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["apple", "banana", "carrot", "date"]);
    assert_eq!(body["counts"], before["counts"], "{body}");
}

#[tokio::test]
async fn authenticated_cluster_anti_entropy_converges() {
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

    // Write only on node 0; node 1 must pull the documents with no client
    // request involved. Peers accept the cluster secret, so this cannot see
    // which credentials are sent; `replicate::tests` checks those headers.
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

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
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
async fn cluster_serves_session_cookie_clients() {
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

    // Database create is broadcast. Peers accept the cluster secret, so this
    // cannot see whether the cookie is forwarded; `api::tests` checks that.
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

    // Reads and writes through a cookie-authenticated client still work.
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

/// The database "a+b" (created as `a%2Bb`) round-trips through the peer URLs,
/// which encode `+` as `%2B`; the receiving node must not read it as a space.
#[tokio::test]
async fn cluster_plus_database_name_round_trips_between_nodes() {
    let nodes = start_cluster_sharded(3, 4, 1);
    let (status, body) = nodes[0].put("/cp%2Bq", json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for (i, node) in nodes.iter().enumerate() {
        let (status, body) = node.put(&format!("/cp%2Bq/doc{i}"), json!({"n": i})).await;
        assert_eq!(status, StatusCode::CREATED, "{}: {body}", node.base);
    }
    for node in &nodes {
        let (status, body) = node.get("/cp%2Bq").await;
        assert_eq!(status, StatusCode::OK, "{}: {body}", node.base);
        assert_eq!(body["db_name"], "cp+q");
        let (_, listed) = node.get("/_all_dbs").await;
        assert!(listed.to_string().contains("\"cp+q\""), "{listed}");
        for i in 0..nodes.len() {
            let (status, doc) = node.get(&format!("/cp%2Bq/doc{i}")).await;
            assert_eq!(status, StatusCode::OK, "{}: {doc}", node.base);
        }
        let (status, body) = node.get("/cp+q").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{}: {body}", node.base);
    }
}

#[tokio::test]
async fn cluster_internal_requests_encode_db_names_and_doc_ids() {
    // `a/b` is a legal database name (CouchDB clients send it as `a%2Fb`) and
    // document ids may hold `?`, `#`, `%` and `/`. Every internal peer URL
    // must percent-encode them so the receiving node decodes exactly once.
    let nodes = start_cluster_sharded(3, 4, 1);
    let db = "/a%2Fb";
    let (status, body) = nodes[0].put(db, json!({})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for node in &nodes {
        let (status, body) = node.get(db).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "db missing via {}: {body}",
            node.base
        );
        assert_eq!(body["db_name"], "a/b");
        let (_, listed) = node.get("/_all_dbs").await;
        assert!(listed.to_string().contains("\"a/b\""), "{listed}");
    }

    let ids = ["q?x=1", "h#frag", "100%", "a%2Fb", "p/q", "plain"];
    let encode = |id: &str| {
        id.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect::<String>()
    };
    for (i, id) in ids.iter().enumerate() {
        let node = &nodes[i % nodes.len()];
        let (status, body) = node
            .put(&format!("{db}/{}", encode(id)), json!({"n": i}))
            .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "put {id} via {}: {body}",
            node.base
        );
    }
    let ddoc = json!({"views": {"v": {
        "map": "function(doc){ emit(doc.n, 1); }",
        "reduce": "_count",
    }}});
    let (status, body) = nodes[0].put(&format!("{db}/_design/d"), ddoc).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Every node reads every document, whether or not it holds a replica.
    for node in &nodes {
        for (i, id) in ids.iter().enumerate() {
            let (status, doc) = node.get(&format!("{db}/{}", encode(id))).await;
            assert_eq!(status, StatusCode::OK, "get {id} via {}: {doc}", node.base);
            assert_eq!(doc["_id"], *id);
            assert_eq!(doc["n"], i);
        }
        let (status, all) = node.get(&format!("{db}/_all_docs")).await;
        assert_eq!(status, StatusCode::OK, "{all}");
        assert_eq!(all["rows"].as_array().map(Vec::len), Some(ids.len() + 1));
        let path = format!("{db}/_design/d/_view/v?reduce=false");
        let (status, view) = node.get(&path).await;
        assert_eq!(status, StatusCode::OK, "{view}");
        assert_eq!(view["rows"].as_array().map(Vec::len), Some(ids.len()));
        let (status, reduced) = node.get(&format!("{db}/_design/d/_view/v")).await;
        assert_eq!(status, StatusCode::OK, "{reduced}");
        assert_eq!(reduced["rows"][0]["value"], ids.len());
        let (status, changes) = node.get(&format!("{db}/_changes")).await;
        assert_eq!(status, StatusCode::OK, "{changes}");
        assert_eq!(
            changes["results"].as_array().map(Vec::len),
            Some(ids.len() + 1)
        );
    }

    // Dropping through a node broadcasts to the others.
    let (status, _) = nodes[1].delete(db).await;
    assert_eq!(status, StatusCode::OK);
    for node in &nodes {
        let (status, _) = node.get(db).await;
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
        "custom": {"map": map, "reduce": "function(keys, values, rereduce){ return rereduce ? sum(values) : sum(values) * 2; }"},
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

    // A key with no rows still reports its insertion point as the offset.
    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?key=%22Munich%22")
        .await;
    assert_eq!(body["offset"], 2);
    assert!(body["rows"].as_array().unwrap().is_empty());
    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?key=%22Munich%22&skip=1")
        .await;
    assert_eq!(body["offset"], 3);
    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?key=%22Munich%22&descending=true")
        .await;
    assert_eq!(body["offset"], 2);

    // A reversed range is an error in the matching direction.
    let (status, body) = s
        .get("/shop/_design/shop/_view/by_city?startkey=%22Paris%22&endkey=%22Berlin%22")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "query_parse_error");
    let (status, body) = s
        .get("/shop/_design/shop/_view/by_city?startkey=%22Paris%22&endkey=%22Berlin%22&descending=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

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

    // Descending reverses the requested key order as well as row order.
    let (_, body) = s
        .get("/shop/_design/shop/_view/by_city?keys=%5B%22Berlin%22%2C%22Paris%22%5D&descending=true")
        .await;
    let ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["d", "c", "b", "a"]);

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

    // Reduce rows honour skip and limit, including grouped output.
    let (_, body) = s
        .get("/shop/_design/shop/_view/count?group=true&limit=1")
        .await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["key"], "Berlin");
    let (_, body) = s
        .get("/shop/_design/shop/_view/count?group=true&skip=1")
        .await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["key"], "Paris");
    let (_, body) = s.get("/shop/_design/shop/_view/count?skip=1").await;
    assert!(body["rows"].as_array().unwrap().is_empty());

    // An ungrouped reduce over no matching rows returns no row.
    let (_, body) = s
        .get("/shop/_design/shop/_view/count?startkey=%22zzz%22")
        .await;
    assert!(body["rows"].as_array().unwrap().is_empty());

    // Multi-key fetches on a reduce view require group=true.
    let (status, body) = s
        .get("/shop/_design/shop/_view/count?keys=%5B%22Berlin%22%2C%22Paris%22%5D")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "query_parse_error");
    let (status, body) = s
        .get("/shop/_design/shop/_view/count?group=true&keys=%5B%22Berlin%22%2C%22Paris%22%5D")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);

    // An explicit group_level=0 overrides group=true and gives a global row.
    let (_, body) = s
        .get("/shop/_design/shop/_view/count?group=true&group_level=0")
        .await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(body["rows"][0]["key"], Value::Null);
    assert_eq!(body["rows"][0]["value"], 4);

    // Contradictory grouping and incompatible keys/ranges are query errors.
    let (status, body) = s
        .get("/shop/_design/shop/_view/count?group=false&group_level=2")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "query_parse_error");
    let (status, body) = s
        .get(
            "/shop/_design/shop/_view/count?keys=%5B%22Berlin%22%2C%22Paris%22%5D&startkey=%22Paris%22",
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "query_parse_error");

    // Grouping a map view and include_docs on a reduce view are query errors.
    let (status, body) = s
        .get("/shop/_design/shop/_view/by_city?group=true&reduce=false")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "query_parse_error");
    let (status, body) = s
        .get("/shop/_design/shop/_view/count?include_docs=true")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "query_parse_error");

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
    let mut nodes = start_cluster_with(3, "3600", 2);
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
    let (_, map) = nodes[0].get("/_node/_local/_dbs/clpart").await;
    let mut even_shards = std::collections::HashSet::new();
    for shard in 0..4 {
        // Read each shard from a node that holds a replica of it.
        let holder = map["replicas"][shard][0].as_u64().unwrap() as usize;
        let (status, body) = nodes[holder]
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
    // A partition hashes as `{partition}:foo`; kill the node that serves the
    // "even" shard for a coordinator without its own replica.
    let (reader, victim) = fallback_pair(&nodes, "clpart", &["even:foo".to_string()]).await;
    nodes[victim].kill();
    let (status, body) = nodes[reader]
        .get("/clpart/_partition/even/_design/p/_view/count")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 12);
    let (status, body) = nodes[reader]
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
    assert_eq!(leaves, ["2-c", "2-b"], "winner first");

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

    let (status, head_headers, body) = s
        .req_headers(Method::HEAD, &format!("/{name}/doc0"), None, &[])
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(head_headers.get("etag"), Some(&etag));
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
                "distinct": {"map": "function(doc){ emit(doc.value, null); }", "reduce": "_approx_count_distinct"}
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
async fn couchdb_approx_count_distinct_counts_keys() {
    // Expected bodies were captured from CouchDB 3.5.3. `_approx_count_distinct`
    // counts distinct emitted keys, not values.
    let s = Server::start(false);
    s.fresh_db("approxkeys").await;
    let (status, _) = s
        .post(
            "/approxkeys/_bulk_docs",
            json!({"docs": [
                {"_id": "d1", "k": "a", "v": 1},
                {"_id": "d2", "k": "b", "v": 2},
                {"_id": "d3", "k": "a", "v": 5}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/approxkeys/_design/d",
            json!({"views": {
                "a": {"map": "function(d){ emit(d.k, d.v); }", "reduce": "_approx_count_distinct"},
                "arr": {"map": "function(d){ emit([d.k, d.v % 3], d.v); }", "reduce": "_approx_count_distinct"}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get("/approxkeys/_design/d/_view/a").await;
    assert_eq!(body["rows"], json!([{"key": null, "value": 2}]));
    let (_, body) = s.get("/approxkeys/_design/d/_view/a?group=true").await;
    assert_eq!(
        body["rows"],
        json!([{"key": "a", "value": 1}, {"key": "b", "value": 1}])
    );
    let (_, body) = s.get("/approxkeys/_design/d/_view/arr").await;
    assert_eq!(body["rows"], json!([{"key": null, "value": 3}]));
    let (_, body) = s.get("/approxkeys/_design/d/_view/arr?group_level=1").await;
    assert_eq!(
        body["rows"],
        json!([{"key": ["a"], "value": 2}, {"key": ["b"], "value": 1}])
    );
    let (_, body) = s.get("/approxkeys/_design/d/_view/arr?group=true").await;
    assert_eq!(
        body["rows"],
        json!([
            {"key": ["a", 1], "value": 1},
            {"key": ["a", 2], "value": 1},
            {"key": ["b", 2], "value": 1}
        ])
    );
}

/// Keys of every JSON type, with repeats, for the estimate checks below.
const APPROX_MIXED_KEY: &str = "var n = doc.n, t = n % 6, k; \
    if (t == 0) k = n * 10000000000; else if (t == 1) k = 'k' + n; \
    else if (t == 2) k = [n, 'x']; else if (t == 3) k = {a: n, b: [n]}; \
    else if (t == 4) k = n + 0.5; \
    else { var r = n % 7; k = r == 0 ? null : r == 1 ? true : r == 2 ? false : [n % 100, n % 3]; }";

fn approx_mixed_ddoc() -> Value {
    json!({"views": {
        "all": {"map": format!("function(doc){{ {APPROX_MIXED_KEY} emit(k, 1); }}"),
                "reduce": "_approx_count_distinct"},
        "lvl": {"map": format!("function(doc){{ {APPROX_MIXED_KEY} emit([doc.n % 4, k], 1); }}"),
                "reduce": "_approx_count_distinct"}
    }})
}

fn approx_mixed_docs() -> Vec<Value> {
    (0..1200)
        .map(|n| json!({"_id": format!("d{n:04}"), "n": n}))
        .collect()
}

async fn assert_approx_mixed_estimates(s: &Server, db: &str) {
    let (_, body) = s.get(&format!("/{db}/_design/d/_view/all")).await;
    assert_eq!(body["rows"], json!([{"key": null, "value": 1054}]));
    let (_, body) = s
        .get(&format!("/{db}/_design/d/_view/lvl?group_level=1"))
        .await;
    assert_eq!(
        body["rows"],
        json!([
            {"key": [0], "value": 303},
            {"key": [1], "value": 226},
            {"key": [2], "value": 298},
            {"key": [3], "value": 232}
        ])
    );
    let (_, body) = s
        .get(&format!(
            "/{db}/_design/d/_view/all?startkey=%22k1%22&endkey=%22k5%22"
        ))
        .await;
    assert_eq!(body["rows"], json!([{"key": null, "value": 110}]));
}

#[tokio::test]
async fn couchdb_approx_count_distinct_estimates_match() {
    // CouchDB's estimate is a HyperLogLog (precision 11) over the SHA-1 of
    // each key's term_to_binary, so 1054 distinct keys read as 1054 only by
    // luck of the registers: these numbers, captured from CouchDB 3.5.3 for
    // keys of every JSON type (integers beyond 32 bits, strings, arrays,
    // objects, floats, booleans, null), need the same hash input for each.
    let s = Server::start(false);
    s.fresh_db("approxmixed").await;
    let (status, _) = s
        .post(
            "/approxmixed/_bulk_docs",
            json!({"docs": approx_mixed_docs()}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s.put("/approxmixed/_design/d", approx_mixed_ddoc()).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_approx_mixed_estimates(&s, "approxmixed").await;
}

#[tokio::test]
async fn couchdb_reduce_duplicate_keys_and_structured_values() {
    // Expected bodies were captured from CouchDB 3.5. Each requested key
    // gets its own row, so a repeated key repeats its reduced value rather
    // than adding it twice; `_sum` adds arrays and objects member by member
    // and `_stats` accepts stats objects.
    let s = Server::start(false);
    s.fresh_db("reducestruct").await;
    let (status, _) = s
        .post(
            "/reducestruct/_bulk_docs",
            json!({"docs": [
                {"_id": "d1", "k": "a", "v": 1},
                {"_id": "d2", "k": "b", "v": 2},
                {"_id": "d3", "k": "a", "v": 5}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/reducestruct/_design/d",
            json!({"views": {
                "sum": {"map": "function(d){ emit(d.k, d.v); }", "reduce": "_sum"},
                "byv": {"map": "function(d){ emit(d.v, d.v); }", "reduce": "_sum"},
                "arr": {"map": "function(d){ emit(d.k, [d.v, 1]); }", "reduce": "_sum"},
                "obj": {"map": "function(d){ emit(d.k, {x: d.v}); }", "reduce": "_sum"},
                "mixed": {"map": "function(d){ emit(d.k, d._id == 'd1' ? 1 : [1]); }", "reduce": "_sum"},
                "bad": {"map": "function(d){ emit(d.k, 'x'); }", "reduce": "_sum"},
                "stobj": {
                    "map": "function(d){ emit(d.k, {sum: d.v, count: 1, min: d.v, max: d.v, sumsqr: d.v * d.v}); }",
                    "reduce": "_stats"
                },
                "starr": {"map": "function(d){ emit(d.k, [d.v]); }", "reduce": "_stats"},
                "stbad": {"map": "function(d){ emit(d.k, 'x'); }", "reduce": "_stats"}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let rows = |body: &Value| body["rows"].clone();
    let view = |path: &str| format!("/reducestruct/_design/d/_view/{path}");

    let (status, body) = s.get(&view("sum?keys=[\"a\",\"a\"]&group=true")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        rows(&body),
        json!([{"key": "a", "value": 6}, {"key": "a", "value": 6}])
    );
    let (_, body) = s
        .get(&view("sum?keys=[\"a\",\"b\",\"a\"]&group=true"))
        .await;
    assert_eq!(
        rows(&body),
        json!([
            {"key": "a", "value": 6},
            {"key": "b", "value": 2},
            {"key": "a", "value": 6}
        ])
    );
    let (_, body) = s
        .get(&view("sum?keys=[\"a\",\"a\"]&group=true&descending=true"))
        .await;
    assert_eq!(
        rows(&body),
        json!([{"key": "a", "value": 6}, {"key": "a", "value": 6}])
    );
    let (_, body) = s.get(&view("sum?keys=[\"zz\",\"a\"]&group=true")).await;
    assert_eq!(rows(&body), json!([{"key": "a", "value": 6}]));
    let (_, body) = s.get(&view("byv?keys=[1,1]&group=true&skip=1")).await;
    assert_eq!(rows(&body), json!([{"key": 1, "value": 1}]));
    let (_, body) = s
        .get(&view("byv?keys=[1,1,5]&group=true&skip=1&limit=2"))
        .await;
    assert_eq!(
        rows(&body),
        json!([{"key": 1, "value": 1}, {"key": 5, "value": 5}])
    );

    let (_, body) = s.get(&view("arr")).await;
    assert_eq!(rows(&body), json!([{"key": null, "value": [8, 3]}]));
    let (_, body) = s.get(&view("arr?group=true")).await;
    assert_eq!(
        rows(&body),
        json!([{"key": "a", "value": [6, 2]}, {"key": "b", "value": [2, 1]}])
    );
    let (_, body) = s.get(&view("obj")).await;
    assert_eq!(rows(&body), json!([{"key": null, "value": {"x": 8}}]));
    let (_, body) = s.get(&view("obj?group=true")).await;
    assert_eq!(
        rows(&body),
        json!([{"key": "a", "value": {"x": 6}}, {"key": "b", "value": {"x": 2}}])
    );
    // A number added to an array is added to its first element.
    let (_, body) = s.get(&view("mixed")).await;
    assert_eq!(rows(&body), json!([{"key": null, "value": [3]}]));
    let (_, body) = s.get(&view("mixed?group=true")).await;
    assert_eq!(
        rows(&body),
        json!([{"key": "a", "value": [2]}, {"key": "b", "value": [1]}])
    );

    // An invalid value is an error row, not a failed request.
    let (status, body) = s.get(&view("bad")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let value = &body["rows"][0]["value"];
    assert_eq!(value["error"], "builtin_reduce_error", "{body}");
    assert_eq!(value["caused_by"], "x");
    assert_eq!(
        value["reason"],
        "The _sum function requires that map values be numbers, arrays of numbers, or objects. Objects cannot be mixed with other data structures. Objects can be arbitrarily nested, provided that the values for all fields are themselves numbers, arrays of numbers, or objects."
    );

    let (_, body) = s.get(&view("stobj")).await;
    assert_eq!(
        rows(&body),
        json!([{"key": null, "value": {"sum": 8, "count": 3, "min": 1, "max": 5, "sumsqr": 30}}])
    );
    let (_, body) = s.get(&view("stobj?group=true")).await;
    assert_eq!(
        rows(&body),
        json!([
            {"key": "a", "value": {"sum": 6, "count": 2, "min": 1, "max": 5, "sumsqr": 26}},
            {"key": "b", "value": {"sum": 2, "count": 1, "min": 2, "max": 2, "sumsqr": 4}}
        ])
    );
    let (_, body) = s.get(&view("starr")).await;
    assert_eq!(
        rows(&body),
        json!([{"key": null, "value": [{"sum": 8, "count": 3, "min": 1, "max": 5, "sumsqr": 30}]}])
    );
    let (status, body) = s.get(&view("stbad")).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "invalid_value");
}

#[tokio::test]
async fn view_map_reads_proto_like_javascript() {
    // CouchDB 3.5: with a document that has no own `__proto__`, both
    // `doc.__proto__` and `doc["__proto__"]` are Object.prototype, which
    // emits as {}; `doc["constructor"]` is a function, which emits as null.
    let s = Server::start(false);
    s.fresh_db("viewproto").await;
    let (status, _) = s.put("/viewproto/p1", json!({"x": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/viewproto/_design/d",
            json!({"views": {
                "bracket": {"map": "function(doc){ emit(doc[\"__proto__\"], 1); }"},
                "dot": {"map": "function(doc){ emit(doc.__proto__, 1); }"},
                "ctor": {"map": "function(doc){ emit(doc[\"constructor\"], 1); }"}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    for (view, key) in [
        ("bracket", json!({})),
        ("dot", json!({})),
        ("ctor", Value::Null),
    ] {
        let (status, body) = s.get(&format!("/viewproto/_design/d/_view/{view}")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["rows"],
            json!([{"id": "p1", "key": key, "value": 1}]),
            "{view}"
        );
    }
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
async fn view_collation_orders_object_member_names_with_icu() {
    // Order captured from CouchDB 3.5: member names collate like strings
    // (ICU), not bytewise, so "a" < "B" and "é" < "f" < "Z".
    let s = Server::start(false);
    s.fresh_db("objcollation").await;
    let keys = [
        json!({"_": 1}),
        json!({"a": 1}),
        json!({"a": 1, "B": 2}),
        json!({"b": 1, "A": 1}),
        json!({"B": 1}),
        json!({"é": 1}),
        json!({"f": 1}),
        json!({"Z": 1}),
    ];
    let docs: Vec<Value> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| json!({"_id": i.to_string(), "k": key}))
        .collect();
    let (status, _) = s
        .post("/objcollation/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/objcollation/_design/d",
            json!({"views": {"v": {"map": "function(doc){ emit(doc.k, null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s.get("/objcollation/_design/d/_view/v").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let got: Vec<Value> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["key"].clone())
        .collect();
    assert_eq!(got, keys.to_vec());
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
    // Keys come from JavaScript, as in CouchDB's view server, so the stored
    // `3.0` is emitted as `3`. CouchDB's own test compares with `===`, where
    // the two are equal.
    let expected: Vec<Value> = values
        .iter()
        .map(|value| {
            if *value == json!(3.0) {
                json!(3)
            } else {
                value.clone()
            }
        })
        .collect();
    assert_eq!(keys, expected);

    let (status, body) = s
        .get("/couchcollation/_design/foo/_view/foo?descending=true")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut reversed = expected.clone();
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
async fn a_recreated_database_does_not_search_the_old_index() {
    let s = Server::start(false);
    seed_search_docs(&s, "searchrecreate").await;
    let url = "/searchrecreate/_design/inventory/_search/fruits?q=*:*";
    let (status, body) = s.get(url).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4, "{body}");

    // The same design and the same number of writes, so only the documents
    // tell the new index from the old one.
    let (status, _) = s.delete("/searchrecreate").await;
    assert_eq!(status, StatusCode::OK);
    s.fresh_db("searchrecreate").await;
    let (status, _) = s
        .post(
            "/searchrecreate/_bulk_docs",
            json!({"docs": [
                {"_id": "fig", "item": "fig"},
                {"_id": "grape", "item": "grape"},
                {"_id": "kiwi", "item": "kiwi"},
                {"_id": "lime", "item": "lime"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put("/searchrecreate/_design/inventory", search_ddoc())
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s.get(url).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["fig", "grape", "kiwi", "lime"], "{body}");
}

#[tokio::test]
async fn purged_documents_leave_search_results() {
    let s = Server::start(false);
    seed_search_docs(&s, "searchpurge").await;
    let url = "/searchpurge/_design/inventory/_search/fruits?q=*:*";
    let (status, body) = s.get(url).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4, "{body}");

    let (_, doc) = s.get("/searchpurge/apple").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    let (status, body) = s.post("/searchpurge/_purge", json!({"apple": [rev]})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // A later write moves the database on, so the index must catch up from
    // its changes, where a fully purged document no longer appears.
    let (status, _) = s.put("/searchpurge/egg", json!({"item": "egg"})).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s.get(url).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let mut ids: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["banana", "carrot", "date", "egg"], "{body}");
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
    // An earlier write, so `now` is not simply sequence 0.
    let (status, _) = s.put("/pouchchanges/zero", json!({"n": 0})).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s.get("/pouchchanges/_changes?since=now").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["results"].as_array().unwrap().is_empty(), "{body}");
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
    assert_eq!(ids, ["zero", "one", "c", "a"]);

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
            "/couchconflict/doc-1?rev=1-foobar",
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

/// `_index_info` is read-only: other methods are 405 with an `Allow` list,
/// not a silent read (or, for PUT and DELETE, a write that goes unchecked).
#[tokio::test]
async fn index_info_only_answers_get_and_head() {
    let s = Server::start(false);
    s.fresh_db("idxinfo").await;
    for method in [Method::POST, Method::PUT, Method::DELETE] {
        let response = s
            .client
            .request(method.clone(), format!("{}/idxinfo/_index_info", s.base))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "{method}"
        );
        let allow = response
            .headers()
            .get("allow")
            .and_then(|v| v.to_str().ok());
        assert_eq!(allow, Some("GET,HEAD"), "{method}");
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["error"], "method_not_allowed", "{method}");
        assert_eq!(body["reason"], "Only GET,HEAD allowed", "{method}");
    }
    let (status, body) = s.get("/idxinfo/_index_info").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 0);
}

#[tokio::test]
async fn search_highlights_match_upper_case_non_ascii_query_terms() {
    let s = Server::start(false);
    s.fresh_db("searchunicode").await;
    let (status, _) = s
        .post(
            "/searchunicode/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "item": "ünï café"},
                {"_id": "b", "item": "İstanbul trip"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put("/searchunicode/_design/inventory", search_ddoc())
        .await;
    assert_eq!(status, StatusCode::CREATED);

    for (query, id, expected) in [
        ("item:Ünï", "a", "<em>ünï</em> café"),
        ("item:CAFÉ", "a", "ünï <em>café</em>"),
        ("item:\"CAFÉ\"", "a", "ünï <em>café</em>"),
        ("item:İstanbul", "b", "<em>İstanbul</em> trip"),
    ] {
        let (status, body) = s
            .post(
                "/searchunicode/_design/inventory/_search/fruits",
                json!({"q": query, "highlight_fields": ["item"]}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let row = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == id)
            .unwrap_or_else(|| panic!("{query}: {body}"));
        assert_eq!(row["highlights"]["item"], json!([expected]), "{query}");
    }
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
        assert_eq!(status, StatusCode::OK, "search via {}: {body}", node.base);
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
        for _ in 0..600 {
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
    for _ in 0..1200 {
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
    for _ in 0..600 {
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
    for _ in 0..600 {
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
    for _ in 0..600 {
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

    // The loop never ends, so only the deadline can answer this request; a
    // hang is for the test runner's failsafe to catch.
    let (status, body) = s.get("/jsloop/_design/v/_view/loop").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "view_error");
}

#[tokio::test]
async fn quickjs_process_sandbox_runs_views_reduce_and_search() {
    if !sandbox_tested("process") {
        return;
    }
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
    if !sandbox_tested("process") {
        return;
    }
    let ddoc = json!({"views": {
        "loop": {"map": "function(doc){ while (true) {} }"},
        "ok": {"map": "function(doc){ var n = doc.v + 1; emit(n, null); }"}
    }});

    // A server with the generous default deadline runs the worker first, so
    // a startup failure cannot satisfy the deadline assertions below.
    let args = vec!["--js-sandbox".to_string(), "process".to_string()];
    let patient = Server::start_with(&args, false);
    patient.fresh_db("jsploop").await;
    let (status, _) = patient.put("/jsploop/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = patient.put("/jsploop/_design/v", ddoc.clone()).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = patient.get("/jsploop/_design/v/_view/ok").await;
    assert_eq!(status, StatusCode::OK, "process worker did not run: {body}");
    assert_eq!(body["rows"][0]["key"], 2);

    // The tight deadline only ever meets the infinite loop, which no machine
    // speed can finish in time.
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
    let (status, _) = s.put("/jsploop/_design/v", ddoc).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s.get("/jsploop/_design/v/_view/loop").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "view_error");
    assert!(
        body["reason"]
            .as_str()
            .unwrap_or_default()
            .contains("timed out"),
        "deadline cause was not reported: {body}"
    );

    // The pool recovers and serves valid work after the deadline kill. The
    // tight deadline can also expire on a slow machine, so ask until a run
    // fits in it.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = s.get("/jsploop/_design/v/_view/ok").await;
        if status == StatusCode::OK {
            assert_eq!(body["rows"][0]["key"], 2, "{body}");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "worker did not recover: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn quickjs_process_pool_handles_concurrent_queries() {
    if !sandbox_tested("process") {
        return;
    }
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
            json!({"views": {"by_city": {"map": "function(doc){ var suffix = '-x'; emit(doc.city + suffix, null); }"}}}),
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
    if !sandbox_tested("wasm") {
        return;
    }
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
async fn sandboxed_reduce_rereduce_receives_null_keys() {
    // The sandboxes run side by side: the wasm one is slow to start.
    tokio::join!(
        assert_rereduce_keys_are_null("process"),
        assert_rereduce_keys_are_null("wasm")
    );
}

async fn assert_rereduce_keys_are_null(mode: &str) {
    // CouchDB passes keys = null to a rereduce. With two shards, the
    // partial reductions of the shards are combined with a rereduce.
    if !sandbox_tested(mode) {
        return;
    }
    let args = vec![
        "--js-sandbox".to_string(),
        mode.to_string(),
        "--q".to_string(),
        "2".to_string(),
    ];
    let s = Server::start_with(&args, false);
    s.fresh_db("jsrereduce").await;
    let docs: Vec<Value> = (0..8).map(|i| json!({"_id": format!("d{i}")})).collect();
    let (status, _) = s
        .post("/jsrereduce/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jsrereduce/_design/v",
            json!({"views": {"total": {
                "map": "function(doc){ emit(null, 1); }",
                "reduce": "function(keys, values, rereduce){ if (rereduce && keys !== null) return -1; return sum(values); }"
            }}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s.get("/jsrereduce/_design/v/_view/total").await;
    assert_eq!(status, StatusCode::OK, "{mode}: {body}");
    assert_eq!(body["rows"][0]["value"], 8, "{mode}: {body}");
}

#[tokio::test]
#[allow(unused_mut, clippy::useless_vec)]
async fn js_sandboxes_agree_on_view_rows() {
    if !sandbox_tested("process") || !sandbox_tested("wasm") {
        return;
    }
    let docs = json!({"docs": [
        {"_id": "a", "city": "Berlin", "price": 10, "s": "héllo"},
        {"_id": "b", "city": "Paris", "price": 20, "s": "😀"},
        {"_id": "c", "city": "Berlin", "price": 30, "s": ""},
        {"_id": "d", "city": "Zürich", "price": 5}
    ]});
    let ddoc = json!({"views": {
        "fields": {"map": "function(doc){ emit(doc.city, doc.price); }"},
        "computed": {"map": "function(doc){ if (doc.price) emit(doc.city + \"-\" + doc.price, doc.price * 2); }"},
        "nested": {"map": "function(doc){ if (doc.city) emit([doc.city, doc.price], {c: doc.city}); }"},
        "unicode": {"map": "function(doc){ if (doc.s !== undefined) emit(doc.s, doc.s.length); }"}
    }});

    let mut servers = vec![
        Server::start(false),
        Server::start_with(&["--js-sandbox".to_string(), "process".to_string()], false),
        Server::start_with(&["--js-sandbox".to_string(), "wasm".to_string()], false),
    ];
    #[cfg(feature = "wasmtime")]
    servers.push(Server::start_with(
        &[
            "--js-sandbox".to_string(),
            "wasm".to_string(),
            "--wasm-runtime".to_string(),
            "wasmtime".to_string(),
        ],
        false,
    ));

    let mut baseline: Option<Vec<Value>> = None;
    for (index, s) in servers.iter().enumerate() {
        s.fresh_db("jsagree").await;
        let (status, _) = s.post("/jsagree/_bulk_docs", docs.clone()).await;
        assert_eq!(status, StatusCode::CREATED, "server {index}");
        let (status, body) = s.put("/jsagree/_design/v", ddoc.clone()).await;
        assert_eq!(status, StatusCode::CREATED, "server {index}: {body}");
        let mut rows = Vec::new();
        for view in ["fields", "computed", "nested", "unicode"] {
            let (status, body) = s
                .get(&format!("/jsagree/_design/v/_view/{view}?reduce=false"))
                .await;
            assert_eq!(status, StatusCode::OK, "server {index} {view}: {body}");
            rows.push(body);
        }
        match &baseline {
            None => baseline = Some(rows),
            Some(expected) => assert_eq!(&rows, expected, "server {index} disagreed"),
        }
    }
}

#[cfg(not(feature = "wasm"))]
#[test]
fn wasm_sandbox_without_the_wasm_feature_fails_at_startup() {
    let _guard = SPAWN_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let output = Command::new(env!("CARGO_BIN_EXE_douchdb"))
        .args([
            "--js-sandbox",
            "wasm",
            "--data",
            "/tmp/douchdb-should-not-be-created",
        ])
        .output()
        .expect("run douchdb");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("`wasm` feature"), "{stderr}");
}

#[tokio::test]
async fn quickjs_wasm_sandbox_enforces_the_deadline() {
    if !sandbox_tested("wasm") {
        return;
    }
    let args = vec![
        "--js-sandbox".to_string(),
        "wasm".to_string(),
        // Long enough for a loaded machine to start QuickJS in a debug build
        // and run the `ok` view. The loop below then runs into this deadline,
        // whatever the machine's speed.
        "--view-timeout-ms".to_string(),
        "30000".to_string(),
    ];
    let s = Server::start_with(&args, false);
    s.fresh_db("jswloop").await;
    let (status, _) = s.put("/jswloop/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jswloop/_design/v",
            json!({"views": {
                "ok": {"map": "function(doc){ var n = doc.v + 1; emit(n, null); }"},
                "loop": {"map": "function(doc){ while (true) {} }"}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Prove the wasm worker runs first, so a startup failure cannot stand in
    // for the deadline below.
    let (status, body) = s.get("/jswloop/_design/v/_view/ok").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["key"], 2);

    // The loop never ends, so only the deadline can answer this request; a
    // hang is for the test runner's failsafe to catch.
    let (status, body) = s.get("/jswloop/_design/v/_view/loop").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "view_error");
}

#[cfg(feature = "wasmtime")]
#[tokio::test]
async fn quickjs_wasmtime_backend_runs_views_reduce_search_and_deadline() {
    if !sandbox_tested("wasm") {
        return;
    }
    let args = vec![
        "--js-sandbox".to_string(),
        "wasm".to_string(),
        "--wasm-runtime".to_string(),
        "wasmtime".to_string(),
        // Long enough for the views above to run on a loaded machine; the
        // loop then runs into it whatever the machine's speed.
        "--view-timeout-ms".to_string(),
        "30000".to_string(),
    ];
    let s = Server::start_with(&args, false);
    s.fresh_db("jswasmtime").await;
    let (status, _) = s
        .post(
            "/jswasmtime/_bulk_docs",
            json!({"docs": [
                {"_id": "a", "city": "Berlin", "price": 10},
                {"_id": "b", "city": "Paris", "price": 20},
                {"_id": "c", "city": "Berlin", "price": 30}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jswasmtime/_design/v",
            json!({
                "views": {
                    "computed": {"map": "function(doc){ var suffix = '-x'; emit(doc.city + suffix, doc.price); }"},
                    "total": {
                        "map": "function(doc){ emit(null, doc.price); }",
                        "reduce": "function(keys, values, rereduce){ return sum(values); }"
                    },
                    "loop": {"map": "function(doc){ while (true) {} }"}
                },
                "indexes": {
                    "cities": {"index": "function(doc){ index('city', doc.city); }"}
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = s
        .get("/jswasmtime/_design/v/_view/computed?reduce=false")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);
    assert_eq!(body["rows"][0]["key"], "Berlin-x");

    let (status, body) = s.get("/jswasmtime/_design/v/_view/total").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["value"], 60);

    let (status, body) = s
        .get("/jswasmtime/_design/v/_search/cities?q=city:Berlin")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 2);

    // The loop never ends, so only the epoch deadline can answer this
    // request; a hang is for the test runner's failsafe to catch.
    let (status, body) = s.get("/jswasmtime/_design/v/_view/loop").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    assert_eq!(body["error"], "view_error");
}

#[tokio::test]
async fn quickjs_sandbox_exposes_no_host_globals() {
    for mode in ["native", "process", "wasm"] {
        if !sandbox_tested(mode) {
            continue;
        }
        let args = vec!["--js-sandbox".to_string(), mode.to_string()];
        let s = Server::start_with(&args, false);
        let db = format!("jsglobals{mode}");
        s.fresh_db(&db).await;
        let (status, _) = s.put(&format!("/{db}/doc"), json!({"v": 1})).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = s
            .put(
                &format!("/{db}/_design/v"),
                json!({"views": {"probe": {"map": "function(doc){ var names = ['require','process','fetch','XMLHttpRequest','WebSocket','Deno','Bun','fs','os','net','console','print']; var found = []; for (var i = 0; i < names.length; i++) { if (typeof globalThis[names[i]] !== 'undefined') found.push(names[i]); } emit(null, found); }"}}}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, body) = s.get(&format!("/{db}/_design/v/_view/probe")).await;
        assert_eq!(status, StatusCode::OK, "{mode}: {body}");
        assert_eq!(body["rows"][0]["value"], json!([]), "{mode}: {body}");
    }
}

#[tokio::test]
async fn view_rows_keep_the_original_id_when_map_changes_doc_id() {
    // CouchDB takes the row id from the stored document, not from the object
    // the map function was handed (couch_mrview_updater).
    for mode in ["native", "process", "wasm"] {
        if !sandbox_tested(mode) {
            continue;
        }
        let args = vec!["--js-sandbox".to_string(), mode.to_string()];
        let s = Server::start_with(&args, false);
        let db = format!("jsmutid{mode}");
        s.fresh_db(&db).await;
        let (status, _) = s.put(&format!("/{db}/p1"), json!({"v": 1})).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = s
            .put(
                &format!("/{db}/_design/v"),
                json!({"views": {
                    "changed": {"map": "function(doc){ doc._id = 'other'; emit(doc.v, null); }"},
                    "deleted": {"map": "function(doc){ delete doc._id; emit(doc.v, null); }"}
                }}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        for view in ["changed", "deleted"] {
            let (status, body) = s
                .get(&format!("/{db}/_design/v/_view/{view}?reduce=false"))
                .await;
            assert_eq!(status, StatusCode::OK, "{mode} {view}: {body}");
            assert_eq!(
                body["rows"].as_array().map(Vec::len),
                Some(1),
                "{mode} {view}: {body}"
            );
            assert_eq!(body["rows"][0]["id"], "p1", "{mode} {view}: {body}");
        }
    }
}

#[tokio::test]
async fn search_hits_keep_the_original_id_when_index_changes_doc_id() {
    // Same rule as for views: the hit id comes from the stored document, and
    // every sandbox must agree.
    for mode in ["native", "process", "wasm"] {
        if !sandbox_tested(mode) {
            continue;
        }
        let args = vec!["--js-sandbox".to_string(), mode.to_string()];
        let s = Server::start_with(&args, false);
        let db = format!("jsidxid{mode}");
        s.fresh_db(&db).await;
        let (status, _) = s.put(&format!("/{db}/p1"), json!({"v": "one"})).await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = s
            .put(
                &format!("/{db}/_design/s"),
                json!({"indexes": {"changed": {
                    "index": "function(doc){ doc._id = 'other'; index('v', doc.v); }"
                }}}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, body) = s
            .get(&format!("/{db}/_design/s/_search/changed?q=v:one"))
            .await;
        assert_eq!(status, StatusCode::OK, "{mode}: {body}");
        assert_eq!(body["rows"][0]["id"], "p1", "{mode}: {body}");
    }
}

#[tokio::test]
async fn pooled_process_worker_keeps_no_js_state_between_databases() {
    // One worker serves both databases. CouchDB keeps JS processes alive
    // between requests and tags them per database (couch_proc_manager); here
    // each request builds a fresh runtime, so nothing can leak.
    if !sandbox_tested("process") {
        return;
    }
    let args = vec![
        "--js-sandbox".to_string(),
        "process".to_string(),
        "--js-workers".to_string(),
        "1".to_string(),
    ];
    let s = Server::start_with(&args, false);
    for db in ["jsleaka", "jsleakb"] {
        s.fresh_db(db).await;
        let (status, _) = s.put(&format!("/{db}/doc"), json!({"v": 1})).await;
        assert_eq!(status, StatusCode::CREATED);
    }
    let (status, _) = s
        .put(
            "/jsleaka/_design/v",
            json!({"views": {"plant": {"map": "function(doc){ var seen = typeof globalThis.planted; globalThis.planted = 'from a'; Object.prototype.polluted = 1; emit(seen, null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jsleakb/_design/v",
            json!({"views": {"probe": {"map": "function(doc){ emit([typeof globalThis.planted, typeof ({}).polluted], null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s.get("/jsleaka/_design/v/_view/plant").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["key"], "undefined", "{body}");
    let (status, body) = s.get("/jsleakb/_design/v/_view/probe").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["rows"][0]["key"],
        json!(["undefined", "undefined"]),
        "{body}"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn process_worker_installs_the_seccomp_filter() {
    if !sandbox_tested("process") {
        return;
    }
    let args = vec![
        "--js-sandbox".to_string(),
        "process".to_string(),
        "--js-workers".to_string(),
        "1".to_string(),
    ];
    let s = Server::start_with(&args, false);
    s.fresh_db("jsseccomp").await;
    let (status, _) = s.put("/jsseccomp/doc", json!({"v": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = s
        .put(
            "/jsseccomp/_design/v",
            json!({"views": {"probe": {"map": "function(doc){ var x = 1; emit(doc._id + '-' + x, null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = s.get("/jsseccomp/_design/v/_view/probe").await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let parent = format!("PPid:\t{}", s.child.id());
    let worker = std::fs::read_dir("/proc")
        .expect("proc")
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .find(|pid| {
            let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
            let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
            cmdline
                .windows(b"--js-worker".len())
                .any(|window| window == b"--js-worker")
                && status.lines().any(|line| line == parent)
        })
        .expect("a process worker");
    let status = std::fs::read_to_string(format!("/proc/{worker}/status")).expect("status");
    assert!(
        status.lines().any(|line| line == "Seccomp:\t2"),
        "worker has no seccomp filter: {status}"
    );
}

#[tokio::test]
async fn changes_pagination_resumes_across_interleaved_shard_pages() {
    use md5::{Digest, Md5};

    let args = vec!["--q".to_string(), "2".to_string()];
    let s = Server::start_with(&args, false);
    s.fresh_db("changespages").await;

    // More than the 1,024-row internal page per shard, with IDs interleaved
    // between the two shards. This exercises the merge cursor and a resume at
    // the last emitted sequence.
    let mut per_shard = [Vec::new(), Vec::new()];
    let mut n = 0usize;
    while per_shard[0].len() < 1_100 || per_shard[1].len() < 1_100 {
        let id = format!("chg{n:05}");
        let digest = Md5::digest(id.as_bytes());
        let shard = (u32::from_be_bytes(digest[..4].try_into().unwrap()) % 2) as usize;
        if per_shard[shard].len() < 1_100 {
            per_shard[shard].push(id);
        }
        n += 1;
    }
    let mut expected = per_shard.concat();
    expected.sort();
    for chunk in expected.chunks(500) {
        let docs: Vec<Value> = chunk.iter().map(|id| json!({"_id": id, "v": 1})).collect();
        let (status, _) = s
            .post("/changespages/_bulk_docs", json!({"docs": docs}))
            .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let mut since = 0u64;
    let mut last_seq = 0u64;
    let mut seen = Vec::new();
    loop {
        let (status, body) = s
            .get(&format!("/changespages/_changes?since={since}&limit=50"))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body["results"].as_array().expect("results");
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let seq = row["seq"].as_u64().expect("numeric seq");
            assert!(
                seq > last_seq,
                "changes rows out of order: {seq} after {last_seq}"
            );
            last_seq = seq;
            seen.push(row["id"].as_str().unwrap().to_string());
        }
        let next = body["last_seq"].as_u64().expect("numeric last_seq");
        assert!(next >= since, "last_seq went backwards");
        if next == since {
            break;
        }
        since = next;
        assert!(seen.len() <= expected.len(), "duplicate rows returned");
    }
    let unique: std::collections::HashSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), expected.len(), "duplicate changes rows");
    seen.sort();
    assert_eq!(seen, expected, "change rows had gaps");

    // One unlimited request has to read past each shard's first internal
    // page of 1,024 rows; the limited pages above never do.
    let (status, body) = s.get("/changespages/_changes?since=0").await;
    assert_eq!(status, StatusCode::OK);
    let mut all: Vec<String> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap().to_string())
        .collect();
    all.sort();
    assert_eq!(
        all, expected,
        "unlimited feed lost rows past the first page"
    );
}

#[tokio::test]
async fn live_changes_cursor_sees_every_document_under_concurrent_writes() {
    // Sequence numbers are global but shards commit independently, so a
    // lower sequence can become visible after a higher one. A reader that
    // forwards `last_seq` while writes overlap must still see every document.
    let args = vec!["--q".to_string(), "4".to_string()];
    let s = Server::start_with(&args, false);
    s.fresh_db("livegap").await;
    let total = 12_000usize;
    let writers = 4usize;
    let written = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let reader = {
        let client = s.client.clone();
        let base = s.base.clone();
        let written = written.clone();
        tokio::spawn(async move {
            let mut seen = std::collections::HashSet::new();
            let mut since = Value::from(0);
            let mut idle = 0;
            let deadline = std::time::Instant::now() + Duration::from_secs(120);
            // Once every write is acknowledged, a few empty pages in a row
            // mean the cursor has reached the end.
            while seen.len() < total && idle < 20 && std::time::Instant::now() < deadline {
                let body: Value = client
                    .get(format!("{base}/livegap/_changes"))
                    .query(&[("since", since.to_string()), ("limit", "500".to_string())])
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                let rows = body["results"].as_array().unwrap();
                for row in rows {
                    seen.insert(row["id"].as_str().unwrap().to_string());
                }
                since = body["last_seq"].clone();
                if rows.is_empty() {
                    if written.load(std::sync::atomic::Ordering::SeqCst) {
                        idle += 1;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                } else {
                    idle = 0;
                }
            }
            seen
        })
    };

    let mut tasks = Vec::new();
    for writer in 0..writers {
        let client = s.client.clone();
        let base = s.base.clone();
        tasks.push(tokio::spawn(async move {
            for batch in (writer..total)
                .step_by(writers)
                .collect::<Vec<_>>()
                .chunks(250)
            {
                let docs: Vec<Value> = batch
                    .iter()
                    .map(|i| json!({"_id": format!("live{i:06}")}))
                    .collect();
                let response = client
                    .post(format!("{base}/livegap/_bulk_docs"))
                    .json(&json!({"docs": docs}))
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::CREATED);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    written.store(true, std::sync::atomic::Ordering::SeqCst);

    let seen = reader.await.unwrap();
    let (_, info) = s.get("/livegap").await;
    assert_eq!(info["doc_count"], total);
    assert_eq!(
        seen.len(),
        total,
        "the live cursor skipped {} documents",
        total - seen.len()
    );
}

#[tokio::test]
async fn cluster_approx_count_distinct_merges_shard_partials() {
    // Owners return HyperLogLog partials and the coordinator unions them, so
    // the estimate is the one CouchDB gives however the keys spread over
    // shards (CouchDB 3.5.3 returns these numbers at any q).
    for (q, replication) in [(2, 3), (4, 1)] {
        let nodes = start_cluster_sharded(3, q, replication);
        for node in &nodes {
            node.fresh_db("clapprox").await;
        }
        let (status, _) = nodes[0]
            .post("/clapprox/_bulk_docs", json!({"docs": approx_mixed_docs()}))
            .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = nodes[0]
            .put("/clapprox/_design/d", approx_mixed_ddoc())
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_approx_mixed_estimates(&nodes[0], "clapprox").await;
    }
}

#[tokio::test]
async fn cluster_view_queries_match_a_single_node() {
    // Owners apply key ranges and limits before rows cross the network, and
    // the coordinator merges their counts. The answer must not depend on how
    // the rows are spread: CouchDB's default q=2, n=3 first, then layouts
    // where nodes hold disjoint shards or only some replicas.
    for (q, replication) in [(2, 3), (4, 1), (8, 2)] {
        assert_cluster_views_match_single_node(q, replication).await;
    }
}

async fn assert_cluster_views_match_single_node(q: u32, replication: usize) {
    let single = Server::start(false);
    let nodes = start_cluster_sharded(3, q, replication);
    let ddoc = json!({"views": {
        "by_n": {"map": "function(doc){ emit(doc.n % 7, doc.n); emit([doc.n % 3, doc.n], null); }"},
        "sum": {"map": "function(doc){ emit(doc.n % 5, doc.n); }", "reduce": "_sum"},
        "cnt": {"map": "function(doc){ emit([doc.n % 3, doc.n % 4], 1); }", "reduce": "_count"},
        "stats": {"map": "function(doc){ emit(doc.n % 5, doc.n); }", "reduce": "_stats"},
        "js": {"map": "function(doc){ emit(doc.n % 5, doc.n); }",
               "reduce": "function(keys, values, rereduce){ return sum(values); }"},
        "acd": {"map": "function(doc){ emit([doc.n % 5, doc.n % 7], doc.n % 2); }",
                "reduce": "_approx_count_distinct"}
    }});
    let docs: Vec<Value> = (0..60)
        .map(|i| json!({"_id": format!("d{i:02}"), "n": i}))
        .collect();
    let servers = [&single, &nodes[0]];
    for server in servers {
        server.fresh_db("clviewparity").await;
    }
    for node in &nodes[1..] {
        node.fresh_db("clviewparity").await;
    }
    for server in servers {
        let (status, _) = server
            .post("/clviewparity/_bulk_docs", json!({"docs": docs}))
            .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = server.put("/clviewparity/_design/v", ddoc.clone()).await;
        assert_eq!(status, StatusCode::CREATED);
    }
    let queries = [
        "by_n",
        "by_n?limit=5",
        "by_n?skip=3&limit=4",
        "by_n?key=3",
        "by_n?key=3&descending=true",
        "by_n?startkey=2&endkey=4",
        "by_n?startkey=2&endkey=4&inclusive_end=false",
        "by_n?startkey=4&endkey=2&descending=true&limit=6",
        "by_n?startkey=3&startkey_docid=d30&limit=5",
        "by_n?startkey=3&startkey_docid=d30&endkey=3&endkey_docid=d50",
        "by_n?keys=[5,1,5]",
        "by_n?keys=[5,1]&descending=true&limit=4",
        "by_n?startkey=[1]&endkey=[1,{}]",
        "by_n?startkey=[1,20]&limit=3&skip=1",
        "by_n?key=99",
        "by_n?include_docs=true&key=6&limit=2",
        "sum",
        "sum?group=true",
        "sum?startkey=1&endkey=3",
        "sum?keys=[0,4]&group=true",
        "sum?reduce=false&key=2&limit=3",
        "sum?key=2",
        "sum?key=99",
        "sum?skip=1",
        "sum?startkey=10",
        "sum?startkey=1&endkey=3&group=true",
        "sum?startkey=1&endkey=3&inclusive_end=false",
        "sum?descending=true&group=true",
        "sum?startkey=3&endkey=1&descending=true",
        "sum?group=true&skip=1&limit=2",
        "sum?keys=[4,0,4]&group=true",
        "sum?keys=[4,0]&group=true&descending=true",
        "sum?keys=[3]",
        "sum?startkey=1&startkey_docid=d30&endkey=1&endkey_docid=d50",
        "cnt",
        "cnt?group_level=1",
        "cnt?group_level=1&startkey=[1]&endkey=[2,{}]",
        "cnt?keys=[[1,2],[1,3],[0,0]]&group_level=1",
        "cnt?group=true&descending=true&limit=3",
        "cnt?startkey=[1]&endkey=[1,{}]",
        "stats?startkey=1&endkey=3",
        "stats?group=true&keys=[2,2]",
        "js",
        "js?group=true",
        "js?startkey=1&endkey=3",
        "js?key=4",
        "js?keys=[1,2]&group=true",
        "acd",
        "acd?startkey=[1]&endkey=[3,{}]",
        "acd?group_level=1",
        "acd?group=true&limit=7&skip=2",
        "acd?keys=[[1,1],[2,2]]&group=true",
        "sum?keys=[1,2]",
        "sum?startkey=3&endkey=1",
    ];
    for query in queries {
        let path = format!("/clviewparity/_design/v/_view/{query}");
        let (status, expected) = single.get(&path).await;
        for node in &nodes {
            let (node_status, actual) = node.get(&path).await;
            assert_eq!(
                node_status, status,
                "q={q} n={replication} {query}: {actual}"
            );
            assert_eq!(
                actual, expected,
                "q={q} n={replication} {query} via {}",
                node.base
            );
        }
    }
}

#[tokio::test]
async fn databases_take_couchdb_shard_defaults_and_per_database_overrides() {
    let s = Server::start(false);
    s.fresh_db("qndefault").await;
    let (_, info) = s.get("/qndefault").await;
    assert_eq!(info["cluster"], json!({"q": 2, "n": 1, "w": 1, "r": 1}));

    let _ = s.delete("/qnthree").await;
    let (status, _) = s.put("/qnthree?q=3", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, info) = s.get("/qnthree").await;
    assert_eq!(info["cluster"]["q"], 3);

    for bad in ["q=0", "q=x", "n=0", "n=-1"] {
        let (status, body) = s.put(&format!("/qnbad?{bad}"), json!({})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
        let (status, _) = s.get("/qnbad").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{bad} created the database");
    }

    // Every node records the requested layout, and the default quorums
    // follow its copy count: one copy is a full write and read quorum.
    let nodes = start_cluster_sharded(3, 4, 3);
    let _ = nodes[0].delete("/qncluster").await;
    let (status, _) = nodes[0].put("/qncluster?q=3&n=1", json!({})).await;
    assert_eq!(status, StatusCode::CREATED);
    let docs: Vec<Value> = (0..30)
        .map(|i| json!({"_id": format!("d{i:02}")}))
        .collect();
    let (status, _) = nodes[1]
        .post("/qncluster/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    for node in &nodes {
        let (_, info) = node.get("/qncluster").await;
        assert_eq!(
            info["cluster"],
            json!({"q": 3, "n": 1, "w": 1, "r": 1}),
            "{}",
            node.base
        );
        let (_, all) = node.get("/qncluster/_all_docs").await;
        assert_eq!(all["total_rows"], 30, "{}", node.base);
        // A single copy satisfies the default read quorum.
        let (status, doc) = node.get("/qncluster/d07").await;
        assert_eq!(status, StatusCode::OK, "{}: {doc}", node.base);
    }
}

#[tokio::test]
async fn cluster_all_docs_keys_with_non_string_entries_get_not_found_rows() {
    let nodes = start_cluster_with(2, "3600", 1);
    for node in &nodes {
        node.fresh_db("cnonstr").await;
    }
    let (status, _) = nodes[0]
        .post(
            "/cnonstr/_bulk_docs",
            json!({"docs": [{"_id": "a"}, {"_id": "b"}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    for node in &nodes {
        let (status, body) = node
            .post("/cnonstr/_all_docs", json!({"keys": [1, "a", null, "b"]}))
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = &body["rows"];
        assert_eq!(rows[0], json!({"key": 1, "error": "not_found"}), "{body}");
        assert_eq!(rows[1]["id"], "a", "{body}");
        assert_eq!(
            rows[2],
            json!({"key": null, "error": "not_found"}),
            "{body}"
        );
        assert_eq!(rows[3]["id"], "b", "{body}");
        assert_eq!(rows.as_array().map(Vec::len), Some(4), "{body}");
    }
}

#[tokio::test]
async fn cluster_all_docs_keys_honour_descending_order() {
    let nodes = start_cluster_with(2, "3600", 1);
    for node in &nodes {
        node.fresh_db("ckeysdesc").await;
    }
    let (status, _) = nodes[0]
        .post(
            "/ckeysdesc/_bulk_docs",
            json!({"docs": [
                {"_id": "a"}, {"_id": "b"}, {"_id": "c"}, {"_id": "d"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    for node in &nodes {
        let (status, body) = node
            .post(
                "/ckeysdesc/_all_docs?descending=true&skip=1&limit=2",
                json!({"keys": ["a", "b", "c", "d"]}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 4);
        let ids: Vec<&str> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["c", "b"], "descending keys via {}", node.base);

        let (status, body) = node
            .post(
                "/ckeysdesc/_all_docs?descending=true",
                json!({"keys": ["a", "missing", "c"]}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let rows = body["rows"].as_array().unwrap();
        assert_eq!(rows[0]["key"], "c");
        assert_eq!(rows[1]["error"], "not_found");
        assert_eq!(rows[2]["id"], "a");
    }
}

#[tokio::test]
async fn cluster_partitioned_all_docs_keys_ranges_and_views() {
    let nodes = start_cluster_with(3, "3600", 1);
    let (status, _) = nodes[0]
        .put("/clpartkeys?partitioned=true", json!({}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let docs = json!({"docs": [
        {"_id": "p1:a", "n": 1},
        {"_id": "p1:b", "n": 2},
        {"_id": "p1:c", "n": 3},
        {"_id": "p2:a", "n": 10},
        {"_id": "p2:b", "n": 20}
    ]});
    let (status, body) = nodes[0].post("/clpartkeys/_bulk_docs", docs).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, _) = nodes[1]
        .put(
            "/clpartkeys/_design/p",
            json!({
                "views": {"by_n": {"map": "function(doc){ emit(doc.n, null); }"}},
                "indexes": {"by_n": {"index": "function(doc){ index('n', doc.n); }"}}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    // Design documents use the document write path; with n=1 the quorum can
    // return before every peer has its local copy. The search fan-out needs
    // the ddoc on every active node.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    for node in &nodes {
        loop {
            let response = node
                .client
                .get(format!("{}/clpartkeys/_design/p", node.base))
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", "test-cluster-secret")
                .send()
                .await
                .unwrap();
            if response.status() == StatusCode::OK {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "design doc did not reach {}: {}",
                node.base,
                response.status()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    for node in &nodes {
        let (status, body) = node.get("/clpartkeys/_partition/p1").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["doc_count"], 3, "{}", node.base);

        let (status, body) = node
            .get("/clpartkeys/_partition/p1/_all_docs?descending=true&limit=2")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let ids: Vec<&str> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["p1:c", "p1:b"], "{}", node.base);

        let (status, body) = node
            .get("/clpartkeys/_partition/p1/_all_docs?startkey=%22p1:b%22&endkey=%22p1:c%22")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 3);
        let ids: Vec<&str> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["p1:b", "p1:c"], "{}", node.base);

        let (status, body) = node
            .post(
                "/clpartkeys/_partition/p1/_all_docs",
                json!({"keys": ["p1:a", "p2:a", "p1:c"]}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 3);
        assert_eq!(body["rows"][0]["id"], "p1:a");
        assert_eq!(body["rows"][1]["error"], "not_found");
        assert_eq!(body["rows"][2]["id"], "p1:c");

        let (status, body) = node
            .post(
                "/clpartkeys/_partition/p1/_all_docs?descending=true",
                json!({"keys": ["p1:a", "p1:b", "p1:c"]}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let ids: Vec<&str> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["p1:c", "p1:b", "p1:a"], "{}", node.base);

        let (status, body) = node.get("/clpartkeys/_partition/p2/_all_docs").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 2);
        assert!(body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["id"].as_str().unwrap().starts_with("p2:")));

        let (status, body) = node
            .get("/clpartkeys/_partition/p1/_design/p/_view/by_n")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 3);
        let keys: Vec<u64> = body["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["key"].as_u64().unwrap())
            .collect();
        assert_eq!(keys, [1, 2, 3], "{}", node.base);

        let (status, body) = node
            .get("/clpartkeys/_partition/p1/_design/p/_search/by_n?q=n:2")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["total_rows"], 1, "{}", node.base);
        assert_eq!(body["rows"][0]["id"], "p1:b");
    }
}

#[tokio::test]
async fn kill9_preserves_acknowledged_documents_and_changes() {
    let mut s = Server::start(false);
    s.fresh_db("durable").await;
    let docs: Vec<Value> = (0..24)
        .map(|i| json!({"_id": format!("d{i:02}"), "n": i}))
        .collect();
    let (status, body) = s.post("/durable/_bulk_docs", json!({"docs": docs})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert!(body.as_array().unwrap().iter().all(|row| row["ok"] == true));

    s.kill();
    s.restart();

    let (status, body) = s.get("/durable/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 24);
    let (status, body) = s.get("/durable/d23").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["n"], 23);
    let (status, body) = s.get("/durable/_changes?since=0").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"].as_array().unwrap().len(), 24);
}

/// Kills the child when dropped, so a failing assertion cannot leave it behind.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A test run that is killed (timeout, Ctrl-C, a CI cancel) never runs
/// `Server::drop`; its servers must exit on their own instead of lingering as
/// orphans (issue #119). The test re-runs itself as the "parent" that owns a
/// server, kills that parent with SIGKILL, and expects the server to be gone.
#[test]
fn server_exits_when_its_test_process_is_killed() {
    use std::io::{BufRead, BufReader};
    const HELPER: &str = "DOUCHDB_TEST_PARENT_DEATH_HELPER";
    if std::env::var_os(HELPER).is_some() {
        let server = Server::start(false);
        println!("server-pid:{}", server.child.id());
        std::thread::sleep(Duration::from_secs(120));
        drop(server);
        return;
    }
    let mut cmd = Command::new(std::env::current_exe().expect("test exe"));
    cmd.args([
        "--exact",
        "server_exits_when_its_test_process_is_killed",
        "--nocapture",
        "--test-threads=1",
    ])
    .env(HELPER, "1")
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::null());
    let mut helper = KillOnDrop(spawn_locked(&mut cmd).expect("spawn helper"));
    let stdout = helper.0.stdout.take().expect("helper stdout");
    let pid: i32 = BufReader::new(stdout)
        .lines()
        .map_while(Result::ok)
        .find_map(|line| line.split_once("server-pid:")?.1.trim().parse().ok())
        .expect("helper did not report its server pid");
    let helper_pid = helper.0.id();
    // SIGKILL: the helper gets no chance to run any Drop.
    drop(helper);

    // SAFETY: kill(2) with signal 0 only probes whether the pid exists.
    let alive = |pid: i32| unsafe { libc::kill(pid, 0) } == 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while alive(pid) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    let leaked = alive(pid);
    if leaked {
        // SAFETY: kill(2) takes plain integers and touches no memory.
        unsafe { libc::kill(pid, libc::SIGKILL) };
    }
    let prefix = format!("douchdb-test-{helper_pid}-");
    if let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) {
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
    assert!(!leaked, "server {pid} outlived its killed test process");
}

#[tokio::test]
async fn anti_entropy_replicates_conflicts_and_tombstones() {
    let nodes = start_cluster_with(2, "1", 2);
    for node in &nodes {
        node.fresh_db("clconflict").await;
    }
    let root = "00000000000000000000000000000000";
    let docs = json!({"new_edits": false, "docs": [
        {"_id": "x", "_rev": "2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
         "_revisions": {"start": 2, "ids": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", root]}, "v": "b"},
        {"_id": "x", "_rev": "2-cccccccccccccccccccccccccccccccc",
         "_revisions": {"start": 2, "ids": ["cccccccccccccccccccccccccccccccc", root]}, "v": "c"},
        {"_id": "y", "_rev": "2-dddddddddddddddddddddddddddddddd", "_deleted": true,
         "_revisions": {"start": 2, "ids": ["dddddddddddddddddddddddddddddddd", root]}}
    ]});
    let response = nodes[0]
        .client
        .post(format!("{}/clconflict/_bulk_docs", nodes[0].base))
        .header("x-douchdb-forwarded", "1")
        .header("x-douchdb-secret", "test-cluster-secret")
        .json(&docs)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        // Forwarded reads stay on this node, so a 200 proves anti-entropy
        // actually copied the revision rather than proxying to node 0.
        let response = nodes[1]
            .client
            .get(format!("{}/clconflict/x?conflicts=true", nodes[1].base))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", "test-cluster-secret")
            .send()
            .await
            .unwrap();
        let status = response.status();
        let doc: Value = response.json().await.unwrap_or(Value::Null);
        let response = nodes[1]
            .client
            .get(format!(
                "{}/clconflict/y?rev=2-dddddddddddddddddddddddddddddddd",
                nodes[1].base
            ))
            .header("x-douchdb-forwarded", "1")
            .header("x-douchdb-secret", "test-cluster-secret")
            .send()
            .await
            .unwrap();
        let status_y = response.status();
        let tombstone: Value = response.json().await.unwrap_or(Value::Null);
        if status == StatusCode::OK
            && status_y == StatusCode::OK
            && doc["_conflicts"].as_array().map(Vec::len) == Some(1)
            && tombstone["_deleted"] == true
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "anti-entropy did not copy conflicts/tombstones: x={doc} y={tombstone}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (status, body) = nodes[1]
        .get("/clconflict/_changes?style=all_docs&since=0")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows = body["results"].as_array().unwrap();
    let x = rows
        .iter()
        .find(|row| row["id"] == "x")
        .unwrap_or_else(|| panic!("missing x in {body}"));
    assert_eq!(x["changes"].as_array().unwrap().len(), 2);
    let y = rows
        .iter()
        .find(|row| row["id"] == "y")
        .unwrap_or_else(|| panic!("missing y in {body}"));
    assert_eq!(y["deleted"], true);
}

#[tokio::test]
async fn anti_entropy_preserves_revision_history_metadata() {
    let nodes = start_cluster_with(2, "1", 2);
    for node in &nodes {
        node.fresh_db("clhistory").await;
    }
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let (status, first) = nodes[0]
        .req_with(
            Method::PUT,
            "/clhistory/hist",
            Some(json!({"v": 1})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{first}");
    let rev1 = first["rev"].as_str().unwrap().to_string();
    let (status, _) = nodes[0]
        .req_with(
            Method::PUT,
            "/clhistory/hist",
            Some(json!({"_rev": rev1, "v": 2})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, doc) = nodes[1]
            .req_with(Method::GET, "/clhistory/hist?revs=true", None, &internal)
            .await;
        if status == StatusCode::OK {
            let ids = doc["_revisions"]["ids"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if ids.len() == 2 {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "anti-entropy lost revision history: {doc}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn observability_metrics_and_health_report_local_state() {
    let s = Server::start(false);
    s.fresh_db("obsmetrics").await;
    let docs: Vec<Value> = (0..6)
        .map(|i| json!({"_id": format!("m{i}"), "n": i}))
        .collect();
    let (status, _) = s
        .post("/obsmetrics/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = s.get("/obsmetrics/m0").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.get("/obsmetrics/_all_docs").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.get("/obsmetrics/_changes").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = s.get("/obsmetrics/_design/missing/_view/x").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, _) = s
        .put(
            "/obsmetrics/_design/metrics",
            json!({
                "views": {"by_n": {"map": "function(doc){ if (doc.n !== undefined) emit(doc.n, null); }"}},
                "indexes": {"by_n": {"index": "function(doc){ index('n', doc.n); }"}}
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, view) = s.get("/obsmetrics/_design/metrics/_view/by_n").await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert!(view["total_rows"].as_u64().unwrap() >= 6, "{view}");
    let (status, search) = s
        .get("/obsmetrics/_design/metrics/_search/by_n?q=n:2")
        .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert_eq!(search["total_rows"], 1);

    // The unauthenticated liveness probe exposes status and no data paths.
    let (status, health) = s.get("/_health").await;
    assert_eq!(status, StatusCode::OK, "{health}");
    assert_eq!(health["status"], "ok", "{health}");
    let names: Vec<&str> = health["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|check| check["name"].as_str().unwrap())
        .collect();
    for name in [
        "process",
        "storage",
        "placement",
        "databases",
        "cluster",
        "replication",
        "http",
    ] {
        assert!(names.contains(&name), "missing {name} check: {health}");
    }

    let (status, health) = s.get("/_node/_local/_health").await;
    assert_eq!(status, StatusCode::OK, "{health}");
    assert_eq!(health["status"], "ok", "{health}");
    assert!(health["checks"].as_array().unwrap().len() >= 7);

    let (status, metrics) = s.get("/_node/_local/_metrics").await;
    assert_eq!(status, StatusCode::OK, "{metrics}");
    assert_eq!(metrics["scope"], "local");
    assert_eq!(metrics["node"]["index"], 0);
    assert!(metrics["process"]["pid"].as_u64().unwrap() > 0);
    #[cfg(target_os = "linux")]
    assert!(metrics["process"]["rss_bytes"].as_u64().unwrap() > 0);
    #[cfg(not(target_os = "linux"))]
    assert!(metrics["process"]["rss_bytes"].is_null());
    assert!(metrics["process"]["uptime_secs"].as_u64().is_some());
    assert!(metrics["http"]["total"].as_u64().unwrap() >= 5);
    assert!(metrics["http"]["errors"].as_u64().unwrap() >= 1);
    assert!(metrics["http"]["latency_ms"]["p95"].as_f64().is_some());
    assert_eq!(metrics["http"]["per_second"].as_array().unwrap().len(), 60);
    assert_eq!(metrics["storage"]["databases"], 1);
    assert!(metrics["storage"]["disk_bytes"].as_u64().unwrap() > 0);
    assert_eq!(metrics["databases"][0]["name"], "obsmetrics");
    assert!(metrics["databases"][0]["doc_count"].as_u64().unwrap() >= 6);
    assert!(metrics["databases"][0]["disk_bytes"].as_u64().unwrap() > 0);
    assert_eq!(metrics["databases"][0]["local_shards"], 2);
    assert_eq!(metrics["health"]["status"], "ok", "{metrics}");
    assert_eq!(metrics["nodes"].as_array().unwrap().len(), 1);
    assert!(metrics["views"]["queries"].as_u64().unwrap() >= 1);
    assert!(metrics["search"]["queries"].as_u64().unwrap() >= 1);
    assert!(metrics["views"]["avg_ms"].as_f64().is_some());
    assert!(metrics["search"]["avg_ms"].as_f64().is_some());
    assert!(
        metrics["http"]["status_classes"]["4xx"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );

    // Route labels are templates, never document ids.
    let routes: Vec<&str> = metrics["http"]["routes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|route| route["route"].as_str().unwrap())
        .collect();
    assert!(routes.contains(&"/{db}/_all_docs"), "{routes:?}");
    assert!(routes.contains(&"/{db}/doc"), "{routes:?}");
    assert!(routes.contains(&"/{db}/_changes"), "{routes:?}");
    assert!(
        !routes.iter().any(|route| route.contains("m0")),
        "route labels must not contain document ids: {routes:?}"
    );
}

#[tokio::test]
async fn cluster_metrics_and_health_aggregate_across_nodes() {
    let mut nodes = start_cluster_with(3, "3600", 2);
    for node in &nodes {
        node.fresh_db("clobs").await;
    }
    let docs: Vec<Value> = (0..12).map(|i| json!({"_id": format!("o{i}")})).collect();
    let (status, _) = nodes[0]
        .post("/clobs/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    for node in &nodes[..2] {
        let (status, body) = node.get("/clobs/_all_docs").await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    let (status, local) = nodes[0].get("/_node/_local/_metrics").await;
    assert_eq!(status, StatusCode::OK, "{local}");
    assert_eq!(local["scope"], "local");

    // Discovery starts with the nodes and replica writes finish in the
    // background; wait for the first pass before treating a not-yet-probed
    // peer or a not-yet-visible shard count as a permanent failure.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let cluster = loop {
        let (status, body) = nodes[0].get("/_node/_local/_metrics?cluster=true").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        if body["health"]["status"] == "ok"
            && body["storage"]["doc_count"].as_u64().unwrap_or(0) >= 12
        {
            break body;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster metrics did not settle: {body}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(cluster["scope"], "cluster");
    // The Process card reads the local process even in cluster scope; a
    // missing `process` key blanked the console page.
    assert!(cluster["process"]["pid"].as_u64().unwrap() > 0);
    assert_eq!(cluster["process"], cluster["local"]["process"]);
    assert_eq!(cluster["summary"]["nodes"], 3);
    assert_eq!(cluster["summary"]["reachable"], 3, "{cluster}");
    assert_eq!(cluster["nodes"].as_array().unwrap().len(), 3);
    assert!(cluster["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .all(|node| node["reachable"] == true));
    assert!(cluster["http"]["total"].as_u64().unwrap() >= local["http"]["total"].as_u64().unwrap());
    assert!(cluster["http"]["routes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|route| route["route"] == "/{db}/_all_docs"));
    assert_eq!(cluster["storage"]["databases"], 1);
    assert!(cluster["storage"]["doc_count"].as_u64().unwrap() >= 12);
    assert!(cluster["databases"][0]["nodes_reporting"].as_u64().unwrap() >= 1);
    assert!(cluster["replication"]["runs"].as_u64().unwrap() >= 1);
    assert!(cluster["fabric"]["fanout_count"].as_u64().unwrap() >= 1);
    assert_eq!(cluster["health"]["status"], "ok", "{cluster}");

    let (status, health) = nodes[0].get("/_node/_local/_health?cluster=true").await;
    assert_eq!(status, StatusCode::OK, "{health}");
    assert_eq!(health["scope"], "cluster");
    assert_eq!(health["status"], "ok", "{health}");
    assert_eq!(health["nodes"].as_array().unwrap().len(), 3);

    // A dead peer makes the cluster view degraded and names it explicitly.
    nodes[2].kill();
    let (status, cluster) = nodes[0].get("/_node/_local/_metrics?cluster=true").await;
    assert_eq!(status, StatusCode::OK, "{cluster}");
    assert_eq!(cluster["summary"]["reachable"], 2, "{cluster}");
    let dead = cluster["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["index"] == 2)
        .expect("dead node listed");
    assert_eq!(dead["reachable"], false);
    assert_eq!(cluster["health"]["status"], "degraded", "{cluster}");
    assert!(!cluster["health"]["failing"].as_array().unwrap().is_empty());

    let (status, health) = nodes[0].get("/_node/_local/_health?cluster=true").await;
    assert_eq!(status, StatusCode::OK, "{health}");
    assert_eq!(health["scope"], "cluster");
    // An unreachable peer has one severity everywhere: degraded, as in the
    // metrics view above and docs/opentelemetry.md.
    assert_eq!(health["status"], "degraded", "{health}");
    assert_eq!(health["nodes"].as_array().unwrap().len(), 3);
    let dead = health["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["index"] == 2)
        .expect("dead node listed");
    assert_eq!(dead["status"], "degraded", "{health}");
    for failing in cluster["health"]["failing"].as_array().unwrap() {
        assert_eq!(failing["status"], "degraded", "{cluster}");
    }
}

// ---------------------------------------------------------------------------
// Multiuser auth, database security and fair admission
//
// These tests define the contract before the implementation. They use a small
// users file with token credentials and the CouchDB-shaped `_security` object.

struct UsersFile {
    path: PathBuf,
}

impl UsersFile {
    fn new(contents: &str) -> UsersFile {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "douchdb-users-{}-{}.json",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::write(&path, contents).expect("write users file");
        UsersFile { path }
    }

    fn arg(&self) -> String {
        self.path.display().to_string()
    }
}

impl Drop for UsersFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

const USERS: &str = r#"{"users":[
    {"name":"alice","token":"alice-token","roles":["analyst"]},
    {"name":"bob","token":"bob-token","roles":[]},
    {"name":"root","token":"root-token","roles":["_admin"]}
]}"#;

fn bearer(token: &str) -> Vec<(&'static str, String)> {
    vec![("Authorization", format!("Bearer {token}"))]
}

fn basic(name: &str, secret: &str) -> Vec<(&'static str, String)> {
    vec![(
        "Authorization",
        format!(
            "Basic {}",
            base64_encode(format!("{name}:{secret}").as_bytes())
        ),
    )]
}

fn admin_auth() -> Vec<(&'static str, String)> {
    basic("admin", "douchdb")
}

fn users_server_args(users: &UsersFile) -> Vec<String> {
    vec!["--users-file".to_string(), users.arg()]
}

#[tokio::test]
async fn users_file_authenticates_tokens_sessions_and_roles() {
    let users = UsersFile::new(USERS);
    let s = Server::start_with(&users_server_args(&users), true);

    let (status, body) = s
        .req_with(Method::GET, "/_session", None, &bearer("alice-token"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["userCtx"]["name"], "alice");
    assert_eq!(body["userCtx"]["roles"], json!(["analyst"]));

    // Basic uses the token as the password.
    let (status, body) = s
        .req_with(
            Method::GET,
            "/_session",
            None,
            &basic("alice", "alice-token"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["userCtx"]["name"], "alice");

    // The console flow: POST name/token, then reuse the signed cookie.
    let (status, headers, body) = s
        .req_headers(
            Method::POST,
            "/_session",
            Some(json!({"name": "alice", "password": "alice-token"})),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cookie = headers
        .get("set-cookie")
        .expect("session cookie")
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let (status, body) = s
        .req_with(Method::GET, "/_session", None, &[("Cookie", cookie)])
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["userCtx"]["name"], "alice");

    // Invalid credentials are rejected, not treated as anonymous.
    let (status, body) = s
        .req_with(Method::GET, "/_session", None, &bearer("wrong-token"))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    let (status, body) = s
        .req_with(
            Method::GET,
            "/_session",
            None,
            &basic("alice", "wrong-token"),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    // A user with the _admin role can perform server-admin operations.
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/root-created",
            Some(json!({})),
            &bearer("root-token"),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test]
async fn server_admin_endpoints_reject_anonymous_and_non_admin_callers() {
    let users = UsersFile::new(USERS);
    let s = Server::start_with(&users_server_args(&users), true);
    let (status, _) = s
        .req_with(Method::PUT, "/alpha", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let endpoints = [
        (Method::GET, "/_db_updates"),
        (Method::GET, "/_dbs_info"),
        (Method::POST, "/_dbs_info"),
        (Method::GET, "/_membership"),
        (Method::GET, "/_node/_local/_dbs"),
        (Method::GET, "/_node/_local/_cluster"),
        (Method::GET, "/_node/_local/_metrics"),
        (Method::GET, "/_node/_local/_health"),
        (Method::POST, "/_node/_local/_cluster/discover"),
        (Method::GET, "/_node/_local/_replication"),
        (Method::POST, "/_node/_local/_replication/run"),
        (Method::POST, "/_node/_local/_replication/pause"),
        (Method::POST, "/_node/_local/_replication/resume"),
        (Method::GET, "/_node/_local/_dbs/alpha"),
        (Method::PUT, "/_node/_local/_dbs/alpha"),
        (Method::GET, "/_node/_local/_dbs/alpha/shards"),
        (Method::POST, "/_node/_local/_dbs/alpha/rebalance"),
        (Method::POST, "/_node/_local/_dbs/alpha/cleanup"),
    ];
    for (method, path) in endpoints {
        let body = || Some(json!({}));
        let (status, anonymous) = s.req_with(method.clone(), path, body(), &[]).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{method} {path}: {anonymous}"
        );
        assert_eq!(anonymous["error"], "unauthorized", "{method} {path}");
        let (status, denied) = s
            .req_with(method.clone(), path, body(), &bearer("alice-token"))
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}: {denied}");
        assert_eq!(
            denied,
            json!({"error": "forbidden", "reason": "You are not authorized to access this db."}),
            "{method} {path}"
        );
        // Only read-only endpoints are called as an admin: the others start
        // rebalances and replication passes.
        if method != Method::GET {
            continue;
        }
        let (status, allowed) = s
            .req_with(method.clone(), path, None, &bearer("root-token"))
            .await;
        assert_ne!(
            status,
            StatusCode::UNAUTHORIZED,
            "{method} {path}: {allowed}"
        );
        assert_ne!(status, StatusCode::FORBIDDEN, "{method} {path}: {allowed}");
    }
}

#[tokio::test]
async fn security_object_enforces_members_admins_roles_and_all_dbs_filtering() {
    let users = UsersFile::new(USERS);
    let s = Server::start_with(&users_server_args(&users), true);
    let alice = bearer("alice-token");
    let bob = bearer("bob-token");

    let (status, body) = s
        .req_with(Method::PUT, "/alpha", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // New databases are admin-only when authentication is configured.
    let (status, _) = s.req_with(Method::GET, "/alpha", None, &alice).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Members can read/write ordinary documents but not design docs or security.
    let members = json!({
        "admins": {"names": [], "roles": []},
        "members": {"names": ["alice", "bob"], "roles": []}
    });
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/alpha/_security",
            Some(members),
            &admin_auth(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = s
        .req_with(Method::PUT, "/alpha/doc", Some(json!({"v": 1})), &alice)
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s.req_with(Method::GET, "/alpha/doc", None, &bob).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["v"], 1);

    let (status, _) = s
        .req_with(
            Method::PUT,
            "/alpha/_design/v",
            Some(json!({"views": {}})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = s
        .req_with(
            Method::PUT,
            "/alpha/_security",
            Some(json!({"members": {"names": ["alice"], "roles": []}})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Database admins can write design docs and change security.
    let admins = json!({
        "admins": {"names": ["alice"], "roles": []},
        "members": {"names": ["alice", "bob"], "roles": []}
    });
    let (status, body) = s
        .req_with(Method::PUT, "/alpha/_security", Some(admins), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/alpha/_design/v",
            Some(json!({"views": {}})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, _) = s
        .req_with(
            Method::PUT,
            "/alpha/_design/v2",
            Some(json!({"views": {}})),
            &bob,
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let (status, body) = s
        .req_with(Method::GET, "/alpha/_security", None, &alice)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["members"]["names"], json!(["alice", "bob"]));
    let (status, _) = s
        .req_with(Method::GET, "/alpha/_security", None, &bob)
        .await;
    assert_eq!(status, StatusCode::OK);

    // A role can grant membership.
    let (status, body) = s
        .req_with(Method::PUT, "/role-db", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/role-db/_security",
            Some(json!({
                "admins": {"names": [], "roles": []},
                "members": {"names": [], "roles": ["analyst"]}
            })),
            &admin_auth(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s.req_with(Method::GET, "/role-db", None, &alice).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = s.req_with(Method::GET, "/role-db", None, &bob).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // _all_dbs is filtered per principal; admins see everything.
    let (status, body) = s.req_with(Method::GET, "/_all_dbs", None, &alice).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let names: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect();
    assert!(names.contains(&"alpha"), "{names:?}");
    assert!(names.contains(&"role-db"), "{names:?}");

    let (status, body) = s.req_with(Method::GET, "/_all_dbs", None, &bob).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let names: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect();
    assert!(names.contains(&"alpha"), "{names:?}");
    assert!(!names.contains(&"role-db"), "{names:?}");

    let (status, body) = s
        .req_with(Method::GET, "/_all_dbs", None, &admin_auth())
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let names: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect();
    assert!(names.contains(&"alpha"), "{names:?}");
    assert!(names.contains(&"role-db"), "{names:?}");

    let (status, _) = s.req_with(Method::GET, "/_all_dbs", None, &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Removing Alice from security takes effect on the next request.
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/alpha/_security",
            Some(json!({
                "admins": {"names": [], "roles": []},
                "members": {"names": ["bob"], "roles": []}
            })),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, _) = s.req_with(Method::GET, "/alpha", None, &alice).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, body) = s.req_with(Method::GET, "/alpha", None, &bob).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn default_security_open_allows_any_authenticated_user() {
    let users = UsersFile::new(USERS);
    let mut args = users_server_args(&users);
    args.push("--default-security".to_string());
    args.push("open".to_string());
    let s = Server::start_with(&args, true);

    let (status, body) = s
        .req_with(Method::PUT, "/open-db", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .req_with(Method::GET, "/open-db", None, &bearer("alice-token"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/open-db/doc",
            Some(json!({"v": 1})),
            &bearer("alice-token"),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    // open means anonymous requests are members too.
    let (status, body) = s.req_with(Method::GET, "/open-db/doc", None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn per_user_admission_caps_concurrency_and_preserves_other_users() {
    let users = UsersFile::new(USERS);
    let mut args = users_server_args(&users);
    args.extend([
        "--max-in-flight".to_string(),
        "8".to_string(),
        "--per-user-in-flight".to_string(),
        "1".to_string(),
    ]);
    let s = Server::start_with(&args, true);
    let alice = bearer("alice-token");
    let bob = bearer("bob-token");

    let (status, body) = s
        .req_with(Method::PUT, "/fair", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/fair/_security",
            Some(json!({
                "admins": {"names": [], "roles": []},
                "members": {"names": ["alice", "bob"], "roles": []}
            })),
            &admin_auth(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Hold Alice's single slot with a long-poll changes request. It waits
    // for the next change, not for a clock: the test ends it with a write. A
    // probe that won the slot first makes the long-poll answer 429 at once,
    // so it asks again until it holds the slot.
    let longpoll = tokio::spawn({
        let client = s.client.clone();
        let url = format!(
            "{}/fair/_changes?feed=longpoll&timeout=300000&since=0",
            s.base
        );
        let auth = bearer("alice-token");
        async move {
            loop {
                let mut request = client.get(&url);
                for (name, value) in &auth {
                    request = request.header(*name, value);
                }
                let status = request.send().await.unwrap().status();
                if status != StatusCode::TOO_MANY_REQUESTS {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = s.req_with(Method::GET, "/fair", None, &alice).await;
        if status == StatusCode::TOO_MANY_REQUESTS {
            assert_eq!(body["error"], "too_many_requests", "{body}");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "alice was never capped: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Another user is not blocked by Alice's slot.
    let (status, body) = s.req_with(Method::GET, "/fair", None, &bob).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A change ends the long-poll and releases the slot.
    let (status, body) = s
        .req_with(Method::PUT, "/fair/wake", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let status = longpoll.await.unwrap();
    assert_eq!(status, StatusCode::OK);

    // Alice's slot is released when the long-poll ends.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, _) = s.req_with(Method::GET, "/fair", None, &alice).await;
        if status == StatusCode::OK {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "alice did not recover: {status}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A streamed feed outlives the handler, so it must keep its admission permit
/// until the body ends or the client goes away; otherwise any number of
/// continuous feeds escape the per-user limit.
#[tokio::test]
async fn streaming_feed_holds_its_admission_permit_until_the_client_disconnects() {
    let users = UsersFile::new(USERS);
    let mut args = users_server_args(&users);
    args.extend([
        "--max-in-flight".to_string(),
        "8".to_string(),
        "--per-user-in-flight".to_string(),
        "1".to_string(),
    ]);
    let s = Server::start_with(&args, true);
    let alice = bearer("alice-token");
    let bob = bearer("bob-token");

    let (status, body) = s
        .req_with(Method::PUT, "/feedlimit", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/feedlimit/_security",
            Some(json!({
                "admins": {"names": [], "roles": []},
                "members": {"names": ["alice", "bob"], "roles": []}
            })),
            &admin_auth(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s
        .req_with(Method::PUT, "/feedlimit/d1", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Alice opens a continuous feed and keeps the body open.
    let mut request = s.client.get(format!(
        "{}/feedlimit/_changes?feed=continuous&since=0&timeout=60000",
        s.base
    ));
    for (name, value) in bearer("alice-token") {
        request = request.header(name, value);
    }
    let mut feed = request.send().await.unwrap();
    assert_eq!(feed.status(), StatusCode::OK);
    assert!(feed.chunk().await.unwrap().is_some());

    // The feed holds her only slot, so a normal request is limited...
    let (status, body) = s.req_with(Method::GET, "/feedlimit", None, &alice).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    // ...while another user is not.
    let (status, body) = s.req_with(Method::GET, "/feedlimit", None, &bob).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Disconnecting frees the slot.
    drop(feed);
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, _) = s.req_with(Method::GET, "/feedlimit", None, &alice).await;
        if status == StatusCode::OK {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "alice's slot was never released: {status}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn global_admission_limit_protects_capacity_across_users() {
    let users = UsersFile::new(USERS);
    let mut args = users_server_args(&users);
    args.extend([
        "--max-in-flight".to_string(),
        "1".to_string(),
        "--per-user-in-flight".to_string(),
        "1".to_string(),
    ]);
    let s = Server::start_with(&args, true);

    let (status, body) = s
        .req_with(Method::PUT, "/global", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/global/_security",
            Some(json!({
                "admins": {"names": [], "roles": []},
                "members": {"names": ["alice", "bob"], "roles": []}
            })),
            &admin_auth(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The long-poll holds the only slot until the test hangs up. A probe that
    // won the slot first makes it answer 429 at once, so it asks again.
    let longpoll = tokio::spawn({
        let client = s.client.clone();
        let url = format!(
            "{}/global/_changes?feed=longpoll&timeout=300000&since=0",
            s.base
        );
        let auth = bearer("alice-token");
        async move {
            loop {
                let mut request = client.get(&url);
                for (name, value) in &auth {
                    request = request.header(*name, value);
                }
                let status = request.send().await.unwrap().status();
                if status != StatusCode::TOO_MANY_REQUESTS {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = s
            .req_with(Method::GET, "/global", None, &bearer("bob-token"))
            .await;
        if status == StatusCode::TOO_MANY_REQUESTS {
            assert_eq!(body["error"], "too_many_requests", "{body}");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "global limit did not reject bob: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Nothing else can be admitted while the long-poll holds the only slot,
    // so no write can wake it. Hanging up releases the slot instead.
    longpoll.abort();

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, _) = s
            .req_with(Method::GET, "/global", None, &bearer("bob-token"))
            .await;
        if status == StatusCode::OK {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "bob did not recover: {status}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Run the server with `extra` arguments and return its stderr if it exits
/// with a failure, or `None` if it kept running past a generous failsafe (it
/// is then killed).
fn startup_failure(extra: &[String]) -> Option<String> {
    use std::io::Read;
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("douchdb-startup-{}-{}", std::process::id(), port));
    let _ = std::fs::remove_dir_all(&dir);
    let mut cmd = server_command();
    cmd.arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--data")
        .arg(&dir)
        .args(extra)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    let mut child = spawn_locked(&mut cmd).expect("spawn douchdb");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut failed = None;
    while std::time::Instant::now() < deadline {
        if let Ok(Some(status)) = child.try_wait() {
            if !status.success() {
                let mut text = String::new();
                if let Some(mut stderr) = child.stderr.take() {
                    let _ = stderr.read_to_string(&mut text);
                }
                failed = Some(text);
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
    failed
}

#[test]
fn admin_without_colon_is_a_startup_error() {
    for admin in ["adminsecret", "", ":", "admin:", ":secret"] {
        let args = ["--admin".to_string(), admin.to_string()];
        let stderr = startup_failure(&args);
        assert!(
            stderr.is_some_and(|text| text.contains("--admin")),
            "--admin {admin:?} must fail at startup naming the flag"
        );
    }
}

#[test]
fn unparsable_numeric_flag_is_a_startup_error_with_exit_code_2() {
    use std::io::Read;
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("douchdb-badq-{}-{}", std::process::id(), port));
    let mut cmd = server_command();
    cmd.arg("--listen")
        .arg(format!("127.0.0.1:{port}"))
        .arg("--data")
        .arg(&dir)
        .args(["--q", "abc"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    let mut child = spawn_locked(&mut cmd).expect("spawn douchdb");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut status = None;
    while std::time::Instant::now() < deadline {
        if let Ok(Some(done)) = child.try_wait() {
            status = Some(done);
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = child.kill();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
    let status = status.expect("--q abc must stop startup, but the server kept running");
    assert_eq!(status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("--q") && stderr.contains("abc"), "{stderr}");
}

/// Send `signal` to a running server.
fn signal_server(server: &Server, signal: i32) {
    // SAFETY: kill(2) takes plain integers and touches no memory.
    let rc = unsafe { libc::kill(server.child.id() as i32, signal) };
    assert_eq!(rc, 0, "kill({signal}) failed");
}

/// Wait up to `limit` for the server process to exit on its own.
fn wait_for_exit(server: &mut Server, limit: Duration) -> Option<std::process::ExitStatus> {
    let deadline = std::time::Instant::now() + limit;
    while std::time::Instant::now() < deadline {
        if let Ok(Some(status)) = server.child.try_wait() {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

/// SIGTERM (Docker, systemd) must shut the server down gracefully like Ctrl-C:
/// an open continuous feed ends with its `last_seq` trailer instead of holding
/// the shutdown, and the process exits 0 well inside the shutdown deadline.
#[tokio::test]
async fn sigterm_ends_a_continuous_feed_with_its_trailer_and_exits_zero() {
    let mut s = Server::start(false);
    s.fresh_db("sigfeed").await;
    let (status, _) = s.put("/sigfeed/a", json!({"n": 1})).await;
    assert_eq!(status, StatusCode::CREATED);
    let client = Client::new();
    let mut response = client
        .get(format!(
            "{}/sigfeed/_changes?feed=continuous&since=0&timeout=300000",
            s.base
        ))
        .send()
        .await
        .expect("open feed");
    let mut text = String::new();
    while !text.contains("\"id\":\"a\"") {
        let chunk = tokio::time::timeout(Duration::from_secs(60), response.chunk())
            .await
            .expect("first change")
            .expect("read feed")
            .expect("feed ended early");
        text.push_str(&String::from_utf8_lossy(&chunk));
    }
    signal_server(&s, libc::SIGTERM);
    let ended = tokio::time::timeout(Duration::from_secs(60), async {
        while let Ok(Some(chunk)) = response.chunk().await {
            text.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await;
    assert!(ended.is_ok(), "the feed was still open: {text}");
    assert!(text.contains("\"last_seq\""), "no trailer: {text}");
    let status = wait_for_exit(&mut s, Duration::from_secs(60)).expect("server did not exit");
    assert_eq!(status.code(), Some(0), "{status:?}");
}

/// Everything acknowledged before a SIGTERM is there after a restart.
#[tokio::test]
async fn sigterm_then_restart_keeps_acknowledged_documents() {
    let mut s = Server::start(false);
    s.fresh_db("sigdocs").await;
    let docs: Vec<Value> = (0..50)
        .map(|i| json!({"_id": format!("d{i:02}"), "n": i}))
        .collect();
    let (status, body) = s.post("/sigdocs/_bulk_docs", json!({"docs": docs})).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    signal_server(&s, libc::SIGTERM);
    let status = wait_for_exit(&mut s, Duration::from_secs(60)).expect("server did not exit");
    assert_eq!(status.code(), Some(0), "{status:?}");
    s.restart();
    let (status, body) = s.get("/sigdocs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["doc_count"], 50, "{body}");
}

/// A second signal while shutting down neither hangs nor changes the outcome.
#[tokio::test]
async fn a_second_sigterm_during_shutdown_does_not_hang() {
    let mut s = Server::start(false);
    s.fresh_db("sigtwice").await;
    signal_server(&s, libc::SIGTERM);
    signal_server(&s, libc::SIGTERM);
    signal_server(&s, libc::SIGINT);
    let status = wait_for_exit(&mut s, Duration::from_secs(60)).expect("server did not exit");
    assert_eq!(status.code(), Some(0), "{status:?}");
}

#[test]
fn users_file_name_colliding_with_admin_is_a_startup_error() {
    let users = UsersFile::new(r#"{"users":[{"name":"admin","token":"long-enough-token"}]}"#);
    let mut args = users_server_args(&users);
    args.extend(["--admin".to_string(), "admin:douchdb".to_string()]);
    let stderr = startup_failure(&args).expect("collision must stop startup");
    assert!(stderr.contains("admin"), "{stderr}");
}

#[test]
fn empty_or_short_user_tokens_are_a_startup_error() {
    for entry in [
        r#"{"name":"a","token":""}"#,
        r#"{"name":"a","token":"short"}"#,
        r#"{"name":"a","token_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}"#,
    ] {
        let users = UsersFile::new(&format!(r#"{{"users":[{entry}]}}"#));
        assert!(
            startup_failure(&users_server_args(&users)).is_some(),
            "accepted {entry}"
        );
    }
}

#[tokio::test]
async fn empty_credentials_never_authenticate() {
    let users = UsersFile::new(USERS);
    let s = Server::start_with(&users_server_args(&users), true);
    let cases: Vec<(&str, Vec<(&'static str, String)>)> = vec![
        (
            "empty bearer",
            vec![("Authorization", "Bearer ".to_string())],
        ),
        ("empty user token", basic("alice", "")),
        ("empty admin password", basic("admin", "")),
        ("empty basic", basic("", "")),
        ("empty cookie", vec![("Cookie", "AuthSession=".to_string())]),
    ];
    for (label, headers) in cases {
        let (status, body) = s
            .req_with(Method::PUT, "/empty-cred-db", Some(json!({})), &headers)
            .await;
        assert!(
            status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN,
            "{label}: {status} {body}"
        );
    }
    for (name, password) in [("alice", ""), ("admin", ""), ("", "")] {
        let (status, _, body) = s
            .req_headers(
                Method::POST,
                "/_session",
                Some(json!({"name": name, "password": password})),
                &[],
            )
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{name:?}: {body}");
    }
}

#[tokio::test]
async fn login_succeeds_while_anonymous_bucket_is_full() {
    let users = UsersFile::new(
        r#"{"users":[
            {"name":"alice","token":"alice-token","roles":[]},
            {"name":"anonymous","token":"anonymous-token","roles":[]}
        ]}"#,
    );
    let mut args = users_server_args(&users);
    args.extend([
        "--default-security".to_string(),
        "open".to_string(),
        "--per-user-in-flight".to_string(),
        "2".to_string(),
        "--max-in-flight".to_string(),
        "16".to_string(),
    ]);
    let s = Server::start_with(&args, true);
    let (status, body) = s
        .req_with(Method::PUT, "/flood", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Hold every anonymous slot with long-poll changes requests. A holder
    // that raced a probe for a slot is rejected and finishes at once, so it is
    // restarted until the bucket is full.
    let hold = || {
        let client = s.client.clone();
        let url = format!(
            "{}/flood/_changes?feed=longpoll&timeout=300000&since=0",
            s.base
        );
        tokio::spawn(async move { client.get(url).send().await.map(|r| r.status()) })
    };
    let mut held = vec![hold(), hold()];
    // The bucket is full once an anonymous probe is rejected: only the two
    // holders can be occupying its slots then.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, _) = s.req_with(Method::GET, "/flood", None, &[]).await;
        if status == StatusCode::TOO_MANY_REQUESTS {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "anonymous bucket never filled: {status}"
        );
        for task in held.iter_mut().filter(|task| task.is_finished()) {
            *task = hold();
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Logins have their own lane.
    let (status, _, body) = s
        .req_headers(
            Method::POST,
            "/_session",
            Some(json!({"name": "alice", "password": "alice-token"})),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "login starved: {body}");
    // A user literally named "anonymous" does not share the bucket.
    let (status, body) = s
        .req_with(Method::GET, "/flood", None, &bearer("anonymous-token"))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // Anonymous traffic is still capped.
    let (status, _) = s.req_with(Method::GET, "/flood", None, &[]).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    for task in held {
        task.abort();
    }
}

fn assert_no_access(status: StatusCode, label: &str) {
    assert!(
        status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED,
        "{label}: expected 401/403, got {status}"
    );
}

#[tokio::test]
async fn cross_user_isolation_is_watertight() {
    let users = UsersFile::new(USERS);
    let s = Server::start_with(&users_server_args(&users), true);
    let admin = admin_auth();
    let alice = bearer("alice-token");
    let bob = bearer("bob-token");

    // Two private databases, one shared database, and one admin-only database
    // with an explicitly empty member list.
    for name in ["alice-db", "bob-db", "shared-db", "admins-only"] {
        let (status, body) = s
            .req_with(Method::PUT, &format!("/{name}"), Some(json!({})), &admin)
            .await;
        assert_eq!(status, StatusCode::CREATED, "{name}: {body}");
    }
    for (db, admins, members) in [
        ("alice-db", json!(["alice"]), json!(["alice"])),
        ("bob-db", json!(["bob"]), json!(["bob"])),
        ("shared-db", json!([]), json!(["alice", "bob"])),
        ("admins-only", json!(["alice"]), json!([])),
    ] {
        let (status, body) = s
            .req_with(
                Method::PUT,
                &format!("/{db}/_security"),
                Some(json!({
                    "admins": {"names": admins, "roles": []},
                    "members": {"names": members, "roles": []}
                })),
                &admin,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{db}: {body}");
    }

    // Distinct documents with the same id in two databases.
    for (db, auth, secret) in [
        ("alice-db", &alice, "alice-secret"),
        ("bob-db", &bob, "bob-secret"),
    ] {
        let (status, body) = s
            .req_with(
                Method::PUT,
                &format!("/{db}/shared"),
                Some(json!({"owner": db, "secret": secret})),
                auth,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{db}: {body}");
        let (status, body) = s
            .req_with(
                Method::PUT,
                &format!("/{db}/only-{db}"),
                Some(json!({"only": db})),
                auth,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{db}: {body}");
        let (status, body) = s
            .req_with(
                Method::PUT,
                &format!("/{db}/_local/checkpoint"),
                Some(json!({"db": db})),
                auth,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{db}: {body}");
        let (status, body) = s
            .req_with(
                Method::PUT,
                &format!("/{db}/_design/v"),
                Some(json!({
                    "views": {"by_owner": {"map": "function(doc){ if (doc.owner) emit(doc.owner, doc.secret); }"}},
                    "indexes": {"by_owner": {"index": "function(doc){ index('owner', doc.owner); index('secret', doc.secret); }"}}
                })),
                &admin,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{db}: {body}");
    }

    // Shared database: members can read/write ordinary docs but not design docs.
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/shared-db/normal",
            Some(json!({"shared": true})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/shared-db/_design/v",
            Some(json!({"views": {}})),
            &admin,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (_, alice_doc) = s
        .req_with(Method::GET, "/alice-db/shared", None, &alice)
        .await;
    let alice_rev = alice_doc["_rev"].as_str().unwrap().to_string();
    let (_, bob_doc) = s.req_with(Method::GET, "/bob-db/shared", None, &bob).await;
    let bob_rev = bob_doc["_rev"].as_str().unwrap().to_string();
    assert_ne!(alice_rev, bob_rev);

    // Alice's own data is visible and complete.
    let (status, body) = s
        .req_with(Method::GET, "/alice-db/shared", None, &alice)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["secret"], "alice-secret");
    let (status, body) = s
        .req_with(
            Method::POST,
            "/alice-db/_all_docs",
            Some(json!({"keys": ["shared", "bob-db-only"]})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["id"], "shared");
    assert_eq!(body["rows"][1]["error"], "not_found");
    let text = serde_json::to_string(&body).unwrap();
    assert!(!text.contains("bob-secret"), "{text}");

    // Another database's revision does not resolve in Alice's database.
    let (status, body) = s
        .req_with(
            Method::GET,
            &format!("/alice-db/shared?rev={bob_rev}"),
            None,
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));
    let (status, body) = s
        .req_with(
            Method::POST,
            "/alice-db/_bulk_get",
            Some(json!({"docs": [{"id": "shared", "rev": bob_rev}]})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));
    let (status, body) = s
        .req_with(
            Method::POST,
            "/alice-db/_revs_diff",
            Some(json!({"shared": [bob_rev]})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));

    // Views, search, changes, all_docs and local docs stay inside the database.
    let (status, body) = s
        .req_with(
            Method::GET,
            "/alice-db/_design/v/_view/by_owner?key=%22bob%22",
            None,
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 0);
    assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));
    let (status, body) = s
        .req_with(
            Method::GET,
            "/alice-db/_design/v/_search/by_owner?q=owner:bob",
            None,
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 0);
    assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));
    let (status, body) = s
        .req_with(Method::GET, "/alice-db/_changes?since=0", None, &alice)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));
    let (status, body) = s
        .req_with(
            Method::GET,
            "/alice-db/_all_docs?include_docs=true",
            None,
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));
    let (status, body) = s
        .req_with(Method::GET, "/alice-db/_local/checkpoint", None, &alice)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["db"], "alice-db");

    // Every data-bearing route on Bob's database is denied to Alice.
    let copy = Method::from_bytes(b"COPY").unwrap();
    type DeniedRequest = (
        &'static str,
        Method,
        Option<Value>,
        Vec<(&'static str, String)>,
    );
    let denied: Vec<DeniedRequest> = vec![
        ("/bob-db", Method::GET, None, vec![]),
        ("/bob-db/shared", Method::GET, None, vec![]),
        ("/bob-db/shared", Method::HEAD, None, vec![]),
        ("/bob-db/shared", Method::DELETE, None, vec![]),
        ("/bob-db/new", Method::PUT, Some(json!({"x": 1})), vec![]),
        ("/bob-db", Method::POST, Some(json!({"x": 1})), vec![]),
        ("/bob-db/_all_docs", Method::GET, None, vec![]),
        (
            "/bob-db/_all_docs",
            Method::POST,
            Some(json!({"keys": ["shared"]})),
            vec![],
        ),
        ("/bob-db/_changes", Method::GET, None, vec![]),
        (
            "/bob-db/_bulk_get",
            Method::POST,
            Some(json!({"docs": [{"id": "shared", "rev": bob_rev}]})),
            vec![],
        ),
        (
            "/bob-db/_revs_diff",
            Method::POST,
            Some(json!({"shared": [bob_rev]})),
            vec![],
        ),
        (
            "/bob-db/_bulk_docs",
            Method::POST,
            Some(json!({"docs": [{"_id": "x"}]})),
            vec![],
        ),
        ("/bob-db/_local/checkpoint", Method::GET, None, vec![]),
        (
            "/bob-db/_local/checkpoint",
            Method::PUT,
            Some(json!({"x": 1})),
            vec![],
        ),
        ("/bob-db/shared?revs=true", Method::GET, None, vec![]),
        ("/bob-db/shared?open_revs=all", Method::GET, None, vec![]),
        ("/bob-db/shared?conflicts=true", Method::GET, None, vec![]),
        ("/bob-db/_design/v", Method::GET, None, vec![]),
        (
            "/bob-db/_design/v/_view/by_owner",
            Method::GET,
            None,
            vec![],
        ),
        (
            "/bob-db/_design/v/_view/by_owner?include_docs=true",
            Method::GET,
            None,
            vec![],
        ),
        (
            "/bob-db/_design/v/_search/by_owner?q=*:*",
            Method::GET,
            None,
            vec![],
        ),
        (
            "/bob-db/_design/v/_search/by_owner?q=*:*&include_docs=true",
            Method::GET,
            None,
            vec![],
        ),
        (
            "/bob-db/_design/v/_search_info/by_owner",
            Method::GET,
            None,
            vec![],
        ),
        ("/bob-db/_index_info", Method::GET, None, vec![]),
        ("/bob-db/_partition/p", Method::GET, None, vec![]),
        ("/bob-db/_partition/p/_all_docs", Method::GET, None, vec![]),
        ("/bob-db/_security", Method::GET, None, vec![]),
        (
            "/bob-db/_security",
            Method::PUT,
            Some(json!({"members": {"names": ["alice"]}})),
            vec![],
        ),
        ("/bob-db/_revs_limit", Method::GET, None, vec![]),
        ("/bob-db/_revs_limit", Method::PUT, Some(json!(100)), vec![]),
        ("/bob-db/_compact", Method::POST, None, vec![]),
        ("/bob-db/_view_cleanup", Method::POST, None, vec![]),
        ("/bob-db/_search_cleanup", Method::POST, None, vec![]),
        (
            "/bob-db/_purge",
            Method::POST,
            Some(json!({"shared": [bob_rev]})),
            vec![],
        ),
        ("/bob-db/_ensure_full_commit", Method::POST, None, vec![]),
        (
            "/bob-db/shared",
            copy.clone(),
            None,
            vec![("Destination", "copied".to_string())],
        ),
        ("/bob-db", Method::DELETE, None, vec![]),
        (
            "/_dbs_info?keys=%5B%22bob-db%22%5D",
            Method::GET,
            None,
            vec![],
        ),
        ("/_db_updates", Method::GET, None, vec![]),
        ("/_node/_local/_dbs/bob-db", Method::GET, None, vec![]),
        (
            "/_node/_local/_dbs/bob-db/shards",
            Method::GET,
            None,
            vec![],
        ),
        (
            "/_node/_local/_dbs/bob-db/security",
            Method::PUT,
            Some(json!({})),
            vec![],
        ),
        ("/_node/_local/_metrics", Method::GET, None, vec![]),
        ("/_node/_local/_health", Method::GET, None, vec![]),
        ("/_node/_local/_cluster", Method::GET, None, vec![]),
        ("/_node/_local/_replication", Method::GET, None, vec![]),
        ("/_membership", Method::GET, None, vec![]),
    ];
    for (path, method, body, headers) in denied {
        let mut request_headers = alice.clone();
        request_headers.extend(headers);
        let (status, response) = s.req_with(method, path, body, &request_headers).await;
        assert_no_access(status, path);
        let text = serde_json::to_string(&response).unwrap();
        assert!(!text.contains("bob-secret"), "{path} leaked data: {text}");
    }

    // _all_dbs is filtered, and Bob's database name is not exposed to Alice.
    let (status, body) = s.req_with(Method::GET, "/_all_dbs", None, &alice).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let names: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect();
    assert!(names.contains(&"alice-db"), "{names:?}");
    assert!(names.contains(&"shared-db"), "{names:?}");
    assert!(!names.contains(&"bob-db"), "{names:?}");

    // Member privilege escalation through design documents is blocked.
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/shared-db/_design/evil",
            Some(json!({"views": {}})),
            &alice,
        )
        .await;
    assert_no_access(status, "design doc PUT");
    assert!(!serde_json::to_string(&body)
        .unwrap()
        .contains("\"ok\":true"));
    let (status, body) = s
        .req_with(
            Method::POST,
            "/shared-db/_bulk_docs",
            Some(json!({"docs": [{"_id": "_design/evil", "views": {}}]})),
            &alice,
        )
        .await;
    assert_no_access(status, "bulk design doc");
    assert!(!serde_json::to_string(&body)
        .unwrap()
        .contains("\"ok\":true"));
    let (status, body) = s
        .req_with(
            copy.clone(),
            "/shared-db/_design/v",
            None,
            &[
                ("Destination", "_design/copied".to_string()),
                ("Authorization", "Bearer alice-token".to_string()),
            ],
        )
        .await;
    assert_no_access(status, "copy design doc");
    assert!(!serde_json::to_string(&body)
        .unwrap()
        .contains("\"ok\":true"));
    let (status, body) = s
        .req_with(
            copy.clone(),
            "/shared-db/normal",
            None,
            &[
                ("Destination", "_design/copied".to_string()),
                ("Authorization", "Bearer alice-token".to_string()),
            ],
        )
        .await;
    assert_no_access(status, "copy into design doc");
    assert!(!serde_json::to_string(&body)
        .unwrap()
        .contains("\"ok\":true"));
    for (path, body) in [
        ("/shared-db/_compact", None),
        ("/shared-db/_view_cleanup", None),
        ("/shared-db/_search_cleanup", None),
        ("/shared-db/_purge", Some(json!({"normal": []}))),
    ] {
        let (status, response) = s.req_with(Method::POST, path, body, &alice).await;
        assert_no_access(status, path);
        assert!(!serde_json::to_string(&response)
            .unwrap()
            .contains("\"ok\":true"));
    }
    let (status, _) = s
        .req_with(
            Method::PUT,
            "/shared-db/_security",
            Some(json!({"members": {"names": ["alice"]}})),
            &alice,
        )
        .await;
    assert_no_access(status, "security escalation");

    // The same design-doc write succeeds for a database admin.
    let (status, body) = s
        .req_with(
            Method::POST,
            "/shared-db/_bulk_docs",
            Some(json!({"docs": [{"_id": "_design/admin", "views": {}}]})),
            &admin,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Admin-only databases with an empty member list are not public.
    let (status, _) = s.req_with(Method::GET, "/admins-only", None, &bob).await;
    assert_no_access(status, "admins-only bob");
    let (status, _) = s.req_with(Method::GET, "/admins-only", None, &[]).await;
    assert_no_access(status, "admins-only anonymous");
    let (status, body) = s.req_with(Method::GET, "/admins-only", None, &alice).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Database names cannot be used for path traversal.
    for path in ["/%2E%2E/bob-db/shared", "/..%2Fbob-db/shared"] {
        let (status, body) = s.req_with(Method::GET, path, None, &alice).await;
        assert_ne!(status, StatusCode::OK, "{path}: {body}");
        assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));
    }
    // A normalized `..` path is just Alice's own database; it must not cross
    // into Bob's data even though the raw path mentioned `bob-db`.
    let (_, body) = s
        .req_with(Method::GET, "/bob-db/%2E%2E/alice-db/shared", None, &alice)
        .await;
    assert!(!serde_json::to_string(&body).unwrap().contains("bob-secret"));

    // Public endpoints do not enumerate databases or data.
    let (status, body) = s.req_with(Method::GET, "/_health", None, &[]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let text = serde_json::to_string(&body).unwrap();
    for needle in ["alice-db", "bob-db", "alice-secret", "bob-secret"] {
        assert!(
            !text.contains(needle),
            "public health leaked {needle}: {text}"
        );
    }
    let (status, _) = s.req_with(Method::GET, "/_all_dbs", None, &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = s.req_with(Method::GET, "/bob-db", None, &[]).await;
    assert_no_access(status, "anonymous bob-db");
}

#[tokio::test]
async fn longpoll_changes_stops_after_security_revocation() {
    let users = UsersFile::new(USERS);
    let s = Server::start_with(&users_server_args(&users), true);
    let admin = admin_auth();
    let alice = bearer("alice-token");

    let (status, body) = s
        .req_with(Method::PUT, "/revoke", Some(json!({})), &admin)
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/revoke/_security",
            Some(json!({
                "admins": {"names": [], "roles": []},
                "members": {"names": ["alice", "bob"], "roles": []}
            })),
            &admin,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let longpoll = tokio::spawn({
        let client = s.client.clone();
        let url = format!(
            "{}/revoke/_changes?feed=longpoll&timeout=300000&since=0",
            s.base
        );
        let auth = bearer("alice-token");
        async move {
            let mut request = client.get(url);
            for (name, value) in auth {
                request = request.header(name, value);
            }
            let response = request.send().await.unwrap();
            let status = response.status();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            (status, body)
        }
    });

    // Revoke Alice and create a document she must never receive. Whether the
    // long-poll is already waiting or starts after this, it must not see the
    // document, and its own timeout is far longer than the test.
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/revoke/_security",
            Some(json!({
                "admins": {"names": [], "roles": []},
                "members": {"names": ["bob"], "roles": []}
            })),
            &admin,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/revoke/after-revocation",
            Some(json!({"secret": "must-not-leak"})),
            &admin,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body) = tokio::time::timeout(Duration::from_secs(60), longpoll)
        .await
        .expect("longpoll did not finish")
        .unwrap();
    assert!(
        status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED,
        "longpoll should close after revocation: {status} {body}"
    );
    assert!(
        !serde_json::to_string(&body)
            .unwrap()
            .contains("must-not-leak"),
        "revoked longpoll leaked data: {body}"
    );

    // Alice can no longer read the database at all.
    let (status, _) = s
        .req_with(Method::GET, "/revoke/after-revocation", None, &alice)
        .await;
    assert_no_access(status, "revoked alice");
}

/// Wait until `GET /{db}/_security` on `node` returns `expected`.
async fn wait_for_security(node: &Server, db: &str, expected: &Value) {
    let admin = admin_auth();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = node
            .req_with(Method::GET, &format!("/{db}/_security"), None, &admin)
            .await;
        if status == StatusCode::OK && &body == expected {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "security did not reconcile on {}: {status} {body}",
                node.base
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn create_db_on_every_node(nodes: &[Server], db: &str) {
    let admin = admin_auth();
    for node in nodes {
        let (status, body) = node
            .req_with(Method::PUT, &format!("/{db}"), Some(json!({})), &admin)
            .await;
        assert!(
            status == StatusCode::CREATED || status == StatusCode::PRECONDITION_FAILED,
            "create {db}: {status} {body}"
        );
    }
}

#[tokio::test]
async fn cluster_security_reconciles_to_peer_that_missed_the_update() {
    let mut nodes = start_cluster_with_auth(3, "1", 3);
    create_db_on_every_node(&nodes, "secsync").await;
    let admin = admin_auth();

    // A majority acknowledges, so the write succeeds even though one peer is down.
    nodes[2].kill();
    let security = json!({
        "admins": {"names": [], "roles": []},
        "members": {"names": ["alice"], "roles": []}
    });
    let (status, body) = nodes[0]
        .req_with(
            Method::PUT,
            "/secsync/_security",
            Some(security.clone()),
            &admin,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // The returning peer converges through anti-entropy, and the client-visible
    // body carries no internal version.
    nodes[2].restart();
    wait_for_security(&nodes[2], "secsync", &security).await;
    wait_for_security(&nodes[1], "secsync", &security).await;
}

#[tokio::test]
async fn cluster_security_without_quorum_is_accepted_and_converges() {
    let mut nodes = start_cluster_with_auth(2, "1", 2);
    create_db_on_every_node(&nodes, "secpend").await;
    let admin = admin_auth();

    nodes[1].kill();
    let security = json!({
        "admins": {"names": [], "roles": []},
        "members": {"names": ["alice"], "roles": []}
    });
    let (status, body) = nodes[0]
        .req_with(
            Method::PUT,
            "/secpend/_security",
            Some(security.clone()),
            &admin,
        )
        .await;
    // Not a failure: the change is kept and will reach the peer.
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["ok"], true);
    wait_for_security(&nodes[0], "secpend", &security).await;

    nodes[1].restart();
    wait_for_security(&nodes[1], "secpend", &security).await;
}

#[tokio::test]
async fn cluster_rebalance_requires_explicit_cleanup() {
    let nodes = start_cluster_with(3, "1", 1);
    for node in &nodes {
        node.fresh_db("noclean").await;
    }
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/noclean", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);
    let docs: Vec<Value> = (0..9)
        .map(|i| json!({"_id": format!("nc{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/noclean/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = nodes[0]
        .post(
            "/_node/_local/_dbs/noclean/rebalance?timeout_ms=20000",
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("cleaned").is_none(), "{body}");

    // The migration completed, but the old node keeps its local copies until
    // cleanup is explicitly requested.
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let (status, counts) = nodes[0]
        .req_with(
            Method::GET,
            "/_node/_local/_dbs/noclean/shards",
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
    assert_eq!(total, 9, "old shards must survive until explicit cleanup");
    let (status, body) = nodes[0].get("/noclean/_all_docs").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 9);
}

/// Rebalance targets the placement database creation uses, so a cluster that
/// has not changed has nothing to move.
#[tokio::test]
async fn rebalance_on_an_unchanged_cluster_moves_nothing() {
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("steady").await;
    }
    let (status, before) = nodes[0].get("/_node/_local/_dbs/steady").await;
    assert_eq!(status, StatusCode::OK, "{before}");

    let (status, body) = nodes[0]
        .post(
            "/_node/_local/_dbs/steady/rebalance?timeout_ms=20000",
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["replicas"], before["replicas"], "{body}");
    assert_eq!(body["version"], before["version"], "{body}");
    let (_, after) = nodes[0].get("/_node/_local/_dbs/steady").await;
    assert_eq!(
        after, before,
        "an unchanged cluster's rebalance moved shards"
    );
}

#[tokio::test]
async fn rebalance_verifies_deleted_only_shards_before_cleanup() {
    let nodes = start_cluster_with(3, "1", 1);
    for node in &nodes {
        node.fresh_db("delreb").await;
    }
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/delreb", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);

    let docs: Vec<Value> = (0..8)
        .map(|i| json!({"_id": format!("del{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/delreb/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, all) = nodes[0].get("/delreb/_all_docs").await;
    let deletes: Vec<Value> = all["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| json!({"_id": row["id"], "_rev": row["value"]["rev"], "_deleted": true}))
        .collect();
    let (status, body) = nodes[0]
        .post("/delreb/_bulk_docs", json!({"docs": deletes}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (_, live) = nodes[0].get("/delreb/_all_docs").await;
    assert_eq!(live["total_rows"], 0);

    let (status, body) = nodes[0]
        .post(
            "/_node/_local/_dbs/delreb/rebalance?timeout_ms=20000&cleanup=true",
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // Every tombstone must exist exactly once on the new owners; a deleted-only
    // shard has live count 0, so counts alone cannot establish this.
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let mut deleted = 0u64;
    for node in &nodes {
        let (status, counts) = node
            .req_with(
                Method::GET,
                "/_node/_local/_dbs/delreb/shards",
                None,
                &internal,
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{counts}");
        deleted += counts["shards"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["deleted"].as_u64().unwrap())
            .sum::<u64>();
    }
    assert_eq!(deleted, 8, "tombstones were lost during rebalance cleanup");

    let (status, body) = nodes[0]
        .get("/delreb/_changes?since=0&style=all_docs")
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ids: std::collections::HashSet<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 8, "deleted changes were lost: {body}");
    for row in body["results"].as_array().unwrap() {
        assert_eq!(row["deleted"], true, "{row}");
    }
}

#[tokio::test]
async fn rebalance_verifies_conflict_leaves_before_cleanup() {
    // Background anti-entropy is off, so the target replica only changes when
    // the test triggers a pass.
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("confreb").await;
    }
    // The default placement is the one a rebalance moves the database to.
    let (status, planned) = nodes[0].get("/_node/_local/_dbs/confreb").await;
    assert_eq!(status, StatusCode::OK, "{planned}");
    let pinned = node_to_pin_away_from(&planned);
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/confreb", all_shards_on(pinned))
        .await;
    assert_eq!(status, StatusCode::OK);
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];

    // A document whose shard moves away from the pinned node in the planned
    // map. Some shard does: the pin is chosen so the plan is not all on it.
    let owner_of = |id: &str| {
        planned["replicas"][douchdb::shard_index(id, 4)][0]
            .as_u64()
            .unwrap() as usize
    };
    let id = (0..1000)
        .map(|i| format!("x{i}"))
        .find(|id| owner_of(id) != pinned)
        .expect("the pin was chosen so that some shard moves");
    let target = owner_of(&id);

    let root = "00000000000000000000000000000000";
    let branch = |rev: &str, v: &str| {
        json!({"_id": id, "_rev": format!("2-{rev}"),
               "_revisions": {"start": 2, "ids": [rev, root]}, "v": v})
    };
    let b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let c = "cccccccccccccccccccccccccccccccc";
    let (status, body) = nodes[0]
        .post(
            "/confreb/_bulk_docs",
            json!({"new_edits": false, "docs": [branch(b, "b"), branch(c, "c")]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    // The target already holds the same document with only one leaf: same
    // document count as the source, different content.
    let (status, body) = nodes[target]
        .req_with(
            Method::POST,
            "/confreb/_bulk_docs",
            Some(json!({"new_edits": false, "docs": [branch(b, "b")]})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let rebalance = tokio::spawn({
        let client = nodes[0].client.clone();
        let url = format!(
            "{}/_node/_local/_dbs/confreb/rebalance?timeout_ms=20000&cleanup=true",
            nodes[0].base
        );
        async move {
            let response = client.post(url).json(&json!({})).send().await.unwrap();
            let status = response.status();
            (
                status,
                response.json::<Value>().await.unwrap_or(Value::Null),
            )
        }
    });
    // Counts already match, so only the content digest can hold the
    // rebalance back until the missing leaf has been copied. Nothing copies
    // it until the test asks, so once the freeze is up the rebalance cannot
    // have finished.
    wait_until_frozen(&nodes[0], "confreb").await;
    assert!(
        !rebalance.is_finished(),
        "rebalance completed before the target had every conflict leaf"
    );
    let (status, body) = sync_until_done(&nodes, "confreb", rebalance).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = nodes[0].get(&format!("/confreb/{id}?conflicts=true")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["_conflicts"],
        json!([format!("2-{b}")]),
        "conflict leaf was lost during rebalance cleanup: {body}"
    );
}

#[tokio::test]
async fn quorum_read_prefers_tombstone_over_stale_live_replica() {
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("qtomb").await;
    }
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];

    // Both replicas get the live revision.
    let (status, body) = nodes[0]
        .post("/qtomb/_bulk_docs", json!({"docs": [{"_id": "x", "v": 1}]}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, doc) = nodes[0]
        .req_with(Method::GET, "/qtomb/x", None, &internal)
        .await;
    assert_eq!(status, StatusCode::OK, "{doc}");
    let rev = doc["_rev"].as_str().unwrap().to_string();

    // Delete only on node0. Node1 keeps the stale live revision.
    let (status, body) = nodes[0]
        .req_with(
            Method::POST,
            "/qtomb/_bulk_docs",
            Some(json!({"docs": [{"_id": "x", "_rev": rev, "_deleted": true}]})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, stale) = nodes[1]
        .req_with(Method::GET, "/qtomb/x", None, &internal)
        .await;
    assert_eq!(status, StatusCode::OK, "stale replica: {stale}");
    assert_eq!(stale["v"], 1);

    // A strict quorum read sees the tombstone and a stale live copy. The
    // tombstone is the descendant and must win; the result is 404, not stale data.
    // Coordinating from either replica gives the same answer, including from
    // node1, whose own copy is the stale one.
    for node in &nodes {
        let (status, body) = node.get("/qtomb/x?r=2").await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(!serde_json::to_string(&body).unwrap().contains("\"v\":1"));
    }
}

#[tokio::test]
async fn quorum_read_prefers_conflict_winner_across_replicas() {
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("qconf").await;
    }
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    // Each replica holds only one branch, so the winner and its conflict can
    // only come from merging both replicas.
    let root = "00000000000000000000000000000000";
    let branches = [
        ("b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ("c", "cccccccccccccccccccccccccccccccc"),
    ];
    for (node, (v, id)) in nodes.iter().zip(branches) {
        let docs = json!({"new_edits": false, "docs": [
            {"_id": "x", "_rev": format!("2-{id}"),
             "_revisions": {"start": 2, "ids": [id, root]}, "v": v}
        ]});
        let (status, body) = node
            .req_with(Method::POST, "/qconf/_bulk_docs", Some(docs), &internal)
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    for node in &nodes {
        let (status, body) = node.get("/qconf/x?r=2&conflicts=true").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["v"], "c", "{body}");
        assert_eq!(
            body["_conflicts"],
            json!(["2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]),
            "{body}"
        );
    }
}

#[tokio::test]
async fn quorum_reconciliation_is_coordinator_independent() {
    let nodes = start_cluster_with(3, "3600", 3);
    for node in &nodes {
        node.fresh_db("qsplit").await;
    }
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let root = "00000000000000000000000000000000";
    let branches = [
        json!({"new_edits": false, "docs": [
            {"_id": "x", "_rev": "2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
             "_revisions": {"start": 2, "ids": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", root]}, "v": "b"}
        ]}),
        json!({"new_edits": false, "docs": [
            {"_id": "x", "_rev": "2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
             "_revisions": {"start": 2, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", root]}, "v": "a"}
        ]}),
        json!({"new_edits": false, "docs": [
            {"_id": "x", "_rev": "3-cccccccccccccccccccccccccccccccc", "_deleted": true,
             "_revisions": {"start": 3, "ids": ["cccccccccccccccccccccccccccccccc", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", root]}}
        ]}),
    ];
    for (node, branch) in nodes.iter().zip(branches) {
        let (status, body) = node
            .req_with(Method::POST, "/qsplit/_bulk_docs", Some(branch), &internal)
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    // The forwarded writes above are local-only, so the replicas really do
    // disagree. If they were converged, the coordinator test below would prove
    // nothing.
    let mut local_winners = std::collections::BTreeSet::new();
    for node in &nodes {
        let (status, body) = node.get("/qsplit/x?r=1").await;
        if status == StatusCode::OK {
            local_winners.insert(body["_rev"].as_str().unwrap().to_string());
        } else {
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        }
    }
    assert!(
        local_winners.contains("2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        "{local_winners:?}"
    );
    assert!(
        local_winners.contains("2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        "{local_winners:?}"
    );

    for node in &nodes {
        let (status, body) = node
            .get("/qsplit/x?r=3&conflicts=true&deleted_conflicts=true")
            .await;
        assert_eq!(status, StatusCode::OK, "{}: {body}", node.base);
        assert_eq!(body["v"], "a", "{} chose {body}", node.base);
        assert_eq!(body["_rev"], "2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(body["_conflicts"].as_array().unwrap().len(), 0, "{}", body);
        assert!(
            body["_deleted_conflicts"]
                .as_array()
                .unwrap()
                .contains(&json!("3-cccccccccccccccccccccccccccccccc")),
            "{}",
            body
        );
    }
}

#[tokio::test]
async fn quorum_peer_urls_encode_reserved_document_ids() {
    use md5::{Digest, Md5};

    let nodes = start_cluster_with(3, "3600", 2);
    nodes[0].fresh_db("qencode").await;
    // Decoys: an unencoded path would read one of these instead of the
    // reserved-character document.
    let (status, body) = nodes[0]
        .post(
            "/qencode/_bulk_docs",
            json!({"docs": [
                {"_id": "special", "value": "decoy-hash"},
                {"_id": "question", "value": "decoy-query"},
                {"_id": "slash", "value": "decoy-slash"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, map) = nodes[0].get("/_node/_local/_dbs/qencode").await;
    assert_eq!(status, StatusCode::OK, "{map}");
    let q = map["q"].as_u64().unwrap() as u32;
    let replicas = map["replicas"].as_array().unwrap();

    // A document id whose URL meaning changes unless the path segment is
    // escaped: '#' truncates the URL fragment, '?' starts a query, and '/' is a
    // path separator.
    for id in ["special#id", "question?id", "slash/id"] {
        let shard =
            (u32::from_be_bytes(Md5::digest(id.as_bytes())[..4].try_into().unwrap()) % q) as usize;
        let owners: Vec<usize> = replicas[shard]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| node.as_u64().unwrap() as usize)
            .collect();
        let coordinator = (0..nodes.len())
            .find(|node| !owners.contains(node))
            .expect("n=2 of three nodes leaves a non-owner");
        let (status, body) = nodes[0]
            .post(
                "/qencode/_bulk_docs",
                json!({"docs": [{"_id": id, "value": id}]}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let encoded = match id {
            "special#id" => "special%23id",
            "question?id" => "question%3Fid",
            "slash/id" => "slash%2Fid",
            _ => unreachable!(),
        };
        let (status, body) = nodes[coordinator]
            .get(&format!("/qencode/{encoded}?r=2"))
            .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "id {id} via non-owner {coordinator}: {body}"
        );
        assert_eq!(body["value"], id, "id {id}: {body}");
    }
}

#[tokio::test]
async fn quorum_read_preserves_conditional_and_explicit_revision_semantics() {
    let nodes = start_cluster_with(2, "3600", 2);
    nodes[0].fresh_db("qhttp").await;
    let (status, body) = nodes[0]
        .post("/qhttp/_bulk_docs", json!({"docs": [{"_id": "x", "v": 1}]}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (_, doc) = nodes[0].get("/qhttp/x?r=2").await;
    let rev = doc["_rev"].as_str().unwrap().to_string();
    assert!(
        doc.get("_revisions").is_none(),
        "unsolicited _revisions: {doc}"
    );

    let (status, body) = nodes[0].get("/qhttp/x?r=2&revs=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body.get("_revisions").is_some(),
        "requested revs missing: {body}"
    );
    let (status, body) = nodes[0]
        .req_with(Method::HEAD, "/qhttp/x?r=2", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = nodes[0].get("/qhttp/x?r=2&open_revs=all").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.is_array(), "open_revs shape: {body}");

    let (status, headers, body) = nodes[0]
        .req_headers(
            Method::GET,
            "/qhttp/x?r=2",
            None,
            &[("If-None-Match", format!("\"{rev}\""))],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED, "{body}");
    assert_eq!(headers.get("etag"), Some(&format!("\"{rev}\"")));

    let (status, deleted) = nodes[0]
        .req(Method::DELETE, &format!("/qhttp/x?rev={rev}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let tombstone_rev = deleted["rev"]
        .as_str()
        .or_else(|| deleted["_rev"].as_str())
        .expect("delete returned a tombstone rev")
        .to_string();

    let (status, body) = nodes[0]
        .get(&format!("/qhttp/x?rev={tombstone_rev}&r=2"))
        .await;
    assert_eq!(status, StatusCode::OK, "explicit tombstone: {body}");
    assert_eq!(body["_deleted"], true);
    let (status, body) = nodes[0].get("/qhttp/x?r=2").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn rebalance_keeps_reads_on_old_map_and_freezes_writes() {
    let nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("rebread").await;
    }
    let (_, planned) = nodes[0].get("/_node/_local/_dbs/rebread").await;
    let (status, _) = nodes[0]
        .put(
            "/_node/_local/_dbs/rebread",
            all_shards_on(node_to_pin_away_from(&planned)),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let docs: Vec<Value> = (0..9)
        .map(|i| json!({"_id": format!("reb{i}"), "n": i}))
        .collect();
    let (status, _) = nodes[0]
        .post("/rebread/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let rebalance = tokio::spawn({
        let client = nodes[0].client.clone();
        let url = format!(
            "{}/_node/_local/_dbs/rebread/rebalance?timeout_ms=300000",
            nodes[0].base
        );
        async move {
            client
                .post(url)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status()
        }
    });

    // Wait until the rebalance has frozen writes. Until then, writes are
    // ordinary new documents and are harmless.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut attempt = 0usize;
    loop {
        let (status, body) = nodes[0]
            .req_with(
                Method::PUT,
                &format!("/rebread/paused{attempt}"),
                Some(json!({"attempt": attempt})),
                &[],
            )
            .await;
        if status == StatusCode::SERVICE_UNAVAILABLE {
            assert_eq!(body["error"], "rebalance_in_progress", "{body}");
            break;
        }
        assert!(
            status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
            "unexpected write status before freeze: {status} {body}"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "rebalance never froze writes"
        );
        attempt += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The freeze is cluster-wide: a client writing through another node must
    // also see 503, not just the coordinator.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = nodes[1]
            .req_with(
                Method::PUT,
                "/rebread/peerprobe",
                Some(json!({"peer": true})),
                &[],
            )
            .await;
        if status == StatusCode::SERVICE_UNAVAILABLE {
            assert_eq!(body["error"], "rebalance_in_progress", "{body}");
            break;
        }
        assert!(
            status == StatusCode::CREATED || status == StatusCode::ACCEPTED,
            "unexpected peer write status before freeze: {status} {body}"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "peer writes were never frozen"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Destructive cleanup cannot run against the old map mid-migration.
    let (status, body) = nodes[1]
        .post("/_node/_local/_dbs/rebread/cleanup", json!({}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "rebalance_in_progress", "{body}");

    // Nor can a direct active-map PUT replace the placement the rebalance is
    // verifying. Only the migration's internal activation is allowed.
    let (status, body) = nodes[1]
        .put("/_node/_local/_dbs/rebread", all_shards_on(2))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "rebalance_in_progress", "{body}");

    // Only one rebalance may own the database at a time.
    let (status, body) = nodes[0]
        .post(
            "/_node/_local/_dbs/rebread/rebalance?timeout_ms=300000",
            json!({}),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "rebalance_in_progress", "{body}");

    // Reads still use the old active placement, not the incomplete target.
    // Anti-entropy is off, so the target owners (nodes 1 and 2 hold shards 1
    // and 2 in the balanced map) are still empty: a read routed by the target
    // map would miss. Read every document through node 1.
    for i in 0..9 {
        let (status, body) = nodes[1].get(&format!("/rebread/reb{i}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "read reb{i} during migration: {body}"
        );
        assert_eq!(body["n"], i);
    }

    // Let the target replicas catch up so the rebalance completes. How a
    // rebalance that runs out of time leaves the placement and the writes is
    // checked by `cluster_rebalance_timeout_does_not_clean_old_replicas`.
    assert_eq!(
        sync_until_done(&nodes, "rebread", rebalance).await,
        StatusCode::OK
    );
    for i in 0..9 {
        let (status, body) = nodes[0].get(&format!("/rebread/reb{i}")).await;
        assert_eq!(status, StatusCode::OK, "read after rebalance: {body}");
    }
    let (status, _) = nodes[0]
        .req_with(
            Method::PUT,
            "/rebread/after",
            Some(json!({"ok": true})),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, map) = nodes[0].get("/_node/_local/_dbs/rebread").await;
    assert_eq!(status, StatusCode::OK, "{map}");
    assert_eq!(map["replicas"], planned["replicas"], "{map}");
}

/// Wait until writes to `db` through `node` are refused because a rebalance
/// froze them. The probe deletes a document that does not exist, so it
/// changes nothing before the freeze either.
async fn wait_until_frozen(node: &Server, db: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = node.delete(&format!("/{db}/freeze-probe-missing")).await;
        if status == StatusCode::SERVICE_UNAVAILABLE {
            assert_eq!(body["error"], "rebalance_in_progress", "{body}");
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "writes were never frozen: {status} {body}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Anti-entropy is off in these clusters, so a rebalance waiting for its
/// target replicas to catch up only moves on when told to. Ask every node
/// for a sync pass until `task` ends, then return what it returned.
async fn sync_until_done<T>(nodes: &[Server], db: &str, task: tokio::task::JoinHandle<T>) -> T {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !task.is_finished() {
        assert!(
            std::time::Instant::now() < deadline,
            "the rebalance did not finish"
        );
        for node in nodes {
            let (status, body) = node
                .post(
                    &format!("/_node/_local/_replication/run?db={db}"),
                    json!({}),
                )
                .await;
            assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    task.await.unwrap()
}

const INTERNAL_HEADERS: [(&str, &str); 2] = [
    ("x-douchdb-forwarded", "1"),
    ("x-douchdb-secret", "test-cluster-secret"),
];

fn internal_headers() -> Vec<(&'static str, String)> {
    INTERNAL_HEADERS
        .iter()
        .map(|(name, value)| (*name, value.to_string()))
        .collect()
}

#[tokio::test]
async fn migration_put_rejects_a_conflicting_migration() {
    let nodes = start_cluster_with(3, "3600", 1);
    nodes[0].fresh_db("migconflict").await;
    let path = "/_node/_local/_dbs/migconflict/migration";
    let internal = internal_headers();

    let (status, body) = nodes[0]
        .req_with(Method::PUT, path, Some(all_shards_on(1)), &internal)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // The same migration may be announced again.
    let (status, body) = nodes[0]
        .req_with(Method::PUT, path, Some(all_shards_on(1)), &internal)
        .await;
    assert_eq!(status, StatusCode::OK, "idempotent re-PUT: {body}");
    // A different migration must not replace the one in progress.
    let (status, body) = nodes[0]
        .req_with(Method::PUT, path, Some(all_shards_on(2)), &internal)
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "rebalance_in_progress", "{body}");

    // Once the migration is cleared another one may start.
    let (status, body) = nodes[0]
        .req_with(Method::DELETE, path, None, &internal)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = nodes[0]
        .req_with(Method::PUT, path, Some(all_shards_on(2)), &internal)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn migration_delete_requires_the_migration_id() {
    let nodes = start_cluster_with(3, "3600", 1);
    nodes[0].fresh_db("migowner").await;
    let path = "/_node/_local/_dbs/migowner/migration";
    let mine = format!("{path}?id=coordinator-a");
    let rival = format!("{path}?id=coordinator-b");
    let internal = internal_headers();

    let (status, body) = nodes[0]
        .req_with(Method::PUT, &mine, Some(all_shards_on(1)), &internal)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A rival coordinator, or a clear without an id, must not unfreeze it.
    for wrong in [rival.as_str(), path] {
        let (status, body) = nodes[0]
            .req_with(Method::DELETE, wrong, None, &internal)
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{wrong}: {body}");
        // Still frozen: a different migration is still refused.
        let (status, body) = nodes[0]
            .req_with(Method::PUT, &rival, Some(all_shards_on(2)), &internal)
            .await;
        assert_eq!(status, StatusCode::CONFLICT, "{wrong}: {body}");
    }

    // The owner clears it, and clearing again is harmless.
    for _ in 0..2 {
        let (status, body) = nodes[0]
            .req_with(Method::DELETE, &mine, None, &internal)
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (status, body) = nodes[0]
        .req_with(Method::PUT, &rival, Some(all_shards_on(2)), &internal)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn migration_freeze_waits_for_detached_replica_writes() {
    let nodes = start_cluster_with(3, "3600", 3);
    for node in &nodes {
        node.fresh_db("migdrain").await;
    }
    let (status, _) = nodes[0]
        .put(
            "/_node/_local/_dbs/migdrain",
            json!({"q": 4, "node_count": 3, "replicas": vec![vec![0, 1, 2]; 4]}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    // Warm the validator cache: the first write consults every node.
    let (status, _) = nodes[0].put("/migdrain/warm", json!({"v": 0})).await;
    assert_eq!(status, StatusCode::CREATED);

    // A stopped node never answers, so its replica write stays in flight as a
    // detached task after the write has been acknowledged by a quorum.
    let pid = nodes[2].child.id().to_string();
    let signal = |name: &str| {
        let status = std::process::Command::new("kill")
            .args([name, &pid])
            .status()
            .expect("kill");
        assert!(status.success());
    };
    signal("-STOP");
    let (status, body) = nodes[0]
        .put("/migdrain/inflight?w=2", json!({"v": 1}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Freezing writes must wait until that write has landed everywhere,
    // otherwise it can arrive after the final digest check.
    let freeze = tokio::spawn({
        let client = nodes[0].client.clone();
        let url = format!("{}/_node/_local/_dbs/migdrain/migration", nodes[0].base);
        async move {
            client
                .put(url)
                .header("x-douchdb-forwarded", "1")
                .header("x-douchdb-secret", "test-cluster-secret")
                .json(&all_shards_on(1))
                .send()
                .await
                .unwrap()
                .status()
        }
    });
    // The freeze refuses client writes before it waits for in-flight ones, so
    // refused writes show it has started waiting. The stopped node cannot
    // finish its write, so the freeze cannot have finished either.
    wait_until_frozen(&nodes[0], "migdrain").await;
    let finished_early = freeze.is_finished();
    signal("-CONT");
    assert!(
        !finished_early,
        "freeze completed while a replica write was still in flight"
    );
    assert_eq!(freeze.await.unwrap(), StatusCode::OK);
}

#[tokio::test]
async fn revs_limit_pruning_preserves_short_conflict_leaves() {
    let s = Server::start(false);
    s.fresh_db("prunelimit").await;
    let (status, body) = s.put("/prunelimit/_revs_limit", json!(2)).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let root = "11111111111111111111111111111111";
    let short = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let (status, body) = s
        .post(
            "/prunelimit/_bulk_docs",
            json!({"new_edits": false, "docs": [
                {"_id": "x", "_rev": "4-dddddddddddddddddddddddddddddddd",
                 "_revisions": {"start": 4, "ids": [
                    "dddddddddddddddddddddddddddddddd",
                    "cccccccccccccccccccccccccccccccc",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    root
                 ]}, "branch": "deep"},
                {"_id": "x", "_rev": format!("2-{short}"),
                 "_revisions": {"start": 2, "ids": [short, root]}, "branch": "short"}
            ]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, body) = s.get("/prunelimit/x?conflicts=true&revs=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["_rev"], "4-dddddddddddddddddddddddddddddddd");
    assert_eq!(body["branch"], "deep");
    assert_eq!(
        body["_conflicts"],
        json!([format!("2-{short}")]),
        "short live conflict leaf was lost by pruning: {body}"
    );
    assert_eq!(
        body["_revisions"]["ids"].as_array().unwrap().len(),
        2,
        "each branch is stemmed to the limit: {body}"
    );

    // The short branch is still readable by its own revision.
    let (status, body) = s.get(&format!("/prunelimit/x?rev=2-{short}")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["branch"], "short");

    let (status, body) = s.get("/prunelimit/x?open_revs=all").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.as_array().unwrap().len(), 2, "{body}");
}

#[tokio::test]
async fn search_cleanup_serializes_with_builds_and_stale_queries() {
    let s = Server::start(false);
    seed_search_docs(&s, "searchrace").await;
    let docs: Vec<Value> = (0..120)
        .map(|i| {
            json!({
                "_id": format!("bulk{i:03}"),
                "item": format!("item{i}"),
                "place": "p",
                "state": "s",
                "price": i as f64
            })
        })
        .collect();
    let (status, body) = s
        .post("/searchrace/_bulk_docs", json!({"docs": docs}))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let url = "/searchrace/_design/inventory/_search/fruits?q=*:*&limit=0";
    for round in 0..4 {
        // Change the database sequence so every query below would see a stale
        // cached index unless it rebuilds.
        let id = format!("race{round}");
        let (status, body) = s
            .post(
                "/searchrace/_bulk_docs",
                json!({"docs": [{"_id": id, "item": id, "place": "p", "state": "s", "price": 1.0}]}),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let expected = 4 + 120 + round + 1;

        let ((s1, b1), (s2, b2), (s3, b3), (cs, cb)) = tokio::join!(
            s.get(url),
            s.get(url),
            s.get(url),
            s.post("/searchrace/_search_cleanup", json!({})),
        );
        for (status, body) in [(s1, b1), (s2, b2), (s3, b3)] {
            assert_eq!(status, StatusCode::OK, "round {round}: {body}");
            assert_eq!(body["total_rows"], expected, "round {round}: {body}");
        }
        assert_eq!(cs, StatusCode::ACCEPTED, "round {round}: {cb}");
    }

    // Cleanup must not leave the index in a state that cannot be rebuilt.
    let (status, body) = s.get(url).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total_rows"], 4 + 120 + 4);
}

#[tokio::test]
async fn quorum_promotes_a_conflict_that_is_no_replicas_winner() {
    let nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("qpromote").await;
    }
    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let root = "00000000000000000000000000000000";
    // Node 0 has two live branches; its local winner is 2-b..., while 2-a...
    // is only a conflict. Node 1 holds a tombstone descending from 2-b...
    let branches = [
        (
            0,
            json!({"new_edits": false, "docs": [
                {"_id": "x", "_rev": "2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                 "_revisions": {"start": 2, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", root]}, "v": "a"},
                {"_id": "x", "_rev": "2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                 "_revisions": {"start": 2, "ids": ["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", root]}, "v": "b"}
            ]}),
        ),
        (
            1,
            json!({"new_edits": false, "docs": [
                {"_id": "x", "_rev": "3-cccccccccccccccccccccccccccccccc", "_deleted": true,
                 "_revisions": {"start": 3, "ids": [
                    "cccccccccccccccccccccccccccccccc",
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    root
                 ]}}
            ]}),
        ),
    ];
    for (node, body) in branches {
        let (status, body) = nodes[node]
            .req_with(Method::POST, "/qpromote/_bulk_docs", Some(body), &internal)
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    // Prove the split state: node 0's local winner is 2-b and 2-a is a
    // conflict, while node 1 has the tombstone.
    let (status, local) = nodes[0].get("/qpromote/x?r=1&conflicts=true").await;
    assert_eq!(status, StatusCode::OK, "{local}");
    assert_eq!(local["_rev"], "2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    assert_eq!(
        local["_conflicts"],
        json!(["2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]),
        "{local}"
    );

    // The merged tree has maximal leaves 2-a (live) and 3-c (deleted), so the
    // live conflict must win even though it is no replica's local winner.
    for node in &nodes {
        let (status, body) = node
            .get("/qpromote/x?r=2&conflicts=true&deleted_conflicts=true")
            .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["_rev"], "2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "{body}");
        assert_eq!(body["v"], "a", "{body}");
        assert_eq!(body["_conflicts"], json!([]), "{body}");
        assert_eq!(
            body["_deleted_conflicts"],
            json!(["3-cccccccccccccccccccccccccccccccc"]),
            "{body}"
        );
    }
}

#[tokio::test]
async fn returning_node_repairs_design_documents_after_being_offline() {
    let mut nodes = start_cluster_with(3, "3600", 1);
    for node in &nodes {
        node.fresh_db("designrep").await;
    }
    let (status, _) = nodes[0]
        .put("/_node/_local/_dbs/designrep", all_shards_on(0))
        .await;
    assert_eq!(status, StatusCode::OK);

    nodes[1].kill();
    let (status, body) = nodes[0]
        .put(
            "/designrep/_design/v",
            json!({"views": {"by_v": {"map": "function(doc){ emit(doc.v, null); }"}}}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    nodes[1].restart();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, body) = nodes[1].get("/designrep/_design/v").await;
        if status == StatusCode::OK {
            assert_eq!(body["_id"], "_design/v", "{body}");
            break;
        }
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert!(
            std::time::Instant::now() < deadline,
            "design document was not repaired by anti-entropy"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn revision_shaped_reads_do_not_bypass_the_read_quorum() {
    let mut nodes = start_cluster_with(2, "3600", 2);
    for node in &nodes {
        node.fresh_db("qshape").await;
    }
    let (status, body) = nodes[0]
        .post(
            "/qshape/_bulk_docs",
            json!({"docs": [{"_id": "ordinary", "v": 1}]}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // Both replicas answer: revision-shaped responses keep their shape.
    let (status, body) = nodes[0].get("/qshape/ordinary?r=2&revs_info=true").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("_revs_info").is_some(), "{body}");
    let (status, body) = nodes[0].get("/qshape/ordinary?r=2&open_revs=all").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.is_array(), "{body}");

    // With one replica down, every r=2 request must report the unmet quorum,
    // including false-valued and revision-shaped options.
    nodes[1].kill();
    for query in [
        "r=2",
        "r=2&revs_info=false",
        "r=2&revs_info=true",
        "r=2&open_revs=all",
    ] {
        let (status, body) = nodes[0].get(&format!("/qshape/ordinary?{query}")).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "query {query}: {body}"
        );
        assert_eq!(body["error"], "quorum_not_met", "query {query}: {body}");
    }
}

#[tokio::test]
async fn quorum_merges_ancestry_across_replicas_with_different_history_lengths() {
    let nodes = start_cluster_with(3, "3600", 3);
    for node in &nodes {
        node.fresh_db("histories").await;
    }
    // Node 1 keeps only the newest revision, so its tombstone history is
    // shorter than node 2's for the identical leaf.
    let (status, body) = nodes[1].put("/histories/_revs_limit", json!(1)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, limit) = nodes[1].get("/histories/_revs_limit").await;
    assert_eq!(limit, 1);

    let internal = [
        ("x-douchdb-forwarded", "1".to_string()),
        ("x-douchdb-secret", "test-cluster-secret".to_string()),
    ];
    let root = "00000000000000000000000000000000";
    let live = "2-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let tombstone = "3-cccccccccccccccccccccccccccccccc";
    // Node 0 only knows the live ancestor. Nodes 1 and 2 know the tombstone;
    // node 1 prunes its history to the tombstone alone, node 2 keeps the path.
    let (status, body) = nodes[0]
        .req_with(
            Method::POST,
            "/histories/_bulk_docs",
            Some(json!({"new_edits": false, "docs": [
                {"_id": "x", "_rev": live,
                 "_revisions": {"start": 2, "ids": [
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    root
                 ]}, "v": "stale ancestor"}
            ]})),
            &internal,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for node in [1usize, 2] {
        let (status, body) = nodes[node]
            .req_with(
                Method::POST,
                "/histories/_bulk_docs",
                Some(json!({"new_edits": false, "docs": [
                    {"_id": "x", "_rev": tombstone, "_deleted": true,
                     "_revisions": {"start": 3, "ids": [
                        "cccccccccccccccccccccccccccccccc",
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        root
                     ]}}
                ]})),
                &internal,
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
    }

    // Local histories differ for the same leaf id.
    let (status, short) = nodes[1]
        .get(&format!("/histories/x?r=1&rev={tombstone}&revs=true"))
        .await;
    assert_eq!(status, StatusCode::OK, "{short}");
    assert_eq!(
        short["_revisions"]["ids"].as_array().unwrap().len(),
        1,
        "{short}"
    );
    let (status, full) = nodes[2]
        .get(&format!("/histories/x?r=1&rev={tombstone}&revs=true"))
        .await;
    assert_eq!(status, StatusCode::OK, "{full}");
    assert_eq!(
        full["_revisions"]["ids"].as_array().unwrap().len(),
        3,
        "{full}"
    );

    // The complete history from node 2 must eliminate the stale live ancestor
    // regardless of which coordinator answered and which history arrived first.
    for node in &nodes {
        let (status, body) = node.get("/histories/x?r=3").await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{} returned stale data: {body}",
            node.base
        );
        assert_eq!(body["reason"], "deleted", "{body}");

        // Requested history is deterministic: the longest supplied path.
        let (status, body) = node
            .get(&format!("/histories/x?r=3&rev={tombstone}&revs=true"))
            .await;
        assert_eq!(status, StatusCode::OK, "{}: {body}", node.base);
        assert_eq!(
            body["_revisions"]["ids"].as_array().unwrap().len(),
            3,
            "{}: {body}",
            node.base
        );
    }
}

#[test]
fn grafana_dashboards_are_valid_and_provisioned() {
    for name in ["douchdb-overview.json", "douchdb-fabric.json"] {
        let path = format!("docker/grafana/dashboards/{name}");
        let text = std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("missing dashboard {path}: {error}");
        });
        let dashboard: Value = serde_json::from_str(&text)
            .unwrap_or_else(|error| panic!("invalid dashboard {path}: {error}"));
        assert!(dashboard["panels"]
            .as_array()
            .is_some_and(|panels| !panels.is_empty()));
        assert!(dashboard["uid"].as_str().is_some());
    }
    let provider = std::fs::read_to_string("docker/grafana/provisioning/dashboards/douchdb.yaml")
        .expect("dashboard provider");
    assert!(provider.contains("/otel-lgtm/grafana-dashboards"));
    let compose = std::fs::read_to_string("docker-compose.yml").expect("compose file");
    assert!(compose.contains("./docker/grafana/dashboards:/otel-lgtm/grafana-dashboards:ro"));
}

#[tokio::test]
async fn changes_since_with_huge_node_index_does_not_abort() {
    let s = Server::start(false);
    s.fresh_db("hugesince").await;
    s.put("/hugesince/a", json!({"v": 1})).await;

    // base64url({"99999999999":1}) is a legacy cluster token naming a node far
    // outside the cluster. It must be treated as a malformed since (a 400, as
    // in CouchDB), not allocated as a vector.
    let (status, body) = s
        .get("/hugesince/_changes?since=eyI5OTk5OTk5OTk5OSI6MX0")
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["reason"],
        "Malformed sequence supplied in 'since' parameter."
    );

    let (status, _) = s.get("/hugesince").await;
    assert_eq!(status, StatusCode::OK);
}

// couch_httpd_db:db_req(#httpd{method='POST'}) and chttpd_db:db_req(POST) feed
// the document through the same update path as PUT: design docs need db admin
// (couch_db:check_is_admin / chttpd_db:db_doc_req), validate_doc_update runs,
// and `_local/` ids are stored as local documents.
#[tokio::test]
async fn post_db_enforces_design_admin_vdu_and_local_routing() {
    let users = UsersFile::new(USERS);
    let s = Server::start_with(&users_server_args(&users), true);
    let alice = bearer("alice-token");

    let (status, body) = s
        .req_with(Method::PUT, "/postgate", Some(json!({})), &admin_auth())
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let members = json!({
        "admins": {"names": [], "roles": []},
        "members": {"names": ["alice"], "roles": []}
    });
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/postgate/_security",
            Some(members),
            &admin_auth(),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // A member may not create a design document through POST.
    let (status, body) = s
        .req_with(
            Method::POST,
            "/postgate",
            Some(json!({"_id": "_design/evil", "views": {}})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, _) = s
        .req_with(Method::GET, "/postgate/_design/evil", None, &admin_auth())
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // validate_doc_update applies to POSTed documents.
    let (status, body) = s
        .req_with(
            Method::PUT,
            "/postgate/_design/v",
            Some(json!({
                "validate_doc_update": "function (newDoc) { \
                    if (newDoc.bad) { throw {forbidden: 'bad doc'}; } }"
            })),
            &admin_auth(),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let (status, body) = s
        .req_with(
            Method::POST,
            "/postgate",
            Some(json!({"_id": "x", "bad": true})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["reason"], "bad doc");
    let (status, _) = s
        .req_with(Method::GET, "/postgate/x", None, &admin_auth())
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body) = s
        .req_with(
            Method::POST,
            "/postgate",
            Some(json!({"_id": "y", "bad": false})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    // A `_local/` id is stored as a local document, not a shard document.
    let (status, body) = s
        .req_with(
            Method::POST,
            "/postgate",
            Some(json!({"_id": "_local/note", "v": 1})),
            &alice,
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["id"], "_local/note");
    assert_eq!(body["rev"], "0-1");
    let (status, body) = s
        .req_with(Method::GET, "/postgate/_local/note", None, &alice)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["v"], 1);
}
