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
            });
        }
        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&dir);
        None
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
            ];
            match Server::spawn_on(ports[i], &extra, false, attempt) {
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
async fn attachments_roundtrip_with_couchdb_digests() {
    let s = Server::start(false);
    s.fresh_db("att").await;
    let data = base64_encode(b"hi there");
    let (status, _) = s
        .put(
            "/att/doc",
            json!({"_attachments": {
                "a.txt": {"content_type": "text/plain", "data": data},
                "b.bin": {"content_type": "application/octet-stream", "data": base64_encode(b"bee")}
            }}),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);

    let (_, body) = s.get("/att/doc").await;
    let a = &body["_attachments"]["a.txt"];
    assert_eq!(a["content_type"], "text/plain");
    assert_eq!(a["length"], 8);
    assert_eq!(a["stub"], true);
    let digest = a["digest"].as_str().unwrap();
    assert!(digest.starts_with("md5-"));
    // CouchDB's digest is base64 of the raw md5, not hex.
    assert_eq!(digest.len(), 4 + 24);
    assert!(!digest[4..].chars().all(|c| c.is_ascii_hexdigit()));

    let (_, body) = s.get("/att/doc?attachments=true").await;
    assert_eq!(
        body["_attachments"]["a.txt"]["data"],
        base64_encode(b"hi there")
    );

    let response = s
        .client
        .get(format!("{}/att/doc/a.txt", s.base))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "hi there");

    // Replacing one attachment must not drop the other.
    let rev = body["_rev"].as_str().unwrap();
    let (_, body) = s.get(&format!("/att/doc?rev={rev}")).await;
    let rev = body["_rev"].as_str().unwrap();
    let (status, _) = s
        .put(
            &format!("/att/doc/a.txt?rev={rev}"),
            Value::String(String::new()),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (_, body) = s.get("/att/doc").await;
    assert!(body["_attachments"]["a.txt"].is_object());
    assert!(body["_attachments"]["b.bin"].is_object());
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

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
