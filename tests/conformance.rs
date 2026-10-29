//! Per-endpoint wire conformance tests.
//!
//! Each test starts a real douchdb process on an ephemeral port and talks to it
//! over HTTP, exactly as PouchDB does. The shapes asserted here are CouchDB's,
//! not ours: status codes, `{"error","reason"}` bodies, ETags, `_all_docs`
//! offsets, `_bulk_get` envelopes and attachment digests all have clients that
//! sniff them.

use reqwest::{Client, Method, StatusCode};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

struct Server {
    child: Child,
    base: String,
    dir: PathBuf,
    client: Client,
}

impl Server {
    fn start(admin: bool) -> Server {
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let dir =
            std::env::temp_dir().join(format!("douchdb-test-{}-{}", std::process::id(), port));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_douchdb"));
        cmd.arg("--listen")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--data")
            .arg(&dir)
            .arg("--sync-secs")
            .arg("3600")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if admin {
            cmd.arg("--admin").arg("admin:douchdb");
        }
        let child = cmd.spawn().expect("spawn douchdb");
        let base = format!("http://127.0.0.1:{port}");
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        let server = Server {
            child,
            base,
            dir,
            client,
        };
        // Wait for the listener, not for a fixed amount of time.
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return server;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("douchdb did not start");
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

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
