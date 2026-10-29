//! douchdb: a CouchDB wire-compatible database on LevelDB.
//!
//! Milestone 0 scaffolding: the rev-hash and key-encoding foundation plus a
//! skeleton HTTP surface. See PLAN.md.

// Milestone 0: the rev-hash, ETF and codec modules are complete and tested but
// not yet called by the HTTP layer. Remove this once milestone 1 lands.
#![allow(dead_code)]

mod etf;
mod rev;
mod store;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let addr = std::env::var("DOUCHDB_ADDR").unwrap_or_else(|_| "127.0.0.1:5984".into());
    let app = Router::new()
        .route("/", get(welcome))
        .route("/_up", get(up))
        .fallback(not_found);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("listening on {addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Clients gate features on these fields, so we answer in CouchDB's shape.
async fn welcome() -> Json<Value> {
    Json(json!({
        "couchdb": "Welcome",
        "version": "3.3.3",
        "vendor": { "name": "douchdb" },
        "features": ["access-ready", "partitioned", "pluggable-storage-engines", "reshard", "scheduler"],
    }))
}

async fn up() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "not_found", "reason": "missing" })),
    )
        .into_response()
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
