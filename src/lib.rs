//! douchdb: a CouchDB wire-compatible database on LevelDB.

pub mod admission;
pub mod api;
pub mod auth;
pub mod cli;
pub mod cluster;
pub mod cluster_http;
pub mod cluster_ops;
pub mod cluster_state;
pub mod db;
pub mod discovery;
pub mod error;
pub mod etf;
pub mod fabric;
pub mod hll;
pub mod js;
#[cfg(feature = "wasmtime")]
pub mod js_wasmtime;
pub mod json;
pub mod metrics;
pub mod replicate;
pub mod rev;
pub mod revtree;
pub mod search;
pub mod server;
pub mod state;
pub mod store;
pub mod telemetry;
pub mod ui;
pub mod util;
pub mod validate;
pub mod view;
pub mod worker;

pub use cli::{parse_args, Args};
pub use server::{run, run_worker};
