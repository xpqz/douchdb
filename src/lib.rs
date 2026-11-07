//! douchdb: a CouchDB wire-compatible database on LevelDB.

pub(crate) mod admission;
pub(crate) mod api;
pub(crate) mod auth;
pub(crate) mod cli;
pub(crate) mod cluster;
pub(crate) mod cluster_http;
pub(crate) mod cluster_ops;
pub(crate) mod cluster_state;
pub(crate) mod db;
pub(crate) mod discovery;
pub(crate) mod error;
pub(crate) mod etf;
pub(crate) mod fabric;
pub(crate) mod hll;
pub(crate) mod index_cache;
pub(crate) mod js;
#[cfg(feature = "wasmtime")]
pub(crate) mod js_wasmtime;
pub(crate) mod json;
pub(crate) mod metrics;
pub(crate) mod params;
pub(crate) mod replicate;
pub(crate) mod rev;
pub(crate) mod revtree;
pub(crate) mod search;
pub(crate) mod server;
pub(crate) mod state;
pub(crate) mod store;
pub(crate) mod telemetry;
pub(crate) mod ui;
pub(crate) mod util;
pub(crate) mod validate;
pub(crate) mod view;
pub(crate) mod worker;

pub use cli::parse_args;
pub use db::shard_index;
pub use server::{run, run_worker};
