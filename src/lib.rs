//! douchdb: a CouchDB wire-compatible database on LevelDB.

mod admission;
mod api;
mod auth;
mod cli;
mod cluster;
mod cluster_state;
mod db;
mod discovery;
mod error;
mod etf;
mod fabric;
mod js;
#[cfg(feature = "wasmtime")]
mod js_wasmtime;
mod json;
mod metrics;
mod observability;
mod replicate;
mod rev;
mod revtree;
mod search;
mod server;
mod store;
mod telemetry;
mod ui;
mod validate;
mod view;

pub use cli::{parse_args, Args};
pub use server::run;
