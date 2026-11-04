//! The `douchdb` server binary. All server code lives in the library crate.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    douchdb::run(douchdb::parse_args()).await
}
