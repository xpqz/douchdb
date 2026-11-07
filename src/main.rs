//! The `douchdb` server binary. All server code lives in the library crate.

fn main() -> anyhow::Result<()> {
    let args = douchdb::parse_args();
    // The JS worker installs seccomp, so it must start no runtime threads.
    if args.worker {
        douchdb::run_worker();
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(douchdb::run(args))
}
