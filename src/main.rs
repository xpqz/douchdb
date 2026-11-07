//! The `douchdb` server binary. All server code lives in the library crate.

fn main() -> anyhow::Result<()> {
    let args = douchdb::parse_args();
    // The JS worker installs seccomp, so it must start no runtime threads.
    if args.worker {
        douchdb::run_worker();
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(douchdb::run(args));
    // Dropping the runtime would wait for blocking tasks without limit; a
    // shutdown must never hang.
    runtime.shutdown_timeout(std::time::Duration::from_secs(2));
    match result {
        Err(error) => match error.downcast_ref::<douchdb::ConfigError>() {
            Some(config) => {
                eprintln!("{config}");
                std::process::exit(2);
            }
            None => Err(error),
        },
        Ok(()) => Ok(()),
    }
}
