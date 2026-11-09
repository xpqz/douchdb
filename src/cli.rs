//! Command-line arguments for the `douchdb` server.

use std::path::PathBuf;

pub struct Args {
    pub listen: String,
    pub data: PathBuf,
    pub q: u32,
    pub n: usize,
    pub revs_limit: u64,
    pub admin: Option<(String, String)>,
    pub users_file: Option<String>,
    pub session_secret: String,
    /// `--default-security`: `Some(true)` for admin-only, `Some(false)` for
    /// open, `None` to follow whether authentication is configured.
    pub default_admin_only: Option<bool>,
    pub max_in_flight: usize,
    pub per_user_in_flight: usize,
    pub per_user_rate: usize,
    pub heavy_in_flight: usize,
    pub heavy_per_user: usize,
    pub background_in_flight: usize,
    pub compact_concurrency: usize,
    pub nodes: Vec<String>,
    pub me: usize,
    pub advertise: String,
    pub seed_nodes: Vec<String>,
    pub sync_secs: u64,
    pub js_sandbox: crate::js::Sandbox,
    pub wasm_runtime: crate::js::WasmRuntime,
    pub js_workers: usize,
    pub worker: bool,
    pub cors_origins: Vec<String>,
    pub cors_credentials: bool,
    pub cors_headers: Vec<String>,
    pub cors_methods: Vec<String>,
    pub cors_exposed_headers: Vec<String>,
    pub cors_max_age: u64,
    pub view_timeout_ms: u64,
    pub view_memory_mb: usize,
    pub max_request_mb: usize,
    pub rebalance_write_drain_ms: u64,
    pub rebalance_stable_wait_ms: u64,
    pub rebalance_freeze_ms: u64,
    pub js_worker_startup_ms: u64,
    pub max_open_indexes: usize,
    pub max_open_dbs: usize,
    pub peer_http2: bool,
    pub cluster_secret: String,
    pub otel_endpoint: Option<String>,
}

const HELP: &str = "douchdb [--listen ADDR] [--data DIR] [--q N] [--n N] [--revs-limit N]\n\
                     \x20       [--admin user:pass]         (or DOUCHDB_ADMIN)\n\
                     \x20       [--users-file FILE] [--session-secret SECRET]\n\
                     \x20       [--default-security open|admin-only]\n\
                     \x20       [--max-in-flight N] [--per-user-in-flight N] [--per-user-rate N]\n\
                     \x20       [--heavy-in-flight N] [--heavy-per-user N] [--background-in-flight N]\n\
                     \x20       [--compact-concurrency N]\n\
                     \x20       [--nodes URL,URL] [--me INDEX] [--sync-secs N]\n\
                     \x20       [--advertise URL] [--seed-nodes URL,URL]\n\
                     \x20       [--js-sandbox native|process|wasm] [--wasm-runtime wasmi|wasmtime]\n\
                     \x20       [--js-workers N]\n\
                     \x20       [--cors-origins ORIGIN,ORIGIN] [--cors-credentials]\n\
                     \x20       [--cors-headers HEADER,HEADER] [--cors-methods METHOD,METHOD]\n\
                     \x20       [--cors-exposed-headers HEADER,HEADER] [--cors-max-age SECS]\n\
                     \x20       [--view-timeout-ms N]\n\
                     \x20       [--view-memory-mb N] [--max-request-mb N]\n\
                     \x20       [--max-open-indexes N] [--max-open-dbs N]\n\
                     \x20       [--rebalance-write-drain-ms N] [--rebalance-stable-wait-ms N]\n\
                     \x20       [--rebalance-freeze-ms N] [--js-worker-startup-ms N]\n\
                     \x20       [--cluster-secret SECRET]   (or DOUCHDB_CLUSTER_SECRET)\n\
                     \x20       [--peer-http2]\n\
                     \x20       [--otel-endpoint URL]       (or OTEL_EXPORTER_OTLP_ENDPOINT)";

/// Parse the process arguments and environment. A bad command line prints a
/// message naming the offending flag and exits with status 2, so a typo can
/// never silently turn into a different configuration.
pub fn parse_args() -> Args {
    match parse_from(std::env::args().skip(1), |name| std::env::var_os(name)) {
        Ok(Some(args)) => args,
        Ok(None) => {
            println!("{HELP}");
            std::process::exit(0);
        }
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    }
}

const KNOWN_FLAGS: &[&str] = &[
    "--listen",
    "--data",
    "--q",
    "--n",
    "--revs-limit",
    "--admin",
    "--users-file",
    "--session-secret",
    "--default-security",
    "--max-in-flight",
    "--per-user-in-flight",
    "--per-user-rate",
    "--heavy-in-flight",
    "--heavy-per-user",
    "--background-in-flight",
    "--compact-concurrency",
    "--nodes",
    "--me",
    "--advertise",
    "--seed-nodes",
    "--sync-secs",
    "--js-sandbox",
    "--wasm-runtime",
    "--js-worker",
    "--js-workers",
    "--view-timeout-ms",
    "--view-memory-mb",
    "--max-request-mb",
    "--rebalance-write-drain-ms",
    "--rebalance-stable-wait-ms",
    "--rebalance-freeze-ms",
    "--js-worker-startup-ms",
    "--max-open-indexes",
    "--max-open-dbs",
    "--peer-http2",
    "--cluster-secret",
    "--otel-endpoint",
    "--cors-origins",
    "--cors-credentials",
    "--cors-headers",
    "--cors-methods",
    "--cors-exposed-headers",
    "--cors-max-age",
    "--help",
    "-h",
];

/// The command line, with the `VALUE` of a `--flag=VALUE` argument held aside
/// until the flag's handler asks for it.
struct ArgIter {
    rest: std::vec::IntoIter<String>,
    inline: Option<String>,
}

/// Whether `arg` is exactly a known flag, or the `--flag=...` form of one.
fn is_known_flag(arg: &str) -> bool {
    let name = if arg.starts_with("--") {
        arg.split_once('=').map_or(arg, |(name, _)| name)
    } else {
        arg
    };
    KNOWN_FLAGS.contains(&name)
}

/// The value of `flag`: the `=VALUE` part, or else the next argument. A next
/// argument that is itself a known flag is refused rather than swallowed (a
/// forgotten value would otherwise silently eat the following flag); the
/// `=` form passes such a string through.
fn value(it: &mut ArgIter, flag: &str) -> Result<String, String> {
    if let Some(inline) = it.inline.take() {
        return Ok(inline);
    }
    let next = it
        .rest
        .next()
        .ok_or_else(|| format!("{flag} requires a value"))?;
    if is_known_flag(&next) {
        return Err(format!(
            "{flag} needs a value; got the flag {next}. Use {flag}=VALUE if the value really is that string"
        ));
    }
    Ok(next)
}

/// Parse an unsigned integer flag value that must lie in `min..=max`.
fn number<T>(it: &mut ArgIter, flag: &str, min: u64, max: u64) -> Result<T, String>
where
    T: TryFrom<u64>,
{
    let raw = value(it, flag)?;
    let parsed: u64 = raw
        .trim()
        .parse()
        .map_err(|_| format!("{flag} expects a non-negative integer, got {raw:?}"))?;
    if parsed < min || parsed > max {
        let range = if max == u64::MAX {
            format!("at least {min}")
        } else {
            format!("between {min} and {max}")
        };
        return Err(format!("{flag} must be {range}, got {raw:?}"));
    }
    T::try_from(parsed).map_err(|_| format!("{flag} value {raw:?} is out of range"))
}

fn environment(
    env: &impl Fn(&str) -> Option<std::ffi::OsString>,
    name: &str,
) -> Result<Option<String>, String> {
    match env(name) {
        None => Ok(None),
        Some(raw) => raw
            .into_string()
            .map(Some)
            .map_err(|_| format!("{name} is not valid UTF-8")),
    }
}

/// Parse `user:password`. A malformed value must stop startup: ignoring it
/// would leave the server running with authentication disabled.
fn admin(value: &str, source: &str) -> Result<(String, String), String> {
    match value.split_once(':') {
        Some((user, pass)) if !user.is_empty() && !pass.is_empty() => {
            Ok((user.to_string(), pass.to_string()))
        }
        _ => Err(format!(
            "{source} expects user:password with a non-empty user and password"
        )),
    }
}

fn list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// Parse `argv` (without the program name) with `env` supplying environment
/// variables. `Ok(None)` means `--help` was requested.
pub fn parse_from(
    argv: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<Option<Args>, String> {
    let mut args = Args {
        listen: "127.0.0.1:5984".into(),
        data: PathBuf::from("data"),
        q: 2,
        n: 3,
        revs_limit: 1000,
        admin: environment(&env, "DOUCHDB_ADMIN")?
            .map(|value| admin(&value, "DOUCHDB_ADMIN"))
            .transpose()?,
        users_file: None,
        session_secret: environment(&env, "DOUCHDB_SESSION_SECRET")?.unwrap_or_default(),
        default_admin_only: None,
        max_in_flight: 64,
        per_user_in_flight: 16,
        per_user_rate: 0,
        heavy_in_flight: 8,
        heavy_per_user: 1,
        background_in_flight: 2,
        compact_concurrency: 1,
        nodes: Vec::new(),
        me: 0,
        advertise: String::new(),
        seed_nodes: Vec::new(),
        sync_secs: 5,
        js_sandbox: crate::js::Sandbox::Native,
        wasm_runtime: crate::js::WasmRuntime::Wasmi,
        js_workers: 2,
        worker: false,
        cors_origins: Vec::new(),
        cors_credentials: false,
        cors_headers: Vec::new(),
        cors_methods: Vec::new(),
        cors_exposed_headers: Vec::new(),
        cors_max_age: 600,
        view_timeout_ms: 5000,
        view_memory_mb: 64,
        max_request_mb: 64,
        rebalance_write_drain_ms: crate::cluster_ops::DEFAULT_WRITE_DRAIN_MS,
        rebalance_stable_wait_ms: crate::cluster_ops::DEFAULT_STABLE_WAIT_MS,
        rebalance_freeze_ms: crate::db::DEFAULT_REBALANCE_FREEZE_MS,
        js_worker_startup_ms: crate::js::DEFAULT_WORKER_STARTUP_MS,
        max_open_indexes: crate::index_cache::DEFAULT_MAX_OPEN,
        max_open_dbs: crate::state::DEFAULT_MAX_OPEN_DBS,
        peer_http2: false,
        cluster_secret: environment(&env, "DOUCHDB_CLUSTER_SECRET")?.unwrap_or_default(),
        otel_endpoint: environment(&env, "OTEL_EXPORTER_OTLP_ENDPOINT")?,
    };
    let argv: Vec<String> = argv.into_iter().collect();
    let mut it = ArgIter {
        rest: argv.into_iter(),
        inline: None,
    };
    let mut me_given = false;
    while let Some(arg) = it.rest.next() {
        let mut flag = arg.as_str();
        if let Some((name, inline)) = arg.split_once('=').filter(|_| arg.starts_with("--")) {
            flag = name;
            it.inline = Some(inline.to_string());
        }
        match flag {
            "--listen" => {
                args.listen = value(&mut it, flag)?;
                if args.listen.trim().is_empty() {
                    return Err("--listen requires a non-empty address".to_string());
                }
            }
            "--data" => {
                let dir = value(&mut it, flag)?;
                if dir.is_empty() {
                    return Err("--data requires a non-empty directory".to_string());
                }
                args.data = PathBuf::from(dir);
            }
            "--q" => args.q = number(&mut it, flag, 1, u32::MAX.into())?,
            "--n" => args.n = number(&mut it, flag, 1, u32::MAX.into())?,
            "--revs-limit" => args.revs_limit = number(&mut it, flag, 1, u64::MAX)?,
            "--admin" => args.admin = Some(admin(&value(&mut it, flag)?, "--admin")?),
            "--users-file" => args.users_file = Some(value(&mut it, flag)?),
            "--session-secret" => args.session_secret = value(&mut it, flag)?,
            "--default-security" => {
                args.default_admin_only = Some(match value(&mut it, flag)?.as_str() {
                    "open" => false,
                    "admin-only" => true,
                    other => {
                        return Err(format!(
                            "unknown --default-security value: {other:?} (expected open or admin-only)"
                        ));
                    }
                })
            }
            "--max-in-flight" => args.max_in_flight = number(&mut it, flag, 0, u32::MAX.into())?,
            "--per-user-in-flight" => {
                args.per_user_in_flight = number(&mut it, flag, 0, u32::MAX.into())?
            }
            "--per-user-rate" => args.per_user_rate = number(&mut it, flag, 0, u32::MAX.into())?,
            "--heavy-in-flight" => {
                args.heavy_in_flight = number(&mut it, flag, 0, u32::MAX.into())?
            }
            "--heavy-per-user" => args.heavy_per_user = number(&mut it, flag, 0, u32::MAX.into())?,
            "--background-in-flight" => {
                args.background_in_flight = number(&mut it, flag, 0, u32::MAX.into())?
            }
            "--compact-concurrency" => {
                args.compact_concurrency = number(&mut it, flag, 1, u32::MAX.into())?
            }
            "--nodes" => {
                args.nodes = value(&mut it, flag)?
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            }
            "--me" => {
                args.me = number(&mut it, flag, 0, u32::MAX.into())?;
                me_given = true;
            }
            "--advertise" => args.advertise = value(&mut it, flag)?,
            "--seed-nodes" => {
                args.seed_nodes = value(&mut it, flag)?
                    .split(',')
                    .map(str::trim)
                    .filter(|node| !node.is_empty())
                    .map(|node| node.trim_end_matches('/').to_string())
                    .collect()
            }
            "--sync-secs" => args.sync_secs = number(&mut it, flag, 0, u64::MAX)?,
            "--js-sandbox" => {
                args.js_sandbox = match value(&mut it, flag)?.as_str() {
                    "native" => crate::js::Sandbox::Native,
                    "process" => crate::js::Sandbox::Process,
                    "wasm" => crate::js::Sandbox::Wasm,
                    other => {
                        return Err(format!(
                            "unknown --js-sandbox value: {other:?} (expected native, process or wasm)"
                        ));
                    }
                }
            }
            "--wasm-runtime" => {
                args.wasm_runtime = match value(&mut it, flag)?.as_str() {
                    "wasmi" => crate::js::WasmRuntime::Wasmi,
                    "wasmtime" => crate::js::WasmRuntime::Wasmtime,
                    other => {
                        return Err(format!(
                            "unknown --wasm-runtime value: {other:?} (expected wasmi or wasmtime)"
                        ));
                    }
                }
            }
            "--js-worker" => args.worker = true,
            "--js-workers" => args.js_workers = number(&mut it, flag, 1, 64)?,
            "--view-timeout-ms" => args.view_timeout_ms = number(&mut it, flag, 1, u64::MAX)?,
            "--view-memory-mb" => args.view_memory_mb = number(&mut it, flag, 1, u32::MAX.into())?,
            "--max-request-mb" => args.max_request_mb = number(&mut it, flag, 1, u32::MAX.into())?,
            "--rebalance-write-drain-ms" => {
                args.rebalance_write_drain_ms = number(&mut it, flag, 1, u64::MAX)?
            }
            "--rebalance-stable-wait-ms" => {
                args.rebalance_stable_wait_ms = number(&mut it, flag, 1, u64::MAX)?
            }
            "--rebalance-freeze-ms" => {
                args.rebalance_freeze_ms = number(&mut it, flag, 1, u64::MAX)?
            }
            "--js-worker-startup-ms" => {
                args.js_worker_startup_ms = number(&mut it, flag, 1, u64::MAX)?
            }
            "--max-open-indexes" => {
                args.max_open_indexes = number(&mut it, flag, 1, u32::MAX.into())?
            }
            "--max-open-dbs" => args.max_open_dbs = number(&mut it, flag, 1, u32::MAX.into())?,
            "--peer-http2" => args.peer_http2 = true,
            "--cluster-secret" => args.cluster_secret = value(&mut it, flag)?,
            "--otel-endpoint" => args.otel_endpoint = Some(value(&mut it, flag)?),
            "--cors-origins" => args.cors_origins = list(&value(&mut it, flag)?),
            "--cors-credentials" => args.cors_credentials = true,
            "--cors-headers" => args.cors_headers = list(&value(&mut it, flag)?),
            "--cors-methods" => args.cors_methods = list(&value(&mut it, flag)?),
            "--cors-exposed-headers" => args.cors_exposed_headers = list(&value(&mut it, flag)?),
            "--cors-max-age" => args.cors_max_age = number(&mut it, flag, 0, u32::MAX.into())?,
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument: {other}")),
        }
        if it.inline.is_some() {
            return Err(format!("{flag} does not take a value"));
        }
    }
    // An index outside the node list would silently make this node advertise
    // the wrong URL (or, in a cluster, own the wrong shards).
    if me_given && args.nodes.is_empty() {
        return Err("--me requires --nodes".to_string());
    }
    if !args.nodes.is_empty() && args.me >= args.nodes.len() {
        return Err(format!(
            "--me {} is out of range: --nodes lists {} node(s), so --me must be 0..{}",
            args.me,
            args.nodes.len(),
            args.nodes.len() - 1
        ));
    }
    Ok(Some(args))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    fn no_env(_: &str) -> Option<OsString> {
        None
    }

    fn parse(argv: &[&str]) -> Result<Option<Args>, String> {
        parse_from(argv.iter().map(|a| a.to_string()), no_env)
    }

    fn error(argv: &[&str]) -> String {
        match parse(argv) {
            Err(message) => message,
            Ok(_) => panic!("{argv:?} should be rejected"),
        }
    }

    const NUMERIC: &[&str] = &[
        "--q",
        "--n",
        "--revs-limit",
        "--max-in-flight",
        "--per-user-in-flight",
        "--per-user-rate",
        "--heavy-in-flight",
        "--heavy-per-user",
        "--background-in-flight",
        "--compact-concurrency",
        "--me",
        "--sync-secs",
        "--js-workers",
        "--view-timeout-ms",
        "--rebalance-write-drain-ms",
        "--rebalance-stable-wait-ms",
        "--rebalance-freeze-ms",
        "--js-worker-startup-ms",
        "--view-memory-mb",
        "--max-request-mb",
        "--max-open-indexes",
        "--max-open-dbs",
        "--cors-max-age",
    ];

    const NEEDS_VALUE: &[&str] = &[
        "--listen",
        "--data",
        "--admin",
        "--users-file",
        "--session-secret",
        "--default-security",
        "--nodes",
        "--advertise",
        "--seed-nodes",
        "--js-sandbox",
        "--wasm-runtime",
        "--cluster-secret",
        "--otel-endpoint",
        "--cors-origins",
        "--cors-headers",
        "--cors-methods",
        "--cors-exposed-headers",
    ];

    #[test]
    fn defaults_when_no_arguments() {
        let args = parse(&[]).unwrap().unwrap();
        assert_eq!((args.q, args.n, args.me), (2, 3, 0));
        assert_eq!(args.listen, "127.0.0.1:5984");
    }

    #[test]
    fn valid_values_are_applied() {
        let args = parse(&["--q", "4", "--n", "2", "--nodes", "a,b", "--me", "1"])
            .unwrap()
            .unwrap();
        assert_eq!((args.q, args.n, args.me), (4, 2, 1));
        assert_eq!(args.nodes, ["a", "b"]);
    }

    #[test]
    fn help_is_not_an_error() {
        assert!(parse(&["--help"]).unwrap().is_none());
    }

    #[test]
    fn every_flag_with_a_value_errors_when_the_value_is_missing() {
        for flag in NUMERIC.iter().chain(NEEDS_VALUE) {
            let message = error(&[flag]);
            assert!(message.contains(flag), "{flag}: {message}");
        }
    }

    #[test]
    fn a_value_flag_does_not_swallow_a_following_known_flag() {
        let message = error(&["--session-secret", "--data", "x"]);
        assert!(message.contains("--session-secret"), "{message}");
        assert!(message.contains("--data"), "{message}");
        assert!(message.contains("--session-secret=VALUE"), "{message}");
        // The `--flag=...` form of a known flag is just as much a flag.
        let message = error(&["--session-secret", "--data=x"]);
        assert!(message.contains("--data"), "{message}");
        // Every value flag, numeric or not, refuses a known flag as its value.
        for flag in NUMERIC.iter().chain(NEEDS_VALUE) {
            let message = error(&[flag, "--peer-http2"]);
            assert!(message.contains(flag), "{flag}: {message}");
        }
    }

    #[test]
    fn equals_form_passes_a_value_that_looks_like_a_flag() {
        let args = parse(&["--session-secret=--data"]).unwrap().unwrap();
        assert_eq!(args.session_secret, "--data");
        let args = parse(&["--data=/tmp/x", "--q=4", "--admin=a:b"])
            .unwrap()
            .unwrap();
        assert_eq!(args.data, PathBuf::from("/tmp/x"));
        assert_eq!(args.q, 4);
        assert_eq!(args.admin, Some(("a".to_string(), "b".to_string())));
        // Only the first `=` splits, so the value may contain more.
        let args = parse(&["--session-secret=a=b"]).unwrap().unwrap();
        assert_eq!(args.session_secret, "a=b");
        // A flag that takes no value rejects one.
        assert!(error(&["--peer-http2=yes"]).contains("--peer-http2"));
    }

    #[test]
    fn a_value_that_merely_starts_with_dashes_is_accepted() {
        let args = parse(&["--session-secret", "--weird-secret"])
            .unwrap()
            .unwrap();
        assert_eq!(args.session_secret, "--weird-secret");
        let args = parse(&["--session-secret", "-x"]).unwrap().unwrap();
        assert_eq!(args.session_secret, "-x");
    }

    #[test]
    fn every_value_flag_is_a_known_flag() {
        for flag in NUMERIC.iter().chain(NEEDS_VALUE) {
            assert!(KNOWN_FLAGS.contains(flag), "{flag}");
        }
    }

    #[test]
    fn numeric_flags_reject_unparsable_values() {
        for flag in NUMERIC {
            for bad in ["abc", "3x", "", "-1", "1.5"] {
                let message = error(&[flag, bad]);
                assert!(message.contains(flag), "{flag} {bad:?}: {message}");
                assert!(message.contains(&format!("{bad:?}")), "{message}");
            }
        }
    }

    #[test]
    fn numeric_flags_reject_out_of_range_values() {
        for flag in [
            "--q",
            "--n",
            "--revs-limit",
            "--compact-concurrency",
            "--js-workers",
            "--view-timeout-ms",
            "--view-memory-mb",
            "--max-request-mb",
            "--max-open-indexes",
            "--max-open-dbs",
        ] {
            assert!(error(&[flag, "0"]).contains(flag), "{flag} 0");
        }
        assert!(error(&["--js-workers", "65"]).contains("--js-workers"));
        assert!(error(&["--q", "99999999999"]).contains("--q"));
        // Zero means "unlimited" or "off" for these.
        for flag in [
            "--max-in-flight",
            "--per-user-in-flight",
            "--per-user-rate",
            "--heavy-in-flight",
            "--heavy-per-user",
            "--background-in-flight",
            "--sync-secs",
        ] {
            assert!(parse(&[flag, "0"]).is_ok(), "{flag} 0");
        }
    }

    #[test]
    fn peer_http2_is_off_unless_asked_for() {
        assert!(!parse(&[]).unwrap().unwrap().peer_http2);
        assert!(parse(&["--peer-http2"]).unwrap().unwrap().peer_http2);
    }

    #[test]
    fn wall_clock_limits_default_parse_and_reject_invalid() {
        let defaults = parse(&[]).unwrap().unwrap();
        assert_eq!(defaults.view_timeout_ms, 5000);
        assert_eq!(defaults.rebalance_write_drain_ms, 15_000);
        assert_eq!(defaults.rebalance_stable_wait_ms, 10_000);
        assert_eq!(defaults.js_worker_startup_ms, 10_000);
        assert_eq!(defaults.rebalance_freeze_ms, 420_000);
        let args = parse(&[
            "--view-timeout-ms",
            "9000",
            "--rebalance-write-drain-ms",
            "31000",
            "--rebalance-stable-wait-ms",
            "22000",
            "--js-worker-startup-ms",
            "45000",
            "--rebalance-freeze-ms",
            "900000",
        ])
        .unwrap()
        .unwrap();
        assert_eq!(args.view_timeout_ms, 9000);
        assert_eq!(args.rebalance_write_drain_ms, 31_000);
        assert_eq!(args.rebalance_stable_wait_ms, 22_000);
        assert_eq!(args.js_worker_startup_ms, 45_000);
        assert_eq!(args.rebalance_freeze_ms, 900_000);
        for flag in [
            "--view-timeout-ms",
            "--rebalance-write-drain-ms",
            "--rebalance-stable-wait-ms",
            "--js-worker-startup-ms",
            "--rebalance-freeze-ms",
        ] {
            assert!(error(&[flag, "0"]).contains(flag), "{flag} 0");
            assert!(error(&[flag, "soon"]).contains(flag), "{flag} soon");
            assert!(error(&[flag, "-5"]).contains(flag), "{flag} -5");
            assert!(error(&[flag]).contains(flag), "{flag} missing value");
        }
    }

    #[test]
    fn me_must_index_the_node_list() {
        let message = error(&["--nodes", "a,b", "--me", "2"]);
        assert!(message.contains("--me"), "{message}");
        let message = error(&["--nodes", "a", "--me", "1"]);
        assert!(message.contains("--me"), "{message}");
        // The node list may follow --me.
        assert!(parse(&["--me", "1", "--nodes", "a,b"]).is_ok());
    }

    #[test]
    fn enum_flags_reject_unknown_values_in_the_parser() {
        for (flag, bad) in [
            ("--js-sandbox", "wasn"),
            ("--js-sandbox", ""),
            ("--wasm-runtime", "wasmtim"),
            ("--wasm-runtime", ""),
            ("--default-security", "closed"),
            ("--default-security", ""),
        ] {
            let message = error(&[flag, bad]);
            assert!(message.contains(flag), "{flag} {bad:?}: {message}");
        }
        // A wasm runtime typo is rejected even when the sandbox is not wasm.
        assert!(
            error(&["--js-sandbox", "native", "--wasm-runtime", "wasmx"])
                .contains("--wasm-runtime")
        );
        for good in ["native", "process", "wasm"] {
            assert!(parse(&["--js-sandbox", good]).is_ok(), "{good}");
        }
        for good in ["wasmi", "wasmtime"] {
            assert!(parse(&["--wasm-runtime", good]).is_ok(), "{good}");
        }
        for good in ["open", "admin-only"] {
            assert!(parse(&["--default-security", good]).is_ok(), "{good}");
        }
    }

    #[test]
    fn me_without_nodes_is_rejected() {
        let message = error(&["--me", "1"]);
        assert!(
            message.contains("--me") && message.contains("--nodes"),
            "{message}"
        );
        assert!(error(&["--me", "0"]).contains("--nodes"));
    }

    #[test]
    fn empty_listen_and_data_are_rejected() {
        assert!(error(&["--listen", ""]).contains("--listen"));
        assert!(error(&["--data", ""]).contains("--data"));
    }

    #[test]
    fn admin_must_be_user_and_password() {
        for bad in ["adminsecret", "", ":", "admin:", ":secret"] {
            assert!(error(&["--admin", bad]).contains("--admin"), "{bad:?}");
        }
        assert!(error(&["--admin"]).contains("--admin"));
        let args = parse(&["--admin", "a:b:c"]).unwrap().unwrap();
        assert_eq!(args.admin, Some(("a".to_string(), "b:c".to_string())));
    }

    #[test]
    fn admin_can_come_from_the_environment() {
        let env = |name: &str| (name == "DOUCHDB_ADMIN").then(|| OsString::from("a:b"));
        let args = parse_from(Vec::new(), env).unwrap().unwrap();
        assert_eq!(args.admin, Some(("a".to_string(), "b".to_string())));
        // The flag wins over the environment.
        let argv = vec!["--admin".to_string(), "c:d".to_string()];
        let args = parse_from(argv, env).unwrap().unwrap();
        assert_eq!(args.admin, Some(("c".to_string(), "d".to_string())));
        // A malformed value stops startup instead of disabling authentication.
        let bad = |name: &str| (name == "DOUCHDB_ADMIN").then(|| OsString::from("nopassword"));
        match parse_from(Vec::new(), bad) {
            Err(message) => assert!(message.contains("DOUCHDB_ADMIN"), "{message}"),
            Ok(_) => panic!("malformed DOUCHDB_ADMIN accepted"),
        }
    }

    #[test]
    fn unknown_arguments_are_rejected() {
        assert!(error(&["--bogus"]).contains("--bogus"));
    }

    #[test]
    fn environment_variables_are_read_and_validated() {
        let env = |name: &str| match name {
            "DOUCHDB_CLUSTER_SECRET" => Some(OsString::from("cluster")),
            "DOUCHDB_SESSION_SECRET" => Some(OsString::from("session")),
            "OTEL_EXPORTER_OTLP_ENDPOINT" => Some(OsString::from("http://otel")),
            _ => None,
        };
        let args = parse_from(Vec::new(), env).unwrap().unwrap();
        assert_eq!(args.cluster_secret, "cluster");
        assert_eq!(args.session_secret, "session");
        assert_eq!(args.otel_endpoint.as_deref(), Some("http://otel"));
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_environment_values_are_rejected() {
        use std::os::unix::ffi::OsStringExt;
        for name in [
            "DOUCHDB_ADMIN",
            "DOUCHDB_CLUSTER_SECRET",
            "DOUCHDB_SESSION_SECRET",
            "OTEL_EXPORTER_OTLP_ENDPOINT",
        ] {
            let env = |asked: &str| (asked == name).then(|| OsString::from_vec(vec![0xff, 0xfe]));
            match parse_from(Vec::new(), env) {
                Err(message) => assert!(message.contains(name), "{message}"),
                Ok(_) => panic!("{name} accepted"),
            }
        }
    }
}
