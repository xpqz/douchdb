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
    pub default_security: String,
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
    pub js_sandbox: String,
    pub wasm_runtime: String,
    pub js_workers: usize,
    pub worker: bool,
    pub cors_origins: Vec<String>,
    pub view_timeout_ms: u64,
    pub view_memory_mb: usize,
    pub max_request_mb: usize,
    pub max_open_indexes: usize,
    pub max_open_dbs: usize,
    pub cluster_secret: String,
    pub otel_endpoint: Option<String>,
}

const HELP: &str = "douchdb [--listen ADDR] [--data DIR] [--q N] [--n N] [--revs-limit N]\n\
                     \x20       [--admin user:pass] [--users-file FILE] [--session-secret SECRET]\n\
                     \x20       [--default-security open|admin-only]\n\
                     \x20       [--max-in-flight N] [--per-user-in-flight N] [--per-user-rate N]\n\
                     \x20       [--heavy-in-flight N] [--heavy-per-user N] [--background-in-flight N]\n\
                     \x20       [--compact-concurrency N]\n\
                     \x20       [--nodes URL,URL] [--me INDEX] [--sync-secs N]\n\
                     \x20       [--advertise URL] [--seed-nodes URL,URL]\n\
                     \x20       [--js-sandbox native|process|wasm] [--wasm-runtime wasmi|wasmtime]\n\
                     \x20       [--js-workers N]\n\
                     \x20       [--cors-origins ORIGIN,ORIGIN] [--view-timeout-ms N]\n\
                     \x20       [--view-memory-mb N] [--max-request-mb N]\n\
                     \x20       [--max-open-indexes N] [--max-open-dbs N]\n\
                     \x20       [--cluster-secret SECRET]   (or DOUCHDB_CLUSTER_SECRET)\n\
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

type ArgIter = std::vec::IntoIter<String>;

fn value(it: &mut ArgIter, flag: &str) -> Result<String, String> {
    it.next().ok_or_else(|| format!("{flag} requires a value"))
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
        admin: None,
        users_file: None,
        session_secret: environment(&env, "DOUCHDB_SESSION_SECRET")?.unwrap_or_default(),
        default_security: String::new(),
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
        js_sandbox: "native".to_string(),
        wasm_runtime: "wasmi".to_string(),
        js_workers: 2,
        worker: false,
        cors_origins: Vec::new(),
        view_timeout_ms: 5000,
        view_memory_mb: 64,
        max_request_mb: 64,
        max_open_indexes: crate::index_cache::DEFAULT_MAX_OPEN,
        max_open_dbs: crate::state::DEFAULT_MAX_OPEN_DBS,
        cluster_secret: environment(&env, "DOUCHDB_CLUSTER_SECRET")?.unwrap_or_default(),
        otel_endpoint: environment(&env, "OTEL_EXPORTER_OTLP_ENDPOINT")?,
    };
    let argv: Vec<String> = argv.into_iter().collect();
    let mut it = argv.into_iter();
    while let Some(arg) = it.next() {
        let flag = arg.as_str();
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
            "--admin" => {
                // A malformed value must stop startup: ignoring it would leave
                // the server running with authentication disabled.
                let value = it.next().unwrap_or_default();
                match value.split_once(':') {
                    Some((user, pass)) if !user.is_empty() && !pass.is_empty() => {
                        args.admin = Some((user.to_string(), pass.to_string()));
                    }
                    _ => {
                        return Err(
                            "--admin expects user:password with a non-empty user and password"
                                .to_string(),
                        );
                    }
                }
            }
            "--users-file" => args.users_file = Some(value(&mut it, flag)?),
            "--session-secret" => args.session_secret = value(&mut it, flag)?,
            "--default-security" => args.default_security = value(&mut it, flag)?,
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
            "--me" => args.me = number(&mut it, flag, 0, u32::MAX.into())?,
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
            "--js-sandbox" => args.js_sandbox = value(&mut it, flag)?,
            "--wasm-runtime" => args.wasm_runtime = value(&mut it, flag)?,
            "--js-worker" => args.worker = true,
            "--js-workers" => args.js_workers = number(&mut it, flag, 1, 64)?,
            "--view-timeout-ms" => args.view_timeout_ms = number(&mut it, flag, 1, u64::MAX)?,
            "--view-memory-mb" => args.view_memory_mb = number(&mut it, flag, 1, u32::MAX.into())?,
            "--max-request-mb" => args.max_request_mb = number(&mut it, flag, 1, u32::MAX.into())?,
            "--max-open-indexes" => {
                args.max_open_indexes = number(&mut it, flag, 1, u32::MAX.into())?
            }
            "--max-open-dbs" => args.max_open_dbs = number(&mut it, flag, 1, u32::MAX.into())?,
            "--cluster-secret" => args.cluster_secret = value(&mut it, flag)?,
            "--otel-endpoint" => args.otel_endpoint = Some(value(&mut it, flag)?),
            "--cors-origins" => args.cors_origins = list(&value(&mut it, flag)?),
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    // An index outside the node list would silently make this node advertise
    // the wrong URL (or, in a cluster, own the wrong shards).
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
        "--view-memory-mb",
        "--max-request-mb",
        "--max-open-indexes",
        "--max-open-dbs",
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
    fn me_must_index_the_node_list() {
        let message = error(&["--nodes", "a,b", "--me", "2"]);
        assert!(message.contains("--me"), "{message}");
        let message = error(&["--nodes", "a", "--me", "1"]);
        assert!(message.contains("--me"), "{message}");
        // The node list may follow --me.
        assert!(parse(&["--me", "1", "--nodes", "a,b"]).is_ok());
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
