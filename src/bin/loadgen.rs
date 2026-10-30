//! Small native HTTP load generator for cluster benchmarks.
//!
//! It uses one persistent `reqwest` client, many concurrent workers, and
//! round-robins requests across the supplied base URLs. This keeps the client
//! out of the Python/threading path when measuring server scaling.
//!
//! cargo run --release --bin loadgen -- \
//!   --base http://127.0.0.1:5984,http://127.0.0.1:5985 \
//!   --path '/perf/doc-{id}' --start 0 --count 10000 --concurrency 64

use std::time::{Duration, Instant};

use futures::future::join_all;
use serde_json::json;

struct Args {
    bases: Vec<String>,
    path: String,
    start: usize,
    count: usize,
    concurrency: usize,
    pad: usize,
    timeout: Duration,
}

fn parse_args() -> Args {
    let mut args = Args {
        bases: vec!["http://127.0.0.1:5984".to_string()],
        path: "/perf/doc-{id}".to_string(),
        start: 0,
        count: 1000,
        concurrency: 32,
        pad: 6,
        timeout: Duration::from_secs(120),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--base" => {
                args.bases = it
                    .next()
                    .unwrap_or_default()
                    .split(',')
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .collect();
            }
            "--path" => args.path = it.next().unwrap_or(args.path),
            "--start" => args.start = it.next().and_then(|v| v.parse().ok()).unwrap_or(0),
            "--count" => args.count = it.next().and_then(|v| v.parse().ok()).unwrap_or(1000),
            "--concurrency" => {
                args.concurrency = it.next().and_then(|v| v.parse().ok()).unwrap_or(32)
            }
            "--pad" => args.pad = it.next().and_then(|v| v.parse().ok()).unwrap_or(6),
            "--timeout" => {
                args.timeout =
                    Duration::from_secs(it.next().and_then(|v| v.parse().ok()).unwrap_or(120))
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    assert!(!args.bases.is_empty(), "at least one --base is required");
    assert!(args.count > 0, "--count must be positive");
    assert!(args.concurrency > 0, "--concurrency must be positive");
    args
}

fn percentile(values: &[f64], pct: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() as f64 * pct / 100.0) as usize).saturating_sub(1);
    values[index.min(values.len() - 1)]
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = parse_args();
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency + 8)
        .timeout(args.timeout)
        .build()
        .expect("build HTTP client");

    let workers = args.concurrency.min(args.count);
    let base_count = args.bases.len();
    let mut futures = Vec::with_capacity(workers);
    let mut cursor = args.start;
    for worker in 0..workers {
        let size = args.count / workers + usize::from(worker < args.count % workers);
        let lo = cursor;
        let hi = cursor + size;
        cursor = hi;
        let client = client.clone();
        let bases = args.bases.clone();
        let path = args.path.clone();
        let pad = args.pad;
        futures.push(async move {
            let mut latencies = Vec::with_capacity(hi - lo);
            for id in lo..hi {
                let base = &bases[id % base_count];
                let rendered = path.replace("{id}", &format!("{id:0pad$}", pad = pad));
                let url = format!("{base}{rendered}");
                let started = Instant::now();
                let response = client
                    .get(&url)
                    .send()
                    .await
                    .map_err(|error| format!("{url}: {error}"))?;
                let response = response
                    .error_for_status()
                    .map_err(|error| format!("{url}: {error}"))?;
                let _ = response
                    .bytes()
                    .await
                    .map_err(|error| format!("{url}: {error}"))?;
                latencies.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            Ok::<_, String>(latencies)
        });
    }

    let started = Instant::now();
    let results = join_all(futures).await;
    let wall = started.elapsed().as_secs_f64();
    let mut latencies = Vec::with_capacity(args.count);
    for result in results {
        match result {
            Ok(mut values) => latencies.append(&mut values),
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(1);
            }
        }
    }
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    println!(
        "{}",
        json!({
            "count": latencies.len(),
            "wall_s": (wall * 1000.0).round() / 1000.0,
            "per_s": (latencies.len() as f64 / wall).round(),
            "latency_ms": {
                "p50": (percentile(&latencies, 50.0) * 1000.0).round() / 1000.0,
                "p95": (percentile(&latencies, 95.0) * 1000.0).round() / 1000.0,
                "p99": (percentile(&latencies, 99.0) * 1000.0).round() / 1000.0,
                "max": (latencies.last().copied().unwrap_or(0.0) * 1000.0).round() / 1000.0,
            },
            "bases": base_count,
            "path": args.path,
        })
    );
}
