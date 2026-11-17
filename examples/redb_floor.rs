//! The floor for `_bulk_docs`: what bare redb costs to commit the bytes a
//! douchdb shard stores for the same documents, in the same batches.
//!
//! ```text
//! redb_floor --docs <path.jsonl> --batch <n> --q <1|2> --shard <shard.redb> [--sorted]
//! ```
//!
//! `--docs` is the JSON Lines file that was imported (its `_id`s give the
//! order and the batch boundaries); `--shard` is a q=1 shard file that
//! import produced, from which every document's stored metadata, body and
//! sequence entry are read. Each batch is split by shard and written to new
//! shard files, one per shard thread, with the tables, key types, sealed
//! values (value ‖ CRC32C) and per-commit settings rows of a douchdb shard,
//! in one two-phase `Durability::Immediate` transaction per batch per shard.
//! The shards of a batch commit concurrently; the next batch starts when
//! all have. Syncs and bytes written are counted around redb's file backend.
//!
//! Prints one JSON line: docs/s, and per batch the insert time (begin to
//! the end of the inserts) and the commit time, summed over the shards.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Instant;

use redb::backends::FileBackend;
use redb::{
    BackendError, Builder, Database, ReadableDatabase, ReadableTable, StorageBackend,
    TableDefinition,
};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const BY_SEQ: TableDefinition<u64, &[u8]> = TableDefinition::new("by_seq");
const BODIES: TableDefinition<(&str, &str), &[u8]> = TableDefinition::new("bodies");
const SETTINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("settings");

/// douchdb's default page cache per shard file (`--cache-mb 16`).
const CACHE_BYTES: usize = 16 << 20;

/// What a shard stores for one document.
struct Doc {
    id: String,
    meta: Vec<u8>,
    rev: String,
    body: Vec<u8>,
}

#[derive(Debug, Default)]
struct Counters {
    syncs: AtomicU64,
    bytes: AtomicU64,
}

/// redb's file backend, counting syncs and bytes written.
#[derive(Debug)]
struct Counted {
    inner: FileBackend,
    counters: Arc<Counters>,
}

impl StorageBackend for Counted {
    fn len(&self) -> Result<u64, std::io::Error> {
        self.inner.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), std::io::Error> {
        self.inner.read(offset, out)
    }

    fn set_len(&self, len: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(len)
    }

    fn sync_data(&self) -> Result<(), std::io::Error> {
        self.counters.syncs.fetch_add(1, Ordering::Relaxed);
        self.inner.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.counters
            .bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.write(offset, data)
    }

    fn close(&self) -> Result<(), std::io::Error> {
        self.inner.close()
    }

    // The locks keep redb's refusal of a second open of the same file.
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.inner.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.inner.try_lock_shared_range(start, end)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.lock_range(start, end)
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.lock_shared_range(start, end)
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.unlock_range(start, end)
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.inner.query_lock_range(start, end)
    }
}

fn seal(value: &[u8]) -> Vec<u8> {
    let mut sealed = Vec::with_capacity(value.len() + 4);
    sealed.extend_from_slice(value);
    sealed.extend_from_slice(&crc32c::crc32c(value).to_le_bytes());
    sealed
}

/// The value inside a sealed stored value (the checksum dropped).
fn unsealed(stored: &[u8]) -> Vec<u8> {
    stored[..stored.len() - 4].to_vec()
}

struct Args {
    docs: PathBuf,
    shard: PathBuf,
    batch: usize,
    q: u32,
    sorted: bool,
}

fn args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let (mut docs, mut shard, mut batch, mut q, mut sorted) = (None, None, None, None, false);
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--docs" => docs = Some(PathBuf::from(value()?)),
            "--shard" => shard = Some(PathBuf::from(value()?)),
            "--batch" => batch = Some(value()?.parse().map_err(|e| format!("--batch: {e}"))?),
            "--q" => q = Some(value()?.parse().map_err(|e| format!("--q: {e}"))?),
            "--sorted" => sorted = true,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let usage = "usage: redb_floor --docs <path.jsonl> --batch <n> --q <1|2> --shard <shard.redb> [--sorted]";
    let args = Args {
        docs: docs.ok_or(usage)?,
        shard: shard.ok_or(usage)?,
        batch: batch.ok_or(usage)?,
        q: q.ok_or(usage)?,
        sorted,
    };
    if args.batch == 0 || !(1..=2).contains(&args.q) {
        return Err(usage.to_string());
    }
    Ok(args)
}

/// The document ids of `path`, in file order.
fn ids(path: &Path) -> Vec<String> {
    #[derive(serde::Deserialize)]
    struct Id {
        _id: String,
    }
    let file = std::fs::File::open(path).expect("open --docs");
    std::io::BufReader::new(file)
        .lines()
        .map(|line| {
            serde_json::from_str::<Id>(&line.expect("read --docs"))
                .expect("a document with an _id")
                ._id
        })
        .collect()
}

/// Every document's stored metadata and body in the shard file `path`.
fn stored(path: &Path) -> HashMap<String, Doc> {
    let db = Database::open(path).expect("open --shard");
    let txn = db.begin_read().expect("read --shard");
    let meta = txn.open_table(META).expect("meta table");
    let bodies = txn.open_table(BODIES).expect("bodies table");
    let mut docs = HashMap::new();
    for entry in bodies.iter().expect("bodies") {
        let (key, body) = entry.expect("body");
        let (id, rev) = key.value();
        let meta = meta.get(id).expect("meta").expect("metadata of a body");
        docs.insert(
            id.to_string(),
            Doc {
                id: id.to_string(),
                meta: unsealed(meta.value()),
                rev: rev.to_string(),
                body: unsealed(body.value()),
            },
        );
    }
    docs
}

/// A shard file being written, and its running settings.
struct Shard {
    db: Database,
    seq: u64,
    docs: u64,
}

impl Shard {
    /// A new shard file with every table and its settings, as douchdb
    /// creates one.
    fn create(path: &Path, counters: Arc<Counters>) -> Shard {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .expect("create shard file");
        let backend = Counted {
            inner: FileBackend::new(file).expect("file backend"),
            counters,
        };
        let db = Builder::new()
            .set_cache_size(CACHE_BYTES)
            .create_with_backend(backend)
            .expect("create shard");
        let mut txn = db.begin_write().expect("begin");
        txn.set_two_phase_commit(true);
        {
            drop(txn.open_table(META).expect("meta"));
            drop(txn.open_table(BY_SEQ).expect("by_seq"));
            drop(txn.open_table(BODIES).expect("bodies"));
            let mut settings = txn.open_table(SETTINGS).expect("settings");
            settings
                .insert("uuid", seal(b"0123456789abcdef0123456789abcdef").as_slice())
                .expect("uuid");
            for counter in ["seq", "doc_count", "doc_del_count"] {
                settings
                    .insert(counter, seal(&0u64.to_be_bytes()).as_slice())
                    .expect("counter");
            }
        }
        txn.commit().expect("commit");
        Shard {
            db,
            seq: 0,
            docs: 0,
        }
    }

    /// Write `batch` in one transaction: `(insert_us, commit_us)`.
    fn write(&mut self, mut batch: Vec<(u64, Arc<Doc>)>, sorted: bool) -> (u64, u64) {
        let started = Instant::now();
        if sorted {
            batch.sort_by(|a, b| a.1.id.cmp(&b.1.id));
        }
        let mut txn = self.db.begin_write().expect("begin");
        txn.set_two_phase_commit(true);
        {
            let mut meta = txn.open_table(META).expect("meta");
            let mut bodies = txn.open_table(BODIES).expect("bodies");
            let mut by_seq = txn.open_table(BY_SEQ).expect("by_seq");
            for (seq, doc) in &batch {
                bodies
                    .insert(
                        (doc.id.as_str(), doc.rev.as_str()),
                        seal(&doc.body).as_slice(),
                    )
                    .expect("body");
                by_seq
                    .insert(*seq, seal(doc.id.as_bytes()).as_slice())
                    .expect("by_seq");
                meta.insert(doc.id.as_str(), seal(&doc.meta).as_slice())
                    .expect("meta");
            }
            self.seq += batch.len() as u64;
            self.docs += batch.len() as u64;
            let mut settings = txn.open_table(SETTINGS).expect("settings");
            for (name, value) in [("seq", self.seq), ("doc_count", self.docs)] {
                settings
                    .insert(name, seal(&value.to_be_bytes()).as_slice())
                    .expect("setting");
            }
        }
        let inserted = Instant::now();
        txn.commit().expect("commit");
        (
            (inserted - started).as_micros() as u64,
            inserted.elapsed().as_micros() as u64,
        )
    }
}

fn main() {
    let args = match args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    let mut stored = stored(&args.shard);
    let docs: Vec<Arc<Doc>> = ids(&args.docs)
        .iter()
        .map(|id| Arc::new(stored.remove(id).expect("every document is in --shard")))
        .collect();

    let dir = std::env::temp_dir().join(format!("redb-floor-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let counters = Arc::new(Counters::default());
    let mut shards = Vec::new();
    for index in 0..args.q {
        let path = dir.join(format!("shard-{index:04}.redb"));
        let mut shard = Shard::create(&path, Arc::clone(&counters));
        let (jobs, work) = mpsc::channel::<Vec<(u64, Arc<Doc>)>>();
        let (done, results) = mpsc::channel();
        let sorted = args.sorted;
        let thread = std::thread::spawn(move || {
            for batch in work {
                done.send(shard.write(batch, sorted)).expect("report");
            }
        });
        shards.push((jobs, results, thread));
    }
    // Creating the files is not part of the measurement.
    let syncs_before = counters.syncs.load(Ordering::Relaxed);
    let bytes_before = counters.bytes.load(Ordering::Relaxed);

    let mut seqs = vec![0u64; args.q as usize];
    let (mut insert_us, mut commit_us, mut commits) = (0u64, 0u64, 0u64);
    let started = Instant::now();
    for batch in docs.chunks(args.batch) {
        let mut split: Vec<Vec<(u64, Arc<Doc>)>> = vec![Vec::new(); args.q as usize];
        for doc in batch {
            let shard = douchdb::shard_index(&doc.id, args.q);
            seqs[shard] += 1;
            split[shard].push((seqs[shard], Arc::clone(doc)));
        }
        let mut sent = Vec::new();
        for (index, part) in split.into_iter().enumerate() {
            if !part.is_empty() {
                shards[index].0.send(part).expect("send batch");
                sent.push(index);
            }
        }
        for index in sent {
            let (insert, commit) = shards[index].1.recv().expect("shard result");
            insert_us += insert;
            commit_us += commit;
            commits += 1;
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let syncs = counters.syncs.load(Ordering::Relaxed) - syncs_before;
    let bytes = counters.bytes.load(Ordering::Relaxed) - bytes_before;
    for (jobs, _, thread) in shards {
        drop(jobs);
        thread.join().expect("shard thread");
    }
    let _ = std::fs::remove_dir_all(&dir);

    let batches = docs.len().div_ceil(args.batch) as f64;
    println!(
        "{}",
        serde_json::json!({
            "batch": args.batch,
            "q": args.q,
            "sorted": args.sorted,
            "docs": docs.len(),
            "commits": commits,
            "docs_per_s": docs.len() as f64 / elapsed,
            "insert_us_per_batch": insert_us as f64 / batches,
            "commit_us_per_batch": commit_us as f64 / batches,
            "syncs_per_commit": syncs as f64 / commits as f64,
            "bytes_per_commit": bytes as f64 / commits as f64,
        })
    );
}
