//! A shard against an in-memory model: random document writes, deletions,
//! purges, `_local` writes, failing and panicking jobs, grouped as the actor
//! groups them, with compactions, reopens and power cuts in between. After
//! each step the shard's tables, counters and in-memory indexes must say
//! exactly what the model says.

use super::tests::{write_doc, Rng};
use super::*;
use crate::kv::powercut::{PowerCut, PowerSwitch};
use proptest::prelude::*;
use std::collections::VecDeque;

/// Document ids the operations draw from: few, so documents are updated,
/// deleted, purged and recreated many times over.
const IDS: u64 = 48;

#[derive(Clone, Debug, PartialEq)]
struct Doc {
    /// The revisions on the document's only branch, root first.
    revs: Vec<String>,
    deleted: bool,
    seq: u64,
}

#[derive(Clone, Default)]
struct Model {
    docs: BTreeMap<String, Doc>,
    bodies: BTreeMap<(String, String), Vec<u8>>,
    locals: BTreeMap<String, Vec<u8>>,
    seq: u64,
    /// Purge sequence -> (uuid, id, revs), as kept after trimming.
    purges: BTreeMap<u64, (String, String, Vec<String>)>,
    purged_revs: BTreeMap<(String, String), u64>,
    purge_seq: u64,
    purged_infos_limit: u64,
}

impl Model {
    fn new() -> Model {
        Model {
            purged_infos_limit: DEFAULT_PURGED_INFOS_LIMIT,
            ..Model::default()
        }
    }

    fn live(&self) -> BTreeMap<String, String> {
        self.docs
            .iter()
            .filter(|(_, doc)| !doc.deleted)
            .map(|(id, doc)| (id.clone(), doc.revs.last().unwrap().clone()))
            .collect()
    }

    fn write(&mut self, id: &str, rev: String, deleted: bool, body: Vec<u8>) {
        self.seq += 1;
        let doc = self.docs.entry(id.to_string()).or_insert(Doc {
            revs: Vec::new(),
            deleted,
            seq: 0,
        });
        doc.revs.push(rev.clone());
        doc.deleted = deleted;
        doc.seq = self.seq;
        self.bodies.insert((id.to_string(), rev), body);
    }

    fn purge(&mut self, id: &str, uuid: String) {
        let doc = self.docs.remove(id).unwrap();
        self.bodies.retain(|(owner, _), _| owner != id);
        let seq = self.purge_seq + 1;
        let floor = seq.saturating_sub(self.purged_infos_limit);
        let gone: Vec<u64> = self.purges.range(..=floor).map(|(seq, _)| *seq).collect();
        for old in gone {
            let (_, owner, revs) = self.purges.remove(&old).unwrap();
            for rev in revs {
                let key = (owner.clone(), rev);
                if self.purged_revs.get(&key) == Some(&old) {
                    self.purged_revs.remove(&key);
                }
            }
        }
        let leaf = doc.revs.last().unwrap().clone();
        self.purged_revs.insert((id.to_string(), leaf.clone()), seq);
        self.purges.insert(seq, (uuid, id.to_string(), vec![leaf]));
        self.purge_seq = seq;
    }

    /// Compaction keeps each document's leaf body only.
    fn compact(&mut self) {
        let docs = &self.docs;
        self.bodies
            .retain(|(id, rev), _| docs.get(id).is_some_and(|doc| doc.revs.last() == Some(rev)));
    }
}

/// One job of a group.
enum Op {
    Put(String),
    Delete(String),
    Purge(String),
    PutLocal(String),
    DeleteLocal(String),
    SetPurgeLimit(u64),
    /// Takes a sequence, then fails before writing: it fails alone.
    FailBeforeWrite,
    /// Writes, then fails: the group fails.
    FailAfterWrite,
    /// Writes, then panics: the group fails.
    Panic,
}

/// Choose a job valid against `model` (which already holds the effects of
/// the group's earlier jobs).
fn choose(rng: &mut Rng, model: &Model, failures: bool) -> Op {
    let id = format!("d{:02}", rng.below(IDS));
    let local = format!("l{}", rng.below(8));
    match rng.below(100) {
        0..=44 => Op::Put(id),
        45..=59 if model.docs.get(&id).is_some_and(|doc| !doc.deleted) => Op::Delete(id),
        60..=67 if model.docs.contains_key(&id) => Op::Purge(id),
        68..=79 => Op::PutLocal(local),
        80..=85 if model.locals.contains_key(&local) => Op::DeleteLocal(local),
        86 => Op::SetPurgeLimit(1 + rng.below(4)),
        87..=89 => Op::FailBeforeWrite,
        90 if failures => Op::FailAfterWrite,
        91 if failures => Op::Panic,
        _ => Op::Put(id),
    }
}

/// A job as the model test builds it.
type TestJob = Box<dyn FnOnce(&mut ShardTxn<'_>) -> std::result::Result<(), ApiError> + Send>;

/// The job for `op`, which applies it to `model` too, as the shard should.
/// `n` makes revisions and uuids unique.
fn job(op: Op, n: u64, model: &mut Model) -> TestJob {
    match op {
        Op::Put(id) => {
            let body = format!("{{\"n\":{n}}}").into_bytes();
            model.write(
                &id,
                format!(
                    "{}-{n:032x}",
                    model.docs.get(&id).map_or(0, |doc| doc.revs.len()) + 1
                ),
                false,
                body.clone(),
            );
            Box::new(move |txn| write_doc(txn, &id, false, &body, n).map(drop))
        }
        Op::Delete(id) => {
            let pos = model.docs[&id].revs.len() + 1;
            model.write(&id, format!("{pos}-{n:032x}"), true, b"{}".to_vec());
            Box::new(move |txn| write_doc(txn, &id, true, b"{}", n).map(drop))
        }
        Op::Purge(id) => {
            let uuid = format!("purge-{n}");
            model.purge(&id, uuid.clone());
            Box::new(move |txn| {
                // Everything is read before the first write.
                let meta = txn.get_meta(&id)?.expect("the model has the document");
                let old = meta_state(&meta);
                let mut tree = meta.tree.clone();
                let leaves = tree.leaves();
                let mut dead = Vec::new();
                for leaf in &leaves {
                    dead.extend(tree.purge_leaf(leaf));
                }
                let info = PurgeInfo {
                    uuid,
                    id: id.clone(),
                    revs: leaves,
                };
                txn.record_purge(&info, &[])?;
                for rev in dead {
                    txn.remove_body(&id, &rev)?;
                }
                txn.remove_seq(meta.seq)?;
                txn.remove_meta(&id, old)?;
                Ok(())
            })
        }
        Op::PutLocal(id) => {
            let value = format!("{n}").into_bytes();
            model.locals.insert(id.clone(), value.clone());
            Box::new(move |txn| Ok(txn.put_local(&id, &value)?))
        }
        Op::DeleteLocal(id) => {
            model.locals.remove(&id);
            Box::new(move |txn| Ok(txn.remove_local(&id)?))
        }
        Op::SetPurgeLimit(limit) => {
            model.purged_infos_limit = limit;
            Box::new(move |txn| Ok(txn.set_purged_infos_limit(limit)?))
        }
        Op::FailBeforeWrite => Box::new(|txn| {
            txn.next_seq();
            Err(ApiError::conflict())
        }),
        Op::FailAfterWrite => Box::new(move |txn| {
            write_doc(txn, "d00", false, b"{}", n)?;
            Err(ApiError::bad_request("failed after writing"))
        }),
        Op::Panic => Box::new(move |txn| {
            txn.put_local("l0", b"panic")?;
            panic!("a job panicked after writing");
        }),
    }
}

/// Whether `op` makes its group fail.
fn fails_group(op: &Op) -> bool {
    matches!(op, Op::FailAfterWrite | Op::Panic)
}

/// What the shard holds, read in one transaction, in the model's terms.
#[derive(Debug, PartialEq)]
struct Seen {
    docs: BTreeMap<String, Doc>,
    by_seq: BTreeMap<u64, String>,
    bodies: BTreeMap<(String, String), Vec<u8>>,
    locals: BTreeMap<String, Vec<u8>>,
    purges: BTreeMap<u64, (String, String, Vec<String>)>,
    purged_revs: BTreeMap<(String, String), u64>,
    purge_uuids: BTreeMap<String, u64>,
    purge_seq: u64,
    counts: (u64, u64),
    latest: BTreeMap<String, u64>,
    live: BTreeMap<String, String>,
}

fn seen(shard: &mut Shard) -> Seen {
    shard
        .read(|txn| {
            let mut docs = BTreeMap::new();
            txn.scan(
                META,
                Table::Meta,
                (Unbounded, Unbounded),
                false,
                |id, stored| {
                    let meta = parse_meta(id, stored)?;
                    let winner = meta.tree.winner().unwrap();
                    docs.insert(
                        id.to_string(),
                        Doc {
                            revs: meta.tree.path(&winner).unwrap(),
                            deleted: meta.tree.is_deleted(&winner),
                            seq: meta.seq,
                        },
                    );
                    assert_eq!(meta.tree.leaves(), [winner], "one branch");
                    Ok(true)
                },
            )?;
            let mut by_seq = BTreeMap::new();
            txn.scan(
                BY_SEQ,
                Table::BySeq,
                (Unbounded, Unbounded),
                false,
                |seq, id| {
                    by_seq.insert(seq, String::from_utf8(id.to_vec()).unwrap());
                    Ok(true)
                },
            )?;
            let mut bodies = BTreeMap::new();
            txn.scan(
                BODIES,
                Table::Bodies,
                (Unbounded, Unbounded),
                false,
                |(id, rev), body| {
                    bodies.insert((id.to_string(), rev.to_string()), body.to_vec());
                    Ok(true)
                },
            )?;
            let mut purges = BTreeMap::new();
            txn.scan(
                PURGE_INFOS,
                Table::PurgeInfos,
                (Unbounded, Unbounded),
                false,
                |seq, info| {
                    let info: PurgeInfo = serde_json::from_slice(info).unwrap();
                    purges.insert(seq, (info.uuid, info.id, info.revs));
                    Ok(true)
                },
            )?;
            let mut purged_revs = BTreeMap::new();
            txn.scan(
                PURGED_REVS,
                Table::PurgedRevs,
                (Unbounded, Unbounded),
                false,
                |(id, rev), seq| {
                    purged_revs.insert(
                        (id.to_string(), rev.to_string()),
                        decode_u64(Table::PurgedRevs, seq)?,
                    );
                    Ok(true)
                },
            )?;
            let mut purge_uuids = BTreeMap::new();
            txn.scan(
                PURGE_UUIDS,
                Table::PurgeUuids,
                (Unbounded, Unbounded),
                false,
                |uuid, seq| {
                    purge_uuids.insert(uuid.to_string(), decode_u64(Table::PurgeUuids, seq)?);
                    Ok(true)
                },
            )?;
            Ok(Seen {
                docs,
                by_seq,
                bodies,
                locals: txn.locals()?.into_iter().collect(),
                purges,
                purged_revs,
                purge_uuids,
                purge_seq: txn.purge_seq()?,
                counts: txn.counts(),
                latest: txn
                    .latest_changes()?
                    .through(u64::MAX)
                    .map(|(seq, id)| (id.to_string(), seq))
                    .collect(),
                live: txn
                    .live_docs()?
                    .iter()
                    .map(|(id, doc)| (id.clone(), doc.rev.clone()))
                    .collect(),
            })
        })
        .unwrap()
}

fn expected(model: &Model) -> Seen {
    let live = model.live();
    Seen {
        docs: model.docs.clone(),
        by_seq: model
            .docs
            .iter()
            .map(|(id, doc)| (doc.seq, id.clone()))
            .collect(),
        bodies: model.bodies.clone(),
        locals: model.locals.clone(),
        purges: model.purges.clone(),
        purged_revs: model.purged_revs.clone(),
        purge_uuids: model
            .purges
            .iter()
            .map(|(seq, (uuid, _, _))| (uuid.clone(), *seq))
            .collect(),
        purge_seq: model.purge_seq,
        counts: (live.len() as u64, (model.docs.len() - live.len()) as u64),
        latest: model
            .docs
            .iter()
            .map(|(id, doc)| (id.clone(), doc.seq))
            .collect(),
        live,
    }
}

fn reopen(switch: &PowerSwitch) -> Shard {
    Shard::over_backend(switch, 1000).unwrap()
}

/// Run `operations` jobs from `seed` against a fresh shard and the model.
fn run(seed: u64, operations: u64) {
    let (_, switch) = PowerCut::new();
    let mut shard = Shard::over_backend(&switch, 1000).unwrap();
    let mut model = Model::new();
    let mut rng = Rng(seed | 1);
    let mut n = 0;
    while n < operations {
        match rng.below(100) {
            0 => {
                drop(shard);
                shard = reopen(&switch);
                assert_eq!(seen(&mut shard), expected(&model), "after reopen, op {n}");
            }
            1 => {
                switch.cut();
                drop(shard);
                shard = reopen(&switch);
                assert_eq!(
                    seen(&mut shard),
                    expected(&model),
                    "after a power cut, op {n}"
                );
            }
            2 | 3 => {
                shard.compact().unwrap();
                model.compact();
            }
            4 => shard.release_indexes(),
            _ => {}
        }

        // A group: jobs chosen against the model as the group leaves it.
        let size = 1 + rng.below(6);
        let failures = rng.below(4) == 0;
        let mut pending = model.clone();
        let mut queue: VecDeque<Box<dyn ShardJob>> = VecDeque::new();
        let mut receivers = Vec::new();
        let mut fails = false;
        for _ in 0..size {
            n += 1;
            let op = choose(&mut rng, &pending, failures);
            fails |= fails_group(&op);
            let (job, answer) = call_job(job(op, n, &mut pending));
            queue.push_back(job);
            receivers.push(answer);
        }
        let first = queue.pop_front().unwrap();
        let (replies, error) = shard.run_group(first, || queue.pop_front());
        for reply in replies {
            reply(error.as_ref());
        }
        // A failing job ends its group: the jobs after it never run.
        drop(queue);
        let mut acknowledged = 0;
        for mut answer in receivers {
            if let Ok(Ok(())) = answer.try_recv() {
                acknowledged += 1;
            }
        }
        if fails {
            // Every job of a failed group fails with it, and none of its
            // writes stay.
            assert!(
                error.is_some(),
                "op {n}: a failing job did not fail its group"
            );
            assert_eq!(
                acknowledged, 0,
                "op {n}: a job of a failed group was acknowledged"
            );
        } else {
            assert!(error.is_none(), "op {n}: {error:?}");
            model = pending;
        }
        assert_eq!(shard.seq(), model.seq, "op {n}");
        if rng.below(8) == 0 || fails {
            assert_eq!(seen(&mut shard), expected(&model), "op {n}");
        }
    }
    assert_eq!(seen(&mut shard), expected(&model), "at the end");
    switch.cut();
    drop(shard);
    let mut shard = reopen(&switch);
    assert_eq!(
        seen(&mut shard),
        expected(&model),
        "after the final power cut"
    );
}

#[test]
fn shard_matches_a_model_under_random_operations() {
    run(0x5eed_d0c5_db00_0001, 20_000);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 16, ..ProptestConfig::default() })]

    #[test]
    fn shard_matches_a_model_from_any_seed(seed in any::<u64>()) {
        run(seed, 400);
    }
}
