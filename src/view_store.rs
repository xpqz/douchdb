//! The key-ordered form of a view index (docs/superpowers/specs/
//! 2026-11-24-on-disk-view-index-design.md): every row once more under a
//! `ViewKey` that redb orders by CouchDB collation, and blocks of about
//! `BLOCK_ROWS` rows that count them.
//!
//! A `ViewKey` is the shard (u32, big-endian), then either nothing (the
//! shard's sentinel, below every entry of the shard) or an entry: the
//! partition (length and text; empty unless the view is queried by
//! partition), the view key in `collate`'s encoded form, and a tail. A stored
//! row's tail is `Row(doc id, ordinal)`; the other tails are bounds for range
//! scans. Keys compare by shard, sentinel before entry, partition by bytes,
//! view key by collation, then tail. A key that does not parse sorts after
//! every well-formed one, and such keys among themselves by bytes, so the
//! order stays total.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use redb::{Key, ReadableTable, Table, TableDefinition, TypeName, Value as RedbValue};
use serde_json::Value;

use crate::js::JsError;

/// Rows a block holds, about: a block splits past twice this and merges
/// below half of it. Small in tests, so splits and merges happen.
pub(crate) const BLOCK_ROWS: u64 = if cfg!(test) { 4 } else { 256 };

/// `ViewKey` → the row's value as sealed JSON.
pub(crate) const BY_KEY: TableDefinition<ViewKey, &[u8]> = TableDefinition::new("by_key");
/// A block's fence `ViewKey` → the sealed count of the rows from the fence up
/// to the next fence of the shard.
pub(crate) const BLOCKS: TableDefinition<ViewKey, &[u8]> = TableDefinition::new("blocks");
/// A block's fence → its sealed reduce partial. Absent means unknown.
pub(crate) const PARTIALS: TableDefinition<ViewKey, &[u8]> = TableDefinition::new("partials");

/// The redb key type of the key-ordered tables.
#[derive(Debug)]
pub(crate) struct ViewKey;

impl RedbValue for ViewKey {
    type SelfType<'a> = &'a [u8];
    type AsBytes<'a> = &'a [u8];

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
    where
        Self: 'a,
    {
        data
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a &'b [u8]) -> &'a [u8]
    where
        Self: 'b,
    {
        value
    }

    fn type_name() -> TypeName {
        TypeName::new(&format!(
            "douchdb::ViewKey/{}",
            crate::collate::CODEC_VERSION
        ))
    }
}

impl Key for ViewKey {
    fn compare(a: &[u8], b: &[u8]) -> Ordering {
        match (parse(a), parse(b)) {
            (Some(a), Some(b)) => cmp_parsed(&a, &b),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => a.cmp(b),
        }
    }
}

const SENTINEL: u8 = 0;
const ENTRY: u8 = 1;

const BEFORE_KEY: u8 = 0;
const BEFORE_ID: u8 = 1;
const ROW: u8 = 2;
const AFTER_ID: u8 = 3;
const AFTER_KEY: u8 = 4;

/// What follows the view key.
#[derive(Debug, PartialEq)]
enum Tail<'a> {
    BeforeKey,
    BeforeId(&'a str),
    Row(&'a str, u32),
    AfterId(&'a str),
    AfterKey,
}

/// A `ViewKey`, read.
struct Parsed<'a> {
    shard: u32,
    /// `None` for the shard's sentinel.
    entry: Option<(&'a [u8], &'a [u8], Tail<'a>)>,
}

fn parse(bytes: &[u8]) -> Option<Parsed<'_>> {
    let shard = u32::from_be_bytes(bytes.get(..4)?.try_into().ok()?);
    let (&kind, rest) = bytes.get(4..)?.split_first()?;
    match kind {
        SENTINEL if rest.is_empty() => Some(Parsed { shard, entry: None }),
        ENTRY => {
            let (partition, rest) = take_text(rest)?;
            let (key, rest) = crate::collate::split_key(rest)?;
            let (&tag, rest) = rest.split_first()?;
            let tail = match tag {
                BEFORE_KEY if rest.is_empty() => Tail::BeforeKey,
                AFTER_KEY if rest.is_empty() => Tail::AfterKey,
                BEFORE_ID | AFTER_ID => {
                    let id = whole_text(rest)?;
                    if tag == BEFORE_ID {
                        Tail::BeforeId(id)
                    } else {
                        Tail::AfterId(id)
                    }
                }
                ROW => {
                    let split = rest.len().checked_sub(4)?;
                    let ordinal = u32::from_be_bytes(rest[split..].try_into().ok()?);
                    Tail::Row(whole_text(&rest[..split])?, ordinal)
                }
                _ => return None,
            };
            Some(Parsed {
                shard,
                entry: Some((partition, key, tail)),
            })
        }
        _ => None,
    }
}

fn cmp_parsed(a: &Parsed<'_>, b: &Parsed<'_>) -> Ordering {
    a.shard
        .cmp(&b.shard)
        .then_with(|| match (&a.entry, &b.entry) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some((a_partition, a_key, a_tail)), Some((b_partition, b_key, b_tail))) => a_partition
                .cmp(b_partition)
                .then_with(|| crate::collate::cmp_split(a_key, b_key))
                .then_with(|| cmp_tails(a_tail, b_tail)),
        })
}

fn cmp_tails(a: &Tail<'_>, b: &Tail<'_>) -> Ordering {
    // Before every id, at an id, after every id.
    fn place<'a>(tail: &Tail<'a>) -> (u8, &'a str, u8, u32) {
        match *tail {
            Tail::BeforeKey => (0, "", 0, 0),
            Tail::BeforeId(id) => (1, id, 0, 0),
            Tail::Row(id, ordinal) => (1, id, 1, ordinal),
            Tail::AfterId(id) => (1, id, 2, 0),
            Tail::AfterKey => (2, "", 0, 0),
        }
    }
    let (a, b) = (place(a), place(b));
    a.0.cmp(&b.0)
        .then_with(|| a.1.as_bytes().cmp(b.1.as_bytes()))
        .then_with(|| a.2.cmp(&b.2))
        .then_with(|| a.3.cmp(&b.3))
}

fn take_text(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let len = usize::from(u16::from_be_bytes(bytes.get(..2)?.try_into().ok()?));
    Some((bytes.get(2..2 + len)?, &bytes[2 + len..]))
}

fn whole_text(bytes: &[u8]) -> Option<&str> {
    let (text, rest) = take_text(bytes)?;
    if !rest.is_empty() {
        return None;
    }
    std::str::from_utf8(text).ok()
}

fn put_text(text: &[u8], out: &mut Vec<u8>) -> Result<(), JsError> {
    let len = u16::try_from(text.len())
        .map_err(|_| JsError::Index(format!("view key part of {} bytes", text.len())))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(text);
    Ok(())
}

/// The sentinel of `shard`: the fence of its first block, below every entry.
pub(crate) fn sentinel(shard: u32) -> Vec<u8> {
    let mut out = shard.to_be_bytes().to_vec();
    out.push(SENTINEL);
    out
}

/// The `ViewKey` of a stored row.
pub(crate) fn row_key(
    shard: u32,
    partition: &str,
    key: &Value,
    id: &str,
    ordinal: u32,
) -> Result<Vec<u8>, JsError> {
    let mut out = shard.to_be_bytes().to_vec();
    out.push(ENTRY);
    put_text(partition.as_bytes(), &mut out)?;
    crate::collate::encode_into(key, &mut out);
    out.push(ROW);
    put_text(id.as_bytes(), &mut out)?;
    out.extend_from_slice(&ordinal.to_be_bytes());
    Ok(out)
}

/// A stored row's view key and doc id.
pub(crate) struct RowKey<'a> {
    pub key: Value,
    pub id: &'a str,
}

/// Read a stored row's `ViewKey`; damage is an error.
pub(crate) fn read_row_key(bytes: &[u8]) -> Result<RowKey<'_>, JsError> {
    let damaged = || JsError::Index("view index: damaged row key".to_string());
    let parsed = parse(bytes).ok_or_else(damaged)?;
    match parsed.entry {
        Some((_, key, Tail::Row(id, _))) => Ok(RowKey {
            key: crate::collate::decode(key).ok_or_else(damaged)?,
            id,
        }),
        _ => Err(damaged()),
    }
}

fn shard_of(fence: &[u8]) -> Option<u32> {
    parse(fence).map(|parsed| parsed.shard)
}

fn is_sentinel(fence: &[u8]) -> bool {
    parse(fence).is_some_and(|parsed| parsed.entry.is_none())
}

fn store_error(error: impl std::fmt::Display) -> JsError {
    JsError::Index(error.to_string())
}

fn read_count(stored: &[u8]) -> Result<u64, JsError> {
    let bytes = crate::kv::unseal(stored)
        .map_err(|corrupt| JsError::Index(format!("view index: {corrupt}")))?;
    Ok(u64::from_le_bytes(bytes.try_into().map_err(store_error)?))
}

fn put_count(
    table: &mut Table<'_, ViewKey, &[u8]>,
    fence: &[u8],
    count: u64,
) -> Result<(), JsError> {
    table
        .insert(fence, crate::kv::seal(&count.to_le_bytes()).as_slice())
        .map_err(store_error)?;
    Ok(())
}

/// The fence of the block `key` falls in: the greatest fence at or below it.
/// Every shard has its sentinel, so this stays inside the key's shard.
fn fence_of(blocks: &Table<'_, ViewKey, &[u8]>, key: &[u8]) -> Result<Vec<u8>, JsError> {
    let found = blocks
        .range::<&[u8]>(..=key)
        .map_err(store_error)?
        .next_back()
        .transpose()
        .map_err(store_error)?
        .map(|(fence, _)| fence.value().to_vec());
    match found {
        Some(fence) if shard_of(&fence) == shard_of(key) => Ok(fence),
        _ => Err(JsError::Index("view index: a row has no block".to_string())),
    }
}

/// The tables an update changes, open in its write transaction.
pub(crate) struct Tables<'t> {
    pub by_key: Table<'t, ViewKey, &'static [u8]>,
    pub blocks: Table<'t, ViewKey, &'static [u8]>,
    pub partials: Table<'t, ViewKey, &'static [u8]>,
}

impl<'t> Tables<'t> {
    pub(crate) fn open(txn: &'t redb::WriteTransaction) -> Result<Self, JsError> {
        Ok(Tables {
            by_key: txn.open_table(BY_KEY).map_err(store_error)?,
            blocks: txn.open_table(BLOCKS).map_err(store_error)?,
            partials: txn.open_table(PARTIALS).map_err(store_error)?,
        })
    }
}

/// The counting a set of row removals and insertions owes the blocks, applied
/// once all of them are known.
#[derive(Default)]
pub(crate) struct BlockEdits {
    /// Fence → change in its count.
    deltas: BTreeMap<Vec<u8>, i64>,
    /// Shard → change in its total.
    pub totals: BTreeMap<u32, i64>,
}

impl BlockEdits {
    /// Count a row removed (`-1`) or added (`+1`).
    pub(crate) fn count(
        &mut self,
        tables: &Tables<'_>,
        key: &[u8],
        change: i64,
    ) -> Result<(), JsError> {
        let fence = fence_of(&tables.blocks, key)?;
        *self.deltas.entry(fence).or_default() += change;
        if let Some(shard) = shard_of(key) {
            *self.totals.entry(shard).or_default() += change;
        }
        Ok(())
    }

    /// Apply the counts, then split and merge the blocks they touched until
    /// each holds between half and twice `BLOCK_ROWS` rows (a shard's sentinel
    /// block may hold fewer when it is the shard's only block). Every block
    /// whose rows change loses its partial.
    pub(crate) fn finish(self, tables: &mut Tables<'_>) -> Result<(), JsError> {
        let mut work = BTreeSet::new();
        for (fence, delta) in self.deltas {
            let count = match tables.blocks.get(fence.as_slice()).map_err(store_error)? {
                Some(stored) => read_count(stored.value())?,
                None => return Err(JsError::Index("view index: a block is missing".to_string())),
            };
            let count = u64::try_from(i64::try_from(count).map_err(store_error)? + delta).map_err(
                |_| JsError::Index("view index: a block count went negative".to_string()),
            )?;
            put_count(&mut tables.blocks, &fence, count)?;
            tables
                .partials
                .remove(fence.as_slice())
                .map_err(store_error)?;
            work.insert(fence);
        }
        while let Some(fence) = work.pop_first() {
            let Some(count) = tables
                .blocks
                .get(fence.as_slice())
                .map_err(store_error)?
                .map(|stored| read_count(stored.value()))
                .transpose()?
            else {
                // Merged away already.
                continue;
            };
            if count > 2 * BLOCK_ROWS {
                split(tables, &fence, count)?;
            } else if count < BLOCK_ROWS / 2 {
                if let Some(survivor) = merge(tables, &fence, count)? {
                    work.insert(survivor);
                }
            }
        }
        Ok(())
    }
}

/// Split the block at `fence` into pieces of `BLOCK_ROWS` rows, the last
/// taking the rest.
fn split(tables: &mut Tables<'_>, fence: &[u8], count: u64) -> Result<(), JsError> {
    let pieces = count / BLOCK_ROWS;
    let mut fences = Vec::new();
    for (at, entry) in tables
        .by_key
        .range::<&[u8]>(fence..)
        .map_err(store_error)?
        .enumerate()
        .take(usize::try_from(count).map_err(store_error)?)
    {
        let (key, _) = entry.map_err(store_error)?;
        let at = at as u64;
        if at > 0 && at.is_multiple_of(BLOCK_ROWS) && at / BLOCK_ROWS < pieces {
            fences.push(key.value().to_vec());
        }
    }
    put_count(&mut tables.blocks, fence, BLOCK_ROWS)?;
    for (piece, new_fence) in fences.iter().enumerate() {
        // Fence `piece` starts block `piece + 1`; the last block is
        // `pieces - 1`.
        let rows = if piece as u64 + 2 == pieces {
            count - (pieces - 1) * BLOCK_ROWS
        } else {
            BLOCK_ROWS
        };
        put_count(&mut tables.blocks, new_fence, rows)?;
        tables
            .partials
            .remove(new_fence.as_slice())
            .map_err(store_error)?;
    }
    tables.partials.remove(fence).map_err(store_error)?;
    Ok(())
}

/// Merge the underfull block at `fence` with a neighbour in its shard: into
/// its predecessor, or for the sentinel block, its successor into it. Returns
/// the surviving fence, or `None` when the block has no neighbour.
fn merge(tables: &mut Tables<'_>, fence: &[u8], count: u64) -> Result<Option<Vec<u8>>, JsError> {
    let shard = shard_of(fence);
    let neighbour = if is_sentinel(fence) {
        tables
            .blocks
            .range::<&[u8]>((Bound::Excluded(fence), Bound::Unbounded))
            .map_err(store_error)?
            .next()
    } else {
        tables
            .blocks
            .range::<&[u8]>(..fence)
            .map_err(store_error)?
            .next_back()
    }
    .transpose()
    .map_err(store_error)?
    .map(|(neighbour, stored)| {
        Ok::<_, JsError>((neighbour.value().to_vec(), read_count(stored.value())?))
    })
    .transpose()?
    .filter(|(neighbour, _)| shard_of(neighbour) == shard);
    let Some((neighbour, neighbour_count)) = neighbour else {
        return Ok(None);
    };
    let (survivor, gone) = if is_sentinel(fence) {
        (fence.to_vec(), neighbour)
    } else {
        (neighbour, fence.to_vec())
    };
    tables.blocks.remove(gone.as_slice()).map_err(store_error)?;
    tables
        .partials
        .remove(gone.as_slice())
        .map_err(store_error)?;
    put_count(&mut tables.blocks, &survivor, count + neighbour_count)?;
    tables
        .partials
        .remove(survivor.as_slice())
        .map_err(store_error)?;
    Ok(Some(survivor))
}

/// Write every shard's blocks from scratch, after a rebuild filled `by_key`:
/// each shard gets its sentinel and a fence every `BLOCK_ROWS` rows, the last
/// block taking the rest. `totals` is each shard's row count.
pub(crate) fn write_blocks(tables: &mut Tables<'_>, totals: &[u64]) -> Result<(), JsError> {
    tables.blocks.retain(|_, _| false).map_err(store_error)?;
    tables.partials.retain(|_, _| false).map_err(store_error)?;
    let mut fences: Vec<(Vec<u8>, u64)> = Vec::new();
    for (shard, &total) in totals.iter().enumerate() {
        let shard = u32::try_from(shard).map_err(store_error)?;
        let pieces = (total / BLOCK_ROWS).max(1);
        fences.push((
            sentinel(shard),
            if pieces == 1 { total } else { BLOCK_ROWS },
        ));
        if pieces == 1 {
            continue;
        }
        let start = sentinel(shard);
        for (at, entry) in tables
            .by_key
            .range::<&[u8]>(start.as_slice()..)
            .map_err(store_error)?
            .enumerate()
            .take(usize::try_from(total).map_err(store_error)?)
        {
            let (key, _) = entry.map_err(store_error)?;
            let at = at as u64;
            if at > 0 && at.is_multiple_of(BLOCK_ROWS) && at / BLOCK_ROWS < pieces {
                let rows = if at / BLOCK_ROWS + 1 == pieces {
                    total - at
                } else {
                    BLOCK_ROWS
                };
                fences.push((key.value().to_vec(), rows));
            }
        }
    }
    for (fence, rows) in fences {
        put_count(&mut tables.blocks, &fence, rows)?;
    }
    Ok(())
}

/// Check the invariants of the key-ordered tables against `rows` (the
/// by-document table's keys): each row once in `by_key`, block counts that
/// add up, block sizes in range, a sentinel per shard, and `totals`.
#[cfg(test)]
pub(crate) fn check(tables: &Tables<'_>, rows: &[Vec<u8>], totals: &[u64]) -> Result<(), String> {
    let by_key: Vec<Vec<u8>> = tables
        .by_key
        .iter()
        .map_err(|e| e.to_string())?
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(|e| e.to_string())
        })
        .collect::<Result<_, _>>()?;
    let mut expected = rows.to_vec();
    expected.sort_by(|a, b| ViewKey::compare(a, b));
    if by_key != expected {
        return Err(format!(
            "by_key has {} rows, rows {}",
            by_key.len(),
            expected.len()
        ));
    }
    let blocks: Vec<(Vec<u8>, u64)> = tables
        .blocks
        .iter()
        .map_err(|e| e.to_string())?
        .map(|entry| {
            let (fence, stored) = entry.map_err(|e| e.to_string())?;
            Ok((
                fence.value().to_vec(),
                read_count(stored.value()).map_err(|e| e.to_string())?,
            ))
        })
        .collect::<Result<_, String>>()?;
    for (shard, &total) in totals.iter().enumerate() {
        let shard = shard as u32;
        let mine: Vec<&(Vec<u8>, u64)> = blocks
            .iter()
            .filter(|(fence, _)| shard_of(fence) == Some(shard))
            .collect();
        if mine.first().map(|(fence, _)| fence.as_slice()) != Some(sentinel(shard).as_slice()) {
            return Err(format!("shard {shard} has no sentinel"));
        }
        let in_shard: Vec<&Vec<u8>> = by_key
            .iter()
            .filter(|key| shard_of(key) == Some(shard))
            .collect();
        if in_shard.len() as u64 != total {
            return Err(format!(
                "shard {shard} total {total}, rows {}",
                in_shard.len()
            ));
        }
        for (at, (fence, count)) in mine.iter().enumerate() {
            let next = mine.get(at + 1).map(|(next, _)| next);
            let actual = in_shard
                .iter()
                .filter(|key| {
                    ViewKey::compare(key, fence) != Ordering::Less
                        && next.is_none_or(|next| ViewKey::compare(key, next) == Ordering::Less)
                })
                .count() as u64;
            if actual != *count {
                return Err(format!(
                    "shard {shard} block {at} counts {count}, holds {actual}"
                ));
            }
            let low = if mine.len() == 1 { 0 } else { BLOCK_ROWS / 2 };
            if !(low..=2 * BLOCK_ROWS).contains(count) {
                return Err(format!("shard {shard} block {at} holds {count} rows"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn number(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    #[test]
    fn keys_order_by_shard_partition_key_then_tail() {
        let row = |shard, partition: &str, key: Value, id: &str, ordinal| {
            row_key(shard, partition, &key, id, ordinal).unwrap()
        };
        let ordered = [
            sentinel(0),
            row(0, "", json!(null), "z", 0),
            row(0, "", json!(1), "a", 0),
            // 1.0 collates equal to 1, so the id decides.
            row(0, "", number("1.0"), "b", 0),
            row(0, "", json!(1), "b", 1),
            row(0, "", json!("a"), "a", 0),
            row(0, "p", json!(null), "a", 0),
            sentinel(1),
            row(1, "", json!(0), "a", 0),
        ];
        for (at, a) in ordered.iter().enumerate() {
            for (bt, b) in ordered.iter().enumerate() {
                assert_eq!(ViewKey::compare(a, b), at.cmp(&bt), "{at} {bt}");
            }
        }
        let read = read_row_key(&ordered[3]).unwrap();
        assert_eq!((read.key, read.id), (number("1.0"), "b"));
    }

    /// Integers beyond 2^53 and the float 2^53 keep their exact order in a
    /// table, and each is found again.
    #[test]
    fn exact_numbers_insert_and_look_up() {
        let db = crate::kv::open_with(redb::backends::InMemoryBackend::new(), 1 << 20).unwrap();
        let txn = crate::kv::begin_write(&db).unwrap();
        let texts = ["9007199254740993", "9007199254740992.0", "9007199254740992"];
        {
            let mut table = txn.open_table(BY_KEY).unwrap();
            for (at, text) in texts.iter().enumerate() {
                let key = row_key(0, "", &number(text), &format!("d{at}"), 0).unwrap();
                table.insert(key.as_slice(), text.as_bytes()).unwrap();
            }
            let order: Vec<String> = table
                .iter()
                .unwrap()
                .map(|entry| String::from_utf8(entry.unwrap().1.value().to_vec()).unwrap())
                .collect();
            // 2^53 as a float and as an integer are equal and ordered by id.
            assert_eq!(
                order,
                ["9007199254740992.0", "9007199254740992", "9007199254740993"]
            );
            for (at, text) in texts.iter().enumerate() {
                let key = row_key(0, "", &number(text), &format!("d{at}"), 0).unwrap();
                assert!(table.get(key.as_slice()).unwrap().is_some(), "{text}");
            }
        }
        txn.commit().unwrap();
    }

    #[test]
    fn damaged_keys_sort_after_well_formed_ones() {
        let good = row_key(9, "", &json!({"z": [9]}), "a", 0).unwrap();
        let truncated = &good[..good.len() - 7];
        let bad_kind = [0, 0, 0, 1, 7];
        assert!(read_row_key(truncated).is_err());
        assert_eq!(ViewKey::compare(&good, truncated), Ordering::Less);
        assert_eq!(ViewKey::compare(&bad_kind, &good), Ordering::Greater);
        assert_eq!(
            ViewKey::compare(truncated, &bad_kind),
            truncated.cmp(&bad_kind[..])
        );
    }
}
