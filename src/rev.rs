//! Revision identity and winner selection.
//!
//! Both must match CouchDB byte for byte; see PLAN.md, "The rev hash".

use crate::etf::{self, Term};
use md5::{Digest, Md5};
use serde_json::Value;
use std::fmt;

#[derive(Clone, Debug)]
pub struct Rev {
    pub pos: u64,
    /// Decoded revision id: 16 raw bytes for hex ids, UTF-8 bytes otherwise.
    pub id: Vec<u8>,
    /// Whether the id was written as 32 hex characters and must be rendered
    /// back as hex. A raw 16-byte id is not the same thing.
    pub hex: bool,
}

impl PartialEq for Rev {
    fn eq(&self, other: &Self) -> bool {
        // Provenance does not change the underlying CouchDB rev id.
        self.pos == other.pos && self.id == other.id
    }
}

impl Eq for Rev {}

impl Rev {
    /// The revision id without the position prefix, rendered the way CouchDB
    /// emits it: 16-byte ids as lowercase hex, anything else verbatim.
    pub fn id_string(&self) -> String {
        if self.hex {
            self.id.iter().map(|b| format!("{b:02x}")).collect()
        } else {
            String::from_utf8_lossy(&self.id).into_owned()
        }
    }

    pub fn parse(s: &str) -> Option<Rev> {
        let (pos, id) = s.split_once('-')?;
        if pos.is_empty() || pos.len() > 1 && pos.starts_with('0') {
            return None;
        }
        let pos: u64 = pos.parse().ok()?;
        if pos == 0 {
            return None;
        }
        // couch_doc:parse_revid/2 decodes 32-character ids as hex, otherwise
        // keeps the id as raw bytes.
        let (id, hex) = if id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
            (decode_hex(id)?, true)
        } else {
            (id.as_bytes().to_vec(), false)
        };
        if id.is_empty() {
            return None;
        }
        Some(Rev { pos, id, hex })
    }
}

impl fmt::Display for Rev {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.pos, self.id_string())
    }
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    Some(out)
}

/// The reference for [`new_revid_doc`]: build the term tree and hash it,
/// exactly as `couch_db:new_revid/1` does.
///
/// `body` must already have `_id`, `_rev`, `_revisions`, `_deleted`,
/// `_attachments` and the other metadata fields removed, and must retain the
/// key order it arrived in.
#[cfg(test)]
fn new_revid(deleted: bool, parent: Option<&Rev>, body: &Value) -> [u8; 16] {
    let old_start = parent.map(|r| r.pos).unwrap_or(0);
    let old_rev = match parent {
        Some(r) => Term::Binary(r.id.clone()),
        None => Term::Int(0),
    };

    let term = Term::List(vec![
        Term::Atom(if deleted { "true" } else { "false" }),
        Term::Int(old_start as i64),
        old_rev,
        etf::from_json(body),
        // The attachment list: always empty, as douchdb stores none.
        Term::List(Vec::new()),
    ]);

    Md5::digest(term.encode()).into()
}

/// Like [`new_revid`], for the members of a whole document: every key that is
/// not in [`META_FIELDS`](crate::db::META_FIELDS) is the body, in order. The
/// hash is fed without building the body or its term.
pub fn new_revid_doc(
    deleted: bool,
    parent: Option<&Rev>,
    doc: &serde_json::Map<String, Value>,
) -> [u8; 16] {
    let old_start = parent.map(|r| r.pos).unwrap_or(0);
    let old_rev = match parent {
        Some(r) => Term::Binary(r.id.clone()),
        None => Term::Int(0),
    };
    // The version byte and the header of the five-element outer list.
    let mut head = vec![131, 108, 0, 0, 0, 5];
    for term in [
        Term::Atom(if deleted { "true" } else { "false" }),
        Term::Int(old_start as i64),
        old_rev,
    ] {
        term.encode_into(&mut head);
    }
    let mut hasher = Md5::new();
    etf::hash_revision(
        &mut hasher,
        &head,
        doc.iter()
            .filter(|(key, _)| !crate::db::META_FIELDS.contains(&key.as_str())),
    );
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hex16(id: [u8; 16]) -> String {
        id.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn rev_roundtrip() {
        let r = Rev::parse("3-aabbccddeeff00112233445566778899").unwrap();
        assert_eq!(r.pos, 3);
        assert_eq!(r.id.len(), 16);
        assert_eq!(r.to_string(), "3-aabbccddeeff00112233445566778899");
    }

    #[test]
    fn non_hex_revs_are_kept_as_bytes() {
        let r = Rev::parse("1-tombstone").unwrap();
        assert_eq!(r.id, b"tombstone");
        assert_eq!(r.to_string(), "1-tombstone");
        // A 16-character raw id is not a decoded 32-character hex id.
        let r = Rev::parse("1-0123456789abcdef").unwrap();
        assert_eq!(r.id_string(), "0123456789abcdef");
        assert_eq!(r.to_string(), "1-0123456789abcdef");
    }

    #[test]
    fn rejects_malformed_revs() {
        assert!(Rev::parse("abc").is_none());
        assert!(Rev::parse("0-abc").is_none());
        assert!(Rev::parse("1-").is_none());
        // Only 32-character *hex* ids are decoded; anything else stays literal,
        // which is why CouchDB accepts non-hex rev ids from other implementations.
        let literal = Rev::parse("1-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz").unwrap();
        assert_eq!(literal.id.len(), 32);
    }

    #[test]
    fn revid_is_deterministic_and_order_sensitive() {
        let body = json!({"x": 1, "y": 2});
        let reordered = json!({"y": 2, "x": 1});
        let a = new_revid(false, None, &body);
        assert_eq!(a, new_revid(false, None, &body));
        assert_ne!(a, new_revid(false, None, &reordered));
        assert_ne!(a, new_revid(true, None, &body));
    }

    #[test]
    fn parent_rev_changes_the_hash() {
        let body = json!({"x": 1});
        let parent = Rev::parse("1-aabbccddeeff00112233445566778899").unwrap();
        assert_ne!(
            new_revid(false, Some(&parent), &body),
            new_revid(false, None, &body)
        );
    }

    /// Revs recorded from a live Apache CouchDB 3.5.3 (`PUT /db/doc`).
    #[test]
    fn matches_couchdb_3_5() {
        let cases = [
            (json!({"a": 1}), "23202479633c2b380f79507a776743d5"),
            (
                json!({"b": {"y": 2, "x": [1, 2.5, null, true]}, "a": "h\u{e9}llo"}),
                "41692bf1af92c4fee2f4783a50ceb756",
            ),
            (json!({"b": 1, "a": 2}), "afc27c384a7b497e8b717857527a0dc7"),
            // Small-integer arrays hash as Erlang strings (STRING_EXT);
            // both revs verified against live CouchDB 3.5.
            (json!({"a": [1, 2, 3]}), "308048961a84beb0c9e99e07facf0f5f"),
            (json!({"a": [0, 255]}), "ebdd13a7ecf6387443aa094a0d18b4d6"),
        ];
        for (body, expected) in cases {
            let got = hex16(new_revid(false, None, &body));
            assert_eq!(got, expected, "body {body}");
        }
    }

    /// The request path parses bodies with `json::from_slice`, not `json!`;
    /// the integers it yields must hash the same way.
    #[test]
    fn parsed_request_bodies_match_couchdb_3_5() {
        // Revs recorded from a live Apache CouchDB 3.5 (`POST /db`).
        let cases = [
            (r#"{"a":[1,2,3]}"#, "308048961a84beb0c9e99e07facf0f5f"),
            (r#"{"a":[0,255]}"#, "ebdd13a7ecf6387443aa094a0d18b4d6"),
            (r#"{"a":[1,256]}"#, "d0259a7a9a59173bfe7d4da469ac3cb6"),
            (r#"{"a":[1,2.0]}"#, "3b562cec9ddf9f74ee2f9b8a9de3f536"),
        ];
        for (raw, expected) in cases {
            let body = crate::json::from_slice(raw.as_bytes()).unwrap();
            let got = hex16(new_revid(false, None, &body));
            assert_eq!(got, expected, "body {raw}");
        }
    }

    #[test]
    fn streaming_revid_matches_term_encoding() {
        let mut wide = serde_json::Map::new();
        for i in 0..300 {
            wide.insert(format!("k{i}"), json!(i));
        }
        let mut deep = json!("leaf");
        for i in 0..64 {
            deep = if i % 2 == 0 {
                json!([deep])
            } else {
                json!({"d": deep})
            };
        }
        let texts = [
            "{}",
            r#"{"a":[1,2,3],"b":{"c":[[],{}],"d":[255,256,-1,300]},"e":[0,1.0,2]}"#,
            r#"{"big":123456789012345678901234567890,"neg":-18446744073709551616,"u":18446744073709551615,"i":-9223372036854775808}"#,
            r#"{"f":1.5,"e":1e400,"x":1.25e-7,"nz":-0,"nzf":-0.0,"z":0.0}"#,
            r#"{"é":"ünï \u0000 日本","t":true,"f":false,"n":null,"":""}"#,
            r#"{"_id":"x","_rev":"1-a","_deleted":true,"_attachments":{},"_revisions":1,"_conflicts":[],"_deleted_conflicts":[],"_revs_info":[],"_local_seq":3,"keep":1,"_other":2}"#,
        ];
        let mut docs: Vec<serde_json::Map<String, Value>> = texts
            .iter()
            .map(|t| {
                serde_json::from_str::<Value>(t)
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .clone()
            })
            .collect();
        docs.push(wide);
        docs.push(
            json!({"_id": "d", "deep": deep, "long": "x".repeat(20000), "bytes": vec![7; 70000]})
                .as_object()
                .unwrap()
                .clone(),
        );
        let parent = Rev::parse("3-aabbccddeeff00112233445566778899").unwrap();
        for doc in &docs {
            let mut body = doc.clone();
            body.retain(|k, _| !crate::db::META_FIELDS.contains(&k.as_str()));
            let body = Value::Object(body);
            for deleted in [false, true] {
                for parent in [None, Some(&parent)] {
                    assert_eq!(
                        new_revid_doc(deleted, parent, doc),
                        new_revid(deleted, parent, &body),
                        "{body}"
                    );
                }
            }
        }
    }
}
