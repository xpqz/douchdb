//! A client document turned into what its shard writes, before it reaches the
//! shard: the shard's single writer then does only the work that needs stored
//! state.

use serde::{Serialize, Serializer};
use serde_json::{Map, Value};

use crate::db::{validate_doc, META_FIELDS};
use crate::error::{ApiError, DbResult};
use crate::rev::{new_revid_doc, Rev};

/// A document of a write, split and serialised.
pub(crate) struct Prepared {
    pub id: String,
    /// With `new_edits=true` in canonical form; with `new_edits=false` as
    /// given, which is the revision a replicated write stores.
    pub rev: Option<String>,
    pub deleted: bool,
    /// `new_edits=false` only.
    pub revisions: Option<Value>,
    /// The body members (everything but the metadata), serialised in order.
    pub body: Vec<u8>,
    /// The body members of a `_design/` document, for its checks.
    pub members: Option<Map<String, Value>>,
    /// `new_edits=true`: the revision the edit writes over the parent the
    /// client's `_rev` names, or over no parent without `_rev`. `None` when
    /// `_rev` is not a revision or is at the last position; the shard rejects
    /// the document.
    pub new_rev: Option<String>,
}

fn is_body(key: &str) -> bool {
    !META_FIELDS.contains(&key)
}

/// The body members of a document, serialised without building a new map.
struct Body<'a>(&'a Map<String, Value>);

impl Serialize for Body<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.0.iter().filter(|(key, _)| is_body(key)))
    }
}

/// Split a client document into its metadata and body, with the errors and
/// in the order `couch_doc` reports them, serialise its body and, for a new
/// edit, compute its revision id. `id` is the document's id as the caller
/// checked it, generated when the document has no `_id`.
pub(crate) fn prepare(doc: &Value, id: String, new_edits: bool) -> DbResult<Prepared> {
    validate_doc(doc)?;
    let Some(obj) = doc.as_object() else {
        return Err(ApiError::bad_request("Document must be a JSON object"));
    };
    let mut rev = None;
    let mut deleted = false;
    let mut revisions = None;
    for (key, value) in obj {
        match key.as_str() {
            "_rev" => match value {
                Value::String(s) => rev = Some(s),
                _ => return Err(ApiError::bad_request("Invalid rev format")),
            },
            "_deleted" => deleted = matches!(value, Value::Bool(true)),
            "_revisions" => revisions = Some(value),
            _ => {}
        }
    }
    let body = serde_json::to_vec(&Body(obj)).map_err(ApiError::internal)?;
    let members = id.starts_with("_design/").then(|| {
        obj.iter()
            .filter(|(key, _)| is_body(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    });
    let (rev, revisions, new_rev) = if new_edits {
        // No revision follows one at the last position; the shard rejects
        // such an edit.
        let leaf = |parent: Option<&Rev>| {
            let pos = parent.map_or(0, |p| p.pos).checked_add(1)?;
            let id = new_revid_doc(deleted, parent, obj).to_vec();
            Some(Rev { pos, id, hex: true }.to_string())
        };
        let (rev, new_rev) = match rev.map(|raw| (raw, Rev::parse(raw))) {
            None => (None, leaf(None)),
            Some((_, Some(parent))) => (Some(parent.to_string()), leaf(Some(&parent))),
            Some((raw, None)) => (Some(raw.clone()), None),
        };
        (rev, None, new_rev)
    } else {
        (rev.cloned(), revisions.cloned(), None)
    };
    Ok(Prepared {
        id,
        rev,
        deleted,
        revisions,
        body,
        members,
        new_rev,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn prepared(doc: &Value, new_edits: bool) -> Prepared {
        let id = doc["_id"].as_str().unwrap().to_string();
        prepare(doc, id, new_edits).unwrap_or_else(|e| panic!("{doc}: {e:?}"))
    }

    #[test]
    fn prepare_splits_metadata_from_the_body() {
        let upper = "2-AABBCCDDEEFF00112233445566778899";
        let lower = "2-aabbccddeeff00112233445566778899";
        let revisions = json!({"start": 2, "ids": ["aabbccddeeff00112233445566778899", "b"]});
        // (document, new_edits, rev, deleted, revisions, body)
        let cases = [
            (
                json!({"_id": "a", "n": 1}),
                true,
                None,
                false,
                None,
                r#"{"n":1}"#,
            ),
            (
                json!({"_id": "a", "n": 1}),
                false,
                None,
                false,
                None,
                r#"{"n":1}"#,
            ),
            (
                json!({"z": 1, "_id": "c", "a": {"y": 2, "b": [3, 1]}, "s": "\u{e9}"}),
                true,
                None,
                false,
                None,
                r#"{"z":1,"a":{"y":2,"b":[3,1]},"s":"é"}"#,
            ),
            (
                json!({"_id": "a", "_rev": upper, "v": 2}),
                true,
                Some(lower),
                false,
                None,
                r#"{"v":2}"#,
            ),
            (
                json!({"_id": "a", "_rev": upper, "v": 2}),
                false,
                Some(upper),
                false,
                None,
                r#"{"v":2}"#,
            ),
            (
                json!({"_id": "a", "_rev": lower, "_deleted": true, "why": "kept"}),
                true,
                Some(lower),
                true,
                None,
                r#"{"why":"kept"}"#,
            ),
            // Nothing but metadata: the body is `{}`.
            (
                json!({"_id": "m", "_rev": "1-a", "_deleted": true, "_conflicts": [], "_revisions": revisions}),
                true,
                Some("1-a"),
                true,
                None,
                "{}",
            ),
            (
                json!({"_id": "m", "_rev": "1-a", "_deleted": true, "_conflicts": [], "_revisions": revisions}),
                false,
                Some("1-a"),
                true,
                Some(revisions.clone()),
                "{}",
            ),
            (
                json!({"_id": "r", "_replication_id": "x", "k": 1}),
                true,
                None,
                false,
                None,
                r#"{"_replication_id":"x","k":1}"#,
            ),
            (
                json!({"_id": "a", "_deleted": false, "big": 1.0, "n": -0.0}),
                false,
                None,
                false,
                None,
                r#"{"big":1.0,"n":-0.0}"#,
            ),
        ];
        for (doc, new_edits, rev, deleted, revisions, body) in cases {
            let prepared = prepared(&doc, new_edits);
            assert_eq!(prepared.id, doc["_id"].as_str().unwrap(), "{doc}");
            assert_eq!(prepared.rev.as_deref(), rev, "{doc} {new_edits}");
            assert_eq!(prepared.deleted, deleted, "{doc}");
            assert_eq!(prepared.revisions, revisions, "{doc} {new_edits}");
            assert_eq!(String::from_utf8(prepared.body).unwrap(), body, "{doc}");
            assert_eq!(prepared.members, None, "{doc}");
        }
    }

    #[test]
    fn prepare_keeps_design_members() {
        let doc = json!({"_id": "_design/d", "_rev": "1-a", "views": {"v": {"map": "f"}}, "language": "javascript"});
        for new_edits in [true, false] {
            let prepared = prepared(&doc, new_edits);
            let members = json!({"views": {"v": {"map": "f"}}, "language": "javascript"});
            assert_eq!(prepared.members.as_ref(), members.as_object());
            assert_eq!(
                String::from_utf8(prepared.body).unwrap(),
                r#"{"views":{"v":{"map":"f"}},"language":"javascript"}"#
            );
        }
    }

    #[test]
    fn prepare_rejects_malformed_documents() {
        let bad_request = (400, "bad_request");
        let doc_validation = (400, "doc_validation");
        let cases = [
            (json!([1]), bad_request, "Document must be a JSON object"),
            (json!("doc"), bad_request, "Document must be a JSON object"),
            (
                json!({"_id": "a", "_rev": 1}),
                bad_request,
                "Invalid rev format",
            ),
            (
                json!({"_id": "a", "_attachments": {}}),
                bad_request,
                "douchdb does not support attachments",
            ),
            (
                json!({"_id": "a", "_bogus": 1}),
                doc_validation,
                "Bad special document member: _bogus",
            ),
            (
                json!({"_id": "a", "_deleted": "yes"}),
                doc_validation,
                "Bad special document member: _deleted",
            ),
            (
                json!({"_id": "a", "_revisions": {"start": "x"}}),
                doc_validation,
                "_revisions.start isn't an integer.",
            ),
            // The members are checked before `_rev`'s type.
            (
                json!({"_id": "a", "_rev": 1, "_bogus": 1}),
                doc_validation,
                "Bad special document member: _bogus",
            ),
        ];
        for (doc, (status, error), reason) in cases {
            for new_edits in [true, false] {
                let got = prepare(&doc, "a".to_string(), new_edits).err().unwrap();
                assert_eq!(
                    (got.status, got.error, got.reason.as_str()),
                    (status, error, reason),
                    "{doc} {new_edits}"
                );
            }
        }
    }

    #[test]
    fn prepared_rev_is_for_the_client_parent() {
        let parent = "2-aabbccddeeff00112233445566778899";
        // Rendered independently of `Rev::id_string`, which formats the
        // revision under test.
        let hex = |hash: [u8; 16]| -> String { hash.iter().map(|b| format!("{b:02x}")).collect() };
        let doc = json!({"_id": "a", "_rev": parent, "z": 1, "a": [1, 2]});
        let want = new_revid_doc(false, Rev::parse(parent).as_ref(), doc.as_object().unwrap());
        assert_eq!(
            prepared(&doc, true).new_rev,
            Some(format!("3-{}", hex(want)))
        );

        let doc = json!({"_id": "a", "_deleted": true, "k": "v"});
        let want = new_revid_doc(true, None, doc.as_object().unwrap());
        assert_eq!(
            prepared(&doc, true).new_rev,
            Some(format!("1-{}", hex(want)))
        );

        // An upper-case `_rev` names the same parent as its lower-case form.
        let doc = json!({"_id": "a", "_rev": "2-AABBCCDDEEFF00112233445566778899"});
        let want = new_revid_doc(false, Rev::parse(parent).as_ref(), doc.as_object().unwrap());
        assert_eq!(
            prepared(&doc, true).new_rev,
            Some(format!("3-{}", hex(want)))
        );

        // A replicated write keeps its revision; the shard computes none.
        assert_eq!(prepared(&doc, false).new_rev, None);

        // Not a revision: none, and the shard rejects the document.
        let doc = json!({"_id": "a", "_rev": "nope"});
        assert_eq!(prepared(&doc, true).new_rev, None);
    }

    /// The shard recomputes a hash from the stored body bytes when it picks a
    /// parent of its own; that must equal the hash of the document as sent.
    #[test]
    fn body_bytes_hash_like_the_document() {
        let texts = [
            r#"{"_id":"x","a":[1,2,3],"b":{"c":[[],{}],"d":[255,256,-1,300]},"e":[0,1.0,2]}"#,
            r#"{"_id":"x","big":123456789012345678901234567890,"neg":-18446744073709551616,"u":18446744073709551615,"i":-9223372036854775808}"#,
            r#"{"_id":"x","f":1.5,"e":1e400,"x":1.25e-7,"nz":-0,"nzf":-0.0,"z":0.0}"#,
            r#"{"_id":"x","é":"ünï \u0000 日本","t":true,"f":false,"n":null,"":""}"#,
            r#"{"_id":"x","_deleted":true}"#,
        ];
        let parent = Rev::parse("3-aabbccddeeff00112233445566778899").unwrap();
        for text in texts {
            let doc = crate::json::from_slice(text.as_bytes()).unwrap();
            let prepared = prepared(&doc, true);
            let decoded = crate::db::body_members(&prepared.body).unwrap();
            assert_eq!(
                new_revid_doc(prepared.deleted, Some(&parent), &decoded),
                new_revid_doc(prepared.deleted, Some(&parent), doc.as_object().unwrap()),
                "{text}"
            );
        }
    }
}
