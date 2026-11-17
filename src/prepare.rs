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
    /// `new_edits=true`: the revision id for the parent the client's `_rev`
    /// names, or for no parent without `_rev`. `None` when `_rev` is not a
    /// revision; the shard rejects the document.
    pub rev_hash: Option<[u8; 16]>,
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
/// edit, compute its revision id.
pub(crate) fn prepare(doc: &Value, new_edits: bool) -> DbResult<Prepared> {
    validate_doc(doc)?;
    let Some(obj) = doc.as_object() else {
        return Err(ApiError::bad_request("Document must be a JSON object"));
    };
    let mut id = None;
    let mut rev = None;
    let mut deleted = false;
    let mut revisions = None;
    for (key, value) in obj {
        match key.as_str() {
            "_id" => match value {
                Value::String(s) => id = Some(s),
                _ => {
                    return Err(ApiError::new(
                        400,
                        "illegal_docid",
                        "Document id must be a string",
                    ))
                }
            },
            "_rev" => match value {
                Value::String(s) => rev = Some(s),
                _ => return Err(ApiError::bad_request("Invalid rev format")),
            },
            "_deleted" => deleted = matches!(value, Value::Bool(true)),
            "_revisions" => revisions = Some(value),
            _ => {}
        }
    }
    let id = id.cloned().unwrap_or_else(crate::util::uuid);
    if id.is_empty() {
        return Err(ApiError::new(
            400,
            "illegal_docid",
            "Document id must not be empty",
        ));
    }
    let body = serde_json::to_vec(&Body(obj)).map_err(ApiError::internal)?;
    let members = id.starts_with("_design/").then(|| {
        obj.iter()
            .filter(|(key, _)| is_body(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    });
    let (rev, revisions, rev_hash) = if new_edits {
        let (rev, rev_hash) = match rev.map(|raw| (raw, Rev::parse(raw))) {
            None => (None, Some(new_revid_doc(deleted, None, obj))),
            Some((_, Some(parent))) => (
                Some(parent.to_string()),
                Some(new_revid_doc(deleted, Some(&parent), obj)),
            ),
            Some((raw, None)) => (Some(raw.clone()), None),
        };
        (rev, None, rev_hash)
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
        rev_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::split_doc;
    use serde_json::json;

    fn same_error(got: &crate::error::ApiError, want: &crate::error::ApiError) {
        assert_eq!(
            (got.status, got.error, &got.reason),
            (want.status, want.error, &want.reason)
        );
    }

    #[test]
    fn prepare_matches_split_doc() {
        let cases: Vec<Value> =
            serde_json::from_str(include_str!("../tests/golden/bulk_write.json")).unwrap();
        let mut docs: Vec<(Value, bool)> = Vec::new();
        for case in &cases {
            for request in case["requests"].as_array().unwrap() {
                let new_edits = request["new_edits"].as_bool().unwrap();
                for doc in request["docs"].as_array().unwrap() {
                    docs.push((doc.clone(), new_edits));
                }
            }
        }
        // Nothing but metadata: the body is `{}`.
        docs.push((
            json!({"_id": "m", "_rev": "1-a", "_deleted": true, "_conflicts": []}),
            true,
        ));
        docs.push((json!({"_id": "r", "_replication_id": "x", "k": 1}), true));
        let mut checked = 0;
        for (doc, new_edits) in docs {
            let prepared = prepare(&doc, new_edits);
            match split_doc(doc.clone()) {
                Ok(split) => {
                    let prepared = prepared.unwrap_or_else(|e| panic!("{doc}: {e:?}"));
                    assert_eq!(prepared.id, split.id, "{doc}");
                    assert_eq!(prepared.rev, split.rev, "{doc}");
                    assert_eq!(prepared.deleted, split.deleted, "{doc}");
                    let revisions = if new_edits { None } else { split.revisions };
                    assert_eq!(prepared.revisions, revisions, "{doc}");
                    let design = split.id.starts_with("_design/");
                    assert_eq!(
                        prepared.members.as_ref(),
                        design.then_some(&split.body),
                        "{doc}"
                    );
                    assert_eq!(
                        prepared.body,
                        serde_json::to_vec(&Value::Object(split.body)).unwrap(),
                        "{doc}"
                    );
                    checked += 1;
                }
                Err(want) => same_error(&prepared.err().unwrap(), &want),
            }
        }
        assert!(checked > 20, "{checked}");
    }

    #[test]
    fn prepare_errors_match_split_doc() {
        let docs = [
            json!([1]),
            json!("doc"),
            json!({"_id": 5}),
            json!({"_id": "a", "_rev": 1}),
            json!({"_id": ""}),
            json!({"_id": "a", "_attachments": {}}),
            json!({"_id": "a", "_bogus": 1}),
            json!({"_id": "a", "_deleted": "yes"}),
            json!({"_id": "a", "_revisions": {"start": "x"}}),
        ];
        for doc in docs {
            for new_edits in [true, false] {
                let want = split_doc(doc.clone()).err().unwrap();
                let got = prepare(&doc, new_edits).err().unwrap();
                same_error(&got, &want);
            }
        }
    }

    #[test]
    fn prepared_hash_is_for_the_client_parent() {
        let parent = "2-aabbccddeeff00112233445566778899";
        let doc = json!({"_id": "a", "_rev": parent, "z": 1, "a": [1, 2]});
        let prepared = prepare(&doc, true).unwrap();
        let want = new_revid_doc(false, Rev::parse(parent).as_ref(), doc.as_object().unwrap());
        assert_eq!(prepared.rev_hash, Some(want));

        let doc = json!({"_id": "a", "_deleted": true, "k": "v"});
        let prepared = prepare(&doc, true).unwrap();
        let want = new_revid_doc(true, None, doc.as_object().unwrap());
        assert_eq!(prepared.rev_hash, Some(want));

        // An upper-case `_rev` names the same parent as its lower-case form.
        let doc = json!({"_id": "a", "_rev": "2-AABBCCDDEEFF00112233445566778899"});
        let prepared = prepare(&doc, true).unwrap();
        assert_eq!(prepared.rev.as_deref(), Some(parent));
        let want = new_revid_doc(false, Rev::parse(parent).as_ref(), doc.as_object().unwrap());
        assert_eq!(prepared.rev_hash, Some(want));

        // A replicated write keeps its revision; the shard computes none.
        let prepared = prepare(&doc, false).unwrap();
        assert_eq!(
            prepared.rev.as_deref(),
            Some("2-AABBCCDDEEFF00112233445566778899")
        );
        assert_eq!(prepared.rev_hash, None);
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
            let prepared = prepare(&doc, true).unwrap();
            let decoded = crate::db::body_members(&prepared.body).unwrap();
            assert_eq!(
                new_revid_doc(prepared.deleted, Some(&parent), &decoded),
                new_revid_doc(prepared.deleted, Some(&parent), doc.as_object().unwrap()),
                "{text}"
            );
        }
    }
}
