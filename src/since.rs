//! Opaque `since` tokens, shaped like CouchDB 2+/3: `"<sum of seqs>-<opaque>"`.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

/// One shard copy's position in a changes feed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyPos {
    pub node: usize,
    pub shard: usize,
    pub seq: u64,
    pub uuid: String,
}

/// A malformed `since` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinceError;

impl std::fmt::Display for SinceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Malformed sequence supplied in 'since' parameter.")
    }
}

impl std::error::Error for SinceError {}

/// A parsed `since` parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Since {
    Zero,
    Now,
    Token(Vec<CopyPos>),
}

/// Encodes copy positions as `"<sum of seqs>-<base64url of a JSON list>"`.
pub fn encode(copies: &[CopyPos]) -> String {
    let sum: u128 = copies.iter().map(|c| u128::from(c.seq)).sum();
    let rows: Vec<(usize, usize, u64, &str)> = copies
        .iter()
        .map(|c| (c.node, c.shard, c.seq, c.uuid.as_str()))
        .collect();
    let json = serde_json::to_vec(&rows).unwrap_or_default();
    format!("{sum}-{}", URL_SAFE_NO_PAD.encode(json))
}

/// Decodes a token produced by [`encode`]. The numeric prefix is only checked
/// for shape; the opaque part carries the positions.
pub fn decode(raw: &str) -> Result<Vec<CopyPos>, SinceError> {
    let (prefix, opaque) = raw.split_once('-').ok_or(SinceError)?;
    if prefix.is_empty() || !prefix.bytes().all(|b| b.is_ascii_digit()) {
        return Err(SinceError);
    }
    let bytes = URL_SAFE_NO_PAD.decode(opaque).map_err(|_| SinceError)?;
    let rows: Vec<(usize, usize, u64, String)> =
        serde_json::from_slice(&bytes).map_err(|_| SinceError)?;
    Ok(rows
        .into_iter()
        .map(|(node, shard, seq, uuid)| CopyPos {
            node,
            shard,
            seq,
            uuid,
        })
        .collect())
}

/// Parses a `since` query value: absent or `"0"` is the start, `"now"` the
/// current end, anything else must be a token from [`encode`].
pub fn parse(raw: Option<&str>) -> Result<Since, SinceError> {
    match raw {
        None | Some("0") => Ok(Since::Zero),
        Some("now") => Ok(Since::Now),
        Some(token) => decode(token).map(Since::Token),
    }
}

/// An internal-replication checkpoint kept on the pulling (target) node: it
/// read the source copy `source_uuid` up to `seq`, and the target copy
/// `target_uuid` was at `target_seq` once those changes were written.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Checkpoint {
    pub source_uuid: String,
    pub seq: u64,
    pub target_uuid: String,
    pub target_seq: u64,
}

/// Where to resume the token position `want` on the copy `serving_uuid`, as
/// CouchDB's `mem3_rep:find_source_seq` does. The same copy resumes at its own
/// position; another one at the `target_seq` of the latest checkpoint that
/// recorded a pull from `want`'s copy into it at or before `want.seq`, so it
/// never skips what that copy has not received; with none, from 0.
pub fn resume_position(want: &CopyPos, serving_uuid: &str, checkpoints: &[Checkpoint]) -> u64 {
    if want.uuid == serving_uuid {
        return want.seq;
    }
    checkpoints
        .iter()
        .filter(|c| {
            c.source_uuid == want.uuid && c.target_uuid == serving_uuid && c.seq <= want.seq
        })
        .max_by_key(|c| c.seq)
        .map_or(0, |c| c.target_seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<CopyPos> {
        vec![
            CopyPos {
                node: 0,
                shard: 0,
                seq: 12,
                uuid: "abc".into(),
            },
            CopyPos {
                node: 1,
                shard: 3,
                seq: 30,
                uuid: "d-e_f".into(),
            },
            CopyPos {
                node: 2,
                shard: 7,
                seq: u64::MAX / 4,
                uuid: String::new(),
            },
        ]
    }

    #[test]
    fn encode_decode_round_trips() {
        let s = sample();
        assert_eq!(decode(&encode(&s)).unwrap(), s);
        assert_eq!(parse(Some(&encode(&s))).unwrap(), Since::Token(s));
        assert_eq!(decode(&encode(&[])).unwrap(), vec![]);
    }

    #[test]
    fn zero_and_now_parse_like_couchdb() {
        assert_eq!(parse(None).unwrap(), Since::Zero);
        assert_eq!(parse(Some("0")).unwrap(), Since::Zero);
        assert_eq!(parse(Some("now")).unwrap(), Since::Now);
    }

    #[test]
    fn garbage_is_an_error_not_zero() {
        for bad in ["", "abc", "5", "5-", "5-!!!", "-x", "x-y", "NOW", "5-e30"] {
            assert!(parse(Some(bad)).is_err(), "{bad:?} should be rejected");
        }
        assert_eq!(
            SinceError.to_string(),
            "Malformed sequence supplied in 'since' parameter."
        );
    }

    #[test]
    fn encoded_prefix_is_the_sum_of_positions() {
        let t = encode(&sample());
        let sum = 12 + 30 + u64::MAX / 4;
        assert!(t.starts_with(&format!("{sum}-")), "{t}");
        assert!(encode(&[]).starts_with("0-"));
        let opaque = t.split_once('-').unwrap().1;
        assert!(opaque
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    fn cp(source: &str, seq: u64, target_seq: u64) -> Checkpoint {
        Checkpoint {
            source_uuid: source.into(),
            seq,
            target_uuid: "serving".into(),
            target_seq,
        }
    }

    fn want(seq: u64) -> CopyPos {
        CopyPos {
            node: 0,
            shard: 1,
            seq,
            uuid: "wanted".into(),
        }
    }

    #[test]
    fn resume_on_same_copy_uses_its_position() {
        assert_eq!(
            resume_position(&want(9), "wanted", &[cp("wanted", 5, 2)]),
            9
        );
    }

    #[test]
    fn resume_on_other_copy_uses_the_latest_checkpoint_not_past_the_position() {
        let checkpoints = [
            cp("wanted", 4, 3),
            cp("wanted", 8, 7),
            cp("wanted", 12, 11),
            cp("someone-else", 9, 99),
        ];
        assert_eq!(resume_position(&want(9), "serving", &checkpoints), 7);
        assert_eq!(resume_position(&want(8), "serving", &checkpoints), 7);
        assert_eq!(resume_position(&want(12), "serving", &checkpoints), 11);
    }

    #[test]
    fn resume_without_a_matching_checkpoint_is_zero() {
        assert_eq!(resume_position(&want(9), "serving", &[]), 0);
        assert_eq!(
            resume_position(&want(3), "serving", &[cp("wanted", 4, 3)]),
            0
        );
        assert_eq!(
            resume_position(&want(9), "serving", &[cp("other", 4, 3)]),
            0
        );
        let mut elsewhere = cp("wanted", 4, 3);
        elsewhere.target_uuid = "another-target".into();
        assert_eq!(resume_position(&want(9), "serving", &[elsewhere]), 0);
    }
}
