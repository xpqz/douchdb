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
        f.write_str("Malformed since parameter")
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
        assert_eq!(SinceError.to_string(), "Malformed since parameter");
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
}
