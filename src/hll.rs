//! The HyperLogLog sketch behind `_approx_count_distinct`.
//!
//! CouchDB keeps one `hyper` sketch (precision 11, 2048 registers) per
//! reduction, inserts `term_to_binary(Key)` for every emitted key, merges
//! partials by taking register maxima, and reports `round(card(Sketch))`.
//! Hashing the same bytes into the same registers makes the estimates equal
//! to CouchDB's, and the register-wise maximum makes the merge exact, so
//! partials from blocks, shards and nodes combine to what one pass gives.

use base64::Engine;
use serde_json::Value;

use crate::etf;

mod bias;

const PRECISION: u32 = 11;
/// Largest linear-counting estimate used instead of the corrected one.
const THRESHOLD: f64 = 1800.0;
const REGISTERS: usize = 1 << PRECISION;

/// Partial sketches are exchanged as base64 strings of one of two layouts:
/// a tag byte, then either `(index: u16 be, rank: u8)` triples for the
/// non-empty registers, or all registers. A few keys make a short string.
const SPARSE: u8 = 0;
const DENSE: u8 = 1;

pub(crate) struct Sketch {
    registers: Vec<u8>,
}

impl Sketch {
    pub(crate) fn new() -> Sketch {
        Sketch {
            registers: vec![0; REGISTERS],
        }
    }

    /// Add one emitted key, hashed as CouchDB does.
    pub(crate) fn insert_key(&mut self, key: &Value) {
        let digest = sha1(&etf::from_json(key).encode());
        let [high, low, ..] = digest;
        let index = usize::from(u16::from_be_bytes([high, low]) >> (16 - PRECISION));
        // The rank is one more than the run of zero bits that follows the
        // index bits.
        let zeros = (PRECISION as usize..64)
            .take_while(|bit| {
                digest
                    .get(bit / 8)
                    .is_some_and(|byte| (byte >> (7 - bit % 8)) & 1 == 0)
            })
            .count();
        let rank = u8::try_from(zeros + 1).unwrap_or(u8::MAX);
        if let Some(register) = self.registers.get_mut(index) {
            *register = (*register).max(rank);
        }
    }

    pub(crate) fn union(&mut self, other: &Sketch) {
        for (mine, theirs) in self.registers.iter_mut().zip(&other.registers) {
            *mine = (*mine).max(*theirs);
        }
    }

    /// `couch_hyper:card/1`: HyperLogLog++ with its empirical bias
    /// correction, and linear counting while that is at most `THRESHOLD`.
    pub(crate) fn count(&self) -> u64 {
        let m = REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self
            .registers
            .iter()
            .map(|register| (-f64::from(*register)).exp2())
            .fold(0.0, |total, term| total + term);
        let raw = alpha * m.powi(2) / sum;
        let corrected = if raw <= 5.0 * m { raw - bias(raw) } else { raw };
        let empty = self
            .registers
            .iter()
            .filter(|register| **register == 0)
            .count();
        let linear = if empty == 0 {
            corrected
        } else {
            m * (m / empty as f64).ln()
        };
        let estimate = if linear <= THRESHOLD {
            linear
        } else {
            corrected
        };
        estimate.round() as u64
    }

    pub(crate) fn to_json(&self) -> Value {
        let filled: Vec<(usize, u8)> = self
            .registers
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, rank)| *rank > 0)
            .collect();
        let mut bytes = Vec::new();
        if filled.len() * 3 < REGISTERS {
            bytes.push(SPARSE);
            for (index, rank) in filled {
                bytes.extend((index as u16).to_be_bytes());
                bytes.push(rank);
            }
        } else {
            bytes.push(DENSE);
            bytes.extend_from_slice(&self.registers);
        }
        Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub(crate) fn from_json(value: &Value) -> Result<Sketch, String> {
        let bad = || "invalid _approx_count_distinct partial".to_string();
        let text = value.as_str().ok_or_else(bad)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(text)
            .map_err(|_| bad())?;
        let mut sketch = Sketch::new();
        match bytes.split_first() {
            Some((&SPARSE, triples)) if triples.len() % 3 == 0 => {
                for triple in triples.chunks_exact(3) {
                    let &[high, low, rank] = triple else {
                        return Err(bad());
                    };
                    let index = usize::from(u16::from_be_bytes([high, low]));
                    *sketch.registers.get_mut(index).ok_or_else(bad)? = rank;
                }
            }
            Some((&DENSE, registers)) if registers.len() == REGISTERS => {
                sketch.registers.copy_from_slice(registers);
            }
            _ => return Err(bad()),
        }
        Ok(sketch)
    }
}

/// The mean bias of the six empirical estimates nearest to `raw`.
fn bias(raw: f64) -> f64 {
    let mut nearest: Vec<(f64, usize)> = bias::ESTIMATES
        .iter()
        .enumerate()
        .map(|(index, estimate)| ((raw - estimate).powi(2), index))
        .collect();
    nearest.sort_by(|a, b| a.0.total_cmp(&b.0));
    let total = nearest
        .iter()
        .take(6)
        .filter_map(|(_, index)| bias::BIASES.get(*index))
        .fold(0.0, |total, bias| total + bias);
    total / nearest.len().min(6) as f64
}

/// Distinct-key estimate over `keys`, as a partial reduction.
pub(crate) fn partial<'a>(keys: impl IntoIterator<Item = &'a Value>) -> Value {
    let mut sketch = Sketch::new();
    for key in keys {
        sketch.insert_key(key);
    }
    sketch.to_json()
}

/// Union partial sketches into one partial.
pub(crate) fn combine(partials: &[Value]) -> Result<Value, String> {
    let mut merged = Sketch::new();
    for part in partials {
        merged.union(&Sketch::from_json(part)?);
    }
    Ok(merged.to_json())
}

/// The reported count for a partial.
pub(crate) fn finish(partial: &Value) -> Result<Value, String> {
    Ok(Value::from(Sketch::from_json(partial)?.count()))
}

fn sha1(data: &[u8]) -> [u8; 20] {
    let mut state: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let mut message = data.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend(((data.len() as u64) * 8).to_be_bytes());
    for block in message.chunks_exact(64) {
        let mut w = [0u32; 80];
        for (word, bytes) in w.iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = state;
        for (i, word) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | (!b & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let next = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            (a, b, c, d, e) = (next, a, b.rotate_left(30), c, d);
        }
        for (slot, add) in state.iter_mut().zip([a, b, c, d, e]) {
            *slot = slot.wrapping_add(add);
        }
    }
    let mut digest = [0u8; 20];
    for (out, word) in digest.chunks_exact_mut(4).zip(state) {
        out.copy_from_slice(&word.to_be_bytes());
    }
    digest
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn sha1_matches_the_standard_vectors() {
        assert_eq!(
            hex(&sha1(b"abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(hex(&sha1(b"")), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_eq!(
            hex(&sha1(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
        );
    }

    #[test]
    fn estimates_match_couchdb() {
        // Counts read from CouchDB 3.5.3 for n distinct integer keys.
        for (n, expected) in [(10, 10), (1000, 1005), (100_000, 98_599)] {
            let keys: Vec<Value> = (0..n).map(|i| json!(i)).collect();
            assert_eq!(
                Sketch::from_json(&partial(&keys)).unwrap().count(),
                expected
            );
        }
    }

    #[test]
    fn bias_corrected_estimates_match_couchdb() {
        // Above the linear-counting threshold CouchDB corrects the raw
        // estimate with its empirical bias table: 2500 string keys read as
        // 2529 from CouchDB 3.5.3.
        let keys: Vec<Value> = (0..2500).map(|i| json!(format!("b{i}"))).collect();
        assert_eq!(Sketch::from_json(&partial(&keys)).unwrap().count(), 2529);
    }

    #[test]
    fn partials_union_exactly() {
        let keys: Vec<Value> = (0..5000).map(|i| json!(format!("k{i}"))).collect();
        let whole = partial(&keys);
        let parts: Vec<Value> = keys.chunks(77).map(partial).collect();
        assert_eq!(combine(&parts).unwrap(), whole);
        assert_eq!(
            finish(&whole).unwrap(),
            finish(&combine(&parts).unwrap()).unwrap()
        );
    }

    #[test]
    fn rejects_malformed_partials() {
        assert!(finish(&json!(3)).is_err());
        assert!(finish(&json!("!!")).is_err());
        assert!(finish(&json!("AAA=")).is_err());
        assert!(finish(&json!("AP//AQ==")).is_err());
    }
}
