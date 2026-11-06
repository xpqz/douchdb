//! Minimal Erlang External Term Format encoder.
//!
//! CouchDB derives revision ids as
//! `md5(term_to_binary([Deleted, OldStart, OldRev, Body, Atts], [{minor_version, 1}]))`,
//! so wire compatibility requires byte-identical ETF output, including the
//! leading version byte.

const VERSION_MAGIC: u8 = 131;
const SMALL_INTEGER_EXT: u8 = 97;
const INTEGER_EXT: u8 = 98;
const SMALL_BIG_EXT: u8 = 110;
const LARGE_BIG_EXT: u8 = 111;
const NEW_FLOAT_EXT: u8 = 70;
const SMALL_TUPLE_EXT: u8 = 104;
const LARGE_TUPLE_EXT: u8 = 105;
const NIL_EXT: u8 = 106;
const LIST_EXT: u8 = 108;
const BINARY_EXT: u8 = 109;
const ATOM_EXT: u8 = 100;

/// The subset of Erlang terms needed for rev hashing.
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Atom(&'static str),
    Int(i64),
    /// A JSON integer above `i64::MAX`. CouchDB's `jiffy` keeps it unsigned;
    /// casting it to `i64` would wrap and produce a different revision hash.
    UInt(u64),
    /// A JSON integer outside both `i64` and `u64`, kept as a little-endian
    /// magnitude so the revision hash matches CouchDB's bignum encoding.
    BigInt {
        negative: bool,
        magnitude: Vec<u8>,
    },
    Float(f64),
    Binary(Vec<u8>),
    List(Vec<Term>),
    Tuple(Vec<Term>),
}

impl Term {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![VERSION_MAGIC];
        self.encode_into(&mut out);
        out
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            // CouchDB passes minor_version 1, which predates UTF-8 atoms: this
            // is Latin-1 ATOM_EXT, not the modern SMALL_ATOM_UTF8_EXT. All the
            // atoms we encode are ASCII, so bytes and characters coincide.
            Term::Atom(a) => {
                debug_assert!(a.is_ascii() && a.len() < 256);
                out.push(ATOM_EXT);
                out.extend((a.len() as u16).to_be_bytes());
                out.extend_from_slice(a.as_bytes());
            }
            Term::Int(i) if (0..=255).contains(i) => {
                out.push(SMALL_INTEGER_EXT);
                out.push(*i as u8);
            }
            Term::Int(i) if (*i >= i32::MIN as i64) && (*i <= i32::MAX as i64) => {
                out.push(INTEGER_EXT);
                out.extend((*i as i32).to_be_bytes());
            }
            Term::Int(i) => {
                // SMALL_BIG_EXT: sign byte, count, then little-endian magnitude.
                let mut mag = i.unsigned_abs();
                let mut digits = Vec::new();
                while mag > 0 {
                    digits.push((mag & 0xff) as u8);
                    mag >>= 8;
                }
                if digits.is_empty() {
                    digits.push(0);
                }
                out.push(SMALL_BIG_EXT);
                out.push(digits.len() as u8);
                out.push(u8::from(*i < 0));
                out.extend_from_slice(&digits);
            }
            Term::UInt(u) if *u <= 255 => {
                out.push(SMALL_INTEGER_EXT);
                out.push(*u as u8);
            }
            Term::UInt(u) if *u <= i32::MAX as u64 => {
                out.push(INTEGER_EXT);
                out.extend((*u as u32).to_be_bytes());
            }
            Term::UInt(u) => {
                let mut mag = *u;
                let mut digits = Vec::new();
                while mag > 0 {
                    digits.push((mag & 0xff) as u8);
                    mag >>= 8;
                }
                out.push(SMALL_BIG_EXT);
                out.push(digits.len() as u8);
                out.push(0);
                out.extend_from_slice(&digits);
            }
            Term::BigInt {
                negative,
                magnitude,
            } => {
                if magnitude.len() <= u8::MAX as usize {
                    out.push(SMALL_BIG_EXT);
                    out.push(magnitude.len() as u8);
                } else {
                    out.push(LARGE_BIG_EXT);
                    out.extend((magnitude.len() as u32).to_be_bytes());
                }
                out.push(u8::from(*negative));
                out.extend_from_slice(magnitude);
            }
            Term::Float(f) => {
                out.push(NEW_FLOAT_EXT);
                out.extend(f.to_be_bytes());
            }
            Term::Binary(b) => {
                out.push(BINARY_EXT);
                out.extend((b.len() as u32).to_be_bytes());
                out.extend_from_slice(b);
            }
            Term::List(items) if items.is_empty() => out.push(NIL_EXT),
            Term::List(items) => {
                out.push(LIST_EXT);
                out.extend((items.len() as u32).to_be_bytes());
                for item in items {
                    item.encode_into(out);
                }
                out.push(NIL_EXT);
            }
            Term::Tuple(items) if items.len() < 256 => {
                out.push(SMALL_TUPLE_EXT);
                out.push(items.len() as u8);
                for item in items {
                    item.encode_into(out);
                }
            }
            Term::Tuple(items) => {
                out.push(LARGE_TUPLE_EXT);
                out.extend((items.len() as u32).to_be_bytes());
                for item in items {
                    item.encode_into(out);
                }
            }
        }
    }
}

fn is_integer(raw: &str) -> bool {
    !raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E'))
}

/// Convert a decimal integer literal to a little-endian magnitude.
fn decimal_magnitude(raw: &str) -> (bool, Vec<u8>) {
    let (negative, digits) = match raw.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, raw),
    };
    let mut magnitude = vec![0u8];
    for digit in digits.bytes() {
        let mut carry = u16::from(digit - b'0');
        for byte in &mut magnitude {
            let value = u16::from(*byte) * 10 + carry;
            *byte = value as u8;
            carry = value >> 8;
        }
        while carry > 0 {
            magnitude.push(carry as u8);
            carry >>= 8;
        }
    }
    while magnitude.len() > 1 && magnitude.last() == Some(&0) {
        magnitude.pop();
    }
    let zero = magnitude.len() == 1 && magnitude[0] == 0;
    (negative && !zero, magnitude)
}

/// Convert a JSON value to the term CouchDB's `jiffy` would have produced.
///
/// Objects become Erlang proplists (`{Props}`) in document order, *not* maps:
/// `couch_doc:from_json_obj/1` builds a proplist and never sorts it, so key
/// order is part of the revision identity.
pub fn from_json(value: &serde_json::Value) -> Term {
    use serde_json::Value;

    match value {
        Value::Null => Term::Atom("null"),
        Value::Bool(true) => Term::Atom("true"),
        Value::Bool(false) => Term::Atom("false"),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Term::Int(i)
            } else if let Some(u) = n.as_u64() {
                Term::UInt(u)
            } else if is_integer(n.as_str()) {
                let (negative, magnitude) = decimal_magnitude(n.as_str());
                Term::BigInt {
                    negative,
                    magnitude,
                }
            } else {
                Term::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => Term::Binary(s.as_bytes().to_vec()),
        Value::Array(a) => Term::List(a.iter().map(from_json).collect()),
        Value::Object(map) => Term::Tuple(vec![Term::List(
            map.iter()
                .map(|(k, v)| Term::Tuple(vec![Term::Binary(k.as_bytes().to_vec()), from_json(v)]))
                .collect(),
        )]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn atoms_are_latin1_under_minor_version_1() {
        // Verified against OTP 27: term_to_binary(false, [{minor_version,1}]).
        assert_eq!(Term::Atom("false").encode(), b"\x83\x64\x00\x05false");
        assert_eq!(Term::Atom("true").encode(), b"\x83\x64\x00\x04true");
    }

    #[test]
    fn integers_pick_the_smallest_form() {
        assert_eq!(Term::Int(0).encode(), vec![131, 97, 0]);
        assert_eq!(Term::Int(255).encode(), vec![131, 97, 255]);
        assert_eq!(Term::Int(256).encode(), vec![131, 98, 0, 0, 1, 0]);
        assert_eq!(Term::Int(-1).encode(), vec![131, 98, 255, 255, 255, 255]);
    }

    #[test]
    fn integer_boundaries_use_the_expected_etf_tags() {
        assert_eq!(Term::Int(i32::MAX as i64).encode()[1], INTEGER_EXT);
        assert_eq!(Term::Int(i32::MAX as i64 + 1).encode()[1], SMALL_BIG_EXT);
        assert_eq!(Term::Int(i32::MIN as i64).encode()[1], INTEGER_EXT);
        assert_eq!(Term::Int(i32::MIN as i64 - 1).encode()[1], SMALL_BIG_EXT);
        assert_eq!(Term::Int(i64::MIN).encode()[1], SMALL_BIG_EXT);
        assert_eq!(Term::Int(i64::MAX).encode()[1], SMALL_BIG_EXT);
        assert_eq!(Term::UInt(255).encode()[1], SMALL_INTEGER_EXT);
        assert_eq!(Term::UInt(256).encode()[1], INTEGER_EXT);
        assert_eq!(Term::UInt(i32::MAX as u64 + 1).encode()[1], SMALL_BIG_EXT);
        assert_eq!(Term::UInt(u64::MAX).encode()[1], SMALL_BIG_EXT);
    }

    #[test]
    fn big_integers_encode_the_full_sign_and_magnitude() {
        // The tag alone is not enough: a broken shift or mask still emits
        // SMALL_BIG_EXT with the wrong digits, and a broken sign test still
        // emits the right tag with the wrong sign byte.
        assert_eq!(
            Term::Int(1_i64 << 32).encode(),
            vec![131, SMALL_BIG_EXT, 5, 0, 0, 0, 0, 0, 1]
        );
        assert_eq!(
            Term::Int(i64::MAX).encode(),
            vec![
                131,
                SMALL_BIG_EXT,
                8,
                0,
                255,
                255,
                255,
                255,
                255,
                255,
                255,
                0x7f
            ]
        );
        assert_eq!(
            Term::Int(i64::MIN).encode(),
            vec![131, SMALL_BIG_EXT, 8, 1, 0, 0, 0, 0, 0, 0, 0, 0x80]
        );
    }

    #[test]
    fn tuple_length_selects_the_small_or_large_form() {
        let small = Term::Tuple(vec![Term::Int(0); 255]).encode();
        assert_eq!(small[1], SMALL_TUPLE_EXT);
        assert_eq!(small[2], 255);

        let large = Term::Tuple(vec![Term::Int(0); 256]).encode();
        assert_eq!(large[1], LARGE_TUPLE_EXT);
        assert_eq!(&large[2..6], &[0, 0, 1, 0], "256 elements, big-endian");
        assert_eq!(large.len(), 6 + 256 * 2);
    }

    #[test]
    fn decimal_magnitude_handles_signed_zero() {
        assert_eq!(decimal_magnitude("0"), (false, vec![0]));
        assert_eq!(decimal_magnitude("-0"), (false, vec![0]));
    }

    #[test]
    fn negative_bignum_keeps_its_sign_when_the_low_byte_is_zero() {
        let value: Value = serde_json::from_str("-18446744073709551616").unwrap();
        let encoded = from_json(&value).encode();
        assert_eq!(encoded[1], SMALL_BIG_EXT);
        assert_eq!(encoded[2], 9, "2^64 needs nine magnitude bytes");
        assert_eq!(encoded[3], 1, "negative sign byte");
        assert_eq!(&encoded[4..], &[0, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn unsigned_integers_above_i64_are_not_wrapped() {
        // CouchDB's jiffy keeps these unsigned. Casting to i64 would encode a
        // negative number and change every revision hash containing one.
        assert_eq!(Term::UInt(255).encode(), vec![131, 97, 255]);
        assert_eq!(Term::UInt(256).encode(), vec![131, 98, 0, 0, 1, 0]);
        assert_eq!(
            Term::UInt(1 << 63).encode(),
            vec![131, 110, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0x80]
        );
        assert_eq!(
            Term::UInt(u64::MAX).encode(),
            vec![131, 110, 8, 0, 255, 255, 255, 255, 255, 255, 255, 255]
        );
        assert_eq!(from_json(&json!(1u64 << 63)), Term::UInt(1 << 63));
        assert_eq!(from_json(&json!(u64::MAX)), Term::UInt(u64::MAX));
    }

    #[test]
    fn big_integers_use_erlang_big_ext() {
        let above_u64: Value = serde_json::from_str("18446744073709551616").unwrap();
        assert_eq!(
            from_json(&above_u64),
            Term::BigInt {
                negative: false,
                magnitude: vec![0, 0, 0, 0, 0, 0, 0, 0, 1],
            }
        );
        assert_eq!(
            from_json(&above_u64).encode(),
            vec![131, 110, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        );

        let below_i64: Value = serde_json::from_str("-9223372036854775809").unwrap();
        assert_eq!(
            from_json(&below_i64).encode(),
            vec![131, 110, 8, 1, 1, 0, 0, 0, 0, 0, 0, 0x80]
        );

        let huge: Value = serde_json::from_str(&format!("1{}", "0".repeat(700))).unwrap();
        let encoded = from_json(&huge).encode();
        assert_eq!(encoded[1], LARGE_BIG_EXT, "magnitude above 255 bytes");
        assert_eq!(
            u32::from_be_bytes(encoded[2..6].try_into().unwrap()) as usize,
            encoded.len() - 7
        );
    }

    #[test]
    fn lists_and_tuples() {
        assert_eq!(Term::List(vec![]).encode(), vec![131, NIL_EXT]);
        assert_eq!(
            Term::List(vec![Term::Int(1)]).encode(),
            vec![131, 108, 0, 0, 0, 1, 97, 1, 106]
        );
        assert_eq!(
            Term::Tuple(vec![Term::Int(1), Term::Int(2)]).encode(),
            vec![131, 104, 2, 97, 1, 97, 2]
        );
    }

    #[test]
    fn lists_of_bytes_use_string_ext() {
        // Erlang's term_to_binary packs a list of integers 0..=255 as
        // STRING_EXT (107, u16 length, raw bytes), which is what jiffy's
        // [1,2,3] becomes.
        let list = |items: &[i64]| Term::List(items.iter().map(|i| Term::Int(*i)).collect());
        assert_eq!(list(&[1, 2, 3]).encode(), vec![131, 107, 0, 3, 1, 2, 3]);
        assert_eq!(list(&[0, 255]).encode(), vec![131, 107, 0, 2, 0, 255]);
        assert_eq!(
            Term::List(vec![Term::UInt(7)]).encode(),
            vec![131, 107, 0, 1, 7]
        );
        // One element outside 0..=255 keeps the whole list as LIST_EXT.
        assert_eq!(list(&[1, 256]).encode()[1], LIST_EXT);
        assert_eq!(list(&[1, -1]).encode()[1], LIST_EXT);
        assert_eq!(list(&[1, i64::MAX]).encode()[1], LIST_EXT);
        // Floats are not integers, even when integral.
        assert_eq!(
            Term::List(vec![Term::Int(1), Term::Float(1.0)]).encode()[1],
            LIST_EXT
        );
        assert_eq!(
            Term::List(vec![Term::Int(1), Term::Atom("null")]).encode()[1],
            LIST_EXT
        );
        assert_eq!(Term::List(vec![]).encode(), vec![131, NIL_EXT]);
        // From JSON: a float literal 2.0 is not a small integer.
        let value: Value = serde_json::from_str("[1, 2.0]").unwrap();
        assert_eq!(from_json(&value).encode()[1], LIST_EXT);
        assert_eq!(from_json(&json!([1, 2, 3])).encode()[1], 107);
    }

    #[test]
    fn string_ext_length_limit_is_65535() {
        let at_limit = Term::List(vec![Term::Int(9); 65535]).encode();
        assert_eq!(&at_limit[1..4], &[107, 255, 255]);
        assert_eq!(at_limit.len(), 4 + 65535);

        let over = Term::List(vec![Term::Int(9); 65536]).encode();
        assert_eq!(over[1], LIST_EXT);
        assert_eq!(&over[2..6], &[0, 1, 0, 0]);
    }

    /// Byte-for-byte agreement with OTP 27 and with CouchDB 3.5.3's rev ids.
    #[test]
    fn matches_erlang_term_to_binary() {
        let term = Term::List(vec![
            Term::Atom("false"),
            Term::Int(0),
            Term::Int(0),
            from_json(&json!({"a": 1})),
            Term::List(vec![]),
        ]);
        assert_eq!(
            term.encode(),
            vec![
                0x83, 0x6C, 0, 0, 0, 5, // version, LIST_EXT, 5 elements
                0x64, 0, 5, b'f', b'a', b'l', b's', b'e', // false
                0x61, 0, // 0
                0x61, 0, // 0
                0x68, 1, // tuple, 1 element
                0x6C, 0, 0, 0, 1, // proplist of 1 pair
                0x68, 2, // {key, value}
                0x6D, 0, 0, 0, 1, b'a', // <<"a">>
                0x61, 1,    // 1
                0x6A, // end proplist
                0x6A, // empty attachments
                0x6A, // end list
            ]
        );
    }

    #[test]
    fn json_objects_keep_document_order() {
        let a = json!({"b": 1, "a": 2});
        let b = json!({"a": 2, "b": 1});
        assert_ne!(from_json(&a).encode(), from_json(&b).encode());
        assert_eq!(from_json(&a), from_json(&json!({"b": 1, "a": 2})));
    }

    #[test]
    fn json_object_is_a_one_tuple_of_proplist() {
        let encoded = from_json(&json!({"a": 1})).encode();
        assert_eq!(encoded[1], SMALL_TUPLE_EXT);
        assert_eq!(encoded[2], 1, "object is an arity-1 tuple");
        assert_eq!(encoded[3], LIST_EXT, "containing a proplist");
    }

    use proptest::prelude::*;

    fn trimmed_le(bytes: &[u8]) -> Vec<u8> {
        let mut bytes = bytes.to_vec();
        while bytes.len() > 1 && bytes.last() == Some(&0) {
            bytes.pop();
        }
        bytes
    }

    proptest! {
        #[test]
        fn decimal_magnitude_matches_u128(n in any::<u128>()) {
            let (negative, magnitude) = decimal_magnitude(&n.to_string());
            prop_assert!(!negative);
            prop_assert_eq!(magnitude, trimmed_le(&n.to_le_bytes()));
        }

        #[test]
        fn decimal_magnitude_matches_i128(n in any::<i128>()) {
            let (negative, magnitude) = decimal_magnitude(&n.to_string());
            prop_assert_eq!(negative, n < 0);
            prop_assert_eq!(magnitude, trimmed_le(&n.unsigned_abs().to_le_bytes()));
        }
    }
}
