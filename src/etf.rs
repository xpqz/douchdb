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
                Term::Int(u as i64)
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
    use serde_json::json;

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
}
