//! CouchDB view collation, one implementation for JSON values and for the
//! compact binary form view keys are stored in.
//!
//! The order is CouchDB's (`couch_ejson_compare`): null, false, true,
//! numbers, strings (ICU), arrays (member by member, then length), objects
//! (member by member in stored order: name, then value; then length).
//! Numbers follow Erlang term order: an integer literal is an exact integer,
//! any other number the nearest `f64`, and an integer and a float compare
//! exactly. Numerically equal keys such as `1` and `1.0` compare `Equal`.
//!
//! The comparison is a total preorder, so it can order a B-tree: it is
//! transitive, and `Equal` means the same class of keys.

use std::cmp::Ordering;
use std::sync::OnceLock;

use icu_collator::options::CollatorOptions;
use icu_collator::Collator;
use serde_json::Value;

/// One JSON value as the comparison walks it.
enum Node<'a, I> {
    Null,
    False,
    True,
    /// The number's literal text.
    Number(&'a str),
    String(&'a str),
    /// Members, each without a name.
    Array(I),
    /// Members, each with its name.
    Object(I),
}

impl<I> Node<'_, I> {
    fn rank(&self) -> u8 {
        match self {
            Node::Null => 0,
            Node::False => 1,
            Node::True => 2,
            Node::Number(_) => 3,
            Node::String(_) => 4,
            Node::Array(_) => 5,
            Node::Object(_) => 6,
        }
    }
}

/// A value the collation can walk: a parsed `Value` or an encoded key.
trait Collate<'a>: Copy {
    type Members: Iterator<Item = (&'a str, Self)>;
    fn node(self) -> Node<'a, Self::Members>;
}

/// Compare two walkable values in CouchDB order.
fn cmp<'a, 'b, A: Collate<'a>, B: Collate<'b>>(a: A, b: B) -> Ordering {
    let (a, b) = (a.node(), b.node());
    let (ra, rb) = (a.rank(), b.rank());
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Node::Number(a), Node::Number(b)) => cmp_numbers(a, b),
        (Node::String(a), Node::String(b)) => compare_strings(a, b),
        (Node::Array(a), Node::Array(b)) => cmp_members(a, b, false),
        (Node::Object(a), Node::Object(b)) => cmp_members(a, b, true),
        _ => Ordering::Equal,
    }
}

/// Members one by one (names first when `named`), then the shorter first.
fn cmp_members<'a, 'b, A: Collate<'a>, B: Collate<'b>>(
    mut a: impl Iterator<Item = (&'a str, A)>,
    mut b: impl Iterator<Item = (&'b str, B)>,
    named: bool,
) -> Ordering {
    loop {
        match (a.next(), b.next()) {
            (Some((a_name, a_value)), Some((b_name, b_value))) => {
                let order = if named {
                    compare_strings(a_name, b_name)
                } else {
                    Ordering::Equal
                };
                let order = order.then_with(|| cmp(a_value, b_value));
                if order != Ordering::Equal {
                    return order;
                }
            }
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
        }
    }
}

/// Compare two JSON values in CouchDB view order.
pub fn cmp_json(a: &Value, b: &Value) -> Ordering {
    cmp(a, b)
}

/// The members of a parsed array or object.
enum ValueMembers<'a> {
    Array(std::slice::Iter<'a, Value>),
    Object(serde_json::map::Iter<'a>),
}

impl<'a> Iterator for ValueMembers<'a> {
    type Item = (&'a str, &'a Value);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ValueMembers::Array(items) => items.next().map(|item| ("", item)),
            ValueMembers::Object(members) => {
                members.next().map(|(name, value)| (name.as_str(), value))
            }
        }
    }
}

impl<'a> Collate<'a> for &'a Value {
    type Members = ValueMembers<'a>;
    fn node(self) -> Node<'a, ValueMembers<'a>> {
        match self {
            Value::Null => Node::Null,
            Value::Bool(false) => Node::False,
            Value::Bool(true) => Node::True,
            Value::Number(number) => Node::Number(number.as_str()),
            Value::String(text) => Node::String(text),
            Value::Array(items) => Node::Array(ValueMembers::Array(items.iter())),
            Value::Object(members) => Node::Object(ValueMembers::Object(members.iter())),
        }
    }
}

// ---------------------------------------------------------------------------
// Numbers

/// CouchDB (Erlang term) order of two JSON number literals.
fn cmp_numbers(a: &str, b: &str) -> Ordering {
    match (is_integer_literal(a), is_integer_literal(b)) {
        (true, true) => cmp_decimal_integers(a, b),
        (false, false) => parse_float(a)
            .partial_cmp(&parse_float(b))
            .unwrap_or(Ordering::Equal),
        (true, false) => cmp_integer_float(a, parse_float(b)),
        (false, true) => cmp_integer_float(b, parse_float(a)).reverse(),
    }
}

/// A literal without a fraction or exponent is an integer, as jiffy decodes
/// it.
fn is_integer_literal(raw: &str) -> bool {
    !raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E'))
}

/// The nearest `f64`; a literal beyond its range is infinite.
fn parse_float(raw: &str) -> f64 {
    raw.parse().unwrap_or(0.0)
}

/// Whether a float literal is out of `f64` range. CouchDB (jiffy) rejects
/// such a number.
fn out_of_range(raw: &str) -> bool {
    !is_integer_literal(raw) && !parse_float(raw).is_finite()
}

/// Whether `value` holds a float literal out of `f64` range anywhere, which
/// CouchDB rejects as invalid JSON.
pub(crate) fn has_out_of_range_number(value: &Value) -> bool {
    match value {
        Value::Number(number) => out_of_range(number.as_str()),
        Value::Array(items) => items.iter().any(has_out_of_range_number),
        Value::Object(members) => members.values().any(has_out_of_range_number),
        _ => false,
    }
}

/// Compare an exact integer with a float exactly: by the float's integer
/// part, written out in full, then by its fraction.
fn cmp_integer_float(integer: &str, float: f64) -> Ordering {
    if float.is_infinite() {
        return if float > 0.0 {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let whole = float.trunc();
    // An integral `f64` formats to its exact digits.
    cmp_decimal_integers(integer, &format!("{whole:.0}")).then_with(|| {
        let fraction = float - whole;
        if fraction > 0.0 {
            Ordering::Less
        } else if fraction < 0.0 {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    })
}

/// Compare decimal integer literals exactly. `f64` would collapse values that
/// differ beyond its 53-bit mantissa.
fn cmp_decimal_integers(a: &str, b: &str) -> Ordering {
    fn parts(raw: &str) -> (bool, &str) {
        let (negative, digits) = match raw.strip_prefix('-') {
            Some(digits) => (true, digits),
            None => (false, raw),
        };
        let digits = digits.trim_start_matches('0');
        let digits = if digits.is_empty() { "0" } else { digits };
        (negative && digits != "0", digits)
    }

    let (a_negative, a_digits) = parts(a);
    let (b_negative, b_digits) = parts(b);
    if a_negative != b_negative {
        return if a_negative {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let magnitude = a_digits
        .len()
        .cmp(&b_digits.len())
        .then_with(|| a_digits.cmp(b_digits));
    if a_negative {
        magnitude.reverse()
    } else {
        magnitude
    }
}

// ---------------------------------------------------------------------------
// Strings

/// ICU collation with the root locale, as CouchDB's.
pub(crate) fn compare_strings(a: &str, b: &str) -> Ordering {
    static COLLATOR: OnceLock<Option<icu_collator::CollatorBorrowed<'static>>> = OnceLock::new();
    let collator = COLLATOR
        .get_or_init(|| Collator::try_new(Default::default(), CollatorOptions::default()).ok());
    match collator {
        Some(collator) => collator.compare(a, b),
        None => a.cmp(b),
    }
}

// ---------------------------------------------------------------------------
// The encoded form

/// Bumped whenever the encoding or the order changes; a view index written
/// under another version is rebuilt.
pub(crate) const CODEC_VERSION: u32 = 2;

/// The ICU collation data strings are ordered by. A view index written under
/// other data is rebuilt, so a dependency upgrade that changes string order
/// cannot leave a stored index out of order. A test keeps this in step with
/// Cargo.lock.
pub(crate) const ICU_DATA: &str = "icu_collator_data 2.4.0";

const TAG_NULL: u8 = 0;
const TAG_FALSE: u8 = 1;
const TAG_TRUE: u8 = 2;
const TAG_NUMBER: u8 = 3;
const TAG_STRING: u8 = 4;
const TAG_ARRAY: u8 = 5;
const TAG_OBJECT: u8 = 6;

/// A view key in its stored form: a tag per value; numbers and strings as
/// their text behind a length; arrays and objects as a member count followed
/// by the members, object members as a name (length and text) and a value.
/// Lengths and counts are LEB128. Appended to `out`.
pub(crate) fn encode_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(TAG_NULL),
        Value::Bool(false) => out.push(TAG_FALSE),
        Value::Bool(true) => out.push(TAG_TRUE),
        Value::Number(number) => {
            out.push(TAG_NUMBER);
            put_text(number.as_str(), out);
        }
        Value::String(text) => {
            out.push(TAG_STRING);
            put_text(text, out);
        }
        Value::Array(items) => {
            out.push(TAG_ARRAY);
            put_len(items.len(), out);
            for item in items {
                encode_into(item, out);
            }
        }
        Value::Object(members) => {
            out.push(TAG_OBJECT);
            put_len(members.len(), out);
            for (name, item) in members {
                put_text(name, out);
                encode_into(item, out);
            }
        }
    }
}

fn put_len(mut len: usize, out: &mut Vec<u8>) {
    loop {
        let byte = (len & 0x7f) as u8;
        len >>= 7;
        if len == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn put_text(text: &str, out: &mut Vec<u8>) {
    put_len(text.len(), out);
    out.extend_from_slice(text.as_bytes());
}

/// A length at the start of `bytes`, and the bytes after it.
fn take_len(bytes: &[u8]) -> Option<(usize, &[u8])> {
    let mut len = 0usize;
    for (at, byte) in bytes.iter().enumerate().take(10) {
        len |= usize::from(byte & 0x7f).checked_shl(7 * at as u32)?;
        if byte & 0x80 == 0 {
            return Some((len, &bytes[at + 1..]));
        }
    }
    None
}

/// Text behind a length at the start of `bytes`, and the bytes after it.
fn take_text(bytes: &[u8]) -> Option<(&str, &[u8])> {
    let (len, rest) = take_len(bytes)?;
    let text = std::str::from_utf8(rest.get(..len)?).ok()?;
    Some((text, &rest[len..]))
}

/// The encoded value at the start of `bytes`, and the bytes after it.
fn take_value(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let (&tag, body) = bytes.split_first()?;
    let rest = match tag {
        TAG_NULL | TAG_FALSE | TAG_TRUE => body,
        TAG_NUMBER | TAG_STRING => take_text(body)?.1,
        TAG_ARRAY | TAG_OBJECT => {
            let (count, mut rest) = take_len(body)?;
            for _ in 0..count {
                if tag == TAG_OBJECT {
                    rest = take_text(rest)?.1;
                }
                rest = take_value(rest)?.1;
            }
            rest
        }
        _ => return None,
    };
    Some((&bytes[..bytes.len() - rest.len()], rest))
}

/// The encoded value at the start of `bytes` and the bytes after it, or
/// `None` when they do not start with a well-formed value.
pub(crate) fn split_key(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    take_value(bytes)
}

/// The value of an encoded key split off by `split_key`, or `None` when a
/// number in it does not parse.
pub(crate) fn decode(key: &[u8]) -> Option<Value> {
    to_value(Encoded(key))
}

fn to_value(encoded: Encoded<'_>) -> Option<Value> {
    Some(match encoded.node() {
        Node::Null => Value::Null,
        Node::False => Value::Bool(false),
        Node::True => Value::Bool(true),
        Node::Number(text) => Value::Number(text.parse().ok()?),
        Node::String(text) => Value::String(text.to_string()),
        Node::Array(members) => Value::Array(
            members
                .map(|(_, member)| to_value(member))
                .collect::<Option<_>>()?,
        ),
        Node::Object(members) => Value::Object(
            members
                .map(|(name, member)| Some((name.to_string(), to_value(member)?)))
                .collect::<Option<_>>()?,
        ),
    })
}

/// Compare two encoded keys split off by `split_key`, in CouchDB order.
pub(crate) fn cmp_split(a: &[u8], b: &[u8]) -> Ordering {
    cmp(Encoded(a), Encoded(b))
}

/// One well-formed encoded value.
#[derive(Clone, Copy)]
struct Encoded<'a>(&'a [u8]);

/// The members of an encoded array or object.
struct EncodedMembers<'a> {
    rest: &'a [u8],
    left: usize,
    named: bool,
}

impl<'a> Iterator for EncodedMembers<'a> {
    type Item = (&'a str, Encoded<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let mut name = "";
        if self.named {
            let (text, rest) = take_text(self.rest)?;
            name = text;
            self.rest = rest;
        }
        let (value, rest) = take_value(self.rest)?;
        self.rest = rest;
        Some((name, Encoded(value)))
    }
}

impl<'a> Collate<'a> for Encoded<'a> {
    type Members = EncodedMembers<'a>;
    /// Only called on well-formed values: anything else reads as null.
    fn node(self) -> Node<'a, EncodedMembers<'a>> {
        let Some((&tag, body)) = self.0.split_first() else {
            return Node::Null;
        };
        let text = || take_text(body).map_or("", |(text, _)| text);
        let members = |named| {
            let (left, rest) = take_len(body).unwrap_or((0, &[]));
            EncodedMembers { rest, left, named }
        };
        match tag {
            TAG_FALSE => Node::False,
            TAG_TRUE => Node::True,
            TAG_NUMBER => Node::Number(text()),
            TAG_STRING => Node::String(text()),
            TAG_ARRAY => Node::Array(members(false)),
            TAG_OBJECT => Node::Object(members(true)),
            _ => Node::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    fn number(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
    }

    fn encode(value: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        encode_into(value, &mut out);
        out
    }

    /// Compare two whole encoded keys.
    fn cmp_encoded(a: &[u8], b: &[u8]) -> Ordering {
        let (Some((a, [])), Some((b, []))) = (split_key(a), split_key(b)) else {
            panic!("not one encoded key");
        };
        cmp_split(a, b)
    }

    /// `ICU_DATA` names the collation data the build uses: an upgrade must
    /// bump it, so stored view indexes are rebuilt.
    #[test]
    fn icu_data_matches_the_lock_file() {
        let lock = include_str!("../Cargo.lock");
        let version = lock
            .split("[[package]]")
            .find(|package| package.contains("name = \"icu_collator_data\""))
            .and_then(|package| {
                package
                    .lines()
                    .find_map(|line| line.strip_prefix("version = "))
            })
            .map(|version| version.trim_matches('"'))
            .expect("icu_collator_data in Cargo.lock");
        assert_eq!(ICU_DATA, format!("icu_collator_data {version}"));
    }

    #[test]
    fn integers_and_floats_compare_exactly() {
        let (a, b, c) = (
            number("9007199254740992"),
            number("9007199254740993"),
            number("9007199254740992.0"),
        );
        // f64 cannot tell 2^53 + 1 from 2^53; the integers still differ, and
        // the float equals only the one it is.
        assert_eq!(cmp_json(&a, &b), Ordering::Less);
        assert_eq!(cmp_json(&a, &c), Ordering::Equal);
        assert_eq!(cmp_json(&c, &b), Ordering::Less);
        assert_eq!(cmp_json(&b, &c), Ordering::Greater);
        assert_eq!(cmp_json(&number("1"), &number("1.0")), Ordering::Equal);
        assert_eq!(cmp_json(&number("-0"), &number("0.0")), Ordering::Equal);
        assert_eq!(cmp_json(&number("2"), &number("1.5")), Ordering::Greater);
        assert_eq!(cmp_json(&number("-2"), &number("-1.5")), Ordering::Less);
        assert_eq!(cmp_json(&number("-1"), &number("-1.5")), Ordering::Greater);
        assert_eq!(cmp_json(&number("10"), &number("1e1")), Ordering::Equal);
        assert_eq!(
            cmp_json(&number("100000000000000000000000"), &number("1e23")),
            // 1e23 is 99999999999999991611392 as an f64.
            Ordering::Greater
        );
        assert_eq!(cmp_json(&number("10"), &number("1e400")), Ordering::Less);
        assert_eq!(
            cmp_json(&number("-10"), &number("-1e400")),
            Ordering::Greater
        );
        assert!(has_out_of_range_number(&json!([1, {"a": number("1e400")}])));
        assert!(!has_out_of_range_number(&json!([number("1e300"), 12])));
        assert!(!has_out_of_range_number(&number(
            "123456789012345678901234567890"
        )));
    }

    #[test]
    fn encoded_keys_round_trip_and_order_like_values() {
        let keys = [
            json!(null),
            json!(false),
            json!(true),
            number("-1.5"),
            number("1"),
            number("1.0"),
            number("9007199254740993"),
            json!("a"),
            json!("B"),
            json!("é"),
            json!([]),
            json!([1, "x", [null]]),
            json!({"a": 1, "b": [true]}),
            json!({}),
        ];
        for a in &keys {
            let encoded = encode(a);
            assert_eq!(decode(&encoded).as_ref(), Some(a));
            for b in &keys {
                assert_eq!(cmp_encoded(&encoded, &encode(b)), cmp_json(a, b), "{a} {b}");
            }
        }
    }

    /// JSON keys, numbers drawn to collide as `f64` and to exceed it.
    fn key() -> impl Strategy<Value = Value> {
        let number = prop_oneof![
            (-3i64..3).prop_map(|n| n.to_string()),
            (0u32..4).prop_map(|n| format!("{}", 9_007_199_254_740_992u64 + u64::from(n))),
            (-3i64..3).prop_map(|n| format!("{n}.0")),
            Just("9007199254740992.0".to_string()),
            Just("0.5".to_string()),
            Just("-0.5".to_string()),
            Just("1e1".to_string()),
            Just("1e400".to_string()),
            Just("-1e400".to_string()),
            Just("123456789012345678901234567890".to_string()),
        ]
        .prop_map(|text| number(&text));
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            number,
            prop_oneof![
                Just("a"),
                Just("A"),
                Just("b"),
                Just("é"),
                Just("e\u{301}"),
                Just(""),
            ]
            .prop_map(|text| Value::String(text.to_string())),
        ];
        leaf.prop_recursive(3, 12, 3, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..3).prop_map(Value::Array),
                prop::collection::vec((prop_oneof![Just("a"), Just("b")], inner), 0..3).prop_map(
                    |members| {
                        Value::Object(
                            members
                                .into_iter()
                                .map(|(name, value)| (name.to_string(), value))
                                .collect(),
                        )
                    }
                ),
            ]
        })
    }

    proptest! {
        #[test]
        fn the_order_is_a_total_preorder(a in key(), b in key(), c in key()) {
            let ab = cmp_json(&a, &b);
            prop_assert_eq!(ab, cmp_json(&b, &a).reverse());
            prop_assert_eq!(cmp_json(&a, &a), Ordering::Equal);
            let bc = cmp_json(&b, &c);
            if ab != Ordering::Greater && bc != Ordering::Greater {
                let ac = cmp_json(&a, &c);
                prop_assert!(ac != Ordering::Greater, "{} {} {}", a, b, c);
                if ab == Ordering::Equal && bc == Ordering::Equal {
                    prop_assert_eq!(ac, Ordering::Equal);
                }
            }
        }

        #[test]
        fn encoded_keys_agree_with_values(a in key(), b in key()) {
            let (ea, eb) = (encode(&a), encode(&b));
            prop_assert_eq!(decode(&ea), Some(a.clone()));
            prop_assert_eq!(cmp_encoded(&ea, &eb), cmp_json(&a, &b));
        }
    }
}
