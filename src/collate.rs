//! CouchDB view collation. The comparison walks any value that implements
//! `Collate`, so a stored key form can share every rule with `Value`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    fn number(text: &str) -> Value {
        serde_json::from_str(text).unwrap()
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

    }
}
