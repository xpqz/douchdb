//! JSON parsing with CouchDB's object semantics.
//!
//! `serde_json` keeps the first position of a duplicate object key and keeps
//! the last value. CouchDB's decoder keeps the last value *and* the position of
//! the last occurrence, and the position is part of the revision hash. The
//! custom visitor below matches CouchDB.
//!
//! Numbers are also canonicalised here. With `arbitrary_precision`, serde_json
//! preserves the input spelling (`1e2`, `1.00`); CouchDB decodes floats to an
//! Erlang float and re-encodes them (`100.0`, `1.0`). Integers, including
//! integers too large for `i64`/`u64`, keep their exact value.

use serde::de::{self, Deserialize, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;

/// Parse a JSON value, applying CouchDB's last-occurrence rule to duplicate
/// object keys and canonicalising number spellings.
pub fn from_slice(body: &[u8]) -> Result<Value, serde_json::Error> {
    if has_reserved_number_key(body) {
        return Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("`{NUMBER_TOKEN}` is a reserved JSON object key"),
        )));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let value = OrderedValue::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(value.0)
}

/// serde_json's arbitrary-precision representation uses a private object key
/// internally. User documents cannot use it, because the deserializer cannot
/// distinguish the two once parsing starts.
fn has_reserved_number_key(body: &[u8]) -> bool {
    let mut index = 0;
    while index < body.len() {
        if body[index] != b'"' {
            index += 1;
            continue;
        }
        let start = index;
        index += 1;
        while index < body.len() {
            match body[index] {
                b'\\' => index += 2,
                b'"' => break,
                _ => index += 1,
            }
        }
        if index >= body.len() {
            return false;
        }
        let end = index + 1;
        if let Ok(key) = serde_json::from_slice::<String>(&body[start..end]) {
            let mut next = end;
            while next < body.len() && body[next].is_ascii_whitespace() {
                next += 1;
            }
            if next < body.len() && body[next] == b':' && key == NUMBER_TOKEN {
                return true;
            }
        }
        index = end;
    }
    false
}

struct OrderedValue(Value);

impl<'de> Deserialize<'de> for OrderedValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(OrderedVisitor)
    }
}

struct OrderedVisitor;

impl<'de> Visitor<'de> for OrderedVisitor {
    type Value = OrderedValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(OrderedValue(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(OrderedValue(Value::Number(value.into())))
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_i128(value)
            .map(|number| OrderedValue(Value::Number(number)))
            .ok_or_else(|| E::custom("JSON number out of range"))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(OrderedValue(Value::Number(value.into())))
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_u128(value)
            .map(|number| OrderedValue(Value::Number(number)))
            .ok_or_else(|| E::custom("JSON number out of range"))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E> {
        Ok(OrderedValue(
            Number::from_f64(value).map_or(Value::Null, Value::Number),
        ))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(String::from(value))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(OrderedValue(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(OrderedValue(Value::Null))
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(OrderedValue(Value::Null))
    }

    fn visit_seq<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut values = Vec::new();
        while let Some(value) = visitor.next_element::<OrderedValue>()? {
            values.push(value.0);
        }
        Ok(OrderedValue(Value::Array(values)))
    }

    fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        match visitor.next_key_seed(KeyClassifier)? {
            Some(KeyClass::Number) => {
                let number: NumberFromString = visitor.next_value()?;
                Ok(OrderedValue(Value::Number(normalize_number(number.0))))
            }
            Some(KeyClass::Map(first_key)) => {
                let first_value: OrderedValue = visitor.next_value()?;
                let mut values = Map::new();
                values.insert(first_key, first_value.0);
                while let Some((key, value)) = visitor.next_entry::<String, OrderedValue>()? {
                    if values.contains_key(&key) {
                        values.shift_remove(&key);
                    }
                    values.insert(key, value.0);
                }
                Ok(OrderedValue(Value::Object(values)))
            }
            None => Ok(OrderedValue(Value::Object(Map::new()))),
        }
    }
}

enum KeyClass {
    Map(String),
    Number,
}

// serde_json encodes an arbitrary-precision number as a map with this private
// key. Kept in sync with serde_json 1.0; the numeric tests fail if it changes.
const NUMBER_TOKEN: &str = "$serde_json::private::Number";

struct NumberFromString(Number);

impl<'de> Deserialize<'de> for NumberFromString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        // serde_json also uses this map shape for an object whose only key is
        // its private number token. Reject values that are not numbers rather
        // than constructing an invalid Number.
        let number = serde_json::from_str::<Number>(&raw).map_err(de::Error::custom)?;
        Ok(NumberFromString(number))
    }
}

struct KeyClassifier;

impl<'de> DeserializeSeed<'de> for KeyClassifier {
    type Value = KeyClass;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for KeyClassifier {
    type Value = KeyClass;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string key")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_string(String::from(value))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        if value == NUMBER_TOKEN {
            Ok(KeyClass::Number)
        } else {
            Ok(KeyClass::Map(value))
        }
    }
}

fn normalize_number(number: Number) -> Number {
    let raw = number.as_str();
    if is_integer(raw) {
        if let Ok(value) = raw.parse::<i64>() {
            return value.into();
        }
        if let Ok(value) = raw.parse::<u64>() {
            return value.into();
        }
        return number;
    }
    number.as_f64().and_then(Number::from_f64).unwrap_or(number)
}

fn is_integer(raw: &str) -> bool {
    !raw.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(value: &Value) -> Vec<&str> {
        value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect()
    }

    #[test]
    fn duplicate_keys_move_to_their_last_position() {
        let value = from_slice(br#"{"a":1,"b":2,"a":3}"#).unwrap();
        assert_eq!(keys(&value), ["b", "a"]);
        assert_eq!(value["a"], 3);

        let value = from_slice(br#"{"a":1,"b":2,"c":3,"a":4,"b":5}"#).unwrap();
        assert_eq!(keys(&value), ["c", "a", "b"]);
        assert_eq!(value["a"], 4);
        assert_eq!(value["b"], 5);

        let value = from_slice(br#"{"x":{"a":1,"b":2,"a":3}}"#).unwrap();
        assert_eq!(keys(&value), ["x"]);
        assert_eq!(keys(&value["x"]), ["b", "a"]);
    }

    #[test]
    fn escaped_duplicate_keys_compare_as_decoded_strings() {
        let value = from_slice(br#"{"a":1,"\u0061":2}"#).unwrap();
        assert_eq!(keys(&value), ["a"]);
        assert_eq!(value["a"], 2);
    }

    #[test]
    fn arrays_and_nested_objects_keep_duplicate_order() {
        let value = from_slice(br#"[{"a":1,"a":2},{"b":1,"c":2,"b":3}]"#).unwrap();
        assert_eq!(value[0]["a"], 2);
        assert_eq!(keys(&value[1]), ["c", "b"]);
    }

    #[test]
    fn invalid_private_number_tokens_are_rejected() {
        // serde_json's arbitrary-precision encoding uses a reserved map key.
        // Reject user objects that try to use it, at any depth and through an
        // escaped spelling.
        assert!(from_slice(br#"{"$serde_json::private::Number":"x"}"#).is_err());
        assert!(from_slice(br#"{"nested":{"$serde_json::private::Number":"123"}}"#).is_err());
        assert!(from_slice(br#"{"\u0024serde_json::private::Number":"123"}"#).is_err());
        // The same text as a value is fine.
        assert!(from_slice(br#"{"key":"$serde_json::private::Number"}"#).is_ok());
    }

    #[test]
    fn floats_are_canonicalised_but_integers_are_exact() {
        let value =
            from_slice(br#"{"a":1e2,"b":1.00,"c":0.10,"d":-0,"e":123456789012345678901234567890}"#)
                .unwrap();
        assert_eq!(value["a"].to_string(), "100.0");
        assert_eq!(value["b"].to_string(), "1.0");
        assert_eq!(value["c"].to_string(), "0.1");
        assert_eq!(value["d"].to_string(), "0");
        assert_eq!(value["e"].to_string(), "123456789012345678901234567890");
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = from_slice(&bytes);
        }

        #[test]
        fn arbitrary_text_never_panics(text in ".{0,300}") {
            let _ = from_slice(text.as_bytes());
        }
    }
}
