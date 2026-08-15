//! Conversion from dynamically-decoded SCALE values into JSON.
//!
//! This is the single place where decoded chain data becomes storable JSON, and it exists
//! because the obvious conversion is wrong in two ways that only show up in production:
//!
//! 1. **Integers.** Substrate balances are `u128`. JSON numbers are IEEE-754 doubles for
//!    most consumers, so anything above 2^53 silently loses precision the moment a
//!    JavaScript client (or GraphQL layer) touches it. Every `u128`/`i128`/`u256`/`i256`
//!    is therefore rendered as a **string**.
//! 2. **Byte sequences.** An `AccountId32` decodes as an unnamed composite of 32 `u8`
//!    values. Stored naively that is a 32-element JSON array, which is both bulky and
//!    useless to query. We collapse all-byte sequences to a `0x…` hex string.

use scale_value::{Composite, Primitive, Value, ValueDef};
use serde_json::{Map, Value as Json};

/// Convert a dynamically decoded [`Composite`] (a set of call arguments or event fields)
/// into JSON.
///
/// Named fields become a JSON object; unnamed fields become a JSON array. This mirrors how
/// the pallet actually declares them, so `events.fields->>'amount'` works for a pallet with
/// named fields and `events.fields->0` works for one with positional fields.
pub fn composite_to_json(composite: &Composite<()>) -> Json {
    match composite {
        Composite::Named(fields) => {
            let mut map = Map::with_capacity(fields.len());
            for (name, value) in fields {
                map.insert(name.clone(), value_to_json(value));
            }
            Json::Object(map)
        }
        Composite::Unnamed(values) => {
            // A tuple/array of nothing but bytes is far more useful as hex.
            if let Some(hex) = bytes_to_hex(values) {
                return Json::String(hex);
            }
            Json::Array(values.iter().map(value_to_json).collect())
        }
    }
}

/// Convert a single dynamically decoded [`Value`] into JSON.
pub fn value_to_json(value: &Value<()>) -> Json {
    match &value.value {
        ValueDef::Composite(composite) => composite_to_json(composite),

        ValueDef::Variant(variant) => {
            // `Option` and `Result` are enums in SCALE but have natural JSON shapes, and
            // they appear constantly in call arguments. Special-casing them keeps stored
            // data readable instead of `{"None": {}}`.
            match (variant.name.as_str(), &variant.values) {
                ("None", _) => return Json::Null,
                ("Some", Composite::Unnamed(inner)) if inner.len() == 1 => {
                    return value_to_json(&inner[0]);
                }
                _ => {}
            }

            // Every other enum keeps its discriminant, otherwise the variant name is lost
            // and the data becomes ambiguous.
            let mut map = Map::with_capacity(1);
            map.insert(variant.name.clone(), composite_to_json(&variant.values));
            Json::Object(map)
        }

        ValueDef::Primitive(primitive) => primitive_to_json(primitive),

        // Bit sequences (used by `BitVec` fields) have no natural JSON analogue; a list of
        // booleans is the least lossy representation.
        ValueDef::BitSequence(bits) => Json::Array(bits.iter().map(Json::Bool).collect()),
    }
}

fn primitive_to_json(primitive: &Primitive) -> Json {
    match primitive {
        Primitive::Bool(b) => Json::Bool(*b),
        Primitive::Char(c) => Json::String(c.to_string()),
        Primitive::String(s) => Json::String(s.clone()),

        // See the module docs: these are stringified deliberately. Do not "simplify" this
        // into a JSON number — it will corrupt balances above 2^53.
        Primitive::U128(n) => Json::String(n.to_string()),
        Primitive::I128(n) => Json::String(n.to_string()),
        Primitive::U256(bytes) | Primitive::I256(bytes) => {
            Json::String(format!("0x{}", hex::encode(bytes)))
        }
    }
}

/// Shortest byte sequence we are willing to collapse into a hex string.
///
/// After dynamic decoding, `[u8; 32]` and `(u8, u8)` are indistinguishable — both are an
/// unnamed composite of small integers. A length threshold is the only signal available.
/// 16 keeps the things that are genuinely opaque blobs (H160 addresses at 20, `AccountId32`
/// and hashes at 32, signatures at 64) while leaving short tuples of real numbers alone.
const MIN_HEX_BYTES: usize = 16;

/// If the slice looks like an opaque byte blob, render it as a `0x…` hex string. Returns
/// `None` otherwise, in which case the caller keeps it as a JSON array.
///
/// This is what turns a 32-element array of `u8` (an `AccountId32`, a hash, a signature)
/// into something a human can read and an index can serve.
fn bytes_to_hex(values: &[Value<()>]) -> Option<String> {
    if values.len() < MIN_HEX_BYTES {
        return None;
    }

    let mut bytes = Vec::with_capacity(values.len());
    for value in values {
        let ValueDef::Primitive(Primitive::U128(n)) = &value.value else {
            return None;
        };
        bytes.push(u8::try_from(*n).ok()?);
    }

    Some(format!("0x{}", hex::encode(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u128_value(n: u128) -> Value<()> {
        Value {
            value: ValueDef::Primitive(Primitive::U128(n)),
            context: (),
        }
    }

    #[test]
    fn large_balances_are_stringified_not_truncated() {
        // 10_000_000 DOT in Planck — comfortably past 2^53, where a JSON number would
        // start silently rounding.
        let big = 100_000_000_000_000_000_u128;
        assert!(big > (1u128 << 53));

        let json = value_to_json(&u128_value(big));

        assert_eq!(json, Json::String("100000000000000000".to_string()));
        // Round-tripping through a JSON number would lose the exact value; a string cannot.
        assert_eq!(json.as_str().unwrap().parse::<u128>().unwrap(), big);
    }

    #[test]
    fn u128_max_survives_round_trip() {
        let json = value_to_json(&u128_value(u128::MAX));
        assert_eq!(json.as_str().unwrap().parse::<u128>().unwrap(), u128::MAX);
    }

    #[test]
    fn byte_arrays_collapse_to_hex() {
        // An AccountId32-shaped value: 32 unnamed bytes.
        let account = Composite::Unnamed((0..32).map(|i| u128_value(i as u128)).collect());

        let json = composite_to_json(&account);

        let hex = json
            .as_str()
            .expect("32 bytes should render as a hex string");
        assert!(hex.starts_with("0x"));
        assert_eq!(hex.len(), 2 + 64);
        assert!(hex.starts_with("0x000102030405"));
    }

    #[test]
    fn oversized_values_are_not_mistaken_for_bytes() {
        // 256 does not fit in a u8, so even at blob length this is a list of numbers.
        let mut values: Vec<_> = (0..MIN_HEX_BYTES).map(|_| u128_value(1)).collect();
        values.push(u128_value(256));

        let json = composite_to_json(&Composite::Unnamed(values));

        assert!(json.is_array(), "expected an array, got {json}");
        assert_eq!(
            json.as_array().unwrap().last().unwrap(),
            &Json::String("256".into())
        );
    }

    #[test]
    fn short_tuples_stay_arrays() {
        // A (u8, u8) tuple is indistinguishable from a 2-byte blob after decoding; the
        // length threshold is what stops us mangling small numeric tuples into hex.
        let json = composite_to_json(&Composite::Unnamed(vec![u128_value(1), u128_value(2)]));

        assert_eq!(
            json,
            Json::Array(vec![Json::String("1".into()), Json::String("2".into()),])
        );
    }

    #[test]
    fn named_fields_become_queryable_objects() {
        let fields = Composite::Named(vec![
            ("amount".to_string(), u128_value(42)),
            (
                "reason".to_string(),
                Value {
                    value: ValueDef::Primitive(Primitive::String("fee".into())),
                    context: (),
                },
            ),
        ]);

        let json = composite_to_json(&fields);

        assert_eq!(json["amount"], Json::String("42".into()));
        assert_eq!(json["reason"], Json::String("fee".into()));
    }

    #[test]
    fn option_none_becomes_null_and_some_unwraps() {
        let none = Value {
            value: ValueDef::Variant(scale_value::Variant {
                name: "None".into(),
                values: Composite::Unnamed(vec![]),
            }),
            context: (),
        };
        assert_eq!(value_to_json(&none), Json::Null);

        let some = Value {
            value: ValueDef::Variant(scale_value::Variant {
                name: "Some".into(),
                values: Composite::Unnamed(vec![u128_value(7)]),
            }),
            context: (),
        };
        assert_eq!(value_to_json(&some), Json::String("7".into()));
    }

    #[test]
    fn other_variants_keep_their_name() {
        let variant = Value {
            value: ValueDef::Variant(scale_value::Variant {
                name: "Signed".into(),
                values: Composite::Unnamed(vec![u128_value(1)]),
            }),
            context: (),
        };

        let json = value_to_json(&variant);

        // The variant name is preserved as the key; its payload stays a faithful array.
        assert_eq!(json["Signed"], Json::Array(vec![Json::String("1".into())]));
    }
}
