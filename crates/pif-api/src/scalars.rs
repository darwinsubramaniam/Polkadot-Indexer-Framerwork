//! Custom GraphQL scalars.

use async_graphql::{InputValueError, InputValueResult, Scalar, ScalarType, Value};
use bigdecimal::BigDecimal;

/// A 128-bit integer, transported as a decimal **string**.
///
/// GraphQL's `Int` is 32-bit and JSON numbers are doubles for most clients, so a Substrate
/// balance sent as a number silently loses precision above 2^53. Every balance therefore
/// crosses the wire as a string, matching how it is stored in JSONB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigInt(pub BigDecimal);

#[Scalar(name = "BigInt")]
impl ScalarType for BigInt {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => s
                .parse::<BigDecimal>()
                .map(BigInt)
                .map_err(|e| InputValueError::custom(format!("invalid BigInt: {e}"))),
            Value::Number(n) => n
                .to_string()
                .parse::<BigDecimal>()
                .map(BigInt)
                .map_err(|e| InputValueError::custom(format!("invalid BigInt: {e}"))),
            other => Err(InputValueError::expected_type(other)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<BigDecimal> for BigInt {
    fn from(value: BigDecimal) -> Self {
        BigInt(value)
    }
}

/// Raw bytes rendered as a `0x…` hex string.
///
/// Hashes and account bytes are stored as `BYTEA`; surfacing them as hex is what makes API
/// output match block explorers and copy-pasteable into other tools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hex(pub Vec<u8>);

#[Scalar(name = "Hex")]
impl ScalarType for Hex {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(s) => {
                let stripped = s.strip_prefix("0x").unwrap_or(&s);
                hex::decode(stripped)
                    .map(Hex)
                    .map_err(|e| InputValueError::custom(format!("invalid hex: {e}")))
            }
            other => Err(InputValueError::expected_type(other)),
        }
    }

    fn to_value(&self) -> Value {
        Value::String(format!("0x{}", hex::encode(&self.0)))
    }
}

impl From<Vec<u8>> for Hex {
    fn from(value: Vec<u8>) -> Self {
        Hex(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bigint_serialises_as_a_string_not_a_number() {
        let big: BigDecimal = "100000000000000000".parse().unwrap();
        let value = BigInt(big).to_value();

        // The whole point: a client must never receive this as a JSON number.
        assert!(matches!(value, Value::String(_)), "got {value:?}");
        assert_eq!(value, Value::String("100000000000000000".into()));
    }

    #[test]
    fn bigint_round_trips_at_u128_max() {
        let max = BigDecimal::from(u128::MAX);
        let encoded = BigInt(max.clone()).to_value();
        let decoded = BigInt::parse(encoded).unwrap();

        assert_eq!(decoded.0, max);
    }

    #[test]
    fn hex_round_trips_with_and_without_prefix() {
        let bytes = vec![0xde, 0xad, 0xbe, 0xef];
        assert_eq!(
            Hex(bytes.clone()).to_value(),
            Value::String("0xdeadbeef".into())
        );

        assert_eq!(
            Hex::parse(Value::String("0xdeadbeef".into())).unwrap().0,
            bytes
        );
        assert_eq!(
            Hex::parse(Value::String("deadbeef".into())).unwrap().0,
            bytes
        );
    }

    #[test]
    fn hex_rejects_garbage() {
        assert!(Hex::parse(Value::String("0xzz".into())).is_err());
    }
}
