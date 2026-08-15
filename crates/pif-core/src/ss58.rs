//! SS58 address encoding.
//!
//! Account bytes are stored as SS58 text rather than raw bytes so that addresses in the
//! database match what block explorers and wallets show. The prefix is per-chain (Polkadot
//! 0, Kusama 2, generic Substrate 42), discovered from the node's `system_properties`, so
//! the same account renders correctly for whichever chain it came from.
//!
//! Format: `base58( prefix_bytes || payload || checksum[0..2] )`, where the checksum is
//! `blake2b_512("SS58PRE" || prefix_bytes || payload)`.

use blake2::{Blake2b512, Digest};

const CHECKSUM_PREFIX: &[u8] = b"SS58PRE";
const CHECKSUM_LEN: usize = 2;

/// The generic Substrate prefix, used when a chain does not advertise its own.
pub const DEFAULT_PREFIX: u16 = 42;

/// Encode raw account bytes (typically 32) as an SS58 address for the given network prefix.
pub fn encode(account: &[u8], prefix: u16) -> String {
    let mut body = encode_prefix(prefix);
    body.extend_from_slice(account);

    let mut hasher = Blake2b512::new();
    hasher.update(CHECKSUM_PREFIX);
    hasher.update(&body);
    let checksum = hasher.finalize();

    body.extend_from_slice(&checksum[..CHECKSUM_LEN]);
    bs58::encode(body).into_string()
}

/// Recover the raw account bytes from an SS58 address.
///
/// The checksum is verified, so a mistyped address fails rather than silently resolving to a
/// different but structurally valid account. The network prefix is *not* checked: the same key
/// is written differently per chain, and refusing a Kusama-formatted address for a Polkadot
/// query would reject an address that names exactly the right account.
pub fn decode(address: &str) -> Option<Vec<u8>> {
    let raw = bs58::decode(address).into_vec().ok()?;

    // 1- or 2-byte network prefix, then 32-byte payload and 2-byte checksum.
    let prefix_len = if raw.len() == 32 + CHECKSUM_LEN + 1 {
        1
    } else {
        2
    };
    if raw.len() != prefix_len + 32 + CHECKSUM_LEN {
        return None;
    }

    let (body, checksum) = raw.split_at(prefix_len + 32);

    let mut hasher = Blake2b512::new();
    hasher.update(CHECKSUM_PREFIX);
    hasher.update(body);
    let expected = hasher.finalize();

    if expected[..CHECKSUM_LEN] != *checksum {
        return None;
    }

    Some(body[prefix_len..].to_vec())
}

/// Render a dynamically-decoded account field as SS58.
///
/// `AccountId32` is a newtype around `[u8; 32]`, so [`crate::codec`] yields a one-element
/// array wrapping the hex string rather than a bare string. That extra level is not unwrapped
/// globally — newtypes and one-element `Vec`s are indistinguishable after SCALE decoding, so
/// unwrapping everywhere would silently turn single-element lists into scalars. Callers that
/// *know* a field is an account use this, where the unwrap is safe.
///
/// Returns `None` for anything that is not 32 bytes of hex, so an unexpected shape is a
/// visible failure rather than a plausible-looking wrong address.
pub fn decode_account(value: &serde_json::Value, prefix: u16) -> Option<String> {
    account_bytes(value).map(|bytes| encode(&bytes, prefix))
}

/// The raw 32 bytes behind a decoded account field.
///
/// Separate from [`decode_account`] because callers that build storage keys need the bytes,
/// not the text, and round-tripping through SS58 to get them back would be wasteful and
/// would need a decoder that can fail.
pub fn account_bytes(value: &serde_json::Value) -> Option<Vec<u8>> {
    let hex_str = match value {
        serde_json::Value::String(s) => s.as_str(),
        serde_json::Value::Array(items) if items.len() == 1 => items[0].as_str()?,
        _ => return None,
    };

    let bytes = hex::decode(hex_str.strip_prefix("0x")?).ok()?;
    if bytes.len() != 32 {
        return None;
    }

    Some(bytes)
}

/// SS58 uses a one-byte prefix below 64, and a two-byte encoded form above that.
fn encode_prefix(prefix: u16) -> Vec<u8> {
    if prefix < 64 {
        vec![prefix as u8]
    } else {
        // Upper 2 bits of the first byte are the tag `01`; the remaining 14 bits hold the
        // prefix, low 6 bits first.
        let low = ((prefix & 0b0000_0000_1111_1100) as u8) >> 2 | 0b0100_0000;
        let high = ((prefix >> 8) as u8) | ((prefix & 0b0000_0000_0000_0011) as u8) << 6;
        vec![low, high]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Alice's well-known sr25519 public key, present on every Substrate dev chain:
    /// `0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d`
    const ALICE: [u8; 32] = [
        0xd4, 0x35, 0x93, 0xc7, 0x15, 0xfd, 0xd3, 0x1c, 0x61, 0x14, 0x1a, 0xbd, 0x04, 0xa9, 0x9f,
        0xd6, 0x82, 0x2c, 0x85, 0x58, 0x85, 0x4c, 0xcd, 0xe3, 0x9a, 0x56, 0x84, 0xe7, 0xa5, 0x6d,
        0xa2, 0x7d,
    ];

    #[test]
    fn encodes_alice_for_generic_substrate() {
        // These are the canonical published addresses for Alice; if this test fails the
        // checksum or prefix encoding is wrong, not the expectation.
        assert_eq!(
            encode(&ALICE, 42),
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        );
    }

    #[test]
    fn encodes_alice_for_polkadot_and_kusama() {
        assert_eq!(
            encode(&ALICE, 0),
            "15oF4uVJwmo4TdGW7VfQxNLavjCXviqxT9S1MgbjMNHr6Sp5"
        );
        assert_eq!(
            encode(&ALICE, 2),
            "HNZata7iMYWmk5RvZRTiAsSDhV8366zq2YGb3tLH5Upf74F"
        );
    }

    #[test]
    fn same_account_differs_per_network() {
        // The whole reason the prefix is per-chain: one keypair, different text per chain.
        let polkadot = encode(&ALICE, 0);
        let substrate = encode(&ALICE, DEFAULT_PREFIX);
        assert_ne!(polkadot, substrate);
    }

    #[test]
    fn decode_round_trips_every_prefix_form() {
        for prefix in [0, 2, 42, 128] {
            assert_eq!(
                decode(&encode(&ALICE, prefix)).as_deref(),
                Some(&ALICE[..]),
                "prefix {prefix} did not round-trip"
            );
        }
    }

    #[test]
    fn decode_rejects_a_corrupted_address() {
        // Without the checksum check, a typo would resolve to a different valid-looking
        // account and the caller would query the wrong wallet with no error.
        let address = encode(&ALICE, 0);
        let mut chars: Vec<char> = address.chars().collect();
        chars[5] = if chars[5] == 'a' { 'b' } else { 'a' };

        assert!(decode(&chars.into_iter().collect::<String>()).is_none());
    }

    #[test]
    fn decode_rejects_input_that_is_not_an_address() {
        assert!(decode("").is_none());
        assert!(decode("not-base58-0OIl").is_none());
        assert!(decode("abc").is_none());
    }

    #[test]
    fn handles_two_byte_prefixes() {
        // Prefixes >= 64 use the two-byte form; this must not panic or truncate.
        let addr = encode(&ALICE, 128);
        assert!(!addr.is_empty());
        assert_ne!(addr, encode(&ALICE, 42));
    }
}
