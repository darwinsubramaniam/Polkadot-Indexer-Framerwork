//! Turning decoded `pallet_identity` storage into rows.
//!
//! Everything here works on the JSON that [`pif_core::codec`] produces, not on typed runtime
//! structs, so a runtime upgrade that adds a field cannot break decoding — at worst a new
//! field is carried in `raw` without a column of its own.

use bigdecimal::BigDecimal;
use serde_json::Value as Json;

/// A registrar's verdict on an identity, best-first.
///
/// Only `Reasonable` and `KnownGood` mean a human checked the identity and believed it.
/// `FeePaid` merely means a judgement was *requested*, which is the trap: an unverified
/// account can display any name it likes and will sit at `FeePaid` forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Judgement {
    /// The strongest: the registrar holds proof of identity.
    KnownGood,
    /// The registrar is reasonably certain.
    Reasonable,
    /// A fee was paid; no judgement made yet.
    FeePaid,
    /// The identity is believed to contain errors.
    LowQuality,
    /// Actively wrong, and the deposit was slashed.
    Erroneous,
    /// Judged out of date.
    OutOfDate,
    /// A variant this build does not know about.
    Unknown,
}

impl Judgement {
    /// Whether this judgement means a registrar actually vouched for the identity.
    pub fn is_vouched(self) -> bool {
        matches!(self, Judgement::KnownGood | Judgement::Reasonable)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Judgement::KnownGood => "KnownGood",
            Judgement::Reasonable => "Reasonable",
            Judgement::FeePaid => "FeePaid",
            Judgement::LowQuality => "LowQuality",
            Judgement::Erroneous => "Erroneous",
            Judgement::OutOfDate => "OutOfDate",
            Judgement::Unknown => "Unknown",
        }
    }

    fn parse(name: &str) -> Self {
        match name {
            "KnownGood" => Judgement::KnownGood,
            "Reasonable" => Judgement::Reasonable,
            "FeePaid" => Judgement::FeePaid,
            "LowQuality" => Judgement::LowQuality,
            "Erroneous" => Judgement::Erroneous,
            "OutOfDate" => Judgement::OutOfDate,
            _ => Judgement::Unknown,
        }
    }
}

/// One account's identity, flattened from `IdentityOf`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IdentityRow {
    pub display: Option<String>,
    pub legal: Option<String>,
    pub web: Option<String>,
    pub email: Option<String>,
    pub twitter: Option<String>,
    pub matrix: Option<String>,
    pub github: Option<String>,
    pub discord: Option<String>,
    pub image: Option<String>,
    pub pgp_fingerprint: Option<Vec<u8>>,
    /// `[{"registrar_index": 1, "judgement": "KnownGood"}, ...]`
    pub judgements: Json,
    pub is_verified: bool,
    pub deposit: Option<BigDecimal>,
    /// The whole decoded `Registration`, so nothing is lost.
    pub raw: Json,
}

/// Strip the extra array level a `BoundedVec` carries.
///
/// `BoundedVec<T, S>` is a newtype around `Vec<T>`, and the codec deliberately preserves
/// newtype levels (see the README: newtypes and one-element `Vec`s are indistinguishable
/// after SCALE decoding, so unwrapping globally would corrupt the latter). A real People
/// chain returns an empty judgement list as `[[]]` and `Registrars` as `[[]]`; read at the
/// outer level those look like a one-element list, which yields one fabricated entry -- or,
/// for judgements, drops every real one and makes `is_verified` permanently false.
pub fn unwrap_bounded(value: &Json) -> &Json {
    match value {
        Json::Array(items) if items.len() == 1 && items[0].is_array() => &items[0],
        other => other,
    }
}

/// An integer out of decoded SCALE.
///
/// [`pif_core::codec`] renders **every** integer primitive as a JSON *string*, because
/// `scale_value` widens them all to `u128` and JSON numbers silently round above 2^53. So a
/// `u32` registrar index arrives as `"0"`, not `0`, and `as_u64()` alone finds nothing.
pub fn u64_from_json(value: &Json) -> Option<u64> {
    match value {
        Json::Number(n) => n.as_u64(),
        Json::String(s) => s.parse().ok(),
        Json::Array(items) if items.len() == 1 => u64_from_json(&items[0]),
        _ => None,
    }
}

/// Decode a `Registration` (the value of `Identity::IdentityOf`).
///
/// Returns `None` only if the value is not an object at all — a shape this handler has no
/// business guessing at. Missing *fields* are normal and become `None`.
pub fn parse_registration(raw: &Json) -> Option<IdentityRow> {
    // Some runtime versions typed `IdentityOf` as `(Registration, Option<Username>)` rather
    // than a bare `Registration`, which decodes as a two-element array. Unwrapping it matters:
    // treating the tuple form as unparseable would read as "this account has no identity" and
    // silently wipe every identity on a chain running that runtime.
    if let Json::Array(items) = raw
        && let Some(first) = items.first()
        && first.is_object()
    {
        return parse_registration(first);
    }

    if !raw.is_object() {
        return None;
    }

    // The `IdentityInfo` lives under `info` on every runtime that has shipped this pallet,
    // but fall back to the top level so a restructured runtime degrades to "no fields"
    // rather than to a panic.
    let info = raw.get("info").unwrap_or(raw);

    let (judgements, is_verified) = parse_judgements(raw.get("judgements"));

    Some(IdentityRow {
        display: data_to_string(info.get("display")),
        legal: data_to_string(info.get("legal")),
        web: data_to_string(info.get("web")),
        email: data_to_string(info.get("email")),
        twitter: data_to_string(info.get("twitter")),
        matrix: data_to_string(info.get("matrix").or_else(|| info.get("riot"))),
        github: data_to_string(info.get("github")),
        discord: data_to_string(info.get("discord")),
        image: data_to_string(info.get("image")),
        pgp_fingerprint: info.get("pgp_fingerprint").and_then(bytes_from_json),
        judgements,
        is_verified,
        deposit: raw.get("deposit").and_then(big_decimal_from_json),
        raw: raw.clone(),
    })
}

/// Flatten `Vec<(RegistrarIndex, Judgement)>` and decide whether any of them vouches.
fn parse_judgements(raw: Option<&Json>) -> (Json, bool) {
    let Some(Json::Array(items)) = raw.map(unwrap_bounded) else {
        return (Json::Array(Vec::new()), false);
    };

    let mut out = Vec::with_capacity(items.len());
    let mut vouched = false;

    for item in items {
        // A tuple decodes as a two-element array: index, then the judgement enum.
        let Some(pair) = item.as_array() else {
            continue;
        };
        if pair.len() < 2 {
            continue;
        }

        let index = u64_from_json(&pair[0]);
        let name = variant_name(&pair[1]).unwrap_or("Unknown");
        let judgement = Judgement::parse(name);
        vouched |= judgement.is_vouched();

        out.push(serde_json::json!({
            "registrar_index": index,
            "judgement": judgement.as_str(),
        }));
    }

    (Json::Array(out), vouched)
}

/// The name of a SCALE enum variant as [`pif_core::codec`] renders it: a single-key object.
///
/// `FeePaid` carries a balance so it is `{"FeePaid": "100"}`; `KnownGood` carries nothing so
/// it is `{"KnownGood": []}`. Both are one key, which is all this needs.
fn variant_name(value: &Json) -> Option<&str> {
    match value {
        Json::String(s) => Some(s.as_str()),
        Json::Object(map) if map.len() == 1 => map.keys().next().map(String::as_str),
        _ => None,
    }
}

/// Decode a `Data` field (display name, twitter handle, ...) into text.
///
/// `Data` is a SCALE enum: `None`, `Raw0..Raw32` holding the bytes directly, or one of four
/// hash variants holding a 32-byte digest. Raw bytes are the overwhelmingly common case and
/// are what a human wants to see; a hash cannot be turned back into text, so it is rendered
/// as hex rather than dropped — "this field is set but opaque" is different from "unset".
pub fn data_to_string(value: Option<&Json>) -> Option<String> {
    let value = value?;

    let (variant, payload) = match value {
        // Already collapsed to hex by the codec's byte-sequence rule.
        Json::String(s) => return decode_raw_hex(s),
        Json::Object(map) if map.len() == 1 => {
            let (k, v) = map.iter().next()?;
            (k.as_str(), v)
        }
        _ => return None,
    };

    if variant == "None" {
        return None;
    }

    if let Some(rest) = variant.strip_prefix("Raw") {
        // `Raw0` is a set-but-empty field; treat it as unset rather than as "".
        if rest == "0" {
            return None;
        }
        return payload_to_text(payload);
    }

    // BlakeTwo256 / Sha256 / Keccak256 / ShaThree256 — keep the digest, flagged as such.
    let hex = payload_to_hex(payload)?;
    Some(format!("{variant}:{hex}"))
}

/// `Raw*` payloads arrive either as a hex string (the codec collapses >=16 bytes) or as a
/// short array of byte values.
fn payload_to_text(payload: &Json) -> Option<String> {
    match payload {
        Json::String(s) => decode_raw_hex(s),
        Json::Array(items) => {
            // A newtype wrapper adds one level; unwrap it before treating this as bytes.
            if items.len() == 1 && (items[0].is_string() || items[0].is_array()) {
                return payload_to_text(&items[0]);
            }
            let bytes = bytes_from_array(items)?;
            Some(String::from_utf8_lossy(&bytes).into_owned())
        }
        _ => None,
    }
}

fn decode_raw_hex(s: &str) -> Option<String> {
    let bytes = hex::decode(s.strip_prefix("0x")?).ok()?;
    if bytes.is_empty() {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

fn payload_to_hex(payload: &Json) -> Option<String> {
    match payload {
        Json::String(s) => Some(s.trim_start_matches("0x").to_owned()),
        Json::Array(items) => {
            if items.len() == 1 {
                return payload_to_hex(&items[0]);
            }
            bytes_from_array(items).map(hex::encode)
        }
        _ => None,
    }
}

fn bytes_from_array(items: &[Json]) -> Option<Vec<u8>> {
    items
        .iter()
        .map(|v| u64_from_json(v).and_then(|n| u8::try_from(n).ok()))
        .collect()
}

/// Raw bytes out of a field the codec may have rendered as hex or as an array.
pub fn bytes_from_json(value: &Json) -> Option<Vec<u8>> {
    match value {
        Json::String(s) => hex::decode(s.strip_prefix("0x").unwrap_or(s)).ok(),
        Json::Array(items) => {
            if items.len() == 1 && !items[0].is_number() {
                return bytes_from_json(&items[0]);
            }
            bytes_from_array(items)
        }
        _ => None,
    }
}

/// `u128` arrives as a JSON *string* — see the codec's module docs. Parsing it as a number
/// would silently round anything above 2^53.
pub fn big_decimal_from_json(value: &Json) -> Option<BigDecimal> {
    match value {
        Json::String(s) => s.parse().ok(),
        Json::Number(n) => n.to_string().parse().ok(),
        Json::Array(items) if items.len() == 1 => big_decimal_from_json(&items[0]),
        _ => None,
    }
}

/// A username is a `BoundedVec<u8>`, so it arrives as hex or a byte array; it is ASCII by
/// construction (`alice.dot`).
pub fn username_to_string(value: &Json) -> Option<String> {
    let bytes = bytes_from_json(value)?;
    if bytes.is_empty() {
        return None;
    }
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// "Alice" as the codec renders a short `Raw5`: too few bytes to collapse to hex.
    fn raw_alice() -> Json {
        json!({ "Raw5": [65, 108, 105, 99, 101] })
    }

    #[test]
    fn decodes_a_raw_display_name() {
        assert_eq!(data_to_string(Some(&raw_alice())), Some("Alice".into()));
    }

    #[test]
    fn decodes_a_display_name_the_codec_collapsed_to_hex() {
        // 16+ bytes collapse to a hex string, which must still decode to text.
        let name = "Parity Technologies";
        let value = json!({ "Raw19": format!("0x{}", hex::encode(name)) });
        assert_eq!(data_to_string(Some(&value)), Some(name.into()));
    }

    #[test]
    fn decodes_a_short_name_whose_bytes_arrived_as_strings() {
        // The exact shape a live People chain produced for "Bob Builder":
        //   {"Raw11": [["66", "111", "98", ...]]}
        // The codec stringifies every integer (u128 precision), and byte runs shorter than
        // 16 are not collapsed to hex -- so `as_u64()` alone finds nothing and the display
        // name silently vanishes. Longer names happened to work, which is what made this
        // hide: "Alice Wonderland" is 16 bytes and collapses to hex.
        let value = json!({ "Raw11": [["66", "111", "98", "32", "66", "117", "105",
                                       "108", "100", "101", "114"]] });
        assert_eq!(data_to_string(Some(&value)), Some("Bob Builder".into()));
    }

    #[test]
    fn integers_decode_whether_they_arrive_as_numbers_or_strings() {
        assert_eq!(u64_from_json(&json!("42")), Some(42));
        assert_eq!(u64_from_json(&json!(42)), Some(42));
        assert_eq!(u64_from_json(&json!(["42"])), Some(42));
        assert_eq!(u64_from_json(&json!("not a number")), None);
    }

    #[test]
    fn judgements_survive_the_bounded_vec_level() {
        // A live chain returns an empty judgement list as `[[]]`. Read at the outer level a
        // real list looks like ONE element, every judgement is skipped, and `is_verified`
        // is permanently false -- the exact opposite of what this handler exists to report.
        let reg = json!({
            "info": {},
            "judgements": [[[ "0", { "KnownGood": [] } ]]],
        });

        let row = parse_registration(&reg).unwrap();
        assert!(
            row.is_verified,
            "a vouched judgement must survive the newtype level"
        );
        assert_eq!(row.judgements[0]["registrar_index"], 0);
    }

    #[test]
    fn an_empty_bounded_judgement_list_is_simply_unverified() {
        // Observed live: `"judgements": [[]]`.
        let reg = json!({ "info": {}, "judgements": [[]] });
        let row = parse_registration(&reg).unwrap();

        assert!(!row.is_verified);
        assert_eq!(row.judgements, json!([]), "no fabricated judgement entries");
    }

    #[test]
    fn treats_none_and_raw0_as_unset() {
        // `Raw0` is "set to the empty string", which for a display name is not a name.
        assert_eq!(data_to_string(Some(&json!({ "None": [] }))), None);
        assert_eq!(data_to_string(Some(&json!({ "Raw0": [] }))), None);
        assert_eq!(data_to_string(None), None);
    }

    #[test]
    fn keeps_hashed_fields_as_flagged_hex_rather_than_garbage() {
        // A hash cannot become text. Rendering it lossily would invent a name that is not
        // there; dropping it would lose the fact that the field is set at all.
        let value = json!({ "BlakeTwo256": "0xaabb" });
        assert_eq!(
            data_to_string(Some(&value)),
            Some("BlakeTwo256:aabb".into())
        );
    }

    #[test]
    fn a_vouched_judgement_marks_the_identity_verified() {
        let reg = json!({
            "info": { "display": raw_alice() },
            "judgements": [[[ "0", { "Reasonable": [] } ]]],
            "deposit": "1000"
        });

        let row = parse_registration(&reg).expect("well-formed registration");
        assert_eq!(row.display.as_deref(), Some("Alice"));
        assert!(row.is_verified);
        assert_eq!(
            row.judgements,
            json!([{"registrar_index": 0, "judgement": "Reasonable"}])
        );
    }

    #[test]
    fn fee_paid_alone_is_not_verification() {
        // The important negative: a fee was paid, nobody checked anything. An account can
        // sit here forever displaying any name it likes.
        let reg = json!({
            "info": { "display": raw_alice() },
            "judgements": [[[ "0", { "FeePaid": "500" } ]]],
        });

        let row = parse_registration(&reg).unwrap();
        assert!(!row.is_verified, "FeePaid must not count as verified");
        assert_eq!(row.judgements[0]["judgement"], "FeePaid");
    }

    #[test]
    fn mixed_judgements_are_verified_if_any_vouches() {
        let reg = json!({
            "info": {},
            "judgements": [[[ "0", { "Erroneous": [] } ], [ "1", { "KnownGood": [] } ]]],
        });
        assert!(parse_registration(&reg).unwrap().is_verified);
    }

    #[test]
    fn an_unknown_future_judgement_is_recorded_but_never_counts_as_verified() {
        // Failing open here would let a runtime upgrade silently mark everyone verified.
        let reg = json!({ "info": {}, "judgements": [[[ "0", { "SomethingNew": [] } ]]] });
        let row = parse_registration(&reg).unwrap();
        assert!(!row.is_verified);
        assert_eq!(row.judgements[0]["judgement"], "Unknown");
    }

    #[test]
    fn deposit_keeps_full_u128_precision() {
        let huge = "340282366920938463463374607431768211455";
        let reg = json!({ "info": {}, "judgements": [], "deposit": huge });
        assert_eq!(
            parse_registration(&reg)
                .unwrap()
                .deposit
                .unwrap()
                .to_string(),
            huge
        );
    }

    #[test]
    fn registration_keeps_the_full_raw_value() {
        // Forward compatibility: a field we have no column for must still be recorded.
        let reg =
            json!({ "info": { "display": raw_alice(), "future_field": "x" }, "judgements": [] });
        let row = parse_registration(&reg).unwrap();
        assert_eq!(row.raw["info"]["future_field"], "x");
    }

    #[test]
    fn a_non_object_registration_is_rejected_rather_than_guessed() {
        assert!(parse_registration(&json!("nonsense")).is_none());
        assert!(parse_registration(&json!([1, 2, 3])).is_none());
    }

    #[test]
    fn accepts_the_tuple_shaped_identity_of_some_runtimes_used() {
        // `IdentityOf: (Registration, Option<Username>)`. Rejecting this shape would read as
        // "no identity" and wipe every account on a chain running that runtime.
        let tuple = json!([
            { "info": { "display": raw_alice() }, "judgements": [[[ "0", { "KnownGood": [] } ]]] },
            null
        ]);

        let row = parse_registration(&tuple).expect("tuple form must parse");
        assert_eq!(row.display.as_deref(), Some("Alice"));
        assert!(row.is_verified);
    }

    #[test]
    fn decodes_a_username() {
        let value = json!(format!("0x{}", hex::encode("alice.dot")));
        assert_eq!(username_to_string(&value), Some("alice.dot".into()));
        assert_eq!(username_to_string(&json!([])), None);
    }
}
