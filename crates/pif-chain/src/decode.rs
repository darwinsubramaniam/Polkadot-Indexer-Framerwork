//! Dynamic decoding of a block into storable rows.
//!
//! Nothing here is compiled against a specific runtime. Pallet names, call names, event
//! names and field shapes all come from the metadata the node reports *for that block*, so
//! the same code indexes any Substrate chain and keeps working across runtime upgrades.

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use pif_core::{ChainInfo, codec, ss58};
use pif_db::{BlockData, NewBlock, NewEvent, NewExtrinsic};
use scale_value::Composite;
use subxt::{
    OnlineClient, PolkadotConfig,
    client::{ClientAtBlock, OnlineClientAtBlockImpl},
    events::Phase,
};

use crate::error::{ChainError, Result};

/// Subxt's view of a chain at one block, for the concrete config we support.
pub type AtBlock = ClientAtBlock<PolkadotConfig, OnlineClientAtBlockImpl<PolkadotConfig>>;

/// A decoded block plus the live event handles the typed overlay needs.
pub struct DecodedBlock {
    pub data: BlockData,
    /// `(event_index, pallet, variant)` for each event, so handlers can be dispatched
    /// without re-fetching the block.
    pub event_index: Vec<(u32, String, String)>,
}

/// Resolve a block number to subxt's view of the chain at that block.
///
/// Separate from [`decode_block`] because state-reading callers (a handler's bootstrap sweep)
/// need the block handle without paying to decode every extrinsic and event in it.
pub async fn at_block(
    client: &OnlineClient<PolkadotConfig>,
    chain: &ChainInfo,
    number: u64,
) -> Result<AtBlock> {
    client.at_block(number).await.map_err(|source| {
        if is_pruned_state(&source) {
            ChainError::PrunedState {
                chain: chain.id.clone(),
                number,
            }
        } else {
            ChainError::BlockRead {
                number,
                source: Box::new(source),
            }
        }
    })
}

/// Subxt's view of the chain at the block the finality subscription just delivered.
///
/// The stream hands back a pinned block reference, so this needs no number-to-hash lookup —
/// which is what makes it the only usable entry point under a light client.
pub async fn at_streamed_block(block: &subxt::client::Block<PolkadotConfig>) -> Result<AtBlock> {
    block.at().await.map_err(|source| ChainError::BlockRead {
        number: block.number(),
        source: Box::new(source),
    })
}

/// Subxt's view of the chain at the current finalized block.
///
/// The only block a light client can name without being told about it first.
pub async fn at_current_block(
    client: &OnlineClient<PolkadotConfig>,
    chain: &ChainInfo,
) -> Result<AtBlock> {
    client
        .at_current_block()
        .await
        .map_err(|source| ChainError::CurrentBlock {
            chain: chain.id.clone(),
            source: Box::new(source),
        })
}

/// Fetch and decode a single block.
pub async fn decode_block(
    client: &OnlineClient<PolkadotConfig>,
    rpc: &crate::client::Rpc,
    chain: &ChainInfo,
    number: u64,
) -> Result<(AtBlock, BlockData)> {
    let at = at_block(client, chain, number).await?;
    let data = decode_at(client, rpc, &at, chain).await?;
    Ok((at, data))
}

/// The runtime that **executed** a block, which is the one at its *parent*.
///
/// Not a subtlety — the difference is load-bearing at exactly one block per runtime upgrade,
/// and getting it wrong stops the indexer dead.
///
/// A block carrying `System::set_code` is executed by the *old* runtime: Substrate defers the
/// swap so the new code takes effect from the next block. Its events and extrinsics are
/// therefore encoded against the old metadata. But that block's post-state already holds the
/// new `:code`, so the node answers `state_getRuntimeVersion(this block)` — and
/// `state_getMetadata` — with the *new* runtime. Decoding it with the metadata the node
/// reports for it fails with "Can't decode event topics: Not enough data to fill buffer".
///
/// Resolving at the parent is uniform: away from an upgrade the two are the same runtime, so
/// this changes nothing; at an upgrade it is the only correct answer. Genesis has no parent
/// and executed nothing, so it stands for itself.
///
/// Costs one extra runtime-version lookup per block. Subxt caches metadata per spec version
/// on the client, so the expensive part is paid once per runtime rather than once per block.
async fn executing_runtime(
    client: &OnlineClient<PolkadotConfig>,
    chain: &ChainInfo,
    at: &AtBlock,
    parent_hash: subxt::config::HashFor<PolkadotConfig>,
) -> Result<AtBlock> {
    if at.block_number() == 0 {
        return Ok(at.clone());
    }

    client
        .at_block(parent_hash)
        .await
        .map_err(|source| ChainError::BlockRead {
            number: at.block_number().saturating_sub(1),
            source: Box::new(source),
        })
        .map_err(|e| match e {
            // A pruned parent means the runtime that executed this block is unknowable from
            // this node. Say so precisely rather than as a generic read failure.
            ChainError::BlockRead { number, source } if is_pruned_state(&*source) => {
                ChainError::PrunedState {
                    chain: chain.id.clone(),
                    number,
                }
            }
            other => other,
        })
}

/// Recognise the node's "state has been pruned" response.
///
/// Matched on the message rather than a code because the relevant error is wrapped several
/// layers deep (subxt -> backend -> RPC -> client) by the time it surfaces, and the JSON-RPC
/// code is not preserved through that chain. A false negative merely yields the original
/// less-helpful error, so this is a safe heuristic.
pub(crate) fn is_pruned_state(error: &dyn std::error::Error) -> bool {
    let text = error.to_string().to_lowercase();
    text.contains("state already discarded") || text.contains("unknown block")
}

/// Decode an already-resolved block.
///
/// Takes the client as well as the block because the metadata that decodes a block is not
/// always the metadata the node reports *for* that block — see [`executing_runtime`].
pub async fn decode_at(
    client: &OnlineClient<PolkadotConfig>,
    rpc: &crate::client::Rpc,
    at: &AtBlock,
    chain: &ChainInfo,
) -> Result<BlockData> {
    let number = at.block_number();

    let header = at
        .block_header()
        .await
        .map_err(|source| ChainError::BlockRead {
            number,
            source: Box::new(source),
        })?;

    // Everything below decodes through `runtime`, never through `at`.
    let runtime = executing_runtime(client, chain, at, header.parent_hash).await?;
    let spec_version = runtime.spec_version();

    // Events: take the raw blob from this block and decode it with the executing runtime.
    // `Events::bytes()` does no decoding, so this is safe on every transport — and it is the
    // half that actually breaks in practice, since the event enum layout is what an upgrade
    // most often reshapes.
    let event_bytes = at
        .events()
        .fetch()
        .await
        .map_err(|source| ChainError::Decode {
            number,
            source: Box::new(source),
        })?
        .bytes()
        .to_vec();

    let events = runtime.events().from_bytes(event_bytes);

    // Extrinsics: away from an upgrade the block and its parent share a runtime, so the
    // ordinary path is unchanged and costs nothing extra. Only at an upgrade block do the two
    // differ, and only there is the raw body needed — subxt exposes no way to read the bytes
    // back out of an `Extrinsics` without first decoding them, which is precisely what cannot
    // be done here with the wrong metadata.
    let extrinsics = if runtime.spec_version() == at.spec_version() {
        at.extrinsics()
            .fetch()
            .await
            .map_err(|source| ChainError::Decode {
                number,
                source: Box::new(source),
            })?
    } else {
        let body = rpc
            .chain_get_block(Some(at.block_hash()))
            .await
            .map_err(|source| ChainError::BlockRead {
                number,
                source: Box::new(source),
            })?
            .ok_or(ChainError::UpgradeBlockBodyUnavailable {
                chain: chain.id.clone(),
                number,
            })?
            .block
            .extrinsics
            .into_iter()
            .map(|bytes| bytes.0)
            .collect();

        runtime.extrinsics().from_bytes(body).await
    };

    // Decode events first: they carry the per-extrinsic outcome and fee, which the
    // extrinsic rows need.
    let mut new_events = Vec::with_capacity(events.len() as usize);
    let mut outcomes: Vec<ExtrinsicOutcome> = Vec::new();

    for event in events.iter() {
        let event = event.map_err(|source| ChainError::Decode {
            number,
            source: Box::new(source),
        })?;

        let phase = event.phase();
        let extrinsic_idx = match phase {
            Phase::ApplyExtrinsic(i) => Some(i),
            _ => None,
        };

        let pallet = event.pallet_name().to_owned();
        let variant = event.event_name().to_owned();

        let fields = event
            .decode_fields_unchecked_as::<Composite<()>>()
            .map(|c| codec::composite_to_json(&c))
            // A field we cannot decode must not abort the block: record the event with its
            // raw bytes so the row still exists and the gap check stays meaningful.
            .unwrap_or_else(|e| {
                tracing::warn!(
                    block = number, pallet = %pallet, variant = %variant, error = %e,
                    "falling back to raw field bytes"
                );
                serde_json::json!({ "_raw": format!("0x{}", hex::encode(event.field_bytes())) })
            });

        if let Some(idx) = extrinsic_idx {
            record_outcome(&mut outcomes, idx, &pallet, &variant, &fields);
        }

        new_events.push(NewEvent {
            chain_id: chain.id.clone(),
            block_number: number as i64,
            idx: event.index() as i32,
            pallet,
            variant,
            phase: phase_name(&phase).to_owned(),
            extrinsic_idx: extrinsic_idx.map(|i| i as i32),
            fields,
        });
    }

    let mut new_extrinsics = Vec::with_capacity(extrinsics.len());
    let mut timestamp = None;

    for extrinsic in extrinsics.iter() {
        let extrinsic = extrinsic.map_err(|source| ChainError::Decode {
            number,
            source: Box::new(source),
        })?;

        let idx = extrinsic.index() as u32;
        let pallet = extrinsic.pallet_name().to_owned();
        let call = extrinsic.call_name().to_owned();

        let args = extrinsic
            .decode_call_data_fields_unchecked_as::<Composite<()>>()
            .map(|c| codec::composite_to_json(&c))
            .unwrap_or_else(|e| {
                tracing::warn!(
                    block = number, pallet = %pallet, call = %call, error = %e,
                    "falling back to raw call data"
                );
                serde_json::json!({
                    "_raw": format!("0x{}", hex::encode(extrinsic.call_data_field_bytes()))
                })
            });

        // The block's own timestamp is set by the Timestamp::set inherent, which is why it
        // is read from an extrinsic argument rather than from the header.
        if timestamp.is_none() && pallet == "Timestamp" && call == "set" {
            timestamp = extract_timestamp(&args);
        }

        let outcome = outcomes.iter().find(|o| o.extrinsic_idx == idx);

        new_extrinsics.push(NewExtrinsic {
            chain_id: chain.id.clone(),
            block_number: number as i64,
            idx: idx as i32,
            hash: extrinsic.hash().0.to_vec(),
            pallet,
            call,
            signer: extrinsic
                .address_bytes()
                .map(|bytes| encode_signer(bytes, chain.ss58_prefix)),
            is_signed: extrinsic.is_signed(),
            // Absent System::ExtrinsicSuccess/Failed means the node reported no outcome;
            // treating that as failure would be a lie, so default to success.
            success: outcome.map(|o| o.success).unwrap_or(true),
            fee: outcome.and_then(|o| o.fee.clone()),
            args,
        });
    }

    Ok(BlockData {
        block: NewBlock {
            chain_id: chain.id.clone(),
            number: number as i64,
            hash: at.block_hash().0.to_vec(),
            parent_hash: header.parent_hash.0.to_vec(),
            state_root: header.state_root.0.to_vec(),
            extrinsics_root: header.extrinsics_root.0.to_vec(),
            spec_version: spec_version as i32,
            timestamp,
            extrinsic_count: new_extrinsics.len() as i32,
            event_count: new_events.len() as i32,
        },
        spec_name: String::new(), // filled in by the pipeline, which knows the runtime name
        extrinsics: new_extrinsics,
        events: new_events,
    })
}

/// Per-extrinsic outcome gathered from the events in its phase.
struct ExtrinsicOutcome {
    extrinsic_idx: u32,
    success: bool,
    fee: Option<BigDecimal>,
}

fn record_outcome(
    outcomes: &mut Vec<ExtrinsicOutcome>,
    idx: u32,
    pallet: &str,
    variant: &str,
    fields: &serde_json::Value,
) {
    let entry = match outcomes.iter_mut().find(|o| o.extrinsic_idx == idx) {
        Some(existing) => existing,
        None => {
            outcomes.push(ExtrinsicOutcome {
                extrinsic_idx: idx,
                success: true,
                fee: None,
            });
            outcomes.last_mut().expect("just pushed")
        }
    };

    match (pallet, variant) {
        ("System", "ExtrinsicFailed") => entry.success = false,
        ("System", "ExtrinsicSuccess") => entry.success = true,
        ("TransactionPayment", "TransactionFeePaid") => {
            entry.fee = extract_fee(fields);
        }
        _ => {}
    }
}

/// Pull the fee out of a `TransactionFeePaid` event.
///
/// Field names vary between runtimes, and balances are stored as strings by the codec, so
/// this probes the known names and falls back to positional access.
fn extract_fee(fields: &serde_json::Value) -> Option<BigDecimal> {
    for key in ["actual_fee", "actualFee", "fee"] {
        if let Some(value) = fields.get(key).and_then(json_to_bigdecimal) {
            return Some(value);
        }
    }
    // Positional shape is (who, actual_fee, tip).
    fields.get(1).and_then(json_to_bigdecimal)
}

fn json_to_bigdecimal(value: &serde_json::Value) -> Option<BigDecimal> {
    match value {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

/// Read the millisecond timestamp out of the `Timestamp::set` inherent's argument.
fn extract_timestamp(args: &serde_json::Value) -> Option<DateTime<Utc>> {
    let millis = args
        .get("now")
        .or_else(|| args.get(0))
        .and_then(|v| match v {
            serde_json::Value::String(s) => s.parse::<i64>().ok(),
            serde_json::Value::Number(n) => n.as_i64(),
            _ => None,
        })?;

    DateTime::from_timestamp_millis(millis)
}

/// Render an extrinsic's address bytes as SS58.
///
/// The address is a `MultiAddress`, whose SCALE encoding is a one-byte variant tag followed
/// by the payload; for the common `Id` variant that payload is the 32-byte account.
fn encode_signer(address_bytes: &[u8], prefix: u16) -> String {
    match address_bytes.len() {
        32 => ss58::encode(address_bytes, prefix),
        33 if address_bytes[0] == 0 => ss58::encode(&address_bytes[1..], prefix),
        // Anything else (Index, Raw, Address20, …) has no SS58 form; keep it as hex so the
        // information is not lost.
        _ => format!("0x{}", hex::encode(address_bytes)),
    }
}

fn phase_name(phase: &Phase) -> &'static str {
    match phase {
        Phase::ApplyExtrinsic(_) => "ApplyExtrinsic",
        Phase::Finalization => "Finalization",
        Phase::Initialization => "Initialization",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_fee_from_named_fields() {
        let fields = json!({ "who": "0xaa", "actual_fee": "12345", "tip": "0" });
        assert_eq!(extract_fee(&fields), Some("12345".parse().unwrap()));
    }

    #[test]
    fn extracts_fee_from_positional_fields() {
        let fields = json!(["0xaa", "999", "0"]);
        assert_eq!(extract_fee(&fields), Some("999".parse().unwrap()));
    }

    #[test]
    fn extracts_large_fee_without_precision_loss() {
        // Codec stores u128 as a string precisely so this stays exact past 2^53.
        let fields = json!({ "actual_fee": "340282366920938463463374607431768211455" });
        assert_eq!(
            extract_fee(&fields).unwrap().to_string(),
            "340282366920938463463374607431768211455"
        );
    }

    #[test]
    fn missing_fee_is_none() {
        assert_eq!(extract_fee(&json!({ "who": "0xaa" })), None);
    }

    #[test]
    fn extracts_block_timestamp_from_inherent_args() {
        // 2026-08-15T00:00:00Z
        let args = json!({ "now": "1786838400000" });
        let ts = extract_timestamp(&args).expect("should parse");
        assert_eq!(ts.timestamp_millis(), 1_786_838_400_000);

        // Positional form must work too.
        let positional = json!(["1786838400000"]);
        assert_eq!(
            extract_timestamp(&positional).unwrap().timestamp_millis(),
            1_786_838_400_000
        );
    }

    #[test]
    fn failed_extrinsic_is_recorded_as_failure() {
        let mut outcomes = Vec::new();
        record_outcome(&mut outcomes, 2, "System", "ExtrinsicFailed", &json!({}));
        assert!(!outcomes[0].success);
        assert_eq!(outcomes[0].extrinsic_idx, 2);
    }

    #[test]
    fn fee_and_outcome_merge_for_the_same_extrinsic() {
        // Both events land in the same phase and must accumulate onto one record.
        let mut outcomes = Vec::new();
        record_outcome(
            &mut outcomes,
            1,
            "TransactionPayment",
            "TransactionFeePaid",
            &json!({ "actual_fee": "77" }),
        );
        record_outcome(&mut outcomes, 1, "System", "ExtrinsicSuccess", &json!({}));

        assert_eq!(outcomes.len(), 1);
        assert!(outcomes[0].success);
        assert_eq!(outcomes[0].fee, Some("77".parse().unwrap()));
    }

    #[test]
    fn signer_bytes_render_as_ss58() {
        let account = [1u8; 32];
        let encoded = encode_signer(&account, 42);
        assert!(!encoded.starts_with("0x"), "expected SS58, got {encoded}");
        assert_eq!(encoded, ss58::encode(&account, 42));
    }

    #[test]
    fn multiaddress_id_prefix_is_stripped() {
        let account = [7u8; 32];
        let mut tagged = vec![0u8]; // MultiAddress::Id discriminant
        tagged.extend_from_slice(&account);

        // Must produce the same address as the bare 32 bytes, not a different one.
        assert_eq!(encode_signer(&tagged, 42), ss58::encode(&account, 42));
    }

    #[test]
    fn unknown_address_shapes_fall_back_to_hex() {
        assert_eq!(encode_signer(&[0xab, 0xcd], 42), "0xabcd");
    }

    #[test]
    fn recognises_a_pruned_state_error() {
        // Verbatim from a node running with the default --state-pruning 256.
        let err = std::io::Error::other(
            "Backend error: RPC error: User error: Client error: UnknownBlock: \
             State already discarded for 0xc9e64714ba2b8f70",
        );
        assert!(is_pruned_state(&err));
    }

    #[test]
    fn does_not_mistake_other_failures_for_pruning() {
        let err = std::io::Error::other("connection reset by peer");
        assert!(!is_pruned_state(&err));
    }
}
