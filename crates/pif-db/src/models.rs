//! Row types written by the ingest pipeline and read by the API.

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde_json::Value as Json;

/// Everything decoded from a single block.
///
/// The pipeline builds one of these per block and hands it to
/// [`crate::repo::write_block`], which persists the whole thing in one transaction. Keeping
/// it as a single value is what makes that atomicity natural rather than accidental.
#[derive(Debug, Clone)]
pub struct BlockData {
    pub block: NewBlock,
    /// Runtime name this block was decoded under, e.g. "polkadot". Paired with
    /// `block.spec_version` to record which metadata produced these rows.
    pub spec_name: String,
    pub extrinsics: Vec<NewExtrinsic>,
    pub events: Vec<NewEvent>,
}

#[derive(Debug, Clone)]
pub struct NewBlock {
    pub chain_id: String,
    pub number: i64,
    pub hash: Vec<u8>,
    pub parent_hash: Vec<u8>,
    pub state_root: Vec<u8>,
    pub extrinsics_root: Vec<u8>,
    pub spec_version: i32,
    pub timestamp: Option<DateTime<Utc>>,
    pub extrinsic_count: i32,
    pub event_count: i32,
}

#[derive(Debug, Clone)]
pub struct NewExtrinsic {
    pub chain_id: String,
    pub block_number: i64,
    pub idx: i32,
    pub hash: Vec<u8>,
    pub pallet: String,
    pub call: String,
    pub signer: Option<String>,
    pub is_signed: bool,
    pub success: bool,
    pub fee: Option<BigDecimal>,
    pub args: Json,
}

#[derive(Debug, Clone)]
pub struct NewEvent {
    pub chain_id: String,
    pub block_number: i64,
    pub idx: i32,
    pub pallet: String,
    pub variant: String,
    pub phase: String,
    pub extrinsic_idx: Option<i32>,
    pub fields: Json,
}

/// Where indexing left off for a chain.
#[derive(Debug, Clone)]
pub struct Cursor {
    pub last_indexed_block: i64,
    pub last_indexed_hash: Vec<u8>,
}
