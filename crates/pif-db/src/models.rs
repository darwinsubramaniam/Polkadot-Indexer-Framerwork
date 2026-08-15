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

/// How far each stage of the pipeline has got.
///
/// Replaces the dual meaning of `Cursor.last_indexed_block`, which meant both "fetched" and
/// "processed" because they were the same event. Once fetching and digesting are separate,
/// so are the numbers — and confusing them is how a reader ends up advertising blocks whose
/// rows do not exist yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermarks {
    /// Highest *contiguous* block in the archive, never the highest present. A digest that
    /// treated "the key exists" as "ready" would step over a hole and record a gap as
    /// success.
    pub fetch: i64,
    /// Highest block committed to Postgres. This is what indexing progress means.
    pub digest: i64,
    /// Highest block that has left the hot tier — moved to cold storage, or deleted under
    /// `on_digest = "delete"`. Never above [`Watermarks::digest`]: a segment the digest has
    /// not finished with is not a segment anything may move.
    pub archive: i64,
}

/// One segment of the archive, once it is no longer on the hot tier.
///
/// A row appears when a segment *leaves* the hot tier, not when it is written. While a
/// segment is hot its location is computed from the block number and its presence is a fact
/// about the filesystem, so a row would only be a second copy of something already knowable —
/// and the day the two disagreed, the table would be the one that was wrong. What survives
/// here is what the filesystem genuinely cannot answer: which tier holds it, and the checksum
/// it was verified against on the way there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRecord {
    pub from_block: i64,
    pub to_block: i64,
    /// `hot` or `cold`.
    pub tier: String,
    pub bytes: i64,
    /// CRC32 of the `.seg` file, big-endian.
    pub checksum: Vec<u8>,
}

/// What the archive holds for one runtime, beyond the fact that it existed.
#[derive(Debug, Clone)]
pub struct ArchivedRuntime {
    pub spec_version: i32,
    pub spec_name: String,
    /// The first block *this indexer saw* executing this runtime. At an upgrade that is the
    /// block after the one carrying `set_code`: the upgrade block itself was executed by the
    /// previous runtime and belongs to its row.
    pub first_seen_block: i64,
    pub transaction_version: i32,
    /// 14 | 15 | 16 — which metadata format was archived. A runtime serves several at once
    /// and they are not interchangeable in what they describe.
    pub metadata_version: i16,
    pub metadata_hash: Vec<u8>,
}
