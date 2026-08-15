//! Persistence for indexed chain data.
//!
//! Queries here use sqlx's runtime API rather than the `query!` macros. The macros check
//! SQL against a live database at *compile* time, which would make `cargo build` require a
//! running Postgres (or a committed `.sqlx/` cache that silently goes stale). Keeping the
//! build hermetic is worth more here; the schema is exercised by the integration tests
//! instead.

use pif_core::ChainInfo;
use sqlx::{PgConnection, PgPool};

use crate::models::{ArchivedRuntime, BlockData, Cursor, SegmentRecord, Watermarks};

pub type DbResult<T> = Result<T, sqlx::Error>;

/// Record a chain's discovered identity, or verify it still matches.
///
/// The genesis hash is the chain's real identity while `id` is just a config key. If an
/// operator repoints an existing `id` at a different chain, this is where it surfaces —
/// as a unique-violation on `genesis_hash` rather than as silently interleaved blocks.
pub async fn upsert_chain(pool: &PgPool, chain: &ChainInfo) -> DbResult<()> {
    sqlx::query(
        r#"
        INSERT INTO chains (id, genesis_hash, name, token_symbol, token_decimals, ss58_prefix)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (id) DO UPDATE SET
            name           = EXCLUDED.name,
            token_symbol   = EXCLUDED.token_symbol,
            token_decimals = EXCLUDED.token_decimals,
            ss58_prefix    = EXCLUDED.ss58_prefix
        "#,
    )
    .bind(&chain.id)
    .bind(&chain.genesis_hash)
    .bind(&chain.name)
    .bind(chain.token_symbol.as_deref())
    .bind(chain.token_decimals.map(i16::from))
    .bind(i32::from(chain.ss58_prefix))
    .execute(pool)
    .await?;

    Ok(())
}

/// Fetch the stored genesis hash for a chain id, if the chain is already known.
pub async fn genesis_hash(pool: &PgPool, chain_id: &str) -> DbResult<Option<Vec<u8>>> {
    let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT genesis_hash FROM chains WHERE id = $1")
        .bind(chain_id)
        .fetch_optional(pool)
        .await?;

    Ok(row.map(|r| r.0))
}

/// Read the resume point for a chain.
pub async fn load_cursor(pool: &PgPool, chain_id: &str) -> DbResult<Option<Cursor>> {
    let row: Option<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT last_indexed_block, last_indexed_hash FROM indexer_state WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(last_indexed_block, last_indexed_hash)| Cursor {
        last_indexed_block,
        last_indexed_hash,
    }))
}

/// Persist one block's core rows and advance the cursor, atomically.
///
/// A convenience wrapper for callers with no typed-overlay handlers. The ingest pipeline
/// uses [`write_block_in_tx`] and [`update_cursor`] directly so it can run handlers inside
/// the same transaction.
///
/// Every insert is `ON CONFLICT DO NOTHING`, so replaying an already-indexed block is a
/// no-op rather than an error.
pub async fn write_block(pool: &PgPool, data: &BlockData) -> DbResult<()> {
    let mut tx = pool.begin().await?;
    write_block_in_tx(&mut tx, data).await?;
    update_cursor(
        &mut tx,
        &data.block.chain_id,
        data.block.number,
        &data.block.hash,
    )
    .await?;
    tx.commit().await
}

/// Write a block's core rows into an existing transaction, without committing.
///
/// Split out from [`write_block`] so the ingest pipeline can run typed-overlay handlers
/// inside the *same* transaction, between these rows and the cursor update. Handlers own
/// their tables, so the framework never needs to know what they write.
pub async fn write_block_in_tx(tx: &mut PgConnection, data: &BlockData) -> DbResult<()> {
    write_blocks_in_tx(tx, std::slice::from_ref(data)).await
}

/// Write a run of blocks' core rows into an existing transaction, without committing.
///
/// Four statements for the whole batch — one per table — rather than one per row. Written a
/// row at a time, a block with 200 events costs 200 sequential round-trips *inside* the
/// transaction, and every one of them is a full network wait during which the transaction
/// holds its locks and nothing else in the batch progresses. `UNNEST` collapses that to one
/// round-trip per table per batch, which is what stops the digest being the pipeline's
/// bottleneck now that fetching runs in parallel.
///
/// The order is load-bearing: `extrinsics` and `events` carry a foreign key to `blocks`, so
/// the blocks go in first. Every insert stays `ON CONFLICT DO NOTHING`, so replaying a range
/// already indexed remains a no-op rather than an error.
pub async fn write_blocks_in_tx(tx: &mut PgConnection, batch: &[BlockData]) -> DbResult<()> {
    if batch.is_empty() {
        return Ok(());
    }

    insert_blocks(&mut *tx, batch).await?;
    insert_runtime_versions(&mut *tx, batch).await?;
    insert_extrinsics(&mut *tx, batch).await?;
    insert_events(&mut *tx, batch).await?;

    Ok(())
}

async fn insert_blocks(tx: &mut PgConnection, batch: &[BlockData]) -> DbResult<()> {
    let n = batch.len();
    let mut chain_id = Vec::with_capacity(n);
    let mut number = Vec::with_capacity(n);
    let mut hash = Vec::with_capacity(n);
    let mut parent_hash = Vec::with_capacity(n);
    let mut state_root = Vec::with_capacity(n);
    let mut extrinsics_root = Vec::with_capacity(n);
    let mut spec_version = Vec::with_capacity(n);
    let mut timestamp = Vec::with_capacity(n);
    let mut extrinsic_count = Vec::with_capacity(n);
    let mut event_count = Vec::with_capacity(n);

    for data in batch {
        let block = &data.block;
        chain_id.push(block.chain_id.as_str());
        number.push(block.number);
        hash.push(block.hash.as_slice());
        parent_hash.push(block.parent_hash.as_slice());
        state_root.push(block.state_root.as_slice());
        extrinsics_root.push(block.extrinsics_root.as_slice());
        spec_version.push(block.spec_version);
        timestamp.push(block.timestamp);
        extrinsic_count.push(block.extrinsic_count);
        event_count.push(block.event_count);
    }

    sqlx::query(
        r#"
        INSERT INTO blocks (
            chain_id, number, hash, parent_hash, state_root, extrinsics_root,
            spec_version, timestamp, extrinsic_count, event_count
        )
        SELECT * FROM UNNEST(
            $1::text[], $2::bigint[], $3::bytea[], $4::bytea[], $5::bytea[], $6::bytea[],
            $7::int[], $8::timestamptz[], $9::int[], $10::int[]
        )
        ON CONFLICT (chain_id, number) DO NOTHING
        "#,
    )
    .bind(&chain_id)
    .bind(&number)
    .bind(&hash)
    .bind(&parent_hash)
    .bind(&state_root)
    .bind(&extrinsics_root)
    .bind(&spec_version)
    .bind(&timestamp)
    .bind(&extrinsic_count)
    .bind(&event_count)
    .execute(&mut *tx)
    .await?;

    Ok(())
}

/// Note the runtime each block was decoded under, so it is possible to tell after the fact
/// which metadata produced which rows.
///
/// Deduplicated in Rust rather than left to `ON CONFLICT`: a batch normally holds one runtime
/// for all of its blocks, and `first_seen_block` must come from the *lowest* of them. Blocks
/// arrive in ascending order, so keeping the first sighting of each `spec_version` is exactly
/// that — and it matches what the per-block path recorded, which is what makes batching
/// invisible in the rows.
async fn insert_runtime_versions(tx: &mut PgConnection, batch: &[BlockData]) -> DbResult<()> {
    let mut seen = std::collections::HashSet::new();
    let mut chain_id = Vec::new();
    let mut spec_version = Vec::new();
    let mut spec_name = Vec::new();
    let mut first_seen_block = Vec::new();

    for data in batch {
        let block = &data.block;
        if !seen.insert((block.chain_id.as_str(), block.spec_version)) {
            continue;
        }
        chain_id.push(block.chain_id.as_str());
        spec_version.push(block.spec_version);
        spec_name.push(data.spec_name.as_str());
        first_seen_block.push(block.number);
    }

    sqlx::query(
        r#"
        INSERT INTO runtime_versions (chain_id, spec_version, spec_name, first_seen_block)
        SELECT * FROM UNNEST($1::text[], $2::int[], $3::text[], $4::bigint[])
        ON CONFLICT (chain_id, spec_version) DO NOTHING
        "#,
    )
    .bind(&chain_id)
    .bind(&spec_version)
    .bind(&spec_name)
    .bind(&first_seen_block)
    .execute(&mut *tx)
    .await?;

    Ok(())
}

/// Advance a chain's resume cursor, without committing.
///
/// Called last in the block's transaction, after the core rows and every handler. That
/// ordering is what makes restart-resume correct: the cursor can never be ahead of the data
/// it claims to describe, so a crash mid-block rolls the whole block back rather than
/// leaving a hole.
pub async fn update_cursor(
    tx: &mut PgConnection,
    chain_id: &str,
    number: i64,
    hash: &[u8],
) -> DbResult<()> {
    sqlx::query(
        r#"
        INSERT INTO indexer_state (chain_id, last_indexed_block, last_indexed_hash, updated_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (chain_id) DO UPDATE SET
            last_indexed_block = EXCLUDED.last_indexed_block,
            last_indexed_hash  = EXCLUDED.last_indexed_hash,
            updated_at         = now()
        "#,
    )
    .bind(chain_id)
    .bind(number)
    .bind(hash)
    .execute(&mut *tx)
    .await?;

    Ok(())
}

/// Every extrinsic in the batch, flattened into one insert.
///
/// The whole batch is one statement rather than one per block: the round-trip is what costs,
/// and it is paid per statement, not per row.
async fn insert_extrinsics(tx: &mut PgConnection, batch: &[BlockData]) -> DbResult<()> {
    let n: usize = batch.iter().map(|data| data.extrinsics.len()).sum();
    if n == 0 {
        return Ok(());
    }

    let mut chain_id = Vec::with_capacity(n);
    let mut block_number = Vec::with_capacity(n);
    let mut idx = Vec::with_capacity(n);
    let mut hash = Vec::with_capacity(n);
    let mut pallet = Vec::with_capacity(n);
    let mut call = Vec::with_capacity(n);
    let mut signer = Vec::with_capacity(n);
    let mut is_signed = Vec::with_capacity(n);
    let mut success = Vec::with_capacity(n);
    let mut fee = Vec::with_capacity(n);
    let mut args = Vec::with_capacity(n);

    for extrinsic in batch.iter().flat_map(|data| &data.extrinsics) {
        chain_id.push(extrinsic.chain_id.as_str());
        block_number.push(extrinsic.block_number);
        idx.push(extrinsic.idx);
        hash.push(extrinsic.hash.as_slice());
        pallet.push(extrinsic.pallet.as_str());
        call.push(extrinsic.call.as_str());
        signer.push(extrinsic.signer.as_deref());
        is_signed.push(extrinsic.is_signed);
        success.push(extrinsic.success);
        fee.push(extrinsic.fee.clone());
        args.push(&extrinsic.args);
    }

    sqlx::query(
        r#"
        INSERT INTO extrinsics (
            chain_id, block_number, idx, hash, pallet, call,
            signer, is_signed, success, fee, args
        )
        SELECT * FROM UNNEST(
            $1::text[], $2::bigint[], $3::int[], $4::bytea[], $5::text[], $6::text[],
            $7::text[], $8::bool[], $9::bool[], $10::numeric[], $11::jsonb[]
        )
        ON CONFLICT (chain_id, block_number, idx) DO NOTHING
        "#,
    )
    .bind(&chain_id)
    .bind(&block_number)
    .bind(&idx)
    .bind(&hash)
    .bind(&pallet)
    .bind(&call)
    .bind(&signer)
    .bind(&is_signed)
    .bind(&success)
    .bind(&fee)
    .bind(&args)
    .execute(&mut *tx)
    .await?;

    Ok(())
}

/// Every event in the batch, flattened into one insert.
///
/// The one that matters most for throughput: events outnumber extrinsics by an order of
/// magnitude on a busy block, and they were the bulk of the per-row round-trips.
async fn insert_events(tx: &mut PgConnection, batch: &[BlockData]) -> DbResult<()> {
    let n: usize = batch.iter().map(|data| data.events.len()).sum();
    if n == 0 {
        return Ok(());
    }

    let mut chain_id = Vec::with_capacity(n);
    let mut block_number = Vec::with_capacity(n);
    let mut idx = Vec::with_capacity(n);
    let mut pallet = Vec::with_capacity(n);
    let mut variant = Vec::with_capacity(n);
    let mut phase = Vec::with_capacity(n);
    let mut extrinsic_idx = Vec::with_capacity(n);
    let mut fields = Vec::with_capacity(n);

    for event in batch.iter().flat_map(|data| &data.events) {
        chain_id.push(event.chain_id.as_str());
        block_number.push(event.block_number);
        idx.push(event.idx);
        pallet.push(event.pallet.as_str());
        variant.push(event.variant.as_str());
        phase.push(event.phase.as_str());
        extrinsic_idx.push(event.extrinsic_idx);
        fields.push(&event.fields);
    }

    sqlx::query(
        r#"
        INSERT INTO events (
            chain_id, block_number, idx, pallet, variant, phase, extrinsic_idx, fields
        )
        SELECT * FROM UNNEST(
            $1::text[], $2::bigint[], $3::int[], $4::text[], $5::text[], $6::text[],
            $7::int[], $8::jsonb[]
        )
        ON CONFLICT (chain_id, block_number, idx) DO NOTHING
        "#,
    )
    .bind(&chain_id)
    .bind(&block_number)
    .bind(&idx)
    .bind(&pallet)
    .bind(&variant)
    .bind(&phase)
    .bind(&extrinsic_idx)
    .bind(&fields)
    .execute(&mut *tx)
    .await?;

    Ok(())
}

/// Read a chain's discovered identity back out of the database.
///
/// This is what makes a genuinely offline replay possible: everything `decode` needs about a
/// chain — its id and SS58 prefix — was recorded the first time it was indexed, so replaying
/// from the archive needs no node to ask.
pub async fn load_chain(pool: &PgPool, chain_id: &str) -> DbResult<Option<ChainInfo>> {
    /// `chains`, in column order.
    type ChainRow = (String, Vec<u8>, String, Option<String>, Option<i16>, i32);

    let row: Option<ChainRow> = sqlx::query_as(
        r#"
        SELECT id, genesis_hash, name, token_symbol, token_decimals, ss58_prefix
        FROM chains WHERE id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(
        |(id, genesis_hash, name, token_symbol, token_decimals, ss58_prefix)| ChainInfo {
            id,
            genesis_hash,
            name,
            token_symbol,
            token_decimals: token_decimals.and_then(|d| u8::try_from(d).ok()),
            ss58_prefix: u16::try_from(ss58_prefix).unwrap_or(pif_core::ss58::DEFAULT_PREFIX),
        },
    ))
}

/// The runtime name of the most recent runtime this chain has indexed.
///
/// Read back rather than asked of a node, so a replay can fill in `blocks.spec_name` with
/// the node switched off. The name is stable across a chain's life — Polkadot has never
/// stopped being "polkadot" — so the newest row is a safe answer for older blocks too.
pub async fn latest_spec_name(pool: &PgPool, chain_id: &str) -> DbResult<Option<String>> {
    let row: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT spec_name FROM runtime_versions
        WHERE chain_id = $1
        ORDER BY spec_version DESC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| r.0))
}

/// Read how far each pipeline stage has got.
pub async fn load_watermarks(pool: &PgPool, chain_id: &str) -> DbResult<Option<Watermarks>> {
    let row: Option<(i64, i64, i64)> = sqlx::query_as(
        r#"
        SELECT fetch_watermark, digest_watermark, archive_watermark
        FROM pipeline_watermarks WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(fetch, digest, archive)| Watermarks {
        fetch,
        digest,
        archive,
    }))
}

/// Create a chain's watermark row if it has none, seeded at `at`.
///
/// A no-op once the row exists — a restart must never move a watermark backwards, and the
/// `0002_pipeline` migration has already seeded rows for chains indexed before the archive.
///
/// `at` is the block *before* the first one to index, so a chain starting at genesis seeds
/// at `-1`: nothing has been done yet, and `0` would mean block 0 was already indexed.
/// `LEAST(0, $2)` keeps the archive watermark below it so the table's ordering check holds
/// at that boundary.
pub async fn init_watermarks(pool: &PgPool, chain_id: &str, at: i64) -> DbResult<()> {
    sqlx::query(
        r#"
        INSERT INTO pipeline_watermarks
            (chain_id, fetch_watermark, digest_watermark, archive_watermark)
        VALUES ($1, $2, $2, LEAST(0, $2))
        ON CONFLICT (chain_id) DO NOTHING
        "#,
    )
    .bind(chain_id)
    .bind(at)
    .execute(pool)
    .await?;

    Ok(())
}

/// Move a chain's watermarks *back* to `to`, so a range is fetched and digested again.
///
/// The one place the monotonicity rule is broken on purpose: `--from` and `pif replay` are
/// explicit requests to redo work. `archive_watermark` is clamped rather than reset, because
/// blocks already tiered to cold storage are still there — and the table's ordering check
/// would reject a digest watermark below it.
pub async fn reset_watermarks(pool: &PgPool, chain_id: &str, to: i64) -> DbResult<()> {
    sqlx::query(
        r#"
        INSERT INTO pipeline_watermarks
            (chain_id, fetch_watermark, digest_watermark, archive_watermark)
        VALUES ($1, $2, $2, LEAST(0, $2))
        ON CONFLICT (chain_id) DO UPDATE SET
            fetch_watermark   = $2,
            digest_watermark  = $2,
            archive_watermark = LEAST(pipeline_watermarks.archive_watermark, $2),
            updated_at        = now()
        "#,
    )
    .bind(chain_id)
    .bind(to)
    .execute(pool)
    .await?;

    Ok(())
}

/// Record that every block up to `to` is archived and durable.
///
/// Called only *after* the store has been synced, so the watermark can never claim a block
/// the disk does not hold. `GREATEST` keeps it monotonic even if a re-fetch replays ground
/// the store already covers.
pub async fn advance_fetch_watermark(pool: &PgPool, chain_id: &str, to: i64) -> DbResult<()> {
    sqlx::query(
        r#"
        UPDATE pipeline_watermarks
        SET fetch_watermark = GREATEST(fetch_watermark, $2), updated_at = now()
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .bind(to)
    .execute(pool)
    .await?;

    Ok(())
}

/// Record that every block up to `to` is committed to Postgres, without committing.
///
/// Takes a connection rather than a pool because it belongs *inside* the block's
/// transaction, next to the cursor update: the watermark must never be ahead of the rows it
/// claims to describe, and the only way to guarantee that is to commit them together.
pub async fn advance_digest_watermark(
    tx: &mut PgConnection,
    chain_id: &str,
    to: i64,
) -> DbResult<()> {
    sqlx::query(
        r#"
        UPDATE pipeline_watermarks
        SET digest_watermark = GREATEST(digest_watermark, $2), updated_at = now()
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .bind(to)
    .execute(&mut *tx)
    .await?;

    Ok(())
}

/// Record that every block up to `to` has left the hot tier.
///
/// Monotonic like the other two, and bounded by them: a segment only becomes eligible to move
/// once the digest is finished with it, so this can never overtake `digest_watermark` and the
/// table's ordering check never has to catch it.
pub async fn advance_archive_watermark(pool: &PgPool, chain_id: &str, to: i64) -> DbResult<()> {
    sqlx::query(
        r#"
        UPDATE pipeline_watermarks
        SET archive_watermark = GREATEST(archive_watermark, $2), updated_at = now()
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .bind(to)
    .execute(pool)
    .await?;

    Ok(())
}

/// Record a segment on the cold tier.
///
/// Written **after** the copy has been verified and **before** the hot original is deleted.
/// That order is the whole crash story: a failure anywhere in the sequence leaves two copies
/// of the segment, which costs disk and is recoverable by re-running the pass, rather than
/// none, which is not recoverable without going back to the network.
pub async fn record_segment(
    pool: &PgPool,
    chain_id: &str,
    segment: &SegmentRecord,
) -> DbResult<()> {
    sqlx::query(
        r#"
        INSERT INTO segments (chain_id, from_block, to_block, tier, bytes, checksum)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (chain_id, from_block) DO UPDATE SET
            to_block  = EXCLUDED.to_block,
            tier      = EXCLUDED.tier,
            bytes     = EXCLUDED.bytes,
            checksum  = EXCLUDED.checksum,
            sealed_at = now()
        "#,
    )
    .bind(chain_id)
    .bind(segment.from_block)
    .bind(segment.to_block)
    .bind(&segment.tier)
    .bind(segment.bytes)
    .bind(&segment.checksum)
    .execute(pool)
    .await?;

    Ok(())
}

/// Forget a segment that is no longer anywhere.
///
/// `on_digest = "delete"` is the only thing that produces one. The row goes rather than
/// gaining a third tier value, so `segments` never names a file that cannot be read.
pub async fn forget_segment(pool: &PgPool, chain_id: &str, from_block: i64) -> DbResult<()> {
    sqlx::query("DELETE FROM segments WHERE chain_id = $1 AND from_block = $2")
        .bind(chain_id)
        .bind(from_block)
        .execute(pool)
        .await?;

    Ok(())
}

/// Every segment recorded for a chain, ascending.
pub async fn segments(pool: &PgPool, chain_id: &str) -> DbResult<Vec<SegmentRecord>> {
    let rows: Vec<(i64, i64, String, i64, Vec<u8>)> = sqlx::query_as(
        r#"
        SELECT from_block, to_block, tier, bytes, checksum
        FROM segments WHERE chain_id = $1 ORDER BY from_block
        "#,
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(from_block, to_block, tier, bytes, checksum)| SegmentRecord {
                from_block,
                to_block,
                tier,
                bytes,
                checksum,
            },
        )
        .collect())
}

/// How many segments are recorded on the cold tier, and what they occupy.
pub async fn cold_segment_totals(pool: &PgPool, chain_id: &str) -> DbResult<(i64, i64)> {
    let row: (i64, Option<i64>) = sqlx::query_as(
        "SELECT count(*), sum(bytes) FROM segments WHERE chain_id = $1 AND tier = 'cold'",
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;

    Ok((row.0, row.1.unwrap_or(0)))
}

/// A range of blocks leased to one fetch worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chunk {
    pub from_block: i64,
    pub to_block: i64,
    /// How many times this chunk has been leased, including now. A chunk that keeps coming
    /// back is a chunk something is systematically wrong with.
    pub attempts: i32,
}

/// Create the work queue for a range, one chunk per `chunk_size` blocks.
///
/// Idempotent: chunks already present are left alone, so a restart re-seeds only what is
/// genuinely new. Tiles forward from `from` rather than aligning to a global grid, because
/// `from` is always either a chain's start block or one past a completed chunk — never
/// mid-chunk — so the tiling is stable across runs without an alignment rule to get wrong.
pub async fn seed_chunks(
    pool: &PgPool,
    chain_id: &str,
    from: i64,
    to: i64,
    chunk_size: i64,
) -> DbResult<u64> {
    if to < from || chunk_size < 1 {
        return Ok(0);
    }

    let mut inserted = 0;
    let mut start = from;
    while start <= to {
        let end = (start + chunk_size - 1).min(to);
        let result = sqlx::query(
            r#"
            INSERT INTO fetch_chunks (chain_id, from_block, to_block, state)
            VALUES ($1, $2, $3, 'pending')
            ON CONFLICT (chain_id, from_block) DO NOTHING
            "#,
        )
        .bind(chain_id)
        .bind(start)
        .bind(end)
        .execute(pool)
        .await?;

        inserted += result.rows_affected();
        start = end + 1;
    }

    Ok(inserted)
}

/// Drop a chain's whole work queue.
///
/// Used when the watermarks are reset: a re-index from an arbitrary block would otherwise
/// tile chunks that overlap the old ones, and overlapping ranges make "the contiguous run of
/// completed chunks" mean nothing.
pub async fn reset_chunks(pool: &PgPool, chain_id: &str) -> DbResult<()> {
    sqlx::query("DELETE FROM fetch_chunks WHERE chain_id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Take the lowest claimable chunk, if there is one.
///
/// `FOR UPDATE SKIP LOCKED` is what makes several workers — and, later, several *processes*
/// — safe against each other with no coordinator at all. Two workers asking at the same
/// instant get different chunks rather than one blocking on the other, and a worker that
/// dies simply lets its lease expire.
///
/// `max_to_block` is the max-lag brake, and it bounds the chunk's **end**, not its start: a
/// chunk that began inside the window but finished outside it would leave exactly the blocks
/// at its tail with state the digest can no longer read. Callers keep `chunk_size` at or
/// below the lag so that the lowest chunk is always claimable — bounding the end with chunks
/// larger than the window would claim nothing at all, and the two stages would deadlock
/// waiting for each other.
pub async fn claim_chunk(
    pool: &PgPool,
    chain_id: &str,
    leased_by: &str,
    lease_seconds: i32,
    max_to_block: Option<i64>,
) -> DbResult<Option<Chunk>> {
    let row: Option<(i64, i64, i32)> = sqlx::query_as(
        r#"
        WITH claimed AS (
            SELECT from_block
              FROM fetch_chunks
             WHERE chain_id = $1
               AND state = 'pending'
               AND ($4::bigint IS NULL OR to_block <= $4)
             ORDER BY from_block
             FOR UPDATE SKIP LOCKED
             LIMIT 1
        )
        UPDATE fetch_chunks c
           SET state            = 'leased',
               leased_by        = $2,
               lease_expires_at = now() + ($3 * interval '1 second'),
               attempts         = c.attempts + 1
          FROM claimed
         WHERE c.chain_id = $1 AND c.from_block = claimed.from_block
        RETURNING c.from_block, c.to_block, c.attempts
        "#,
    )
    .bind(chain_id)
    .bind(leased_by)
    .bind(lease_seconds)
    .bind(max_to_block)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(from_block, to_block, attempts)| Chunk {
        from_block,
        to_block,
        attempts,
    }))
}

/// Mark a chunk finished. Called only after its blocks are durable on disk.
///
/// `leased_by` is *kept*, not cleared: on a done chunk it stops being a lease and becomes the
/// record of which endpoint actually fetched that range — which is the first thing anyone
/// asks when one provider turns out to have been serving something odd.
pub async fn complete_chunk(pool: &PgPool, chain_id: &str, from_block: i64) -> DbResult<()> {
    sqlx::query(
        r#"
        UPDATE fetch_chunks
           SET state = 'done', lease_expires_at = NULL, last_error = NULL
         WHERE chain_id = $1 AND from_block = $2
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .execute(pool)
    .await?;

    Ok(())
}

/// Hand a chunk back for someone else to try.
///
/// The ordinary outcome of an endpoint going down mid-chunk. `attempts` was already
/// incremented when it was claimed, so a chunk that fails forever is visible in the table
/// rather than only in the log.
pub async fn release_chunk(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    error: &str,
) -> DbResult<()> {
    sqlx::query(
        r#"
        UPDATE fetch_chunks
           SET state = 'pending', leased_by = NULL, lease_expires_at = NULL, last_error = $3
         WHERE chain_id = $1 AND from_block = $2
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(error)
    .execute(pool)
    .await?;

    Ok(())
}

/// Give up on a chunk after too many attempts.
///
/// Distinct from releasing it: a chunk that has failed against every endpoint is not going
/// to succeed by being offered round again, and leaving it pending would spin the workers
/// forever instead of stopping with something a human can read.
pub async fn fail_chunk(
    pool: &PgPool,
    chain_id: &str,
    from_block: i64,
    error: &str,
) -> DbResult<()> {
    sqlx::query(
        r#"
        UPDATE fetch_chunks
           SET state = 'failed', leased_by = NULL, lease_expires_at = NULL, last_error = $3
         WHERE chain_id = $1 AND from_block = $2
        "#,
    )
    .bind(chain_id)
    .bind(from_block)
    .bind(error)
    .execute(pool)
    .await?;

    Ok(())
}

/// Return chunks whose lease has run out to the pool.
///
/// This is the whole recovery story for a worker — or a whole process — that died holding
/// work: nobody has to notice the death, because the lease expires on its own.
pub async fn reclaim_expired_chunks(pool: &PgPool, chain_id: &str) -> DbResult<u64> {
    let result = sqlx::query(
        r#"
        UPDATE fetch_chunks
           SET state = 'pending', leased_by = NULL, lease_expires_at = NULL
         WHERE chain_id = $1 AND state = 'leased' AND lease_expires_at < now()
        "#,
    )
    .bind(chain_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// The highest block covered by an unbroken run of completed chunks.
///
/// The rule that keeps `fetch_watermark` honest under parallel fetch. Chunk 12800 can finish
/// before chunk 12700, and a watermark that moved on completion alone would advertise a hole
/// as ready. Only the prefix counts: everything up to the first chunk that is not `done`.
pub async fn contiguous_done_end(pool: &PgPool, chain_id: &str) -> DbResult<Option<i64>> {
    let row: (Option<i64>,) = sqlx::query_as(
        r#"
        SELECT max(to_block)
          FROM fetch_chunks
         WHERE chain_id = $1
           AND state = 'done'
           AND from_block < COALESCE(
                 (SELECT min(from_block) FROM fetch_chunks
                   WHERE chain_id = $1 AND state <> 'done'),
                 9223372036854775807)
        "#,
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;

    Ok(row.0)
}

/// How much work is left: `(pending, leased, failed)`.
pub async fn chunk_counts(pool: &PgPool, chain_id: &str) -> DbResult<(i64, i64, i64)> {
    let row: (i64, i64, i64) = sqlx::query_as(
        r#"
        SELECT count(*) FILTER (WHERE state = 'pending'),
               count(*) FILTER (WHERE state = 'leased'),
               count(*) FILTER (WHERE state = 'failed')
          FROM fetch_chunks WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;

    Ok(row)
}

/// Record that a runtime's metadata is in the archive.
///
/// Extends the `runtime_versions` row rather than writing to a table of its own. That table
/// already has this exact grain — one row per `(chain_id, spec_version)` — and two tables at
/// the same grain would have to agree about which block a runtime started at. The day they
/// disagreed, the archive would be the one that was wrong.
///
/// `first_seen_block` is written on insert and never updated: the ingest path already gets
/// it right, and an upgrade block re-asserting the *previous* runtime's row must not drag
/// that row's first block forward.
pub async fn record_archived_runtime(
    pool: &PgPool,
    chain_id: &str,
    runtime: &ArchivedRuntime,
) -> DbResult<()> {
    sqlx::query(
        r#"
        INSERT INTO runtime_versions (
            chain_id, spec_version, spec_name, first_seen_block,
            transaction_version, metadata_version, metadata_hash, metadata_archived_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (chain_id, spec_version) DO UPDATE SET
            transaction_version  = EXCLUDED.transaction_version,
            metadata_version     = EXCLUDED.metadata_version,
            metadata_hash        = EXCLUDED.metadata_hash,
            metadata_archived_at = EXCLUDED.metadata_archived_at
        "#,
    )
    .bind(chain_id)
    .bind(runtime.spec_version)
    .bind(&runtime.spec_name)
    .bind(runtime.first_seen_block)
    .bind(runtime.transaction_version)
    .bind(runtime.metadata_version)
    .bind(&runtime.metadata_hash)
    .execute(pool)
    .await?;

    Ok(())
}

/// Runtimes this chain has indexed but whose metadata is not in the archive.
///
/// The precise answer to "which ranges can I actually replay?". Every row written before the
/// archive existed is in here, which is a fact worth being able to query rather than a gap
/// to be embarrassed about.
pub async fn runtimes_without_metadata(pool: &PgPool, chain_id: &str) -> DbResult<Vec<(i32, i64)>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(
        r#"
        SELECT spec_version, first_seen_block
        FROM runtime_versions
        WHERE chain_id = $1 AND metadata_hash IS NULL
        ORDER BY first_seen_block
        "#,
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Highest block stored for a chain. Used by tests and by `indexerStatus`.
pub async fn max_block(pool: &PgPool, chain_id: &str) -> DbResult<Option<i64>> {
    let row: (Option<i64>,) = sqlx::query_as("SELECT max(number) FROM blocks WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_one(pool)
        .await?;

    Ok(row.0)
}

/// Count blocks missing between the lowest and highest stored block for a chain.
///
/// The indexer is supposed to make this structurally impossible; having it as a cheap query
/// means the guarantee can actually be asserted after a restart rather than assumed.
pub async fn count_gaps(pool: &PgPool, chain_id: &str) -> DbResult<i64> {
    let row: (Option<i64>,) = sqlx::query_as(
        r#"
        SELECT count(*) FROM generate_series(
            (SELECT min(number) FROM blocks WHERE chain_id = $1),
            (SELECT max(number) FROM blocks WHERE chain_id = $1)
        ) AS n
        WHERE NOT EXISTS (
            SELECT 1 FROM blocks b WHERE b.chain_id = $1 AND b.number = n
        )
        "#,
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;

    Ok(row.0.unwrap_or(0))
}
