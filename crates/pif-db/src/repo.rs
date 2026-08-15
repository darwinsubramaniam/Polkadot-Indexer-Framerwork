//! Persistence for indexed chain data.
//!
//! Queries here use sqlx's runtime API rather than the `query!` macros. The macros check
//! SQL against a live database at *compile* time, which would make `cargo build` require a
//! running Postgres (or a committed `.sqlx/` cache that silently goes stale). Keeping the
//! build hermetic is worth more here; the schema is exercised by the integration tests
//! instead.

use pif_core::ChainInfo;
use sqlx::{PgConnection, PgPool};

use crate::models::{ArchivedRuntime, BlockData, Cursor, NewEvent, NewExtrinsic, Watermarks};

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
    let block = &data.block;
    sqlx::query(
        r#"
        INSERT INTO blocks (
            chain_id, number, hash, parent_hash, state_root, extrinsics_root,
            spec_version, timestamp, extrinsic_count, event_count
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (chain_id, number) DO NOTHING
        "#,
    )
    .bind(&block.chain_id)
    .bind(block.number)
    .bind(&block.hash)
    .bind(&block.parent_hash)
    .bind(&block.state_root)
    .bind(&block.extrinsics_root)
    .bind(block.spec_version)
    .bind(block.timestamp)
    .bind(block.extrinsic_count)
    .bind(block.event_count)
    .execute(&mut *tx)
    .await?;

    // Note the runtime this block was decoded under, so it is possible to tell after the
    // fact which metadata produced which rows.
    sqlx::query(
        r#"
        INSERT INTO runtime_versions (chain_id, spec_version, spec_name, first_seen_block)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (chain_id, spec_version) DO NOTHING
        "#,
    )
    .bind(&block.chain_id)
    .bind(block.spec_version)
    .bind(&data.spec_name)
    .bind(block.number)
    .execute(&mut *tx)
    .await?;

    for extrinsic in &data.extrinsics {
        insert_extrinsic(tx, extrinsic).await?;
    }
    for event in &data.events {
        insert_event(tx, event).await?;
    }

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

async fn insert_extrinsic(tx: &mut PgConnection, extrinsic: &NewExtrinsic) -> DbResult<()> {
    sqlx::query(
        r#"
        INSERT INTO extrinsics (
            chain_id, block_number, idx, hash, pallet, call,
            signer, is_signed, success, fee, args
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (chain_id, block_number, idx) DO NOTHING
        "#,
    )
    .bind(&extrinsic.chain_id)
    .bind(extrinsic.block_number)
    .bind(extrinsic.idx)
    .bind(&extrinsic.hash)
    .bind(&extrinsic.pallet)
    .bind(&extrinsic.call)
    .bind(extrinsic.signer.as_deref())
    .bind(extrinsic.is_signed)
    .bind(extrinsic.success)
    .bind(extrinsic.fee.as_ref())
    .bind(&extrinsic.args)
    .execute(&mut *tx)
    .await?;

    Ok(())
}

async fn insert_event(tx: &mut PgConnection, event: &NewEvent) -> DbResult<()> {
    sqlx::query(
        r#"
        INSERT INTO events (
            chain_id, block_number, idx, pallet, variant, phase, extrinsic_idx, fields
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (chain_id, block_number, idx) DO NOTHING
        "#,
    )
    .bind(&event.chain_id)
    .bind(event.block_number)
    .bind(event.idx)
    .bind(&event.pallet)
    .bind(&event.variant)
    .bind(&event.phase)
    .bind(event.extrinsic_idx)
    .bind(&event.fields)
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
