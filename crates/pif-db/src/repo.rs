//! Persistence for indexed chain data.
//!
//! Queries here use sqlx's runtime API rather than the `query!` macros. The macros check
//! SQL against a live database at *compile* time, which would make `cargo build` require a
//! running Postgres (or a committed `.sqlx/` cache that silently goes stale). Keeping the
//! build hermetic is worth more here; the schema is exercised by the integration tests
//! instead.

use pif_core::ChainInfo;
use sqlx::{PgConnection, PgPool};

use crate::models::{BlockData, Cursor, NewEvent, NewExtrinsic};

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
