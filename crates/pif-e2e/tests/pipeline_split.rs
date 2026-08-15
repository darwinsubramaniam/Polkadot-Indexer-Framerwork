//! The pipeline split, end to end against the compose dev node.
//!
//! Run with:
//!     docker compose up -d
//!     cargo test -p pif-e2e --test pipeline_split -- --ignored --nocapture --test-threads=1
//!
//! The claim under test is the one the whole archive rests on: **a re-index costs a
//! re-digest, not a re-download.** Everything else here exists to make that claim
//! falsifiable rather than plausible.

use anyhow::{Context, Result};
use pif_chain::{IndexOptions, pipeline};
use pif_core::{ChainConfig, PipelineConfig};
use pif_e2e::{database_url, dev_node_url};
use sqlx::{PgPool, Row};

/// The range every test in this file indexes. Small enough to be quick, large enough to
/// cross a segment boundary at `segment_size = 8`.
const LAST_BLOCK: u64 = 20;

/// A chain whose archive lives in a directory of its own, so tests cannot see each other's
/// segments and a stale run cannot make a later one pass.
fn chain_config(id: &str, dir: &std::path::Path, handlers: Vec<String>) -> ChainConfig {
    ChainConfig::rpc(id, dev_node_url())
        .with_handlers(handlers)
        .with_pipeline(PipelineConfig {
            hot_path: dir.to_path_buf(),
            // Deliberately tiny: the range above then spans three segment files, so segment
            // rollover is exercised rather than assumed.
            segment_size: 8,
        })
}

async fn pool() -> Result<PgPool> {
    let pool = pif_db::connect(&database_url(), 5)
        .await
        .context("connecting to Postgres; is `docker compose up -d` running?")?;
    pif_db::migrate(&pool).await?;
    Ok(pool)
}

async fn reset(pool: &PgPool, chain_id: &str) -> Result<()> {
    // Everything else cascades from `chains`, including `pipeline_watermarks`.
    sqlx::query("DELETE FROM chains WHERE id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Every row the dynamic core writes for a block, as one comparable value.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    blocks: Vec<(i64, String, String, i32, i32, i32)>,
    extrinsics: Vec<(i64, i32, String, String, String, bool, bool)>,
    events: Vec<(i64, i32, String, String, String, String)>,
}

async fn snapshot(pool: &PgPool, chain_id: &str) -> Result<Snapshot> {
    let blocks = sqlx::query(
        "SELECT number, encode(hash,'hex'), encode(parent_hash,'hex'), spec_version, \
         extrinsic_count, event_count FROM blocks WHERE chain_id = $1 ORDER BY number",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5)))
    .collect();

    let extrinsics = sqlx::query(
        "SELECT block_number, idx, encode(hash,'hex'), pallet, call, is_signed, success \
         FROM extrinsics WHERE chain_id = $1 ORDER BY block_number, idx",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|r| {
        (
            r.get(0),
            r.get(1),
            r.get(2),
            r.get(3),
            r.get(4),
            r.get(5),
            r.get(6),
        )
    })
    .collect();

    let events = sqlx::query(
        "SELECT block_number, idx, pallet, variant, phase, fields::text \
         FROM events WHERE chain_id = $1 ORDER BY block_number, idx",
    )
    .bind(chain_id)
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5)))
    .collect();

    Ok(Snapshot {
        blocks,
        extrinsics,
        events,
    })
}

/// The regression test that matters most: the split must produce the *same rows*.
///
/// Nothing about the archive is worth having if the blocks it yields differ from the ones
/// the network yielded. This indexes the same range twice under different chain ids and
/// compares every column the dynamic core writes.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn the_split_pipeline_writes_the_same_rows() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;

    let chain_id = "e2e-split";
    reset(&pool, chain_id).await?;
    pipeline::run(
        &pool,
        &chain_config(chain_id, dir.path(), vec![]),
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    let reference_id = "e2e-split-again";
    reset(&pool, reference_id).await?;
    let reference_dir = tempfile::tempdir()?;
    pipeline::run(
        &pool,
        &chain_config(reference_id, reference_dir.path(), vec![]),
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    let first = snapshot(&pool, chain_id).await?;
    let second = snapshot(&pool, reference_id).await?;

    assert_eq!(
        first.blocks.len() as u64,
        LAST_BLOCK + 1,
        "expected every block in 0..={LAST_BLOCK}"
    );
    // Compared column by column rather than by count: a pipeline that wrote the right
    // *number* of rows with the wrong contents is the failure worth catching. `chain_id` is
    // the one column that differs by construction, so `Snapshot` does not carry it.
    assert_eq!(first, second);

    Ok(())
}

/// The headline claim: replay reads the archive and touches no network.
///
/// Proved by pointing the chain at a **dead address** before replaying. If any part of the
/// digest still reached for the node, this could not pass — which is the point. A replay
/// that quietly becomes a re-download is the failure the archive exists to prevent, and
/// nobody notices until the bill arrives.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn a_replay_needs_no_network_at_all() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-replay-offline";
    reset(&pool, chain_id).await?;

    pipeline::run(
        &pool,
        &chain_config(chain_id, dir.path(), vec![]),
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    let before = snapshot(&pool, chain_id).await?;

    // Delete the rows but keep the `chains` row: the replay must rebuild them from the
    // archive alone, and it reads the chain's identity (id, ss58 prefix) back out of
    // Postgres rather than asking a node for it.
    sqlx::query("DELETE FROM blocks WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&pool)
        .await?;

    let unreachable =
        ChainConfig::rpc(chain_id, "ws://127.0.0.1:1").with_pipeline(PipelineConfig {
            hot_path: dir.path().to_path_buf(),
            segment_size: 8,
        });

    pipeline::replay(&pool, &unreachable, &pif_e2e::registry(), 0, LAST_BLOCK).await?;

    assert_eq!(
        snapshot(&pool, chain_id).await?,
        before,
        "a replay must reproduce exactly the rows the live run produced"
    );

    Ok(())
}

/// Killing the process between fetching and digesting must leave no gap and no duplicate.
///
/// The direct successor to `indexes_a_live_chain_and_resumes_after_restart`, for a pipeline
/// where "fetched" and "digested" are no longer the same number.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn a_fetch_without_a_digest_resumes_cleanly() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-split-resume";
    reset(&pool, chain_id).await?;
    let config = chain_config(chain_id, dir.path(), vec![]);

    // Fetch only — blocks land in the archive, nothing reaches Postgres.
    pipeline::fetch_only(
        &pool,
        &config,
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    let marks = pif_db::repo::load_watermarks(&pool, chain_id)
        .await?
        .expect("fetch must have created a watermark row");
    assert_eq!(marks.fetch, LAST_BLOCK as i64);
    assert_eq!(
        marks.digest, -1,
        "nothing has been digested yet, so the digest watermark must not have moved"
    );
    assert_eq!(
        count_blocks(&pool, chain_id).await?,
        0,
        "fetching must write no rows: it decodes nothing"
    );

    // Digest only — no further network use for the blocks themselves.
    pipeline::digest_only(
        &pool,
        &config,
        &pif_e2e::registry(),
        IndexOptions {
            from: None,
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    assert_eq!(count_blocks(&pool, chain_id).await?, LAST_BLOCK as i64 + 1);
    assert_eq!(
        pif_db::repo::count_gaps(&pool, chain_id).await?,
        0,
        "the stored chain must be contiguous"
    );

    let marks = pif_db::repo::load_watermarks(&pool, chain_id)
        .await?
        .expect("watermarks");
    assert_eq!(marks.digest, LAST_BLOCK as i64);

    Ok(())
}

/// The metadata registry: every runtime a block ran under must be archived, and recorded.
///
/// Losing one of these makes every block that ran under it permanently undecodable even
/// though the block bytes are intact, so "it was archived" is worth asserting rather than
/// assuming.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn every_runtime_seen_is_archived_and_recorded() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-split-metadata";
    reset(&pool, chain_id).await?;

    pipeline::run(
        &pool,
        &chain_config(chain_id, dir.path(), vec![]),
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    let unarchived = pif_db::repo::runtimes_without_metadata(&pool, chain_id).await?;
    assert!(
        unarchived.is_empty(),
        "every runtime this run indexed should have archived metadata, but {unarchived:?} do not"
    );

    let row = sqlx::query(
        "SELECT metadata_version, length(metadata_hash), transaction_version \
         FROM runtime_versions WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?;

    let metadata_version: i16 = row.get(0);
    let hash_len: i32 = row.get(1);
    let transaction_version: i32 = row.get(2);

    // V14 is the floor: below it metadata carries no type information at all.
    assert!(
        metadata_version >= 14,
        "archived metadata format v{metadata_version} is below the usable floor"
    );
    assert_eq!(hash_len, 32, "metadata hash should be a sha256");
    assert!(transaction_version > 0);

    Ok(())
}

async fn count_blocks(pool: &PgPool, chain_id: &str) -> Result<i64> {
    Ok(
        sqlx::query("SELECT count(*) FROM blocks WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(pool)
            .await?
            .get(0),
    )
}
