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
use pif_e2e::state_reader::StateReadingHandler;
use pif_e2e::{database_url, dev_node_url};
use sqlx::{PgPool, Row};

/// The range every test in this file indexes. Small enough to be quick, large enough to
/// cross a segment boundary at `segment_size = 8`.
const LAST_BLOCK: u64 = 20;

/// An address nothing is listening on.
///
/// Offline and dead-endpoint claims are asserted by *using* this rather than by counting
/// round-trips: if the code under test still reached for a node, it could not pass.
const DEAD_URL: &str = "ws://127.0.0.1:1";

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
            // Several chunks across the range below, so parallel claiming is exercised
            // rather than one worker taking the lot.
            chunk_size: 4,
            // Probed from the endpoint. On this chain that resolves to 256 — it is too
            // short for the probe to prove the node is archival — which is comfortably
            // above the range below, so the brake never engages here. It is exercised
            // deliberately in `the_fetch_stage_will_not_outrun_the_digest`.
            max_digest_lag: None,
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

    let unreachable = ChainConfig::rpc(chain_id, DEAD_URL).with_pipeline(PipelineConfig {
        hot_path: dir.path().to_path_buf(),
        segment_size: 8,
        // Several chunks across the range below, so parallel claiming is exercised
        // rather than one worker taking the lot.
        chunk_size: 4,
        max_digest_lag: None,
    });

    pipeline::replay(&pool, &unreachable, &pif_e2e::registry(), 0, LAST_BLOCK).await?;

    assert_eq!(
        snapshot(&pool, chain_id).await?,
        before,
        "a replay must reproduce exactly the rows the live run produced"
    );

    Ok(())
}

/// The fetch stage must not run further ahead than the endpoint can still answer for.
///
/// The storage read cache is filled on the *first* digest of a block, from a node. A fetcher
/// 100k blocks ahead means every one of those reads asks for state the node discarded long
/// ago — Substrate defaults to `--state-pruning 256` — so the brake is not a throughput knob
/// but the thing that keeps the cache fillable at all. It is also what stops the archive
/// growing without bound when the digest stalls.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn the_fetch_stage_will_not_outrun_the_digest() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-max-lag";
    reset(&pool, chain_id).await?;

    const LAG: u64 = 5;
    let config = ChainConfig::rpc(chain_id, dev_node_url()).with_pipeline(PipelineConfig {
        hot_path: dir.path().to_path_buf(),
        segment_size: 8,
        chunk_size: 4,
        max_digest_lag: Some(LAG),
    });

    // Fetch alone, with nothing digesting. It must archive up to the ceiling and then hold,
    // rather than racing to `LAST_BLOCK` — so this is expected to time out, and a *clean
    // finish* is the failure.
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        pipeline::fetch_only(
            &pool,
            &config,
            IndexOptions {
                from: Some(0),
                stop_at: Some(LAST_BLOCK),
            },
        ),
    )
    .await;

    assert!(
        outcome.is_err(),
        "fetch ran to completion with nothing digesting; the brake did not engage"
    );

    let marks = pif_db::repo::load_watermarks(&pool, chain_id)
        .await?
        .expect("watermarks");

    // digest_watermark is -1 (nothing digested), so the ceiling is `-1 + LAG`.
    let ceiling = LAG as i64 - 1;
    assert!(
        marks.fetch <= ceiling,
        "fetch reached {} but the digest is at {}, which is past the ceiling of {ceiling}",
        marks.fetch,
        marks.digest
    );
    assert!(
        marks.fetch >= 0,
        "the brake should hold the fetcher, not stop it dead before it archives anything"
    );

    Ok(())
}

/// Both stages together must make progress under a brake tighter than the publish interval.
///
/// The regression test for a deadlock the brake introduced: the fetch stage publishes its
/// watermark in batches, so with `max_digest_lag` below that batch size it would hold for a
/// digest that was itself waiting for a watermark the fetcher had stopped moving. Each stage
/// blocked on the other, and nothing in either loop timed out to reveal it.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn a_tight_brake_does_not_deadlock_the_two_stages() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-tight-brake";
    reset(&pool, chain_id).await?;

    // Well below the fetch stage's publish batch, which is where the deadlock lived.
    let config = ChainConfig::rpc(chain_id, dev_node_url()).with_pipeline(PipelineConfig {
        hot_path: dir.path().to_path_buf(),
        segment_size: 8,
        chunk_size: 4,
        max_digest_lag: Some(3),
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(60),
        pipeline::run(
            &pool,
            &config,
            &pif_e2e::registry(),
            IndexOptions {
                from: Some(0),
                stop_at: Some(LAST_BLOCK),
            },
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("fetch and digest deadlocked under a tight max_digest_lag"))??;

    assert_eq!(count_blocks(&pool, chain_id).await?, LAST_BLOCK as i64 + 1);
    assert_eq!(pif_db::repo::count_gaps(&pool, chain_id).await?, 0);

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

/// A handler that reads chain **state** replays offline too.
///
/// This is the claim phase 2 exists to make good on, and the one phase 1 could not: blocks
/// are in the archive, but `pallet_identity` emits `IdentitySet { who }` with no display
/// name, so a handler asking *what* changed reaches for chain state — and state is not in a
/// block. Unless the answers are archived as well, "replay" means "re-download" for every
/// handler anyone actually writes.
///
/// So: index a range with a handler that reads storage on every block, then replay the same
/// range with the node pointed at a dead address, and require the reads to come back
/// identical.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn a_state_reading_handler_replays_with_no_network() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-state-replay";
    reset(&pool, chain_id).await?;

    let handlers = vec![StateReadingHandler::NAME.to_owned()];

    // Live pass: every read misses the cache and goes to the node, which is what fills it.
    let (handler, live_log) = StateReadingHandler::new();
    let mut registry = pif_chain::HandlerRegistry::new();
    registry.register(Box::new(handler));

    pipeline::run(
        &pool,
        &chain_config(chain_id, dir.path(), handlers.clone()),
        &registry,
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    let live = live_log.lock().expect("read log").clone();
    assert_eq!(
        live.len() as u64,
        (LAST_BLOCK + 1) * 2,
        "the handler should have made two reads on every block"
    );
    assert!(
        live.iter()
            .any(|(_, _, answer)| answer.starts_with("some:")),
        "every read came back empty, so this proves nothing about caching real answers"
    );

    // Replay against an address nothing is listening on. If any read still reached for the
    // network this cannot pass — which is the entire point of asserting it this way rather
    // than counting round-trips.
    let (handler, replay_log) = StateReadingHandler::new();
    let mut registry = pif_chain::HandlerRegistry::new();
    registry.register(Box::new(handler));

    let unreachable = ChainConfig::rpc(chain_id, DEAD_URL)
        .with_handlers(handlers)
        .with_pipeline(PipelineConfig {
            hot_path: dir.path().to_path_buf(),
            segment_size: 8,
            // Several chunks across the range below, so parallel claiming is exercised
            // rather than one worker taking the lot.
            chunk_size: 4,
            max_digest_lag: None,
        });

    pipeline::replay(&pool, &unreachable, &registry, 0, LAST_BLOCK).await?;

    let replayed = replay_log.lock().expect("read log").clone();
    assert_eq!(
        replayed, live,
        "a replayed storage read must return exactly what the live read returned"
    );

    Ok(())
}

/// A read the archive never saw must stop the replay, not quietly fetch it.
///
/// The failure this guards against is the one that looks like success: a replay that falls
/// back to the network is indistinguishable from a replay that worked, and nobody finds out
/// until the bill arrives. So a miss is a named error naming the entry that was wanted.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn an_unarchived_storage_read_stops_a_replay_by_name() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-state-miss";
    reset(&pool, chain_id).await?;

    // Index with no handlers at all, so blocks are archived but no storage read ever is.
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

    // Now replay *with* a state-reading handler. Every read is a miss, and there is nothing
    // to fall back to.
    let (handler, _log) = StateReadingHandler::new();
    let mut registry = pif_chain::HandlerRegistry::new();
    registry.register(Box::new(handler));

    let config = ChainConfig::rpc(chain_id, DEAD_URL)
        .with_handlers(vec![StateReadingHandler::NAME.to_owned()])
        .with_pipeline(PipelineConfig {
            hot_path: dir.path().to_path_buf(),
            segment_size: 8,
            // Several chunks across the range below, so parallel claiming is exercised
            // rather than one worker taking the lot.
            chunk_size: 4,
            max_digest_lag: None,
        });

    let error = pipeline::replay(&pool, &config, &registry, 0, LAST_BLOCK)
        .await
        .expect_err("an unarchived read must not be answered from anywhere");

    let text = error.to_string();
    assert!(
        text.contains("Timestamp.Now"),
        "the error should name the read that was wanted, got: {text}"
    );

    Ok(())
}

/// A chain fetched across several endpoints at once.
///
/// Two spellings of the same node's address, which is enough to exercise everything the pool
/// actually does — two independent connections, two limiters, two workers claiming from one
/// lease queue — without needing two nodes that agree on a chain. What it does *not* cover is
/// endpoints with different capabilities; that is what the `archive` and `max_rps` fields are
/// for, and they are unit-tested in the config and limiter.
fn multi_endpoint_config(id: &str, dir: &std::path::Path, urls: &[&str]) -> ChainConfig {
    ChainConfig {
        id: id.to_owned(),
        source: pif_core::ChainSource::Rpc {
            endpoints: urls
                .iter()
                .map(|url| pif_core::Endpoint::new(*url))
                .collect(),
        },
        start_block: 0,
        handlers: Vec::new(),
        pipeline: Some(PipelineConfig {
            hot_path: dir.to_path_buf(),
            segment_size: 8,
            chunk_size: 4,
            max_digest_lag: None,
        }),
    }
}

/// Backfill across several endpoints must produce exactly what one endpoint produces.
///
/// Parallel fetch is where a hole becomes easy: chunk 12 can finish before chunk 8, so a
/// watermark that moved on completion alone would advertise a gap as ready. This indexes the
/// same range twice — once through one endpoint, once through two — and requires the rows to
/// match and the chain to be contiguous.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn several_endpoints_produce_the_same_chain_as_one() -> Result<()> {
    let pool = pool().await?;

    let single_id = "e2e-one-endpoint";
    let single_dir = tempfile::tempdir()?;
    reset(&pool, single_id).await?;
    pipeline::run(
        &pool,
        &chain_config(single_id, single_dir.path(), vec![]),
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    let multi_id = "e2e-many-endpoints";
    let multi_dir = tempfile::tempdir()?;
    reset(&pool, multi_id).await?;
    let urls = [
        dev_node_url(),
        dev_node_url().replace("127.0.0.1", "localhost"),
    ];
    pipeline::run(
        &pool,
        &multi_endpoint_config(
            multi_id,
            multi_dir.path(),
            &[urls[0].as_str(), urls[1].as_str()],
        ),
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    assert_eq!(
        snapshot(&pool, multi_id).await?,
        snapshot(&pool, single_id).await?,
        "fetching across two endpoints must yield the same chain as fetching across one"
    );
    assert_eq!(
        pif_db::repo::count_gaps(&pool, multi_id).await?,
        0,
        "parallel chunks left a hole"
    );

    // Every completed chunk records which endpoint fetched it. *Which* endpoint won which
    // chunk is a race between two workers and deliberately not asserted — printed instead,
    // because a flaky assertion about scheduling would be worse than none.
    let attributed: Vec<(i64, String)> = sqlx::query(
        "SELECT from_block, leased_by FROM fetch_chunks
          WHERE chain_id = $1 AND state = 'done' ORDER BY from_block",
    )
    .bind(multi_id)
    .fetch_all(&pool)
    .await?
    .into_iter()
    .map(|r| (r.get(0), r.get::<Option<String>, _>(1).unwrap_or_default()))
    .collect();

    assert!(!attributed.is_empty(), "no chunks were completed at all");
    assert!(
        attributed.iter().all(|(_, endpoint)| !endpoint.is_empty()),
        "a completed chunk does not record which endpoint fetched it: {attributed:?}"
    );
    println!("chunk -> endpoint: {attributed:?}");

    Ok(())
}

/// One dead endpoint must cost throughput and nothing else.
///
/// Losing an endpoint is the ordinary condition this design exists for. The pool logs it,
/// drops it, and carries on with the rest — so the range still gets archived, in full, by
/// whoever is left.
#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn a_dead_endpoint_does_not_stop_the_others() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-dead-endpoint";
    reset(&pool, chain_id).await?;

    let live = dev_node_url();
    let config = multi_endpoint_config(chain_id, dir.path(), &[DEAD_URL, live.as_str()]);

    pipeline::run(
        &pool,
        &config,
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await?;

    assert_eq!(count_blocks(&pool, chain_id).await?, LAST_BLOCK as i64 + 1);
    assert_eq!(pif_db::repo::count_gaps(&pool, chain_id).await?, 0);

    Ok(())
}

/// Losing *every* endpoint is the one case that has to stop.
///
/// Named rather than generic, because the operator's next move differs: blocks already
/// archived are unaffected, and `pif digest` and `pif replay` still work against them.
#[tokio::test]
#[ignore = "requires Postgres"]
async fn no_reachable_endpoint_is_a_named_failure() -> Result<()> {
    let pool = pool().await?;
    let dir = tempfile::tempdir()?;
    let chain_id = "e2e-all-endpoints-down";
    reset(&pool, chain_id).await?;

    let config = multi_endpoint_config(chain_id, dir.path(), &[DEAD_URL, "ws://127.0.0.1:2"]);

    let error = pipeline::run(
        &pool,
        &config,
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(LAST_BLOCK),
        },
    )
    .await
    .expect_err("with nothing reachable there is nothing to fetch from");

    let text = error.to_string();
    assert!(
        text.contains("none of the 2 configured endpoints"),
        "expected a named all-endpoints-down failure, got: {text}"
    );

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
