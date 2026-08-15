//! End-to-end tests against the docker-compose dev node.
//!
//! Run with:
//!     docker compose up -d
//!     cargo test -p indexer-e2e --features indexer-chain/handler-balances -- --ignored --nocapture
//!
//! These are `#[ignore]`d because they need a live chain and database; `cargo test` on its
//! own stays hermetic.

use anyhow::{Context, Result};
use pif_chain::{IndexOptions, pipeline};
use pif_core::ChainConfig;
use pif_e2e::{database_url, dev_node_url};
use sqlx::{PgPool, Row};

/// Each test gets its own chain id so runs do not fight over the same rows.
fn chain_config(id: &str, handlers: Vec<String>) -> ChainConfig {
    ChainConfig::rpc(id, dev_node_url()).with_handlers(handlers)
}

async fn pool() -> Result<PgPool> {
    let pool = pif_db::connect(&database_url(), 5)
        .await
        .context("connecting to Postgres; is `docker compose up -d` running?")?;
    pif_db::migrate(&pool).await?;
    Ok(pool)
}

/// Remove any rows from a previous run of the same test.
async fn reset(pool: &PgPool, chain_id: &str) -> Result<()> {
    // blocks/extrinsics/events/transfers all cascade from `chains`.
    sqlx::query("DELETE FROM chains WHERE id = $1")
        .bind(chain_id)
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn indexes_a_live_chain_and_resumes_after_restart() -> Result<()> {
    let pool = pool().await?;
    let chain_id = "e2e-resume";
    reset(&pool, chain_id).await?;
    let config = chain_config(chain_id, vec![]);

    // First run: index a small range.
    pipeline::run(
        &pool,
        &config,
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(5),
        },
    )
    .await?;

    let cursor_after_first: i64 =
        sqlx::query("SELECT last_indexed_block FROM indexer_state WHERE chain_id = $1")
            .bind(chain_id)
            .fetch_one(&pool)
            .await?
            .get(0);
    assert_eq!(
        cursor_after_first, 5,
        "cursor should track the last block written"
    );

    // Second run with no `from`: must resume from the cursor rather than re-indexing.
    pipeline::run(
        &pool,
        &config,
        &pif_e2e::registry(),
        IndexOptions {
            from: None,
            stop_at: Some(10),
        },
    )
    .await?;

    let (count, min, max): (i64, i64, i64) = {
        let row = sqlx::query(
            "SELECT count(*), min(number), max(number) FROM blocks WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_one(&pool)
        .await?;
        (row.get(0), row.get(1), row.get(2))
    };

    assert_eq!((min, max), (0, 10));
    // 0..=10 inclusive, each exactly once: proves resume neither skipped nor duplicated.
    assert_eq!(count, 11, "expected 11 distinct blocks, got {count}");
    assert_eq!(
        pif_db::repo::count_gaps(&pool, chain_id).await?,
        0,
        "restart must not leave a hole in the indexed range"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
async fn decodes_events_dynamically_without_compiled_metadata() -> Result<()> {
    let pool = pool().await?;
    let chain_id = "e2e-dynamic";
    reset(&pool, chain_id).await?;

    pipeline::run(
        &pool,
        &chain_config(chain_id, vec![]),
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(5),
        },
    )
    .await?;

    // The chain's identity was discovered from the node, not read from config.
    let row = sqlx::query("SELECT name, genesis_hash, ss58_prefix FROM chains WHERE id = $1")
        .bind(chain_id)
        .fetch_one(&pool)
        .await?;
    let name: String = row.get(0);
    let genesis: Vec<u8> = row.get(1);
    assert!(!name.is_empty(), "chain name should come from system_chain");
    assert_eq!(genesis.len(), 32, "genesis hash should be 32 bytes");

    // Events decoded into queryable JSONB, with no runtime compiled in.
    let success_events: i64 = sqlx::query(
        "SELECT count(*) FROM events
         WHERE chain_id = $1 AND pallet = 'System' AND variant = 'ExtrinsicSuccess'",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);
    assert!(
        success_events > 0,
        "expected System::ExtrinsicSuccess events"
    );

    // Weights are u128-ish values; they must be strings, not JSON numbers, or precision
    // is silently lost for large values.
    let weight: Option<String> = sqlx::query(
        "SELECT fields #>> '{dispatch_info,weight,ref_time}' FROM events
         WHERE chain_id = $1 AND variant = 'ExtrinsicSuccess' LIMIT 1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);
    let weight = weight.expect("ExtrinsicSuccess should carry a decoded dispatch weight");
    assert!(
        weight.parse::<u128>().is_ok(),
        "weight should be a numeric string, got {weight:?}"
    );

    // Timestamps come from the Timestamp::set inherent, not the header.
    let with_timestamp: i64 = sqlx::query(
        "SELECT count(*) FROM blocks WHERE chain_id = $1 AND number > 0 AND timestamp IS NOT NULL",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);
    assert_eq!(
        with_timestamp, 5,
        "every non-genesis block should have a timestamp"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "requires a running dev node and Postgres"]
#[cfg(feature = "handler-balances")]
async fn typed_overlay_projects_a_real_transfer() -> Result<()> {
    // Scoped to this test: it is the only one that submits a transaction, and the whole
    // test is feature-gated, so top-level imports would be unused on a default build.
    use subxt::{OnlineClient, PolkadotConfig, dynamic::Value, transactions::dynamic};
    use subxt_signer::sr25519::dev;

    let pool = pool().await?;
    let chain_id = "e2e-transfer";
    reset(&pool, chain_id).await?;

    // Submit a real balance transfer and wait for it to finalize.
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(dev_node_url()).await?;
    let at = api.at_current_block().await?;

    let transfer = dynamic(
        "Balances",
        "transfer_allow_death",
        vec![
            // MultiAddress::Id(Bob)
            Value::unnamed_variant("Id", [Value::from_bytes(dev::bob().public_key().0)]),
            Value::u128(1_234_567_890_123u128),
        ],
    );

    let events = at
        .transactions()
        .sign_and_submit_then_watch_default(&transfer, &dev::alice())
        .await?
        .wait_for_finalized_success()
        .await?;

    let transfer_block = events.extrinsic_hash();
    println!(
        "transfer finalized, extrinsic hash 0x{}",
        hex::encode(transfer_block.0)
    );

    // Index up to and including the block that contains it.
    let head = {
        let config = chain_config(chain_id, vec!["balances-transfer".to_owned()]);
        let client = pif_chain::ChainClient::connect(&config).await?;
        client.finalized_number().await?
    };

    pipeline::run(
        &pool,
        &chain_config(chain_id, vec!["balances-transfer".to_owned()]),
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(head),
        },
    )
    .await?;

    // The dynamic core must have stored the raw event...
    let raw_events: i64 = sqlx::query(
        "SELECT count(*) FROM events
         WHERE chain_id = $1 AND pallet = 'Balances' AND variant = 'Transfer'",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);
    assert!(
        raw_events > 0,
        "dynamic core should have stored the Transfer event"
    );

    // ...and the typed overlay must have projected it into `transfers`.
    let row = sqlx::query(
        "SELECT from_address, to_address, amount::text FROM transfers
         WHERE chain_id = $1 ORDER BY block_number DESC LIMIT 1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?;

    let from: String = row.get(0);
    let to: String = row.get(1);
    let amount: String = row.get(2);

    assert_eq!(
        amount, "1234567890123",
        "amount must survive as an exact integer"
    );
    assert!(!from.is_empty() && !to.is_empty());
    println!("projected transfer: {from} -> {to}, {amount}");

    Ok(())
}
