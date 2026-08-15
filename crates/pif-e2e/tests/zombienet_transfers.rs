//! Submit real transfers on a zombienet-spawned chain and verify the indexer records them.
//!
//! This closes the loop on the indexer's central claim. `zombienet.rs` shows that a chain
//! created minutes ago can be indexed with no recompile; this shows that *activity* on such
//! a chain flows all the way through both layers:
//!
//!   * the **dynamic core** stores every `Balances::Transfer` as queryable JSONB, driven
//!     purely by the metadata the node reports; and
//!   * the **typed overlay** projects those same events into the `transfers` table with
//!     exact `u128` amounts and SS58 addresses.
//!
//! Because both layers decode the same block, a disagreement between them is a real bug —
//! so the test asserts on both and cross-checks the counts.
//!
//! Run with:
//!     ./scripts/fetch-zombie-cli.sh
//!     docker compose up -d          # for Postgres
//!     cargo test -p indexer-e2e --features handler-balances \
//!         --test zombienet_transfers -- --ignored --nocapture
//!
//! This lives in its own test binary so its network never spawns concurrently with the one
//! in `zombienet.rs` — cargo runs test binaries sequentially, but threads tests within one.

#![cfg(feature = "handler-balances")]

use anyhow::{Context, Result};
use pif_chain::{IndexOptions, pipeline};
use pif_core::{ChainConfig, ss58};
use pif_e2e::{database_url, zombienet::Network};
use sqlx::{PgPool, Row};
use subxt::{OnlineClient, PolkadotConfig, dynamic::Value, transactions::dynamic};
use subxt_signer::sr25519::{Keypair, dev};

/// Alice needs an explicit `initial_balance`: zombienet's default for a node account is
/// only ~2e12, which is less than the transfers below (deliberately, one of them exceeds
/// 2^53). Without this the second transfer fails with `Token error: Funds are unavailable`.
const SINGLE_VALIDATOR: &str = r#"
[relaychain]
chain = "rococo-local"
default_command = "polkadot"
default_image = "parity/polkadot:stable2606"

[[relaychain.nodes]]
name = "alice"
validator = true
initial_balance = 1000000000000000000
"#;

/// Distinct amounts so each transfer is individually identifiable in the database.
/// The last one is deliberately past 2^53, where a JSON number would lose precision.
const TRANSFERS: [(&str, u128); 3] = [
    ("bob", 1_111_111_111_111),
    ("charlie", 2_222_222_222_222),
    ("dave", 9_007_199_254_740_993), // 2^53 + 1
];

fn recipient(name: &str) -> Keypair {
    match name {
        "bob" => dev::bob(),
        "charlie" => dev::charlie(),
        "dave" => dev::dave(),
        other => panic!("unknown dev account {other}"),
    }
}

#[tokio::test]
#[ignore = "spawns a zombienet network via Docker; needs bin/zombie-cli and Postgres"]
async fn records_transfers_made_on_a_freshly_spawned_chain() -> Result<()> {
    let base_dir = std::env::temp_dir().join("indexer-e2e-zombienet-transfers");

    let Some(network) = Network::spawn(SINGLE_VALIDATOR, &base_dir)? else {
        return Ok(()); // environment cannot run zombienet; skip
    };

    let ws_url = network.ws_uri("alice")?;
    println!("zombienet spawned alice at {ws_url}");

    let pool: PgPool = pif_db::connect(&database_url(), 5).await?;
    pif_db::migrate(&pool).await?;

    // A fresh spawn means a fresh genesis, which would (correctly) trip
    // `guard_chain_identity` if this id were still bound to an earlier network.
    let chain_id = "zombienet-transfers";
    sqlx::query("DELETE FROM chains WHERE id = $1")
        .bind(chain_id)
        .execute(&pool)
        .await?;

    let config = ChainConfig {
        id: chain_id.to_owned(),
        ws_url: ws_url.clone(),
        start_block: 0,
        handlers: vec!["balances-transfer".to_owned()],
    };

    // ---- submit real transfers -------------------------------------------------------
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(&ws_url)
        .await
        .context("connecting to the zombienet node")?;

    let alice = dev::alice();
    let alice_ss58 = ss58::encode(&alice.public_key().0, 42);
    let mut expected = Vec::new();

    for (name, amount) in TRANSFERS {
        let to = recipient(name);
        let to_ss58 = ss58::encode(&to.public_key().0, 42);

        let call = dynamic(
            "Balances",
            "transfer_allow_death",
            vec![
                // MultiAddress::Id(<account>)
                Value::unnamed_variant("Id", [Value::from_bytes(to.public_key().0)]),
                Value::u128(amount),
            ],
        );

        // Waiting for finalization between submissions also keeps the nonce in order.
        let at = api.at_current_block().await?;
        at.transactions()
            .sign_and_submit_then_watch_default(&call, &alice)
            .await?
            .wait_for_finalized_success()
            .await
            .with_context(|| format!("transfer of {amount} to {name}"))?;

        println!("submitted: alice -> {name}, {amount}");
        expected.push((to_ss58, amount));
    }

    // ---- index the chain -------------------------------------------------------------
    let client = pif_chain::ChainClient::connect(&config).await?;
    let head = client.finalized_number().await?;
    println!("indexing 0..={head}");

    pipeline::run(
        &pool,
        &config,
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(head),
        },
    )
    .await?;

    // ---- the dynamic core stored the raw events --------------------------------------
    let raw_transfer_events: i64 = sqlx::query(
        "SELECT count(*) FROM events
         WHERE chain_id = $1 AND pallet = 'Balances' AND variant = 'Transfer'",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);

    assert_eq!(
        raw_transfer_events,
        TRANSFERS.len() as i64,
        "dynamic core should have stored one Balances::Transfer event per submission"
    );

    // ---- the typed overlay projected each one ----------------------------------------
    let projected: i64 = sqlx::query("SELECT count(*) FROM transfers WHERE chain_id = $1")
        .bind(chain_id)
        .fetch_one(&pool)
        .await?
        .get(0);

    assert_eq!(
        projected, raw_transfer_events,
        "typed overlay and dynamic core disagree about how many transfers occurred"
    );

    // Each individual transfer must be present with an exact amount and SS58 addresses.
    for (to_ss58, amount) in &expected {
        let row = sqlx::query(
            "SELECT from_address, to_address, amount::text, block_number, event_idx
             FROM transfers
             WHERE chain_id = $1 AND to_address = $2 AND amount::text = $3",
        )
        .bind(chain_id)
        .bind(to_ss58)
        .bind(amount.to_string())
        .fetch_optional(&pool)
        .await?
        .with_context(|| format!("no transfers row for {amount} -> {to_ss58}"))?;

        let from: String = row.get(0);
        let stored_amount: String = row.get(2);
        let block: i64 = row.get(3);

        assert_eq!(from, alice_ss58, "sender should be Alice in SS58 form");
        assert_eq!(
            stored_amount,
            amount.to_string(),
            "amount must survive as an exact integer"
        );
        assert!(block > 0);

        println!("verified: {from} -> {to_ss58}, {stored_amount} (block {block})");
    }

    // The 2^53+1 case specifically: a JSON-number round-trip would have corrupted this.
    let big = TRANSFERS[2].1;
    assert!(big > (1u64 << 53) as u128);
    let big_exact: Option<String> =
        sqlx::query("SELECT amount::text FROM transfers WHERE chain_id = $1 AND amount::text = $2")
            .bind(chain_id)
            .bind(big.to_string())
            .fetch_optional(&pool)
            .await?
            .map(|r| r.get(0));
    assert_eq!(
        big_exact.as_deref(),
        Some(big.to_string().as_str()),
        "a value above 2^53 must round-trip exactly"
    );

    // ---- the two layers agree on the details -----------------------------------------
    // Same block and event index in both tables: the projection is genuinely derived from
    // the stored event, not independently (and possibly differently) decoded.
    let mismatched: i64 = sqlx::query(
        "SELECT count(*) FROM transfers t
         WHERE t.chain_id = $1
           AND NOT EXISTS (
               SELECT 1 FROM events e
               WHERE e.chain_id = t.chain_id
                 AND e.block_number = t.block_number
                 AND e.idx = t.event_idx
                 AND e.pallet = 'Balances' AND e.variant = 'Transfer')",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);
    assert_eq!(
        mismatched, 0,
        "every transfers row must line up with the Balances::Transfer event it came from"
    );

    println!(
        "all {} transfers recorded by both the dynamic core and the typed overlay",
        TRANSFERS.len()
    );
    Ok(())
}
