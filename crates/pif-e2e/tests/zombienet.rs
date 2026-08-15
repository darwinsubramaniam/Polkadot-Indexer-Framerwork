//! End-to-end test against a chain spawned by zombienet.
//!
//! This is the strongest statement of the indexer's central claim: the **same binary**,
//! with no recompilation and no per-chain metadata, indexes a chain it has never seen —
//! one created fresh moments earlier, with a genesis hash that did not exist at build time.
//!
//! Run with:
//!     ./scripts/fetch-zombie-cli.sh
//!     docker compose up -d          # for Postgres
//!     cargo test -p indexer-e2e --test zombienet -- --ignored --nocapture
//!
//! ## Why a single validator
//!
//! A multi-node network never reaches `peers > 0` on this setup, so it sits at block 0 and
//! finalizes nothing. Two distinct causes were identified:
//!
//! 1. **Wrong bootnode port.** The Docker provider builds the bootnode multiaddr from the
//!    node's host-mapped port while the node listens on its container port
//!    (`--bootnodes /ip4/172.17.0.2/tcp/53892/...` vs `--listen-addr .../tcp/30333/ws`).
//!    Setting `p2p_port` per node in the spec fixes this.
//!
//! 2. **Noise handshake failure.** Even with the address corrected and both containers
//!    mutually routable on the same bridge, peering dies with
//!    `failed to decrypt message ... buf_len=1929 frame_size=1945 error=Decrypt` — a
//!    16-byte gap, i.e. the Poly1305 tag, so the AEAD fails to authenticate. Suspected to
//!    stem from running the `linux/amd64` node under emulation on arm64, but unconfirmed.
//!
//! A single validator needs no peer connection: it authors and finalizes alone and
//! exercises the indexer identically. Parachain topologies remain blocked, since a collator
//! must reach the relay over p2p — see `crates/indexer-e2e/README.md`.

use anyhow::Result;
use pif_chain::{IndexOptions, pipeline};
use pif_core::ChainConfig;
use pif_e2e::{database_url, zombienet::Network};
use sqlx::{PgPool, Row};

/// A single-validator relay chain: authors and finalizes without any p2p connection.
const SINGLE_VALIDATOR: &str = r#"
[relaychain]
chain = "rococo-local"
default_command = "polkadot"
default_image = "parity/polkadot:stable2606"

[[relaychain.nodes]]
name = "alice"
validator = true
"#;

#[tokio::test]
#[ignore = "spawns a zombienet network via Docker; needs bin/zombie-cli and Postgres"]
async fn indexes_a_freshly_spawned_chain_with_no_recompile() -> Result<()> {
    let base_dir = std::env::temp_dir().join("indexer-e2e-zombienet");

    let Some(network) = Network::spawn(SINGLE_VALIDATOR, &base_dir)? else {
        return Ok(()); // environment cannot run zombienet; skip
    };

    let ws_url = network.ws_uri("alice")?;
    println!("zombienet spawned alice at {ws_url}");

    let pool: PgPool = pif_db::connect(&database_url(), 5).await?;
    pif_db::migrate(&pool).await?;

    // Every zombienet spawn produces a brand-new genesis, so a chain id reused across runs
    // would (correctly) trip `guard_chain_identity`. Clear it first.
    let chain_id = "zombienet-relay";
    sqlx::query("DELETE FROM chains WHERE id = $1")
        .bind(chain_id)
        .execute(&pool)
        .await?;

    let config = ChainConfig::rpc(chain_id, ws_url);

    // Wait until the chain has finalized enough blocks to be worth indexing.
    let client = pif_chain::ChainClient::connect(&config).await?;
    let target = wait_for_finalized(&client, 6).await?;
    println!("chain finalized up to block {target}");

    pipeline::run(
        &pool,
        &config,
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(target),
        },
    )
    .await?;

    // --- the chain was identified from the node, not from config ---
    let row = sqlx::query("SELECT name, genesis_hash FROM chains WHERE id = $1")
        .bind(chain_id)
        .fetch_one(&pool)
        .await?;
    let name: String = row.get(0);
    let genesis: Vec<u8> = row.get(1);

    assert_eq!(
        name, "Rococo Local Testnet",
        "chain name should come from the node"
    );
    assert_eq!(genesis.len(), 32);

    // This genesis did not exist when the binary was compiled.
    let compose_genesis: Option<Vec<u8>> =
        sqlx::query("SELECT genesis_hash FROM chains WHERE id = 'e2e-dynamic'")
            .fetch_optional(&pool)
            .await?
            .map(|r| r.get(0));
    if let Some(other) = compose_genesis {
        assert_ne!(
            genesis, other,
            "the zombienet chain must be distinct from the compose dev chain"
        );
    }

    // --- blocks were indexed contiguously ---
    let (count, lo, hi): (i64, i64, i64) = {
        let row = sqlx::query(
            "SELECT count(*), min(number), max(number) FROM blocks WHERE chain_id = $1",
        )
        .bind(chain_id)
        .fetch_one(&pool)
        .await?;
        (row.get(0), row.get(1), row.get(2))
    };
    assert_eq!(lo, 0);
    assert_eq!(hi, target as i64);
    assert_eq!(
        count,
        target as i64 + 1,
        "expected every block from 0..={target}"
    );
    assert_eq!(pif_db::repo::count_gaps(&pool, chain_id).await?, 0);

    // --- events were decoded dynamically, with no metadata compiled in ---
    let events: i64 = sqlx::query(
        "SELECT count(*) FROM events
         WHERE chain_id = $1 AND pallet = 'System' AND variant = 'ExtrinsicSuccess'",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);
    assert!(
        events > 0,
        "expected decoded System::ExtrinsicSuccess events"
    );

    // Weights must be numeric strings, not JSON numbers, or large values lose precision.
    let weight: Option<String> = sqlx::query(
        "SELECT fields #>> '{dispatch_info,weight,ref_time}' FROM events
         WHERE chain_id = $1 AND variant = 'ExtrinsicSuccess' LIMIT 1",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);
    assert!(
        weight
            .as_deref()
            .and_then(|w| w.parse::<u128>().ok())
            .is_some(),
        "dispatch weight should decode to a numeric string, got {weight:?}"
    );

    // --- timestamps came from the Timestamp::set inherent ---
    let timestamped: i64 = sqlx::query(
        "SELECT count(*) FROM blocks WHERE chain_id = $1 AND number > 0 AND timestamp IS NOT NULL",
    )
    .bind(chain_id)
    .fetch_one(&pool)
    .await?
    .get(0);
    assert_eq!(
        timestamped, target as i64,
        "every non-genesis block needs a timestamp"
    );

    println!(
        "indexed {count} blocks and {events} ExtrinsicSuccess events from a chain created minutes ago"
    );
    Ok(())
}

/// Poll until the chain has finalized at least `minimum` blocks.
async fn wait_for_finalized(client: &pif_chain::ChainClient, minimum: u64) -> Result<u64> {
    // Node images run under emulation on Apple Silicon; allow plenty of time.
    for _ in 0..60 {
        let head = client.finalized_number().await?;
        if head >= minimum {
            return Ok(head);
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
    anyhow::bail!("chain did not finalize {minimum} blocks in time")
}
