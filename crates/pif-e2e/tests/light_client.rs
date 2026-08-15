//! Light-client transport, against the live Polkadot network.
//!
//! Ignored by default: it dials real bootnodes over the public internet and waits for
//! smoldot to warp-sync from the spec's checkpoint to the head, which takes tens of seconds
//! on a good connection. Needs no node, no database and no Docker — that is rather the
//! point of a light client.
//!
//!   just fetch-chain-specs
//!   cargo test -p pif-e2e --features light-client --test light_client -- --ignored --nocapture
#![cfg(feature = "light-client")]

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use pif_chain::{ChainClient, decode};
use pif_core::ChainConfig;

/// Long enough for a warp sync from the spec's checkpoint, short enough to fail rather than
/// hang a CI run forever.
const SYNC_TIMEOUT: Duration = Duration::from_secs(180);

fn spec(name: &str) -> Option<PathBuf> {
    let path = pif_e2e::repo_root().join("config/specs").join(name);
    if !path.is_file() {
        eprintln!(
            "skipping: {} not found — run `just fetch-chain-specs`",
            path.display()
        );
        return None;
    }
    Some(path)
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "network; run with --ignored"]
async fn follows_polkadot_through_a_light_client() -> Result<()> {
    let Some(chain_spec) = spec("polkadot.json") else {
        return Ok(());
    };

    let config = ChainConfig::light_client("polkadot-lc", chain_spec);

    let chain = tokio::time::timeout(SYNC_TIMEOUT, ChainClient::connect(&config))
        .await
        .context("light client did not sync within the timeout")?
        .context("connecting the light client")?;

    // Identity discovery goes through the same code path as an RPC source: whatever the
    // transport, the chain still has to introduce itself.
    println!("connected: {}", chain.info);
    assert_eq!(chain.info.name, "Polkadot");
    assert_eq!(chain.info.token_symbol.as_deref(), Some("DOT"));
    assert_eq!(chain.info.ss58_prefix, 0);
    assert_eq!(
        hex::encode(&chain.info.genesis_hash),
        "91b171bb158e2d3848fa23a9f1c25182fb8e20313b2c1eb49219da7a70ce90c3"
    );

    // One finalized block, decoded from the reference the stream handed us. This is the
    // path `pipeline::follow_head` takes, and the only one a light client can serve.
    let mut blocks = chain.client.stream_blocks().await?;
    let block = tokio::time::timeout(SYNC_TIMEOUT, blocks.next())
        .await
        .context("no finalized block arrived within the timeout")?
        .context("finalized block stream ended")??;

    let at = decode::at_streamed_block(&block).await?;
    let data = decode::decode_at(&chain.client, &chain.rpc, &at, &chain.info).await?;

    println!(
        "block {} — {} extrinsics, {} events",
        data.block.number,
        data.extrinsics.len(),
        data.events.len()
    );
    assert_eq!(data.block.number as u64, block.number());
    assert!(
        !data.extrinsics.is_empty(),
        "every Substrate block carries at least a timestamp extrinsic"
    );

    Ok(())
}

/// The failure mode that matters most: asking a light client for history.
///
/// Cheap and offline — it never gets as far as starting smoldot.
#[test]
fn a_light_client_chain_refuses_a_start_block() {
    let config = ChainConfig::light_client("polkadot-lc", "specs/polkadot.json").from_block(100);
    let indexer = pif_core::IndexerConfig {
        chains: vec![config],
    };

    let err = indexer.validate().unwrap_err();
    assert!(err.to_string().contains("light client"), "got: {err}");
}
