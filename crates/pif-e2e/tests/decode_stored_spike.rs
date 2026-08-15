//! Spike for IPD-002 §9.1: can a block be decoded from archived bytes with no network?
//!
//! The whole data-pipeline proposal rests on one assumption — that subxt can reconstruct
//! `Events` and `Extrinsics` from *bytes plus metadata*, with no client attached.
//! `Events::bytes()` and `ExtrinsicDetails::bytes()` prove the bytes come out for free; this
//! proves they go back in.
//!
//! The test captures a `RawBlock` from a live node, throws the connection away, rebuilds the
//! block through `OfflineClient`, and asserts that every primitive `decode_at` derives its
//! rows from is byte-identical between the two paths.
//!
//! This covers a *single* runtime. The runtime-upgrade boundary — an archived range spanning
//! two spec versions — is `upgrade_boundary.rs`.
//!
//! Run against the compose dev node or zombienet:
//!
//! ```sh
//! cargo test -p pif-e2e --test decode_stored_spike -- --ignored --nocapture
//! ```

mod common;

use common::offline::{Fetcher, decode_stored_standalone};
use subxt::{OnlineClient, PolkadotConfig, dynamic::Value, transactions::dynamic};
use subxt_signer::sr25519::dev;

fn node_url() -> String {
    std::env::var("PIF_TEST_NODE_URL").unwrap_or_else(|_| "ws://127.0.0.1:9944".to_owned())
}

/// Put a signed extrinsic on chain and return the finalized block it landed in.
///
/// A signed extrinsic is the case that matters: it is the only one carrying an address,
/// a signature and transaction extensions, and transaction-extension decoding is the part
/// of the offline path most likely to need something the archive does not hold. An idle
/// dev chain produces only inherents, so the test has to create this traffic itself.
async fn submit_signed_extrinsic() -> anyhow::Result<u64> {
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(&node_url()).await?;
    let alice = dev::alice();
    let bob = dev::bob();

    let call = dynamic(
        "Balances",
        "transfer_allow_death",
        vec![
            Value::unnamed_variant("Id", [Value::from_bytes(bob.public_key().0)]),
            Value::u128(1_234_567_890),
        ],
    );

    let at = api.at_current_block().await?;
    let in_block = at
        .transactions()
        .sign_and_submit_then_watch_default(&call, &alice)
        .await?
        .wait_for_finalized()
        .await?;
    in_block.wait_for_success().await?;

    let number = api.at_block(in_block.block_hash()).await?.block_number();

    println!("submitted signed transfer, finalized in block {number}");
    Ok(number)
}

#[tokio::test]
#[ignore = "requires a running node; see module docs"]
async fn a_block_decodes_identically_from_archived_bytes() -> anyhow::Result<()> {
    let url = node_url();

    // The signed block first — the case the idle-chain sweep below cannot produce.
    let signed_block = submit_signed_extrinsic().await?;

    let client = OnlineClient::<PolkadotConfig>::from_insecure_url(&url).await?;
    let head = client.at_current_block().await?.block_number();
    drop(client);

    let mut numbers = vec![signed_block];
    numbers.extend((1..=head).rev().take(40).filter(|n| *n != signed_block));

    let mut checked = 0;
    let mut signed_seen = false;

    // One connection, one metadata fetch per spec version — see `Fetcher`.
    let mut fetcher = Fetcher::connect(&url).await?;

    for number in numbers {
        let captured = fetcher.capture(number).await?;
        let raw = captured.raw;
        // This chain never upgrades mid-test, so the naive online decode must work here.
        // `upgrade_boundary.rs` is where its failure mode is the subject.
        let online = captured
            .naive_online
            .map_err(|e| anyhow::anyhow!("online decode of block {number} failed: {e}"))?;
        let offline = decode_stored_standalone(&raw).await?;

        assert_eq!(
            online, offline,
            "block {number} decoded differently offline"
        );

        checked += 1;
        let signed_here = online.extrinsics.iter().filter(|x| x.is_signed).count();
        signed_seen |= signed_here > 0;
        println!(
            "block {number}: {} events, {} extrinsics ({signed_here} signed), \
             {} bytes events blob — identical",
            online.events.len(),
            online.extrinsics.len(),
            raw.events.len()
        );
    }

    assert!(checked > 0, "no blocks checked");
    assert!(
        signed_seen,
        "no signed extrinsic was exercised — the spike proves less than it claims"
    );
    println!("checked {checked} blocks, signed extrinsic exercised");
    Ok(())
}
