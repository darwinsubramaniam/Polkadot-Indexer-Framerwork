//! IPD-002 §9.1.1 gap: does offline decode still work across a **runtime upgrade**?
//!
//! `decode_stored_spike.rs` proved a block can be rebuilt from archived bytes, but every
//! block it tested shared one `spec_version`. The archive keys runtime metadata by spec
//! version precisely so a replay can span an upgrade — and nothing verified that.
//!
//! This test performs a real forward runtime upgrade on a throwaway chain, archives blocks
//! either side of it, then decodes the whole range through **one** offline client holding
//! both metadata versions. If the client picked metadata per *range* rather than per
//! *block*, or if the archive were missing anything an upgrade changes, the blocks on one
//! side of the boundary would decode differently or not at all.
//!
//! ```sh
//! just zn-upgrade-up
//! ./scripts/fetch-westend-runtime.sh
//! cargo test -p pif-e2e --test upgrade_boundary -- --ignored --nocapture
//! just zn-upgrade-down
//! ```
//!
//! The network (`networks/upgrade-boundary.toml`) deliberately starts one release behind the
//! rest of the suite so there is a newer runtime to upgrade *to*; see that file for why the
//! direction matters.

mod common;

use std::time::Duration;

use anyhow::Context;
use common::offline::{Fetcher, Projection, RawBlock, decode_stored, offline_client_for};
use pif_core::ChainConfig;
use subxt::{OnlineClient, PolkadotConfig, dynamic::Value, transactions::dynamic};
use subxt_signer::sr25519::dev;

fn node_url() -> String {
    std::env::var("PIF_UPGRADE_NODE_URL").unwrap_or_else(|_| "ws://127.0.0.1:9977".to_owned())
}

/// The runtime to upgrade *to*, fetched by `scripts/fetch-westend-runtime.sh`.
fn upgrade_wasm() -> anyhow::Result<Vec<u8>> {
    let path = std::env::var("PIF_UPGRADE_WASM").unwrap_or_else(|_| {
        format!(
            "{}/../../.zombienet/runtimes/westend_runtime-v1024001.compact.compressed.wasm",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    std::fs::read(&path).with_context(|| {
        format!("missing runtime wasm at {path} — run ./scripts/fetch-westend-runtime.sh")
    })
}

/// `Sudo::sudo_unchecked_weight(System::set_code(code), 0)`.
///
/// `sudo` rather than `sudo_unchecked_weight` would be rejected: `set_code`'s declared weight
/// is a whole block, so dispatching it through the weight-checking path exhausts block
/// resources. Skipping the weight check is the standard way to apply a runtime upgrade on a
/// dev chain, and is what the `sudo_unchecked_weight` extrinsic exists for.
fn set_code_call(code: Vec<u8>) -> subxt::transactions::DynamicPayload<Vec<Value>> {
    let set_code = Value::named_variant("set_code", [("code", Value::from_bytes(code))]);
    let runtime_call = Value::unnamed_variant("System", [set_code]);
    let weight =
        Value::named_composite([("ref_time", Value::u128(0)), ("proof_size", Value::u128(0))]);

    dynamic("Sudo", "sudo_unchecked_weight", vec![runtime_call, weight])
}

/// Move some value so a block has a signed extrinsic in it, not just inherents.
async fn signed_traffic(api: &OnlineClient<PolkadotConfig>, amount: u128) -> anyhow::Result<u64> {
    let alice = dev::alice();
    let bob = dev::bob();

    let call = dynamic(
        "Balances",
        "transfer_allow_death",
        vec![
            Value::unnamed_variant("Id", [Value::from_bytes(bob.public_key().0)]),
            Value::u128(amount),
        ],
    );

    let in_block = api
        .at_current_block()
        .await?
        .transactions()
        .sign_and_submit_then_watch_default(&call, &alice)
        .await?
        .wait_for_finalized()
        .await?;
    in_block.wait_for_success().await?;

    Ok(api.at_block(in_block.block_hash()).await?.block_number())
}

async fn current_spec_version(url: &str) -> anyhow::Result<u32> {
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(url).await?;
    Ok(api.at_current_block().await?.spec_version())
}

/// Highest *finalized* block. Distinct from the best block, which on a stalled chain sits
/// ahead of finality and would have the test archive blocks that can still change.
async fn finalized_number(url: &str) -> anyhow::Result<u64> {
    let config = ChainConfig::rpc("upgrade-boundary", url);
    let chain = pif_chain::ChainClient::connect(&config).await?;
    Ok(chain.finalized_number().await?)
}

#[tokio::test]
#[ignore = "requires the upgrade-boundary zombienet network; see module docs"]
async fn a_range_spanning_a_runtime_upgrade_decodes_offline() -> anyhow::Result<()> {
    let url = node_url();
    let wasm = upgrade_wasm()?;

    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(&url).await?;
    let spec_before = api.at_current_block().await?.spec_version();
    println!("spec_version before upgrade: {spec_before}");

    // Signed traffic on the OLD runtime, so the pre-upgrade side of the boundary is not
    // all inherents — the same trap §14 records from the first spike run.
    let pre_block = signed_traffic(&api, 1_111_111_111).await?;
    println!("pre-upgrade signed extrinsic in block {pre_block}");

    // ---- the upgrade ------------------------------------------------------------------
    let alice = dev::alice();
    let in_block = api
        .at_current_block()
        .await?
        .transactions()
        .sign_and_submit_then_watch_default(&set_code_call(wasm), &alice)
        .await?
        .wait_for_finalized()
        .await?;

    // NOT `wait_for_success()`, and that is not a shortcut.
    //
    // `wait_for_success` reads the block's events, and this is the one block whose events
    // subxt cannot read: they were emitted by the OLD runtime, while the block's own
    // reported metadata is already the NEW one. Calling it here fails with "Can't decode
    // event topics", which is the very defect this test exists to characterise. Success is
    // confirmed below by the runtime version actually changing — evidence that does not
    // depend on decoding the upgrade block.
    let upgrade_block = api.at_block(in_block.block_hash()).await?.block_number();
    println!("set_code applied in block {upgrade_block}");

    // The new runtime takes effect for blocks *after* the one carrying `set_code`, so the
    // client has to be rebuilt: its cached metadata is now stale.
    drop(api);

    let mut spec_after = spec_before;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        spec_after = current_spec_version(&url).await?;
        if spec_after != spec_before {
            break;
        }
    }
    assert_ne!(
        spec_after, spec_before,
        "runtime upgrade never took effect — still on spec_version {spec_before}"
    );
    println!("spec_version after upgrade: {spec_after}");

    // Signed traffic on the NEW runtime too — but on a strict timeout, because a
    // single-validator westend-local reliably STALLS a few blocks after a forward upgrade
    // (observed stopping dead at block 20). Waiting for finality on a chain that has stopped
    // authoring never returns, so this is best-effort: the boundary itself is already
    // covered by the pre-upgrade transfer and by the `sudo` extrinsic in the upgrade block.
    let api = OnlineClient::<PolkadotConfig>::from_insecure_url(&url).await?;
    let post_block =
        match tokio::time::timeout(Duration::from_secs(45), signed_traffic(&api, 2_222_222_222))
            .await
        {
            Ok(Ok(number)) => {
                println!("post-upgrade signed extrinsic in block {number}");
                Some(number)
            }
            Ok(Err(error)) => {
                println!("post-upgrade transfer rejected ({error}); continuing without it");
                None
            }
            Err(_) => {
                println!(
                    "post-upgrade transfer timed out — chain stopped authoring after the \
                      upgrade; continuing without it"
                );
                None
            }
        };

    // The *finalized* head, not the best block: everything archived below must be final, and
    // on a stalled chain the best block can sit ahead of finality indefinitely.
    let head = finalized_number(&url).await?;
    println!("finalized head: {head}");
    drop(api);

    // ---- archive a range straddling the boundary --------------------------------------
    let mut numbers: Vec<u64> = Vec::new();
    numbers.extend((upgrade_block.saturating_sub(4))..=head.min(upgrade_block + 8));
    for extra in [Some(pre_block), post_block].into_iter().flatten() {
        if !numbers.contains(&extra) {
            numbers.push(extra);
        }
    }
    numbers.sort_unstable();
    numbers.dedup();
    numbers.retain(|n| *n >= 1 && *n <= head);

    assert!(
        numbers.iter().any(|n| *n > upgrade_block),
        "no finalized block after the upgrade at {upgrade_block} (finalized head {head}) — \
         the chain stalled before finalizing past the boundary, so there is nothing to compare"
    );

    // Connected *after* the upgrade, so the client's metadata cache is built against the
    // post-upgrade chain and both spec versions are resolved on demand.
    let mut fetcher = Fetcher::connect(&url).await?;

    let mut archived: Vec<RawBlock> = Vec::new();
    let mut naive: Vec<Result<Projection, String>> = Vec::new();
    for number in &numbers {
        let captured = fetcher.capture(*number).await?;
        archived.push(captured.raw);
        naive.push(captured.naive_online);
    }

    let mut specs: Vec<u32> = archived.iter().map(|b| b.spec_version).collect();
    specs.sort_unstable();
    specs.dedup();
    println!(
        "archived {} blocks ({}..={}), spec versions present: {specs:?}",
        archived.len(),
        numbers.first().unwrap(),
        numbers.last().unwrap()
    );

    assert!(
        specs.len() >= 2,
        "archived range does not span the upgrade — only spec versions {specs:?} present"
    );

    let mut metadata_blobs: Vec<&Vec<u8>> = archived.iter().map(|b| &b.metadata).collect();
    metadata_blobs.sort_unstable();
    metadata_blobs.dedup();
    assert!(
        metadata_blobs.len() >= 2,
        "the two spec versions reported the same metadata — the boundary is not a real upgrade"
    );

    // ---- decode the whole range through ONE offline client ----------------------------
    //
    // One client holding both metadata versions is the point: it must select per block, the
    // way `pif-store` will. Rebuilding a client per block would hide exactly the bug this
    // test exists to catch.
    let offline = offline_client_for(&archived)?;

    let mut signed_total = 0usize;
    let mut naive_failures = Vec::new();

    for (raw, naive_online) in archived.iter().zip(&naive) {
        // The archive must decode every block, upgrade block included.
        let decoded = decode_stored(&offline, raw)
            .await
            .with_context(|| format!("offline decode of block {} failed", raw.number))?;

        signed_total += decoded.extrinsics.iter().filter(|x| x.is_signed).count();

        match naive_online {
            Ok(online) => {
                // Away from the boundary the naive path is a genuine independent reference.
                assert_eq!(
                    *online, decoded,
                    "block {} (spec {}) decoded differently offline",
                    raw.number, raw.spec_version
                );
                println!(
                    "block {} spec {}: {} events, {} extrinsics ({} signed) — \
                     online and offline identical",
                    raw.number,
                    raw.spec_version,
                    decoded.events.len(),
                    decoded.extrinsics.len(),
                    decoded.extrinsics.iter().filter(|x| x.is_signed).count(),
                );
            }
            Err(error) => {
                naive_failures.push(raw.number);
                println!(
                    "block {} spec {}: naive online decode FAILED ({error}); \
                     archive decoded it anyway — {} events, {} extrinsics",
                    raw.number,
                    raw.spec_version,
                    decoded.events.len(),
                    decoded.extrinsics.len(),
                );
            }
        }
    }

    assert!(
        signed_total > 0,
        "no signed extrinsic in the archived range — the test proves less than it claims"
    );

    // The headline result. If this ever becomes empty, subxt has started resolving the
    // executing runtime itself and the parent-runtime rule can be revisited — but silently
    // dropping the rule while this still fails would reintroduce the bug.
    assert!(
        !naive_failures.is_empty(),
        "expected the naive per-block-metadata path to fail at the upgrade block; it did not. \
         Either the upgrade produced no metadata change, or subxt's resolution changed."
    );
    println!(
        "naive per-block-metadata decode failed at block(s) {naive_failures:?}; \
         archiving the PARENT's runtime decoded them correctly"
    );

    println!(
        "OK: {} blocks across spec versions {specs:?} decoded offline from archived bytes",
        archived.len()
    );

    // ---- the indexer's own decoder, across the same boundary ---------------------------
    //
    // Everything above tests the archive. This tests `decode.rs` itself, which is where the
    // defect lived: before the fix, `decode_at` resolved metadata at the block rather than at
    // its parent and raised `ChainError::Decode` on the upgrade block, stopping the chain.
    let chain = pif_chain::ChainClient::connect(&ChainConfig::rpc("upgrade-boundary", &url))
        .await
        .context("connect for the decode_at check")?;

    let mut decoded_specs = Vec::new();
    for number in &numbers {
        let at = pif_chain::decode::at_block(&chain.client, &chain.info, *number)
            .await
            .with_context(|| format!("at_block({number})"))?;

        let data = pif_chain::decode::decode_at(&chain.client, &chain.rpc, &at, &chain.info)
            .await
            .with_context(|| {
                format!("decode_at({number}) — this is the regression the fix addresses")
            })?;

        decoded_specs.push((*number, data.block.spec_version));
        println!(
            "decode_at block {number}: spec {} recorded, {} events, {} extrinsics",
            data.block.spec_version,
            data.events.len(),
            data.extrinsics.len(),
        );
    }

    // The upgrade block must be recorded under the runtime that EXECUTED it — the old one.
    // Recording the reported version here is the same mistake in the database instead of the
    // decoder, and it would make the block undecodable on any later replay.
    let upgrade_row = decoded_specs
        .iter()
        .find(|(n, _)| *n == upgrade_block)
        .expect("the upgrade block is inside the archived range");
    assert_eq!(
        upgrade_row.1 as u32, spec_before,
        "block {upgrade_block} carries set_code, so it was executed by spec {spec_before} \
         and must be recorded as such — got {}",
        upgrade_row.1
    );

    let after: Vec<_> = decoded_specs
        .iter()
        .filter(|(n, _)| *n > upgrade_block)
        .collect();
    assert!(
        after.iter().all(|(_, s)| *s as u32 == spec_after),
        "blocks after the upgrade must be recorded under the new runtime: {after:?}"
    );

    println!(
        "OK: decode_at handled the boundary — block {upgrade_block} recorded under spec \
         {spec_before}, later blocks under {spec_after}"
    );
    Ok(())
}
