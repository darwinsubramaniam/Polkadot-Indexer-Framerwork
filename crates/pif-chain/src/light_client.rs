//! In-process light-client (smoldot) transport.
//!
//! A light client verifies every header against the chain's own finality proofs, so the
//! indexer trusts no node operator. The cost is that it can only follow the chain forward:
//! smoldot answers `chain_getBlockHash(n)` with `null` for any `n` other than genesis or
//! the current best block, because there is no way to *verify* a full node's claim that
//! some hash is block `n`. Everything the pipeline does under a light client therefore has
//! to start from a block the subscription handed it.
//!
//! Compiled only with the `light-client` feature: smoldot is a large dependency and most
//! deployments point at their own node.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

use pif_core::ChainConfig;
use subxt::lightclient::{ChainConfig as SpecConfig, LightClient, LightClientRpc};
use subxt::rpcs::RpcClient;

use crate::error::{ChainError, Result};

/// Relay chains already syncing in this process, keyed by chain-spec path.
///
/// A parachain light client is only as good as the relay chain behind it, and syncing a
/// relay chain is by far the expensive part. Indexing five parachains of the same relay in
/// one process should cost one relay sync, not five — hence the shared map.
type Relays = Mutex<HashMap<String, (LightClient, LightClientRpc)>>;

fn relays() -> &'static Relays {
    static RELAYS: OnceLock<Relays> = OnceLock::new();
    RELAYS.get_or_init(Relays::default)
}

/// Build an RPC client backed by a light client for this chain.
///
/// Must be called from within a tokio runtime: smoldot spawns its own background tasks.
pub fn rpc_client(
    chain_id: &str,
    chain_spec: &Path,
    relay_chain_spec: Option<&Path>,
    bootnodes: &[String],
) -> Result<RpcClient> {
    let rpc = match relay_chain_spec {
        // A relay chain, or any chain that stands alone.
        None => relay(chain_id, chain_spec, bootnodes)?.1,

        // A parachain: attach it to its relay chain's light client, starting that relay
        // sync now if no other chain in this process needed it yet.
        Some(relay_spec) => {
            let (relay_client, _) = relay(chain_id, relay_spec, &[])?;
            let spec = read_spec(chain_id, chain_spec)?;

            relay_client
                .parachain(with_bootnodes(chain_id, spec, bootnodes)?)
                .map_err(|source| ChainError::LightClient {
                    chain: chain_id.to_owned(),
                    source: Box::new(source),
                })?
        }
    };

    Ok(RpcClient::new(rpc))
}

/// Get the light client for a relay chain spec, starting it on first use.
fn relay(
    chain_id: &str,
    spec_path: &Path,
    bootnodes: &[String],
) -> Result<(LightClient, LightClientRpc)> {
    let key = spec_path.display().to_string();

    // Held across the whole block, so two chains starting at once cannot both decide they
    // are the one to sync the relay. Nothing here awaits, so the lock is never held over a
    // yield point.
    let mut relays = relays().lock().expect("light client registry poisoned");

    if let Some(existing) = relays.get(&key) {
        tracing::debug!(chain = %chain_id, spec = %key, "reusing relay chain light client");
        return Ok(existing.clone());
    }

    let spec = read_spec(chain_id, spec_path)?;
    let started =
        LightClient::relay_chain(with_bootnodes(chain_id, spec, bootnodes)?).map_err(|source| {
            ChainError::LightClient {
                chain: chain_id.to_owned(),
                source: Box::new(source),
            }
        })?;

    tracing::info!(chain = %chain_id, spec = %key, "started relay chain light client");
    relays.insert(key, started.clone());
    Ok(started)
}

fn read_spec(chain_id: &str, path: &Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|source| ChainError::ChainSpecRead {
        chain: chain_id.to_owned(),
        path: path.display().to_string(),
        source,
    })
}

/// Override the spec's bootnodes, if the config asked for it.
///
/// Local and zombienet networks ship specs whose bootnode addresses are placeholders, so
/// without this a light client would sit there with nobody to talk to.
fn with_bootnodes<'a>(
    chain_id: &str,
    spec: String,
    bootnodes: &[String],
) -> Result<SpecConfig<'a>> {
    let config = SpecConfig::chain_spec(spec);
    if bootnodes.is_empty() {
        return Ok(config);
    }

    config
        .set_bootnodes(bootnodes)
        .map_err(|source| ChainError::LightClient {
            chain: chain_id.to_owned(),
            source: Box::new(source),
        })
}

/// Connect this chain's configured light-client source.
pub fn connect(config: &ChainConfig) -> Result<RpcClient> {
    let pif_core::ChainSource::LightClient {
        chain_spec,
        relay_chain_spec,
        bootnodes,
    } = &config.source
    else {
        unreachable!("connect called for a non light-client source");
    };

    rpc_client(
        &config.id,
        chain_spec,
        relay_chain_spec.as_deref(),
        bootnodes,
    )
}
