//! Connecting to a chain and discovering what it is.

use std::sync::Mutex;
use std::time::Instant;

use pif_core::{ChainConfig, ChainInfo, ChainSource, ss58};
use subxt::{
    OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::{LegacyRpcMethods, RpcClient},
};

use crate::error::{ChainError, Result};
use crate::limiter::{Decision, Limiter};

/// `subxt_rpcs` has its own config trait, separate from subxt's `Config`; this is the
/// bridge between the two.
pub type Rpc = LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>;

/// How far behind the head [`ChainClient::is_archive`] probes.
///
/// Just past Substrate's `--state-pruning 256` default, so a node running that default
/// answers "pruned" rather than sitting on the boundary.
const PRUNE_PROBE_DEPTH: u64 = 300;

/// A connected chain: the subxt client, raw RPC access, and the chain's discovered identity.
pub struct ChainClient {
    pub client: OnlineClient<PolkadotConfig>,
    pub rpc: Rpc,
    pub info: ChainInfo,
}

impl ChainClient {
    /// Connect to a chain and discover its identity.
    ///
    /// Nothing chain-specific is compiled in: the name, token, SS58 prefix and genesis hash
    /// all come from the node. That is what lets the same binary index a chain it has never
    /// seen before, purely from config.
    ///
    /// The transport — somebody's RPC node, or an in-process light client — is a config
    /// choice, and everything above this function is written against whichever one the
    /// config named. See [`ChainSource`] for what each can and cannot do.
    pub async fn connect(config: &ChainConfig) -> Result<Self> {
        match &config.source {
            // The first endpoint, which is the only one for every config that names just
            // one. Callers that want all of them use [`EndpointPool::connect_all`].
            ChainSource::Rpc { endpoints } => {
                let endpoint = endpoints
                    .first()
                    .ok_or_else(|| ChainError::AllEndpointsDown {
                        chain: config.id.clone(),
                        attempted: 0,
                    })?;
                Self::connect_to(&config.id, &endpoint.url, None).await
            }
            ChainSource::LightClient { .. } => {
                let transport = light_client_transport(config)?;
                Self::from_transport(&config.id, &config.source.to_string(), transport).await
            }
        }
    }

    /// Connect to one named endpoint.
    pub async fn connect_to(chain_id: &str, url: &str, _archive: Option<bool>) -> Result<Self> {
        let transport = rpc_transport(chain_id, url).await?;
        Self::from_transport(chain_id, url, transport).await
    }

    async fn from_transport(chain_id: &str, url: &str, rpc_client: RpcClient) -> Result<Self> {
        let rpc = Rpc::new(rpc_client.clone());

        // Subxt picks its own backend from what the endpoint advertises: `archive_v1_` if
        // the node has it, otherwise `chainHead_v1_` (which is all a light client speaks),
        // otherwise the legacy methods.
        let client = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client)
            .await
            .map_err(|source| ChainError::Connect {
                chain: chain_id.to_owned(),
                url: url.to_owned(),
                source: Box::new(source),
            })?;

        let info = discover(&rpc, chain_id).await?;
        Ok(Self { client, rpc, info })
    }

    /// Whether this endpoint still serves state for blocks well behind the head.
    ///
    /// There is no RPC that reports a node's pruning mode, so this asks the only question
    /// that matters and reads the answer: resolve a block past the default pruning window
    /// and see whether the node still has its state.
    ///
    /// The answer decides how far the fetch stage may run ahead of the digest. It is
    /// deliberately biased towards `false` — on a chain shorter than the probe depth there
    /// is nothing to distinguish, and the cost of guessing "pruned" is only that the fetcher
    /// is held closer to the digest than it strictly needs to be. Guessing "archive" wrongly
    /// costs the storage read cache the state it was going to be built from.
    pub async fn is_archive(&self) -> Result<bool> {
        let head = self.finalized_number().await?;
        let Some(target) = head.checked_sub(PRUNE_PROBE_DEPTH) else {
            return Ok(false);
        };

        match crate::decode::at_block(&self.client, &self.info, target).await {
            Ok(_) => Ok(true),
            Err(ChainError::PrunedState { .. }) => Ok(false),
            // Anything else is a real failure and not evidence about pruning either way.
            Err(e) => Err(e),
        }
    }

    /// Number of the most recently finalized block.
    pub async fn finalized_number(&self) -> Result<u64> {
        let hash = self.rpc.chain_get_finalized_head().await?;
        let header = self
            .rpc
            .chain_get_header(Some(hash))
            .await?
            .ok_or(ChainError::MissingFinalizedHeader)?;

        Ok(header.number)
    }
}

/// One connected endpoint, and what the pool has learned about it.
pub struct PooledEndpoint {
    pub client: ChainClient,
    pub url: String,
    /// Whether this node still serves historical state. Declared in config or probed.
    pub archive: bool,
    limiter: Mutex<Limiter>,
}

impl PooledEndpoint {
    /// Wait until this endpoint's limiter permits a call.
    ///
    /// `Ok(false)` means the breaker is open and the caller should try a different endpoint
    /// rather than sleep here — blocking on a dead provider is exactly what having several
    /// is meant to avoid.
    pub async fn acquire(&self) -> bool {
        loop {
            let decision = {
                let mut limiter = self.limiter.lock().expect("limiter lock poisoned");
                limiter.poll(Instant::now())
            };

            match decision {
                Decision::Ready => return true,
                Decision::Wait(delay) => tokio::time::sleep(delay).await,
                Decision::Open(_) => return false,
            }
        }
    }

    pub fn succeeded(&self) {
        self.limiter
            .lock()
            .expect("limiter lock poisoned")
            .succeeded();
    }

    pub fn failed(&self) {
        let mut limiter = self.limiter.lock().expect("limiter lock poisoned");
        limiter.failed(Instant::now());
        if limiter.is_open() {
            tracing::warn!(
                endpoint = %self.url,
                "circuit breaker opened; leased work returns to the queue for another endpoint"
            );
        }
    }

    pub fn rate(&self) -> f64 {
        self.limiter.lock().expect("limiter lock poisoned").rate()
    }

    pub fn is_open(&self) -> bool {
        self.limiter
            .lock()
            .expect("limiter lock poisoned")
            .is_open()
    }
}

/// Every endpoint configured for one chain, connected and characterised.
///
/// Endpoints are **not interchangeable**, and the pool is where that stops being a comment
/// and becomes enforced: each is genesis-checked against the others at connect, and each is
/// probed for whether it keeps historical state. A pruned node still serves blocks perfectly
/// well, so it stays in the pool — it is simply not asked for old state.
pub struct EndpointPool {
    endpoints: Vec<PooledEndpoint>,
}

impl EndpointPool {
    /// Connect to every endpoint the config names.
    ///
    /// Endpoints that will not connect are logged and skipped, not fatal: losing one costs
    /// throughput, never correctness. Losing *all* of them is
    /// [`ChainError::AllEndpointsDown`].
    pub async fn connect_all(config: &ChainConfig) -> Result<Self> {
        let configured = config.source.endpoints();
        let mut connected = Vec::new();
        let now = Instant::now();

        for endpoint in configured {
            let client =
                match ChainClient::connect_to(&config.id, &endpoint.url, endpoint.archive).await {
                    Ok(client) => client,
                    Err(e) => {
                        tracing::warn!(
                            chain = %config.id, endpoint = %endpoint.url, error = %e,
                            "endpoint unavailable; continuing without it"
                        );
                        continue;
                    }
                };

            // The genesis hash is the chain's real identity, and two endpoints in one pool
            // must be the same chain. Without this check a mis-pasted URL would interleave
            // two chains' blocks into rows keyed the same way, which looks like corruption
            // rather than a config mistake — and with parallel fetch it would not even be
            // reproducible.
            if let Some(first) = connected.first().map(|e: &PooledEndpoint| &e.client.info)
                && first.genesis_hash != client.info.genesis_hash
            {
                return Err(ChainError::EndpointGenesisMismatch {
                    chain: config.id.clone(),
                    first: connected[0].url.clone(),
                    other: endpoint.url.clone(),
                    first_genesis: hex::encode(&first.genesis_hash),
                    other_genesis: hex::encode(&client.info.genesis_hash),
                });
            }

            let archive = match endpoint.archive {
                Some(declared) => declared,
                None => client.is_archive().await.unwrap_or(false),
            };

            tracing::info!(
                chain = %config.id, endpoint = %endpoint.url, archive,
                max_rps = ?endpoint.max_rps, "endpoint ready"
            );

            connected.push(PooledEndpoint {
                limiter: Mutex::new(Limiter::new(endpoint.max_rps, &endpoint.url, now)),
                url: endpoint.url.clone(),
                archive,
                client,
            });
        }

        if connected.is_empty() {
            return Err(ChainError::AllEndpointsDown {
                chain: config.id.clone(),
                attempted: configured.len(),
            });
        }

        Ok(Self {
            endpoints: connected,
        })
    }

    pub fn endpoints(&self) -> &[PooledEndpoint] {
        &self.endpoints
    }

    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// The chain identity, taken from the first endpoint.
    ///
    /// Safe to take from any of them: `connect_all` refuses a pool whose members disagree
    /// about the genesis hash.
    pub fn info(&self) -> &ChainInfo {
        &self.endpoints[0].client.info
    }

    /// An endpoint to follow the head with.
    ///
    /// One, not all: a finality subscription is per connection and blocks arrive one every
    /// few seconds, so there is no parallelism to be had at the head and several
    /// subscriptions would only mean several copies of the same block.
    pub fn head_follower(&self) -> &PooledEndpoint {
        self.endpoints
            .iter()
            .find(|e| e.archive)
            .unwrap_or(&self.endpoints[0])
    }

    /// Whether every endpoint keeps historical state.
    ///
    /// The fetch stage may only run unboundedly ahead of the digest if *all* of them do: a
    /// chunk can be leased by any endpoint, so one pruned member sets the pace for the pool.
    pub fn all_archive(&self) -> bool {
        self.endpoints.iter().all(|e| e.archive)
    }
}

/// Open a WebSocket connection to a node.
async fn rpc_transport(chain_id: &str, url: &str) -> Result<RpcClient> {
    // `from_url` requires TLS; local dev nodes and zombienet both speak plain ws://.
    let client = if url.starts_with("wss://") {
        RpcClient::from_url(url).await
    } else {
        RpcClient::from_insecure_url(url).await
    };

    client.map_err(|source| ChainError::Connect {
        chain: chain_id.to_owned(),
        url: url.to_owned(),
        source: Box::new(source),
    })
}

/// Start (or join) an in-process light client for this chain.
#[cfg(feature = "light-client")]
fn light_client_transport(config: &ChainConfig) -> Result<RpcClient> {
    crate::light_client::connect(config)
}

#[cfg(not(feature = "light-client"))]
fn light_client_transport(config: &ChainConfig) -> Result<RpcClient> {
    Err(ChainError::LightClientUnavailable {
        chain: config.id.clone(),
    })
}

/// Read a chain's identity from the node itself.
async fn discover(rpc: &Rpc, id: &str) -> Result<ChainInfo> {
    let genesis_hash = rpc.genesis_hash().await?;
    let name = rpc.system_chain().await?;
    let properties = rpc.system_properties().await?;

    // `system_properties` is free-form JSON and chains disagree about shapes: some report a
    // bare value, others an array (one entry per parachain asset). Take the first usable
    // entry and fall back to sane defaults rather than failing to index.
    let token_symbol = properties
        .get("tokenSymbol")
        .and_then(first_string)
        .map(str::to_owned);
    let token_decimals = properties
        .get("tokenDecimals")
        .and_then(first_u64)
        .and_then(|d| u8::try_from(d).ok());
    let ss58_prefix = properties
        .get("ss58Format")
        .or_else(|| properties.get("SS58Prefix"))
        .and_then(first_u64)
        .and_then(|p| u16::try_from(p).ok())
        .unwrap_or(ss58::DEFAULT_PREFIX);

    Ok(ChainInfo {
        id: id.to_owned(),
        genesis_hash: genesis_hash.0.to_vec(),
        name,
        token_symbol,
        token_decimals,
        ss58_prefix,
    })
}

fn first_string(value: &serde_json::Value) -> Option<&str> {
    match value {
        serde_json::Value::String(s) => Some(s),
        serde_json::Value::Array(items) => items.first().and_then(|v| v.as_str()),
        _ => None,
    }
}

fn first_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::Array(items) => items.first().and_then(|v| v.as_u64()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reads_scalar_chain_properties() {
        assert_eq!(first_string(&json!("DOT")), Some("DOT"));
        assert_eq!(first_u64(&json!(10)), Some(10));
    }

    #[test]
    fn reads_array_shaped_chain_properties() {
        // Asset-hub style chains report these as arrays; both shapes must work or the
        // indexer would reject perfectly valid chains.
        assert_eq!(first_string(&json!(["KSM", "OTHER"])), Some("KSM"));
        assert_eq!(first_u64(&json!([12, 10])), Some(12));
    }

    #[test]
    fn ignores_unusable_property_shapes() {
        assert_eq!(first_string(&json!(42)), None);
        assert_eq!(first_u64(&json!("nope")), None);
        assert_eq!(first_string(&json!([])), None);
    }
}
