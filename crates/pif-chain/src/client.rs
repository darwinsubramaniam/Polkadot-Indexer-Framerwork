//! Connecting to a chain and discovering what it is.

use pif_core::{ChainConfig, ChainInfo, ss58};
use subxt::{
    OnlineClient, PolkadotConfig,
    config::RpcConfigFor,
    rpcs::{LegacyRpcMethods, RpcClient},
};

use crate::error::{ChainError, Result};

/// `subxt_rpcs` has its own config trait, separate from subxt's `Config`; this is the
/// bridge between the two.
pub type Rpc = LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>;

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
    pub async fn connect(config: &ChainConfig) -> Result<Self> {
        // `from_url` requires TLS; local dev nodes and zombienet both speak plain ws://.
        let rpc_client = if config.ws_url.starts_with("wss://") {
            RpcClient::from_url(&config.ws_url).await
        } else {
            RpcClient::from_insecure_url(&config.ws_url).await
        }
        .map_err(|source| ChainError::Connect {
            chain: config.id.clone(),
            url: config.ws_url.clone(),
            source: Box::new(source),
        })?;

        let rpc = Rpc::new(rpc_client.clone());
        let client = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc_client)
            .await
            .map_err(|source| ChainError::Connect {
                chain: config.id.clone(),
                url: config.ws_url.clone(),
                source: Box::new(source),
            })?;

        let info = discover(&rpc, &config.id).await?;
        Ok(Self { client, rpc, info })
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
