//! Indexer configuration.
//!
//! The config file is what makes this indexer chain-agnostic: adding a chain is a config
//! edit, not a code change. Only the things that cannot be discovered from a node live
//! here. Everything else (chain name, SS58 prefix, token symbol/decimals, genesis hash)
//! is read from the node on first connect.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Top-level config, deserialised from `config/chains.toml`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexerConfig {
    #[serde(default)]
    pub chains: Vec<ChainConfig>,
}

/// A single chain to index.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ChainConfig {
    /// Stable identifier, used as the primary key in every table.
    pub id: String,

    /// WebSocket RPC endpoint of a node for this chain.
    pub ws_url: String,

    /// Block to start from when no cursor is stored yet.
    #[serde(default)]
    pub start_block: u64,

    /// Names of typed-overlay handlers to run for this chain.
    ///
    /// Empty means dynamic decoding only, which is what lets the indexer run against a
    /// chain it was never compiled for.
    #[serde(default)]
    pub handlers: Vec<String>,
}

impl IndexerConfig {
    /// Load and validate config from a TOML file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|source| Error::ConfigRead {
            path: path.display().to_string(),
            source,
        })?;

        let config: IndexerConfig = toml::from_str(&raw).map_err(|source| Error::ConfigParse {
            path: path.display().to_string(),
            source: Box::new(source),
        })?;

        config.validate()?;
        Ok(config)
    }

    /// Reject configs that would fail confusingly much later.
    ///
    /// A duplicate `id` is the dangerous one: two chains would share a cursor and
    /// interleave their blocks into the same rows, which looks like data corruption rather
    /// than a config mistake.
    pub fn validate(&self) -> Result<()> {
        if self.chains.is_empty() {
            return Err(Error::ConfigInvalid(
                "no chains configured; add at least one [[chains]] entry".into(),
            ));
        }

        let mut seen = std::collections::HashSet::new();
        for chain in &self.chains {
            if chain.id.trim().is_empty() {
                return Err(Error::ConfigInvalid("chain id must not be empty".into()));
            }
            if !seen.insert(&chain.id) {
                return Err(Error::ConfigInvalid(format!(
                    "duplicate chain id {:?}: ids are primary keys and must be unique",
                    chain.id
                )));
            }
            if !(chain.ws_url.starts_with("ws://") || chain.ws_url.starts_with("wss://")) {
                return Err(Error::ConfigInvalid(format!(
                    "chain {:?}: ws_url must start with ws:// or wss://, got {:?}",
                    chain.id, chain.ws_url
                )));
            }
        }

        Ok(())
    }

    /// Look up a single chain by id.
    pub fn chain(&self, id: &str) -> Option<&ChainConfig> {
        self.chains.iter().find(|c| c.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml_str: &str) -> Result<IndexerConfig> {
        let config: IndexerConfig = toml::from_str(toml_str).unwrap();
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn parses_a_minimal_chain_entry() {
        let config = parse(
            r#"
            [[chains]]
            id = "dev-local"
            ws_url = "ws://127.0.0.1:9944"
            "#,
        )
        .unwrap();

        let chain = &config.chains[0];
        assert_eq!(chain.id, "dev-local");
        assert_eq!(chain.start_block, 0);
        assert!(chain.handlers.is_empty());
    }

    #[test]
    fn supports_multiple_chains_in_one_process() {
        let config = parse(
            r#"
            [[chains]]
            id = "relay"
            ws_url = "ws://127.0.0.1:9944"

            [[chains]]
            id = "para-1000"
            ws_url = "ws://127.0.0.1:9988"
            start_block = 100
            handlers = ["balances-transfer"]
            "#,
        )
        .unwrap();

        assert_eq!(config.chains.len(), 2);
        assert_eq!(config.chain("para-1000").unwrap().start_block, 100);
        assert_eq!(
            config.chain("para-1000").unwrap().handlers,
            ["balances-transfer"]
        );
        assert!(config.chain("nope").is_none());
    }

    #[test]
    fn rejects_duplicate_chain_ids() {
        let err = parse(
            r#"
            [[chains]]
            id = "same"
            ws_url = "ws://127.0.0.1:9944"

            [[chains]]
            id = "same"
            ws_url = "ws://127.0.0.1:9988"
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("duplicate chain id"), "got: {err}");
    }

    #[test]
    fn rejects_non_websocket_urls() {
        let err = parse(
            r#"
            [[chains]]
            id = "dev"
            ws_url = "http://127.0.0.1:9944"
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("ws://"), "got: {err}");
    }

    #[test]
    fn rejects_empty_config() {
        let err = parse("").unwrap_err();
        assert!(
            err.to_string().contains("no chains configured"),
            "got: {err}"
        );
    }

    #[test]
    fn shipped_config_file_is_valid() {
        // Guards against the committed example drifting out of sync with the parser.
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/chains.toml");
        let config = IndexerConfig::from_path(path).expect("shipped config/chains.toml must parse");
        assert!(!config.chains.is_empty());
    }
}
