//! Indexer configuration.
//!
//! The config file is what makes this indexer chain-agnostic: adding a chain is a config
//! edit, not a code change. Only the things that cannot be discovered from a node live
//! here. Everything else (chain name, SS58 prefix, token symbol/decimals, genesis hash)
//! is read from the node on first connect.

use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Top-level config, deserialised from `config/chains.toml`.
///
/// No `deny_unknown_fields` here, deliberately: it is what lets a new top-level table —
/// `[pipeline]`, say — be added without every existing config file becoming a parse error.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndexerConfig {
    #[serde(default)]
    pub chains: Vec<ChainConfig>,

    /// Defaults for the block archive, applied to every chain that does not override them
    /// in its own `[chains.pipeline]` table.
    #[serde(default)]
    pub pipeline: PipelineConfig,
}

/// Where a chain's block archive lives, and how it is laid out.
///
/// The archive is what makes a re-index cost a re-*digest* rather than a re-download: the
/// fetch stage writes raw blocks here, and the digest stage reads them back instead of
/// asking the network again.
///
/// Only the settings the current pipeline honours are here. The rest of IPD-002's table —
/// `cold_path`, `retention`, `on_digest` — arrives with the phases that act on them, rather
/// than sitting in the config doing nothing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Root directory for the archive. Relative paths resolve against the config file, and
    /// the directory is created on first run rather than being required up front.
    #[serde(default = "default_hot_path")]
    pub hot_path: PathBuf,

    /// Blocks per segment file.
    ///
    /// Fixed-size and aligned, so a block's file is *computed* from its number rather than
    /// looked up. Changing this on an existing store makes previously written segments
    /// unfindable, so it is chosen once per store, not tuned.
    #[serde(default = "default_segment_size")]
    pub segment_size: u64,

    /// How far the fetch stage may run ahead of the digest, in blocks.
    ///
    /// The brake exists because the storage read cache is filled on the *first* digest of a
    /// block, from a node. A fetcher 100k blocks ahead means every one of those reads asks
    /// for state the node discarded long ago — Substrate defaults to `--state-pruning 256`
    /// — so the digest would fail on state it could have had if it had simply kept up.
    ///
    /// Omit to decide from the endpoint: an archive node gets no brake, anything else gets
    /// 256. Set it to bound how far the archive can grow ahead of Postgres regardless.
    #[serde(default)]
    pub max_digest_lag: Option<u64>,
}

fn default_hot_path() -> PathBuf {
    PathBuf::from(".pif-store")
}

fn default_segment_size() -> u64 {
    1000
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            hot_path: default_hot_path(),
            segment_size: default_segment_size(),
            max_digest_lag: None,
        }
    }
}

/// Where a chain's data comes from.
///
/// Two transports, with genuinely different capabilities rather than two spellings of the
/// same thing:
///
/// * [`ChainSource::Rpc`] talks to somebody's node. It can resolve any block by number, so
///   it can backfill history (given an archive node) and is the only option for indexing
///   the past.
/// * [`ChainSource::LightClient`] runs smoldot in-process and verifies headers against the
///   chain's own finality proofs, so it trusts nobody. The trade-off is that it cannot map
///   an arbitrary block *number* to a hash — a light client has no way to verify such an
///   answer — so it can only follow the finalized head forward from now.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum ChainSource {
    /// A WebSocket JSON-RPC endpoint.
    Rpc {
        /// `ws://` or `wss://` endpoint of a node for this chain.
        url: String,
    },

    /// An in-process smoldot light client.
    LightClient {
        /// Path to this chain's chain-spec JSON, relative to the config file.
        chain_spec: PathBuf,

        /// For a parachain, the chain spec of the relay chain it is secured by.
        ///
        /// A parachain's finality is the relay chain's finality, so a light client must
        /// sync the relay chain to verify anything. Omit for a relay chain. Chains that
        /// name the same relay spec share one relay-chain sync.
        #[serde(default)]
        relay_chain_spec: Option<PathBuf>,

        /// Bootnodes to use instead of the ones in the chain spec.
        ///
        /// Useful for local networks, where the spec's bootnodes are wrong or absent.
        #[serde(default)]
        bootnodes: Vec<String>,
    },
}

impl ChainSource {
    /// A light client can only ever index forward from the current finalized head.
    pub fn can_backfill(&self) -> bool {
        matches!(self, ChainSource::Rpc { .. })
    }
}

impl fmt::Display for ChainSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChainSource::Rpc { url } => write!(f, "{url}"),
            ChainSource::LightClient { chain_spec, .. } => {
                write!(f, "light-client:{}", chain_spec.display())
            }
        }
    }
}

/// A single chain to index.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "RawChainConfig")]
pub struct ChainConfig {
    /// Stable identifier, used as the primary key in every table.
    pub id: String,

    /// How to reach this chain.
    pub source: ChainSource,

    /// Block to start from when no cursor is stored yet.
    ///
    /// Ignored — and rejected at validation — for a light client, which always starts at
    /// the finalized head.
    #[serde(default)]
    pub start_block: u64,

    /// Names of typed-overlay handlers to run for this chain.
    ///
    /// Empty means dynamic decoding only, which is what lets the indexer run against a
    /// chain it was never compiled for.
    #[serde(default)]
    pub handlers: Vec<String>,

    /// This chain's block archive.
    ///
    /// `None` until [`IndexerConfig::from_path`] fills it in from the top-level `[pipeline]`
    /// defaults, so a chain built by hand — `ChainConfig::rpc(…)` in a test — still has a
    /// working archive via [`ChainConfig::pipeline`].
    #[serde(default)]
    pub pipeline: Option<PipelineConfig>,
}

impl ChainConfig {
    /// A chain reached over JSON-RPC.
    pub fn rpc(id: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            source: ChainSource::Rpc { url: url.into() },
            start_block: 0,
            handlers: Vec::new(),
            pipeline: None,
        }
    }

    /// A chain followed with an in-process light client.
    pub fn light_client(id: impl Into<String>, chain_spec: impl Into<PathBuf>) -> Self {
        Self {
            id: id.into(),
            source: ChainSource::LightClient {
                chain_spec: chain_spec.into(),
                relay_chain_spec: None,
                bootnodes: Vec::new(),
            },
            start_block: 0,
            handlers: Vec::new(),
            pipeline: None,
        }
    }

    /// Run these handlers for this chain.
    pub fn with_handlers<S: Into<String>>(mut self, handlers: impl IntoIterator<Item = S>) -> Self {
        self.handlers = handlers.into_iter().map(Into::into).collect();
        self
    }

    /// Start here when no cursor is stored.
    pub fn from_block(mut self, start_block: u64) -> Self {
        self.start_block = start_block;
        self
    }

    /// Archive this chain's blocks under `hot_path`.
    pub fn with_pipeline(mut self, pipeline: PipelineConfig) -> Self {
        self.pipeline = Some(pipeline);
        self
    }

    /// Whether this chain's transport can fetch a block by number.
    pub fn can_backfill(&self) -> bool {
        self.source.can_backfill()
    }

    /// This chain's archive settings, falling back to the defaults.
    ///
    /// Returned by value rather than by reference so that a chain built in code — with no
    /// config file and therefore no `[pipeline]` table to inherit from — still archives
    /// somewhere sensible instead of needing every caller to handle `None`.
    pub fn pipeline(&self) -> PipelineConfig {
        self.pipeline.clone().unwrap_or_default()
    }
}

/// The on-disk shape, before normalisation.
///
/// Kept separate from [`ChainConfig`] so that the struct the rest of the codebase sees
/// always has exactly one resolved [`ChainSource`], while the file may spell it either as
/// the modern `[chains.source]` table or the original flat `ws_url`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawChainConfig {
    id: String,
    #[serde(default)]
    source: Option<ChainSource>,
    /// Pre-`source` shorthand for `source = { type = "rpc", url = … }`. Still accepted:
    /// every existing config in the wild uses it, and it is the common case.
    #[serde(default)]
    ws_url: Option<String>,
    #[serde(default)]
    start_block: u64,
    #[serde(default)]
    handlers: Vec<String>,
    /// Per-chain override of the top-level `[pipeline]` defaults.
    ///
    /// Declared *here* and not only on [`ChainConfig`]: every TOML key travels through this
    /// struct, which denies unknown fields, so a `[chains.pipeline]` table missing from it
    /// is a hard parse error rather than a silently ignored one.
    #[serde(default)]
    pipeline: Option<PipelineConfig>,
}

impl TryFrom<RawChainConfig> for ChainConfig {
    type Error = String;

    fn try_from(raw: RawChainConfig) -> std::result::Result<Self, Self::Error> {
        let source = match (raw.source, raw.ws_url) {
            (Some(source), None) => source,
            (None, Some(url)) => ChainSource::Rpc { url },
            (Some(_), Some(_)) => {
                return Err(format!(
                    "chain {:?}: set either `ws_url` or a `[chains.source]` table, not both",
                    raw.id
                ));
            }
            (None, None) => {
                return Err(format!(
                    "chain {:?}: no source configured; add `ws_url = \"ws://…\"` or a \
                     `[chains.source]` table",
                    raw.id
                ));
            }
        };

        Ok(ChainConfig {
            id: raw.id,
            source,
            start_block: raw.start_block,
            handlers: raw.handlers,
            pipeline: raw.pipeline,
        })
    }
}

impl IndexerConfig {
    /// Load and validate config from a TOML file.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|source| Error::ConfigRead {
            path: path.display().to_string(),
            source,
        })?;

        let mut config: IndexerConfig =
            toml::from_str(&raw).map_err(|source| Error::ConfigParse {
                path: path.display().to_string(),
                source: Box::new(source),
            })?;

        // Chain-spec paths are written relative to the config file, not to whatever
        // directory the binary happens to be run from.
        let base = path.parent().unwrap_or_else(|| Path::new("."));
        config.resolve_paths(base)?;

        config.validate()?;
        Ok(config)
    }

    /// Make chain-spec and archive paths absolute against `base`, and check the ones that
    /// must already exist.
    ///
    /// Done here rather than in [`IndexerConfig::validate`] so that validation stays a pure
    /// function of the parsed config, with no filesystem in it.
    fn resolve_paths(&mut self, base: &Path) -> Result<()> {
        // The archive's location is inherited before it is resolved, so a per-chain override
        // and the global default are made absolute the same way.
        for chain in &mut self.chains {
            let mut pipeline = chain
                .pipeline
                .take()
                .unwrap_or_else(|| self.pipeline.clone());
            if pipeline.hot_path.is_relative() {
                pipeline.hot_path = base.join(&pipeline.hot_path);
            }
            // Deliberately *not* checked for existence: the store is created on first run.
            // Requiring it up front would make a fresh checkout fail to start for a reason
            // the operator cannot act on.
            chain.pipeline = Some(pipeline);
        }

        for chain in &mut self.chains {
            let ChainSource::LightClient {
                chain_spec,
                relay_chain_spec,
                ..
            } = &mut chain.source
            else {
                continue;
            };

            for spec in [Some(chain_spec), relay_chain_spec.as_mut()]
                .into_iter()
                .flatten()
            {
                if spec.is_relative() {
                    *spec = base.join(&*spec);
                }
                if !spec.is_file() {
                    return Err(Error::ConfigInvalid(format!(
                        "chain {:?}: chain spec {} does not exist",
                        chain.id,
                        spec.display()
                    )));
                }
            }
        }

        Ok(())
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

            match &chain.source {
                ChainSource::Rpc { url } => {
                    if !(url.starts_with("ws://") || url.starts_with("wss://")) {
                        return Err(Error::ConfigInvalid(format!(
                            "chain {:?}: rpc url must start with ws:// or wss://, got {:?}",
                            chain.id, url
                        )));
                    }
                }

                // Rejected rather than silently ignored: a light client physically cannot
                // fetch block N by number, so a config asking it to start at one would
                // quietly index something else entirely.
                ChainSource::LightClient { .. } if chain.start_block != 0 => {
                    return Err(Error::ConfigInvalid(format!(
                        "chain {:?}: start_block = {} is not possible with a light client, \
                         which can only follow the chain forward from the current finalized \
                         head. Use an rpc source to index history.",
                        chain.id, chain.start_block
                    )));
                }

                ChainSource::LightClient { .. } => {}
            }

            // A zero segment size divides by zero when resolving a block to its file, and
            // an empty root would scatter segments across the working directory.
            if let Some(pipeline) = &chain.pipeline {
                if pipeline.segment_size == 0 {
                    return Err(Error::ConfigInvalid(format!(
                        "chain {:?}: pipeline.segment_size must be greater than zero",
                        chain.id
                    )));
                }
                if pipeline.hot_path.as_os_str().is_empty() {
                    return Err(Error::ConfigInvalid(format!(
                        "chain {:?}: pipeline.hot_path must not be empty",
                        chain.id
                    )));
                }
                // Zero would stall the fetcher against a digest that has not started, which
                // looks exactly like a hang. Omitting the key is how you ask for the
                // default; there is no sensible reading of "run zero blocks ahead".
                if pipeline.max_digest_lag == Some(0) {
                    return Err(Error::ConfigInvalid(format!(
                        "chain {:?}: pipeline.max_digest_lag must be at least 1; omit it to \
                         decide from the endpoint",
                        chain.id
                    )));
                }
            }
        }

        if self.pipeline.segment_size == 0 {
            return Err(Error::ConfigInvalid(
                "pipeline.segment_size must be greater than zero".into(),
            ));
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
        let config: IndexerConfig =
            toml::from_str(toml_str).map_err(|source| Error::ConfigParse {
                path: "<test>".into(),
                source: Box::new(source),
            })?;
        config.validate()?;
        Ok(config)
    }

    /// The whole error chain as one string: a parse rejection puts its reason in the
    /// `toml::de::Error` source, not in the top-level message.
    fn full(error: &Error) -> String {
        let mut text = error.to_string();
        let mut source = std::error::Error::source(error);
        while let Some(e) = source {
            text.push_str(&format!(": {e}"));
            source = e.source();
        }
        text
    }

    fn rpc_url(chain: &ChainConfig) -> &str {
        match &chain.source {
            ChainSource::Rpc { url } => url,
            other => panic!("expected an rpc source, got {other:?}"),
        }
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
        assert_eq!(rpc_url(chain), "ws://127.0.0.1:9944");
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
    fn parses_an_explicit_rpc_source() {
        let config = parse(
            r#"
            [[chains]]
            id = "dev-local"
            start_block = 10

            [chains.source]
            type = "rpc"
            url = "wss://rpc.example.io"
            "#,
        )
        .unwrap();

        let chain = &config.chains[0];
        assert_eq!(rpc_url(chain), "wss://rpc.example.io");
        assert_eq!(chain.start_block, 10);
        assert!(chain.can_backfill());
    }

    #[test]
    fn parses_a_light_client_source() {
        let config = parse(
            r#"
            [[chains]]
            id = "polkadot"

            [chains.source]
            type = "light-client"
            chain_spec = "specs/polkadot.json"
            "#,
        )
        .unwrap();

        let chain = &config.chains[0];
        assert_eq!(
            chain.source,
            ChainSource::LightClient {
                chain_spec: PathBuf::from("specs/polkadot.json"),
                relay_chain_spec: None,
                bootnodes: Vec::new(),
            }
        );
        assert!(!chain.can_backfill());
    }

    #[test]
    fn parses_a_light_client_parachain_with_bootnodes() {
        let config = parse(
            r#"
            [[chains]]
            id = "people"

            [chains.source]
            type = "light-client"
            chain_spec = "specs/people.json"
            relay_chain_spec = "specs/polkadot.json"
            bootnodes = ["/ip4/127.0.0.1/tcp/30333/p2p/12D3Koo"]
            "#,
        )
        .unwrap();

        let ChainSource::LightClient {
            relay_chain_spec,
            bootnodes,
            ..
        } = &config.chains[0].source
        else {
            panic!("expected a light-client source");
        };

        assert_eq!(
            relay_chain_spec.as_deref(),
            Some(Path::new("specs/polkadot.json"))
        );
        assert_eq!(bootnodes.len(), 1);
    }

    #[test]
    fn rejects_a_start_block_on_a_light_client() {
        // The alternative — silently starting at the head — would look like an indexer
        // that skipped a hundred blocks for no reason.
        let err = parse(
            r#"
            [[chains]]
            id = "polkadot"
            start_block = 100

            [chains.source]
            type = "light-client"
            chain_spec = "specs/polkadot.json"
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("light client"), "got: {err}");
    }

    #[test]
    fn rejects_a_chain_with_no_source() {
        let err = parse(
            r#"
            [[chains]]
            id = "nowhere"
            "#,
        )
        .unwrap_err();

        assert!(full(&err).contains("no source configured"), "got: {err}");
    }

    #[test]
    fn rejects_both_spellings_of_the_same_source() {
        let err = parse(
            r#"
            [[chains]]
            id = "both"
            ws_url = "ws://127.0.0.1:9944"

            [chains.source]
            type = "rpc"
            url = "ws://127.0.0.1:9988"
            "#,
        )
        .unwrap_err();

        assert!(full(&err).contains("not both"), "got: {err}");
    }

    #[test]
    fn resolves_chain_spec_paths_against_the_config_file() {
        let dir = std::env::temp_dir().join("pif-config-spec-test");
        std::fs::create_dir_all(dir.join("specs")).unwrap();
        std::fs::write(dir.join("specs/local.json"), "{}").unwrap();
        std::fs::write(
            dir.join("chains.toml"),
            r#"
            [[chains]]
            id = "local"

            [chains.source]
            type = "light-client"
            chain_spec = "specs/local.json"
            "#,
        )
        .unwrap();

        let config = IndexerConfig::from_path(dir.join("chains.toml")).unwrap();
        let ChainSource::LightClient { chain_spec, .. } = &config.chains[0].source else {
            panic!("expected a light-client source");
        };

        assert_eq!(chain_spec, &dir.join("specs/local.json"));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn reports_a_missing_chain_spec_by_path() {
        let dir = std::env::temp_dir().join("pif-config-missing-spec-test");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("chains.toml"),
            r#"
            [[chains]]
            id = "local"

            [chains.source]
            type = "light-client"
            chain_spec = "specs/absent.json"
            "#,
        )
        .unwrap();

        let err = IndexerConfig::from_path(dir.join("chains.toml")).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {err}");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_config_with_no_pipeline_table_still_has_an_archive() {
        // Every deployment predating the archive has exactly this config. It must keep
        // working, and it must still get somewhere to put blocks.
        let config = parse(
            r#"
            [[chains]]
            id = "dev-local"
            ws_url = "ws://127.0.0.1:9944"
            "#,
        )
        .unwrap();

        let pipeline = config.chains[0].pipeline();
        assert_eq!(pipeline.hot_path, PathBuf::from(".pif-store"));
        assert_eq!(pipeline.segment_size, 1000);
    }

    #[test]
    fn a_chain_overrides_the_global_pipeline_defaults() {
        let config = parse(
            r#"
            [pipeline]
            hot_path = "/mnt/ssd/pif"
            segment_size = 4096

            [[chains]]
            id = "relay"
            ws_url = "ws://127.0.0.1:9944"

            [[chains]]
            id = "para"
            ws_url = "ws://127.0.0.1:9988"

            [chains.pipeline]
            hot_path = "/mnt/other/pif"
            "#,
        )
        .unwrap();

        // Inheritance happens in `resolve_paths`, which `parse` does not run, so the global
        // table is read straight off the top level here.
        assert_eq!(config.pipeline.hot_path, PathBuf::from("/mnt/ssd/pif"));
        assert_eq!(config.pipeline.segment_size, 4096);
        assert!(config.chain("relay").unwrap().pipeline.is_none());
        assert_eq!(
            config.chain("para").unwrap().pipeline().hot_path,
            PathBuf::from("/mnt/other/pif")
        );
        // An override that names only one field keeps the type's defaults for the rest,
        // rather than the global table's — worth pinning so the behaviour is a decision.
        assert_eq!(config.chain("para").unwrap().pipeline().segment_size, 1000);
    }

    #[test]
    fn a_misspelled_pipeline_key_is_rejected_rather_than_ignored() {
        // `deny_unknown_fields` on the raw shape is what makes this loud; without it a typo
        // silently archives to the default path and nobody finds out until a replay fails.
        let err = parse(
            r#"
            [[chains]]
            id = "dev"
            ws_url = "ws://127.0.0.1:9944"

            [chains.pipeline]
            hot_paths = "/mnt/ssd/pif"
            "#,
        )
        .unwrap_err();

        assert!(full(&err).contains("hot_paths"), "got: {}", full(&err));
    }

    #[test]
    fn rejects_a_segment_size_of_zero() {
        // It divides by zero on the very first block, several layers away from here.
        let err = parse(
            r#"
            [[chains]]
            id = "dev"
            ws_url = "ws://127.0.0.1:9944"

            [chains.pipeline]
            segment_size = 0
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("segment_size"), "got: {err}");
    }

    #[test]
    fn resolves_the_archive_path_against_the_config_file() {
        let dir = std::env::temp_dir().join("pif-config-pipeline-test");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("chains.toml"),
            r#"
            [pipeline]
            hot_path = "archive"

            [[chains]]
            id = "local"
            ws_url = "ws://127.0.0.1:9944"
            "#,
        )
        .unwrap();

        let config = IndexerConfig::from_path(dir.join("chains.toml")).unwrap();

        // Relative to the config file, not to whatever directory `pif` was run from — and
        // resolved even though the directory does not exist yet.
        assert_eq!(config.chains[0].pipeline().hot_path, dir.join("archive"));
        assert!(!dir.join("archive").exists());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn shipped_config_file_is_valid() {
        // Guards against the committed example drifting out of sync with the parser.
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/chains.toml");
        let config = IndexerConfig::from_path(path).expect("shipped config/chains.toml must parse");
        assert!(!config.chains.is_empty());
    }
}
