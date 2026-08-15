//! Error types shared across the workspace.

use std::fmt;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to read config file {path}")]
    ConfigRead {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse config file {path}")]
    ConfigParse {
        path: String,
        #[source]
        source: Box<toml::de::Error>,
    },

    #[error("invalid config: {0}")]
    ConfigInvalid(String),
}

/// Identity of a chain as discovered from a running node.
///
/// `id` comes from config, but everything else is read from the node. Handlers use this to
/// decide whether they are safe to run — see `EventHandler::supports`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainInfo {
    /// Config key, and the primary key used throughout the database.
    pub id: String,
    /// Genesis hash — the actual cryptographic identity of the chain.
    pub genesis_hash: Vec<u8>,
    /// Human-readable chain name, e.g. "Development".
    pub name: String,
    pub token_symbol: Option<String>,
    pub token_decimals: Option<u8>,
    /// SS58 network prefix used when rendering account addresses.
    pub ss58_prefix: u16,
}

impl fmt::Display for ChainInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}, genesis 0x{})",
            self.id,
            self.name,
            hex::encode(&self.genesis_hash[..self.genesis_hash.len().min(8)])
        )
    }
}
