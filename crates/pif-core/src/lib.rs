//! Shared foundations for the Substrate indexer: configuration, error types, the
//! SCALE-to-JSON codec, and SS58 address encoding.
//!
//! This crate deliberately does not depend on `subxt` or `sqlx` — it is the common base
//! that both the chain and database layers build on.

pub mod codec;
pub mod config;
pub mod error;
pub mod ss58;

pub use codec::{composite_to_json, value_to_json};
pub use config::{ChainConfig, ChainSource, Endpoint, IndexerConfig, OnDigest, PipelineConfig};
pub use error::{ChainInfo, Error, Result};
