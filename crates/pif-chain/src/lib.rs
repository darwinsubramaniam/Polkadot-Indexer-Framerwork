//! Chain ingest: connect to a Substrate node, decode blocks dynamically, and persist them.
//!
//! The decoding path carries no compiled-in knowledge of any particular runtime — pallet,
//! call and event names all come from the metadata the node reports for each block. Typed
//! handlers ([`handlers`]) sit on top as an optional, per-chain projection layer.

pub mod client;
pub mod decode;
pub mod error;
pub mod handlers;
pub mod pipeline;
pub mod storage;

pub use client::ChainClient;
pub use error::{ChainError, Result};
pub use handlers::{BlockContext, EventHandler, HandlerRegistry};
pub use pipeline::{IndexOptions, run};
pub use storage::{StorageAt, SubxtStorage};
