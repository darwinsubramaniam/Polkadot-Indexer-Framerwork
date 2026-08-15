//! Identity and alias indexing for `pallet_identity`, as deployed on the Polkadot People chain.
//!
//! Answers "does this wallet have an alias, and is it verified?" — including from an indexer
//! running against a *different* chain, which is the point: a transfers overlay on AssetHub
//! resolves against People-chain rows in the same database.
//!
//! ## Why this needs storage reads, not just events
//!
//! `pallet_identity`'s events are notifications, not payloads. `IdentitySet { who }` carries
//! no display name; `JudgementGiven { target, registrar_index }` carries no judgement. So the
//! events say *which key changed* and storage says *what it changed to*. That is why this
//! handler needs [`pif_chain::StorageAt`] and why it ships a bootstrap sweep — anything set
//! before the start block is invisible to an event-only reading of the chain.

pub mod bootstrap;
pub mod handler;
pub mod model;
pub mod read;
pub mod resolver;
pub mod store;
pub mod touched;

#[cfg(feature = "api")]
pub mod graphql;

pub use handler::{IdentityHandler, MIGRATOR, NAME};
pub use model::{IdentityRow, Judgement};
pub use resolver::{
    Alias, IdentityResolver, PgIdentityResolver, ResolveError, RpcIdentityResolver,
};

#[cfg(feature = "api")]
pub use graphql::IdentityQuery;
