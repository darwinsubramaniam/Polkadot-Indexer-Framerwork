//! Shared helpers for the end-to-end suite.
//!
//! Deliberately depends on no zombienet Rust crate — see this crate's `Cargo.toml` for why.
//! Networks are driven through the prebuilt `zombie-cli` binary and its `zombie.json` state
//! file, so the only thing crossing that boundary is JSON.

pub mod state_reader;
pub mod zombienet;

use std::path::{Path, PathBuf};

/// Repository root, derived from this crate's manifest location.
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root should exist")
}

/// Path to the vendored `zombie-cli`, if it has been fetched.
pub fn zombie_cli() -> Option<PathBuf> {
    let path = repo_root().join("bin/zombie-cli");
    path.exists().then_some(path)
}

/// Connection string for the test database.
pub fn database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://indexer:indexer@localhost:5433/substrate_indexer".to_owned()
    })
}

/// WebSocket endpoint of the docker-compose dev node.
pub fn dev_node_url() -> String {
    std::env::var("CHAIN_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:9944".to_owned())
}

/// The handlers these tests index with.
///
/// Mirrors what `pif-cli::build_registry` does, and doubles as a demonstration that a
/// downstream crate can assemble its own registry from outside the framework.
pub fn registry() -> pif_chain::HandlerRegistry {
    #[allow(unused_mut)]
    let mut registry = pif_chain::HandlerRegistry::new();

    #[cfg(feature = "handler-balances")]
    registry.register(Box::new(pif_example_balances::BalancesTransferHandler));

    #[cfg(feature = "handler-identity")]
    registry.register(Box::new(pif_identity::IdentityHandler));

    registry
}
