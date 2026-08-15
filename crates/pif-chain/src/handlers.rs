//! Typed-overlay event handlers — the framework's extension point.
//!
//! The dynamic core already stores every event as JSONB, which is what makes the indexer
//! chain-agnostic. Handlers are the opposite trade: written against one chain's pallets,
//! they derive denormalised, strongly-typed tables for the queries that matter most.
//!
//! Nothing here knows what any particular handler *does*. A handler owns its own tables, its
//! own migrations and its own SQL; the framework only decides when to call it and guarantees
//! the call happens inside the block's transaction. That is what lets a downstream crate —
//! `hydration-indexer`, say — add pallet-specific indexing without forking this one.
//!
//! ```ignore
//! let mut registry = HandlerRegistry::new();
//! registry.register(Box::new(OmnipoolSwapHandler));
//! let enabled = registry.select(&chain_config.handlers, &chain_info)?;
//! pipeline::run(&pool, &chain_config, &enabled, options).await?;
//! ```

use async_trait::async_trait;
use pif_core::ChainInfo;
use pif_db::BlockData;
use sqlx::{PgConnection, PgPool};

use crate::error::{ChainError, Result};
use crate::storage::StorageAt;

/// Context for a single block being handled.
pub struct BlockContext<'a> {
    pub chain: &'a ChainInfo,
    pub block_number: u64,
    /// This block's hash, as stored on the `blocks` row.
    pub block_hash: &'a [u8],
    /// Chain state *at this block*.
    ///
    /// Some pallets do not put what changed into the event — `pallet_identity` emits
    /// `IdentitySet { who }` with no display name — so the event says which key changed and
    /// only storage says what it changed to. Reads here are made at this block's hash, so a
    /// handler sees exactly the state the block left behind.
    ///
    /// This costs an RPC round-trip inside the block's transaction, so read only when an
    /// event tells you something actually changed.
    pub storage: &'a dyn StorageAt,
}

/// Derives extra tables from a block's decoded events.
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Name used in `chains.toml`'s `handlers = [...]` list.
    fn name(&self) -> &'static str;

    /// Whether this handler can safely decode events from the given chain.
    ///
    /// Returning `false` disables it for that chain. This is the guard that stops a handler
    /// written for chain A silently producing garbage on chain B.
    fn supports(&self, chain: &ChainInfo) -> bool;

    /// Migrations creating this handler's tables, run by [`run_migrations`].
    ///
    /// Version numbers are **local to this handler** — start at 1. Each handler's history is
    /// tracked in its own table (see [`run_migrations`]), so numbering never has to be
    /// coordinated with the framework or with other handlers.
    fn migrator(&self) -> Option<&'static sqlx::migrate::Migrator> {
        None
    }

    /// Seed state once, before any block is indexed.
    ///
    /// Runs after migrations and **outside** any block transaction, with storage at the block
    /// before the first one to be indexed. Two things make this necessary rather than
    /// optional for a state-backed handler:
    ///
    ///   * events only describe changes, so anything set *before* the start block is
    ///     invisible — on the People chain that is nearly the whole identity set, which
    ///     arrived by migration rather than through blocks we index;
    ///   * a full storage sweep is tens of thousands of keys, which must not happen inside
    ///     the one-transaction-per-block path.
    ///
    /// Implementations must be idempotent: this may run again after a crash. Default is a
    /// no-op, so event-only handlers are unaffected.
    async fn bootstrap(
        &self,
        _chain: &ChainInfo,
        _storage: &dyn StorageAt,
        _pool: &PgPool,
    ) -> Result<()> {
        Ok(())
    }

    /// Derive and persist rows for one block.
    ///
    /// `tx` is the *same* transaction the core rows were written in, so anything written
    /// here commits atomically with the block and is rolled back with it. Returning an error
    /// aborts the whole block rather than leaving a partial projection.
    async fn handle(
        &self,
        ctx: &BlockContext<'_>,
        block: &BlockData,
        tx: &mut PgConnection,
    ) -> Result<()>;
}

/// The handlers available to, or enabled for, a chain.
#[derive(Default)]
pub struct HandlerRegistry {
    handlers: Vec<Box<dyn EventHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make a handler available for selection.
    pub fn register(&mut self, handler: Box<dyn EventHandler>) -> &mut Self {
        self.handlers.push(handler);
        self
    }

    /// Narrow this registry to the handlers a chain's config asks for.
    ///
    /// An unknown name is an error rather than a silent no-op: a typo in `chains.toml` would
    /// otherwise look exactly like "this chain produced no matching events".
    pub fn select(&self, names: &[String], chain: &ChainInfo) -> Result<Selected<'_>> {
        let mut selected = Vec::new();

        for name in names {
            let handler = self
                .handlers
                .iter()
                .find(|h| h.name() == name)
                .ok_or_else(|| ChainError::UnknownHandler(name.clone()))?;

            if !handler.supports(chain) {
                tracing::warn!(
                    handler = %name, chain = %chain.id,
                    "handler does not support this chain and will be skipped"
                );
                continue;
            }

            tracing::info!(handler = %name, chain = %chain.id, "handler enabled");
            selected.push(handler.as_ref());
        }

        Ok(Selected { handlers: selected })
    }

    /// Every registered handler, for running migrations.
    pub fn all(&self) -> impl Iterator<Item = &dyn EventHandler> {
        self.handlers.iter().map(|h| h.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

/// The handlers enabled for one specific chain, borrowed from a [`HandlerRegistry`].
pub struct Selected<'a> {
    handlers: Vec<&'a dyn EventHandler>,
}

impl Selected<'_> {
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Seed every enabled handler, once, before the block loop starts.
    ///
    /// Sequential rather than concurrent: a bootstrap sweep is RPC-bound against a single
    /// node, so running several at once buys nothing and makes the failure ordering harder
    /// to reason about.
    pub async fn bootstrap(
        &self,
        chain: &ChainInfo,
        storage: &dyn StorageAt,
        pool: &PgPool,
    ) -> Result<()> {
        for handler in &self.handlers {
            handler.bootstrap(chain, storage, pool).await.map_err(|e| {
                tracing::error!(
                    handler = handler.name(), chain = %chain.id, error = %e,
                    "handler bootstrap failed; refusing to index with incomplete state"
                );
                e
            })?;
        }
        Ok(())
    }

    /// Run every enabled handler over a decoded block, inside the block's transaction.
    pub async fn run(
        &self,
        ctx: &BlockContext<'_>,
        block: &BlockData,
        tx: &mut PgConnection,
    ) -> Result<()> {
        for handler in &self.handlers {
            handler.handle(ctx, block, tx).await.map_err(|e| {
                tracing::error!(
                    handler = handler.name(), block = ctx.block_number, error = %e,
                    "handler failed; rolling back the block"
                );
                e
            })?;
        }
        Ok(())
    }
}

/// Apply one handler's migrations, tracked in a table of its own.
///
/// sqlx validates the *entire* migrations table against the migrator's own list, so two
/// migrators sharing `_sqlx_migrations` reject each other's rows with "previously applied
/// but is missing in the resolved migrations". Giving each handler its own
/// `_sqlx_migrations_<handler>` table isolates them completely: version numbers are local to
/// the handler, and adding or removing a handler never disturbs anyone else's history.
pub async fn run_migrations(pool: &sqlx::PgPool, handler: &dyn EventHandler) -> Result<()> {
    let Some(migrator) = handler.migrator() else {
        return Ok(());
    };

    let table = migrations_table_name(handler.name());
    let mut owned = sqlx::migrate::Migrator {
        migrations: migrator.migrations.clone(),
        ..sqlx::migrate::Migrator::with_migrations(Vec::new())
    };
    owned.dangerous_set_table_name(table.clone());

    owned
        .run(pool)
        .await
        .map_err(|source| ChainError::Handler {
            handler: handler.name().to_string(),
            block: 0,
            source: Box::new(source),
        })?;

    tracing::info!(handler = handler.name(), table = %table, "handler migrations applied");
    Ok(())
}

/// Per-handler migrations table name.
///
/// Sanitised because it is interpolated into DDL: anything outside `[a-z0-9_]` becomes `_`.
fn migrations_table_name(handler: &str) -> String {
    let slug: String = handler
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("_sqlx_migrations_{slug}")
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Stub(&'static str);

    #[async_trait]
    impl EventHandler for Stub {
        fn name(&self) -> &'static str {
            self.0
        }
        fn supports(&self, _chain: &ChainInfo) -> bool {
            true
        }
        async fn handle(
            &self,
            _: &BlockContext<'_>,
            _: &BlockData,
            _: &mut PgConnection,
        ) -> Result<()> {
            Ok(())
        }
    }

    /// A handler that refuses every chain, to exercise the `supports` guard.
    struct Unsupported;

    #[async_trait]
    impl EventHandler for Unsupported {
        fn name(&self) -> &'static str {
            "unsupported"
        }
        fn supports(&self, _chain: &ChainInfo) -> bool {
            false
        }
        async fn handle(
            &self,
            _: &BlockContext<'_>,
            _: &BlockData,
            _: &mut PgConnection,
        ) -> Result<()> {
            panic!("must never run on an unsupported chain");
        }
    }

    fn chain() -> ChainInfo {
        ChainInfo {
            id: "test".into(),
            genesis_hash: vec![0xaa; 32],
            name: "Test".into(),
            token_symbol: Some("UNIT".into()),
            token_decimals: Some(12),
            ss58_prefix: 42,
        }
    }

    #[test]
    fn empty_config_selects_nothing() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(Stub("a")));

        assert!(registry.select(&[], &chain()).unwrap().is_empty());
    }

    #[test]
    fn selects_only_the_handlers_config_asks_for() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(Stub("a")));
        registry.register(Box::new(Stub("b")));

        let selected = registry.select(&["b".to_string()], &chain()).unwrap();
        assert_eq!(selected.handlers.len(), 1);
        assert_eq!(selected.handlers[0].name(), "b");
    }

    #[test]
    fn a_typo_in_config_is_an_error_not_a_silent_noop() {
        // Without this, `handlers = ["balances-transfr"]` would look identical to a chain
        // that genuinely produced no matching events.
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(Stub("balances-transfer")));

        let err = match registry.select(&["balances-transfr".to_string()], &chain()) {
            Err(e) => e,
            Ok(_) => panic!("an unknown handler name must be rejected"),
        };
        assert!(
            err.to_string().contains("no handler registered"),
            "got: {err}"
        );
    }

    #[test]
    fn a_handler_that_rejects_the_chain_is_skipped() {
        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(Unsupported));

        let selected = registry
            .select(&["unsupported".to_string()], &chain())
            .unwrap();
        assert!(
            selected.is_empty(),
            "unsupported handler must not be selected"
        );
    }

    #[test]
    fn downstream_crates_can_register_their_own_handlers() {
        // The point of the whole refactor: nothing in this crate knows this handler exists.
        struct HydrationOmnipool;

        #[async_trait]
        impl EventHandler for HydrationOmnipool {
            fn name(&self) -> &'static str {
                "omnipool-swap"
            }
            fn supports(&self, _: &ChainInfo) -> bool {
                true
            }
            async fn handle(
                &self,
                _: &BlockContext<'_>,
                _: &BlockData,
                _: &mut PgConnection,
            ) -> Result<()> {
                Ok(())
            }
        }

        let mut registry = HandlerRegistry::new();
        registry.register(Box::new(HydrationOmnipool));

        let selected = registry
            .select(&["omnipool-swap".to_string()], &chain())
            .unwrap();
        assert_eq!(selected.handlers.len(), 1);
    }

    #[test]
    fn each_handler_gets_its_own_migrations_table() {
        // Isolation is what lets handlers number their migrations from 1 independently.
        assert_eq!(
            migrations_table_name("balances-transfer"),
            "_sqlx_migrations_balances_transfer"
        );
        assert_ne!(
            migrations_table_name("omnipool-swap"),
            migrations_table_name("balances-transfer")
        );
    }

    #[test]
    fn migrations_table_name_is_safe_to_interpolate() {
        // The name reaches DDL, so anything outside [a-z0-9_] must not survive.
        let name = migrations_table_name("evil\"; DROP TABLE blocks; --");
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "unsafe table name: {name}"
        );
    }
}
