//! The `identity` handler: `pallet_identity` -> queryable alias tables.
//!
//! Per block it does two things, in this order:
//!
//!   1. read the block's `Identity` events to learn **which keys changed** (see
//!      [`crate::touched`]);
//!   2. re-read **what they changed to** from storage at this block's hash.
//!
//! Step 2 is what costs an RPC round-trip inside the block's transaction, so it only happens
//! when step 1 found something. Identity activity is rare, so the overwhelming majority of
//! blocks cost nothing at all.

use std::collections::BTreeMap;

use async_trait::async_trait;
use pif_chain::error::{ChainError, Result};
use pif_chain::handlers::{BlockContext, EventHandler};
use pif_chain::storage::StorageAt;
use pif_core::ChainInfo;
use pif_db::BlockData;
use sqlx::{PgConnection, PgPool};

use crate::bootstrap;
use crate::read::{self, Account};
use crate::touched;

pub const NAME: &str = "identity";

/// Migrations owning `identities`, `usernames`, `sub_identities`, `identity_registrars`.
///
/// Versions are local to this handler; the framework tracks it in `_sqlx_migrations_identity`.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub struct IdentityHandler;

#[async_trait]
impl EventHandler for IdentityHandler {
    fn name(&self) -> &'static str {
        NAME
    }

    fn supports(&self, _chain: &ChainInfo) -> bool {
        // `ChainInfo` carries no metadata, so the real check — does this runtime have the
        // pallet — cannot happen here. It happens in `bootstrap`, which does have storage
        // access and fails loudly there.
        true
    }

    fn migrator(&self) -> Option<&'static sqlx::migrate::Migrator> {
        Some(&MIGRATOR)
    }

    async fn bootstrap(
        &self,
        chain: &ChainInfo,
        storage: &dyn StorageAt,
        pool: &PgPool,
    ) -> Result<()> {
        bootstrap::run(chain, storage, pool).await
    }

    async fn handle(
        &self,
        ctx: &BlockContext<'_>,
        block: &BlockData,
        tx: &mut PgConnection,
    ) -> Result<()> {
        let touched = touched::collect(&block.events, ctx.chain.ss58_prefix);
        if touched.is_empty() {
            return Ok(());
        }

        let number = i64::try_from(ctx.block_number).map_err(|_| ChainError::Handler {
            handler: NAME.to_string(),
            block: ctx.block_number,
            source: "block number does not fit in i64".into(),
        })?;

        tracing::debug!(
            block = ctx.block_number,
            accounts = touched.accounts.len(),
            usernames = touched.usernames.len(),
            "identity keys touched"
        );

        sync(
            ctx.storage,
            tx,
            &ctx.chain.id,
            ctx.chain.ss58_prefix,
            touched.accounts,
            touched.usernames.into_iter().collect(),
            touched.registrars,
            number,
        )
        .await
        .map_err(|e| wrap(e, ctx.block_number))
    }
}

/// Re-read and persist every touched key.
///
/// Shared with the bootstrap path so that a snapshot row and an update row are produced by
/// exactly the same code.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sync(
    storage: &dyn StorageAt,
    conn: &mut PgConnection,
    chain_id: &str,
    prefix: u16,
    accounts: BTreeMap<String, Vec<u8>>,
    usernames: Vec<String>,
    registrars: bool,
    block: i64,
) -> Result<()> {
    // `SubsOf` can name children that no event mentioned, and those children's labels live in
    // their own `SuperOf`. Queue them rather than recursing, so a deep or cyclic parent/child
    // graph cannot blow the stack, and `done` keeps each account to one read per block.
    let mut queue: Vec<Account> = accounts.into_iter().collect();
    let mut done: Vec<String> = Vec::with_capacity(queue.len());

    while let Some(account) = queue.pop() {
        if done.contains(&account.0) {
            continue;
        }
        done.push(account.0.clone());

        let discovered =
            read::sync_account(storage, conn, chain_id, prefix, &account, block).await?;
        for sub in discovered {
            if !done.contains(&sub.0) {
                queue.push(sub);
            }
        }
    }

    for username in usernames {
        read::sync_username(storage, conn, chain_id, prefix, &username, block).await?;
    }

    if registrars {
        read::sync_registrars(storage, conn, chain_id, prefix, block).await?;
    }

    Ok(())
}

/// Present a failure as this handler's, so the pipeline's rollback log names it.
fn wrap(error: ChainError, block: u64) -> ChainError {
    match error {
        // Pruning is actionable and already well described; do not bury it.
        pruned @ ChainError::PrunedState { .. } => pruned,
        other => ChainError::Handler {
            handler: NAME.to_string(),
            block,
            source: Box::new(other),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_ships_its_own_migrations() {
        let migrator = IdentityHandler.migrator().expect("handler owns tables");
        assert!(migrator.iter().count() > 0);
    }

    #[test]
    fn handler_is_named_for_chains_toml() {
        // This string is what `handlers = ["identity"]` matches; a typo here is a startup
        // error rather than silence, but only because the name is checked against config.
        assert_eq!(IdentityHandler.name(), "identity");
    }
}
