//! The ingest loop for a single chain: catch up to the finalized head, then follow it.

use std::time::Duration;

use pif_core::ChainConfig;
use pif_db::repo;
use sqlx::PgPool;

use crate::client::ChainClient;
use crate::decode::{self, AtBlock};
use crate::error::{ChainError, Result};
use crate::handlers::{BlockContext, HandlerRegistry, Selected};
use crate::storage::SubxtStorage;

/// How long to wait before reconnecting after the block stream drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Options for one chain's indexing run.
#[derive(Default)]
pub struct IndexOptions {
    /// Stop after this block instead of following the head. Used by tests.
    pub stop_at: Option<u64>,
    /// Ignore any stored cursor and start here.
    pub from: Option<u64>,
}

/// Index one chain until `stop_at` is reached, or forever.
///
/// Only *finalized* blocks are consumed. That is a deliberate constraint: a finalized block
/// cannot be reverted, so the indexer needs no reorg handling and the stored chain can never
/// contain an orphaned block. Anything that changes this to follow best-blocks must add
/// rollback logic.
/// `registry` holds every handler the caller knows about; the ones this chain's config
/// names are selected once the chain identifies itself. An unknown name is an error.
pub async fn run(
    pool: &PgPool,
    config: &ChainConfig,
    registry: &HandlerRegistry,
    options: IndexOptions,
) -> Result<()> {
    // Checked before connecting: starting a light client means warp-syncing before anything
    // happens at all, and a request it can never satisfy should not cost that first.
    if let Some(from) = options.from
        && !config.can_backfill()
    {
        return Err(ChainError::LightClientCannotBackfill {
            chain: config.id.clone(),
            number: from,
        });
    }

    let chain = ChainClient::connect(config).await?;
    tracing::info!(chain = %chain.info, "connected");

    guard_chain_identity(pool, config, &chain).await?;
    repo::upsert_chain(pool, &chain.info).await?;

    // Selection happens after connecting because `supports()` inspects the real chain.
    let handlers = &registry.select(&config.handlers, &chain.info)?;

    // `RuntimeVersion` only types `spec_version` and `transaction_version`; everything
    // else, including the runtime's name, arrives in the untyped `other` map.
    let runtime_version = chain.rpc.state_get_runtime_version(None).await?;
    let spec_name = runtime_version
        .other
        .get("specName")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_owned();

    // A light client cannot resolve a block *number* at all, so there is nothing to catch
    // up from: it starts at whatever the finality subscription hands it first.
    if !config.can_backfill() {
        return follow_only(pool, config, &chain, handlers, &spec_name, &options).await;
    }

    let start = match options.from {
        Some(from) => from,
        None => match repo::load_cursor(pool, &config.id).await? {
            // Resume from the block *after* the last one fully committed.
            Some(cursor) => cursor.last_indexed_block as u64 + 1,
            None => config.start_block,
        },
    };

    // Seed handlers that project chain *state* before any block is indexed.
    //
    // Taken at the block *before* the first one we index, so the snapshot represents
    // everything that happened while we were not watching, and the block loop then applies
    // changes on top without double-counting the first block. `saturating_sub` makes
    // `start_block = 0` snapshot genesis, which is empty — correct, and cheap.
    if !handlers.is_empty() {
        let snapshot_at = start.saturating_sub(1);
        let at = decode::at_block(&chain.client, &chain.info, snapshot_at).await?;
        let storage = SubxtStorage::new(&at, &chain.info.id);

        tracing::info!(chain = %config.id, block = snapshot_at, "bootstrapping handlers");
        handlers.bootstrap(&chain.info, &storage, pool).await?;
    }

    let target = chain.finalized_number().await?;
    tracing::info!(chain = %config.id, start, finalized_head = target, "starting catch-up");

    // Catch-up: serial by design in M1. Parallel backfill is a later milestone.
    let catch_up_end = options.stop_at.map(|s| s.min(target)).unwrap_or(target);
    for number in start..=catch_up_end {
        persist_number(pool, &chain, handlers, &spec_name, number).await?;

        if number % 100 == 0 || number == catch_up_end {
            tracing::info!(chain = %config.id, block = number, "indexed");
        }
    }

    if let Some(stop) = options.stop_at
        && catch_up_end >= stop
    {
        tracing::info!(chain = %config.id, stop, "reached stop_at, finishing");
        return Ok(());
    }

    follow_head(pool, config, &chain, handlers, &spec_name, options.stop_at).await
}

/// Start a light-client chain, which can only ever go forwards.
///
/// There is no catch-up phase here, and no `start_block`, because smoldot has no verifiable
/// answer to "what is block N": it would have to take a full node's word for it, which is
/// the exact assumption a light client exists to avoid. Rather than silently indexing a
/// shorter history than asked for, an impossible request is an error (`--from`, and
/// `start_block` at config load) and an unavoidable gap is a loud warning.
async fn follow_only(
    pool: &PgPool,
    config: &ChainConfig,
    chain: &ChainClient,
    handlers: &Selected<'_>,
    spec_name: &str,
    options: &IndexOptions,
) -> Result<()> {
    // A gap left by a restart is reported by `follow_head` once the first block arrives,
    // alongside the identical case of the subscription skipping blocks while running.

    // Handlers that project state are seeded from the current finalized block: it is the
    // oldest state a light client can prove anything about.
    if !handlers.is_empty() {
        let at = decode::at_current_block(&chain.client, &chain.info).await?;
        let number = at.block_number();
        let storage = SubxtStorage::new(&at, &chain.info.id);

        tracing::info!(chain = %config.id, block = number, "bootstrapping handlers");
        handlers.bootstrap(&chain.info, &storage, pool).await?;
    }

    tracing::info!(chain = %config.id, "light client: following the finalized head");
    follow_head(pool, config, chain, handlers, spec_name, options.stop_at).await
}

/// Follow the finalized head, reconnecting if the subscription drops.
async fn follow_head(
    pool: &PgPool,
    config: &ChainConfig,
    chain: &ChainClient,
    handlers: &Selected<'_>,
    spec_name: &str,
    stop_at: Option<u64>,
) -> Result<()> {
    loop {
        let mut blocks = match chain.client.stream_blocks().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::warn!(chain = %config.id, error = %e, "failed to open block stream, retrying");
                tokio::time::sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        tracing::info!(chain = %config.id, "following finalized head");

        while let Some(block) = blocks.next().await {
            let block = match block {
                Ok(block) => block,
                Err(e) => {
                    tracing::warn!(chain = %config.id, error = %e, "block stream error, reconnecting");
                    break;
                }
            };

            let number = block.number();

            // The stream only guarantees it delivers finalized blocks, not that it never
            // skips one under load. Re-deriving the range from the cursor keeps the stored
            // chain gap-free even if the subscription jumps.
            let expected = match repo::load_cursor(pool, &config.id).await? {
                Some(cursor) => cursor.last_indexed_block as u64 + 1,
                None => number,
            };

            // Already stored — a reconnect can replay the block we stopped on.
            if number < expected {
                continue;
            }

            if expected < number {
                if config.can_backfill() {
                    for n in expected..number {
                        index_one(pool, config, chain, handlers, spec_name, n).await?;
                    }
                } else {
                    // Either the indexer was stopped for a while, or the subscription
                    // skipped ahead. Both leave the same hole, and a light client cannot
                    // fetch any of it back — so say so loudly rather than let the stored
                    // chain quietly stop being contiguous.
                    tracing::warn!(
                        chain = %config.id,
                        missing_from = expected,
                        missing_to = number - 1,
                        blocks = number - expected,
                        "gap that a light client cannot backfill; index this range from an \
                         rpc source if you need it"
                    );
                }
            }

            // Indexed from the reference the stream handed us rather than by number: the
            // hash is already known and pinned, which saves a lookup on an rpc source and
            // is the only way through at all on a light client.
            let at = decode::at_streamed_block(&block).await?;
            persist(pool, chain, handlers, spec_name, &at).await?;
            tracing::debug!(chain = %config.id, block = number, "indexed");

            if let Some(stop) = stop_at
                && number >= stop
            {
                tracing::info!(chain = %config.id, stop, "reached stop_at, finishing");
                return Ok(());
            }
        }

        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn index_one(
    pool: &PgPool,
    config: &ChainConfig,
    chain: &ChainClient,
    handlers: &Selected<'_>,
    spec_name: &str,
    number: u64,
) -> Result<()> {
    persist_number(pool, chain, handlers, spec_name, number).await?;
    tracing::debug!(chain = %config.id, block = number, "indexed");
    Ok(())
}

/// Resolve a block by number, then commit it.
///
/// Only reachable for sources that can answer "what is block N" — see
/// [`pif_core::ChainSource`].
async fn persist_number(
    pool: &PgPool,
    chain: &ChainClient,
    handlers: &Selected<'_>,
    spec_name: &str,
    number: u64,
) -> Result<()> {
    let at = decode::at_block(&chain.client, &chain.info, number).await?;
    persist(pool, chain, handlers, spec_name, &at).await
}

/// Decode one block and commit it, together with whatever its handlers derive.
///
/// The core rows, every handler's rows and the cursor share a single transaction, so a
/// handler failure rolls the entire block back and the cursor never runs ahead of the data.
async fn persist(
    pool: &PgPool,
    chain: &ChainClient,
    handlers: &Selected<'_>,
    spec_name: &str,
    at: &AtBlock,
) -> Result<()> {
    let number = at.block_number();
    let mut data = decode::decode_at(&chain.client, &chain.rpc, at, &chain.info).await?;
    data.spec_name = spec_name.to_owned();

    // State at this exact block, for handlers whose pallet reports *that* something changed
    // without reporting *what* it changed to.
    let storage = SubxtStorage::new(at, &chain.info.id);

    let ctx = BlockContext {
        chain: &chain.info,
        block_number: number,
        block_hash: &data.block.hash,
        storage: &storage,
    };

    let mut tx = pool.begin().await?;
    repo::write_block_in_tx(&mut tx, &data).await?;
    handlers.run(&ctx, &data, &mut tx).await?;
    repo::update_cursor(&mut tx, &chain.info.id, data.block.number, &data.block.hash).await?;
    tx.commit().await?;

    Ok(())
}

/// Refuse to index if this chain id is already bound to a different genesis hash.
///
/// `id` is just a config key; the genesis hash is the chain's real identity. Repointing an
/// id at a different chain would interleave two chains into rows keyed the same way, which
/// looks like corruption rather than a config mistake. Better to stop here.
async fn guard_chain_identity(
    pool: &PgPool,
    config: &ChainConfig,
    chain: &ChainClient,
) -> Result<()> {
    let Some(stored) = repo::genesis_hash(pool, &config.id).await? else {
        return Ok(());
    };

    if stored != chain.info.genesis_hash {
        return Err(ChainError::GenesisMismatch {
            chain: config.id.clone(),
            url: config.source.to_string(),
            stored: hex::encode(&stored),
            found: hex::encode(&chain.info.genesis_hash),
        });
    }

    Ok(())
}
