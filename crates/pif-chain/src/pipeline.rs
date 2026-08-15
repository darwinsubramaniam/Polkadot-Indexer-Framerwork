//! The ingest loop for a single chain: catch up to the finalized head, then follow it.
//!
//! Fetching and processing used to be the same event — one function resolved a block over
//! RPC, decoded it, ran handlers and committed, before touching the next number. They are
//! now two loops that share nothing but the watermarks table:
//!
//! * [`crate::fetch`] pulls raw blocks and appends them to the local archive;
//! * [`crate::digest`] reads only from that archive, decodes, and writes to Postgres.
//!
//! Neither blocks the other, and either can fail, retry, or be redone without the other. The
//! payoff is not primarily speed: it is that a re-index stops costing a re-download.

use std::time::Duration;

use pif_core::{ChainConfig, ChainInfo};
use pif_db::repo;
use pif_store::HotStore;
use sqlx::PgPool;

use crate::client::ChainClient;
use crate::decode::{self, AtBlock};
use crate::error::{ChainError, Result};
use crate::handlers::{BlockContext, HandlerRegistry, Selected};
use crate::storage::SubxtStorage;
use crate::{digest, fetch};

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

    let Connected {
        client: chain,
        spec_name,
    } = connect_and_identify(pool, config).await?;

    // Selection happens after connecting because `supports()` inspects the real chain.
    let handlers = &registry.select(&config.handlers, &chain.info)?;

    // A light client cannot resolve a block *number* at all, so there is nothing to catch
    // up from: it starts at whatever the finality subscription hands it first.
    if !config.can_backfill() {
        return follow_only(pool, config, &chain, handlers, &spec_name, &options).await;
    }

    let store = open_store(config)?;

    let start = resolve_start(pool, config, options.from).await?;

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

    // Two loops, run concurrently rather than spawned: `Selected` borrows the registry, and
    // a spawned task would need it to be `'static`. `try_join` also gives the behaviour that
    // matters on failure — if the fetch stage dies, the digest stops rather than spinning
    // against a watermark that will never move again.
    let digest = digest::Digest {
        pool,
        chain: &chain.info,
        store: &store,
        handlers,
        live: Some(&chain),
        spec_name: &spec_name,
    };

    tokio::try_join!(
        fetch::run(
            pool,
            config,
            &chain,
            &store,
            &spec_name,
            start,
            options.stop_at
        ),
        digest.run(start, options.stop_at),
    )?;

    Ok(())
}

/// Fetch blocks into the archive and nothing else — `pif fetch`.
///
/// Useful on its own: it needs no handlers, decodes nothing, and therefore keeps running
/// through a runtime the digest cannot yet read.
pub async fn fetch_only(pool: &PgPool, config: &ChainConfig, options: IndexOptions) -> Result<()> {
    let chain = connect_and_identify(pool, config).await?;
    let store = open_store(config)?;
    let start = resolve_start(pool, config, options.from).await?;

    fetch::run(
        pool,
        config,
        &chain.client,
        &store,
        &chain.spec_name,
        start,
        options.stop_at,
    )
    .await
}

/// Digest already-archived blocks and nothing else — `pif digest`.
///
/// Connects only if a handler might read chain state. With no handlers this touches no
/// network at all, which is what makes it usable against an archive whose chain is
/// unreachable.
pub async fn digest_only(
    pool: &PgPool,
    config: &ChainConfig,
    registry: &HandlerRegistry,
    options: IndexOptions,
) -> Result<()> {
    let store = open_store(config)?;
    let resolved = resolve_offline(pool, config).await?;
    let handlers = &registry.select(&config.handlers, &resolved.info)?;

    let start = match options.from {
        Some(from) => {
            repo::reset_watermarks(pool, &config.id, before(from)).await?;
            from
        }
        None => match repo::load_watermarks(pool, &config.id).await? {
            Some(marks) => (marks.digest + 1) as u64,
            None => config.start_block,
        },
    };

    digest::Digest {
        pool,
        chain: &resolved.info,
        store: &store,
        handlers,
        live: resolved.client.as_ref(),
        spec_name: &resolved.spec_name,
    }
    .run(start, options.stop_at)
    .await
}

/// Re-digest a range from the archive — `pif replay`.
///
/// This is what the archive is *for*. Adding a handler, or fixing a decode bug, becomes a
/// local operation over bytes already held: every insert is `ON CONFLICT DO NOTHING`, so
/// replaying an already-indexed range is idempotent rather than an error.
pub async fn replay(
    pool: &PgPool,
    config: &ChainConfig,
    registry: &HandlerRegistry,
    from: u64,
    to: u64,
) -> Result<()> {
    if to < from {
        return Err(ChainError::EmptyReplayRange { from, to });
    }

    let store = open_store(config)?;
    let resolved = resolve_offline(pool, config).await?;
    let handlers = &registry.select(&config.handlers, &resolved.info)?;

    tracing::info!(chain = %config.id, from, to, "replaying from the archive");

    digest::Digest {
        pool,
        chain: &resolved.info,
        store: &store,
        handlers,
        live: resolved.client.as_ref(),
        spec_name: &resolved.spec_name,
    }
    .replay_range(from, to)
    .await
}

/// What `pif store status` reports for one chain.
pub struct StoreStatus {
    pub chain_id: String,
    pub path: String,
    /// `None` for a chain that has never been indexed here.
    pub watermarks: Option<pif_db::Watermarks>,
    pub usage: pif_store::StoreUsage,
    /// The unbroken run of archived blocks, from the lowest one held — what `pif replay` can
    /// actually reach. `None` when nothing is archived.
    ///
    /// A *run*, not a count, because a hole is the thing worth seeing: the archive can hold
    /// a block above a gap and still not be replayable across it.
    pub replayable: Option<(u64, u64)>,
    /// Runtimes indexed whose metadata is not archived, as `(spec_version, first_block)`.
    ///
    /// Every row written before the archive existed is in here. Not a gap to be embarrassed
    /// about: it is the precise answer to "which ranges can I actually replay?".
    pub runtimes_without_metadata: Vec<(i32, i64)>,
}

/// Report what the archive holds for one chain — `pif store status`.
///
/// Needs no node: everything here is on local disk or in Postgres.
pub async fn store_status(pool: &PgPool, config: &ChainConfig) -> Result<StoreStatus> {
    let store = HotStore::open(&config.pipeline().hot_path, config.pipeline().segment_size)?;

    let replayable = match store.first_block(&config.id)? {
        Some(first) => store
            .contiguous_end(&config.id, first)?
            .map(|last| (first, last)),
        None => None,
    };

    Ok(StoreStatus {
        chain_id: config.id.clone(),
        path: store.root().display().to_string(),
        watermarks: repo::load_watermarks(pool, &config.id).await?,
        usage: store.usage(&config.id)?,
        replayable,
        runtimes_without_metadata: repo::runtimes_without_metadata(pool, &config.id).await?,
    })
}

/// A chain identified well enough to decode it, and a connection only if one is needed.
struct Resolved {
    info: ChainInfo,
    spec_name: String,
    client: Option<ChainClient>,
}

/// Identify a chain from the database where possible, connecting only if it is unavoidable.
///
/// Everything decoding needs about a chain — its id and SS58 prefix — was recorded the first
/// time it was indexed. Reading it back is what lets a replay run with the node switched
/// off, which is the claim the archive exists to make good on. A connection is opened only
/// when a handler will read chain state, because that is not in the archive yet.
async fn resolve_offline(pool: &PgPool, config: &ChainConfig) -> Result<Resolved> {
    let stored = repo::load_chain(pool, &config.id).await?;
    let known = stored.is_some();
    let reads_state = !config.handlers.is_empty();

    if let Some(info) = stored
        && !reads_state
    {
        let spec_name = repo::latest_spec_name(pool, &config.id)
            .await?
            .unwrap_or_else(|| "unknown".to_owned());
        return Ok(Resolved {
            info,
            spec_name,
            client: None,
        });
    }

    if !known {
        tracing::info!(
            chain = %config.id,
            "this chain has never been indexed here, so its identity must come from the node"
        );
    } else {
        tracing::info!(
            chain = %config.id,
            handlers = ?config.handlers,
            "connecting: these handlers read chain state, which is not archived yet"
        );
    }

    let connected = connect_and_identify(pool, config).await?;
    Ok(Resolved {
        info: connected.client.info.clone(),
        spec_name: connected.spec_name,
        client: Some(connected.client),
    })
}

/// A connected chain plus the runtime name the rows record.
struct Connected {
    client: ChainClient,
    spec_name: String,
}

async fn connect_and_identify(pool: &PgPool, config: &ChainConfig) -> Result<Connected> {
    let chain = ChainClient::connect(config).await?;
    tracing::info!(chain = %chain.info, "connected");

    guard_chain_identity(pool, config, &chain).await?;
    repo::upsert_chain(pool, &chain.info).await?;

    let spec_name = spec_name_of(&chain).await?;
    Ok(Connected {
        client: chain,
        spec_name,
    })
}

/// `RuntimeVersion` only types `spec_version` and `transaction_version`; everything else,
/// including the runtime's name, arrives in the untyped `other` map.
async fn spec_name_of(chain: &ChainClient) -> Result<String> {
    let runtime_version = chain.rpc.state_get_runtime_version(None).await?;
    Ok(runtime_version
        .other
        .get("specName")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_owned())
}

/// Open this chain's block archive.
fn open_store(config: &ChainConfig) -> Result<HotStore> {
    let pipeline = config.pipeline();
    let store = HotStore::open(&pipeline.hot_path, pipeline.segment_size)?;
    tracing::info!(
        chain = %config.id,
        path = %store.root().display(),
        segment_size = store.segment_size(),
        "block archive ready"
    );
    Ok(store)
}

/// The watermark value meaning "everything below `start` is done, and nothing else is".
///
/// Signed, and `-1` at genesis. `u64::saturating_sub` would give `0` there, which reads as
/// "block 0 is already indexed" — so `pif index --from 0` would skip the genesis block and
/// the gap would be invisible, since nothing else ever looks below the watermark.
fn before(start: u64) -> i64 {
    start as i64 - 1
}

/// Where indexing should resume, and make sure the watermark row agrees.
///
/// `--from` is an explicit request to redo work, so it moves the watermarks *back* — the one
/// place monotonicity is broken on purpose. Without it the watermarks are seeded once and
/// never touched, so a restart resumes exactly where it stopped.
async fn resolve_start(pool: &PgPool, config: &ChainConfig, from: Option<u64>) -> Result<u64> {
    if let Some(from) = from {
        repo::reset_watermarks(pool, &config.id, before(from)).await?;
        return Ok(from);
    }

    if let Some(marks) = repo::load_watermarks(pool, &config.id).await? {
        // The digest is the stage that decides where indexing resumes; the fetch stage may
        // legitimately be ahead of it, and re-archiving a block it already holds is a
        // harmless overwrite.
        return Ok((marks.digest + 1) as u64);
    }

    // A chain indexed before the archive existed has a cursor but no watermark row — the
    // `0002_pipeline` migration seeds one for every chain it finds, so this is reached only
    // by a chain that has never been indexed at all.
    let start = match repo::load_cursor(pool, &config.id).await? {
        Some(cursor) => cursor.last_indexed_block as u64 + 1,
        None => config.start_block,
    };
    repo::init_watermarks(pool, &config.id, before(start)).await?;
    Ok(start)
}

/// Start a light-client chain, which can only ever go forwards.
///
/// There is no catch-up phase here, and no `start_block`, because smoldot has no verifiable
/// answer to "what is block N": it would have to take a full node's word for it, which is
/// the exact assumption a light client exists to avoid. Rather than silently indexing a
/// shorter history than asked for, an impossible request is an error (`--from`, and
/// `start_block` at config load) and an unavoidable gap is a loud warning.
///
/// **Still single-stage, on purpose.** A split digest resolves each block by *number* to
/// read state for handlers, and that is the one question a light client refuses to answer —
/// so the two halves cannot be separated here the way they are on an rpc source. A light
/// client keeps the fetch-and-process-together path until storage reads come from the
/// archive rather than the node, at which point the digest stops needing a number at all.
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
///
/// The light-client path only — an rpc source follows the head through [`crate::fetch`], so
/// that the head is archived and replayable like the rest of the chain. The backfill branch
/// below is therefore unreachable today, and kept because it is the correct behaviour for
/// any source that both streams and can resolve a block by number.
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
