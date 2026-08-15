//! The fetch stage: pull raw blocks off the network and append them to the local archive.
//!
//! This half never decodes a block. It reads bytes the node already sent — `Events::bytes()`
//! and `ExtrinsicDetails::bytes()` are slices of buffers subxt has already fetched and
//! already paid for — and writes them to disk. The one thing it does interpret is which
//! runtime *executed* each block, because that selects the metadata a replay will need, and
//! metadata is the one thing that cannot be recovered later once a node prunes.
//!
//! It follows that an unreadable runtime stops the *digest*, not the fetch: archiving raw
//! bytes needs no metadata at all, so the backlog keeps accumulating while a fix is built.

use std::collections::HashSet;
use std::time::Duration;

use parity_scale_codec::Encode;
use pif_core::{ChainConfig, ChainInfo};
use pif_db::{ArchivedRuntime, repo};
use pif_store::{HotStore, RawBlock};
use sqlx::PgPool;
use subxt::PolkadotConfig;

use crate::client::ChainClient;
use crate::decode::{self, AtBlock};
use crate::error::{ChainError, Result};
use crate::metadata;

/// How long to wait before reconnecting after the block stream drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Blocks archived between `fsync`s.
///
/// The watermark is advanced only *after* a sync, so it can never claim a block the disk
/// does not hold — which is what removes any need to reconcile Postgres against the store on
/// startup. A crash costs at most this many blocks of re-fetching, and re-fetching is
/// idempotent.
const SYNC_EVERY: u64 = 64;

/// Archives blocks from one chain, holding the connection and the metadata it has seen.
struct Fetcher<'a> {
    pool: &'a PgPool,
    chain: &'a ChainClient,
    store: &'a HotStore,
    spec_name: &'a str,
    /// Runtimes already archived in this run. The store is the durable record; this only
    /// avoids re-asking it once per block.
    seen: HashSet<u32>,
    /// The last executing runtime seen, so an upgrade is "these two disagree" rather than a
    /// separate poll.
    last_spec: Option<u32>,
    /// Blocks written since the last `fsync`.
    unsynced: u64,
}

impl<'a> Fetcher<'a> {
    fn new(
        pool: &'a PgPool,
        chain: &'a ChainClient,
        store: &'a HotStore,
        spec_name: &'a str,
    ) -> Self {
        Self {
            pool,
            chain,
            store,
            spec_name,
            seen: HashSet::new(),
            last_spec: None,
            unsynced: 0,
        }
    }

    fn info(&self) -> &ChainInfo {
        &self.chain.info
    }

    /// Archive the block at `number`, and make it visible to the digest when it is durable.
    async fn archive(&mut self, number: u64) -> Result<()> {
        let at = decode::at_block(&self.chain.client, self.info(), number).await?;
        self.archive_at(&at).await
    }

    /// Archive an already-resolved block.
    async fn archive_at(&mut self, at: &AtBlock) -> Result<()> {
        let raw = self.capture(at).await?;
        self.store.put_block(&self.info().id, &raw)?;
        self.unsynced += 1;

        if self.unsynced >= SYNC_EVERY {
            self.publish(raw.number).await?;
        }
        Ok(())
    }

    /// Make every block archived so far durable, then say so in Postgres.
    ///
    /// The order is the whole point. Sync first, record second: the reverse would let a
    /// crash leave `fetch_watermark` pointing at blocks the disk never received, and the
    /// digest would read a hole as an error rather than a gap it can simply wait out.
    async fn publish(&mut self, through: u64) -> Result<()> {
        self.store.sync()?;
        repo::advance_fetch_watermark(self.pool, &self.info().id, through as i64).await?;
        self.unsynced = 0;
        Ok(())
    }

    /// Build the archive record for one block.
    async fn capture(&mut self, at: &AtBlock) -> Result<RawBlock> {
        let number = at.block_number();

        let header = at
            .block_header()
            .await
            .map_err(|source| ChainError::BlockRead {
                number,
                source: Box::new(source),
            })?;

        // The runtime that *executed* this block, resolved at its parent. Archiving the
        // version the node reports for the block instead would store the wrong decoder for
        // exactly one block per upgrade, and the replay that failed on it would do so long
        // after the mistake could be traced.
        let runtime =
            decode::executing_runtime(&self.chain.client, self.info(), at, header.parent_hash)
                .await?;
        let spec_version = runtime.spec_version();
        let transaction_version = runtime.transaction_version();

        if self
            .last_spec
            .is_some_and(|previous| previous != spec_version)
        {
            // Routine — Polkadot does roughly one a month — and a non-event for the dynamic
            // core, which is compiled against no runtime at all. Worth an `info` line so the
            // boundary is findable, and nothing more.
            tracing::info!(
                chain = %self.info().id,
                block = number,
                from = self.last_spec.unwrap_or_default(),
                to = spec_version,
                "runtime upgrade"
            );
        }
        self.last_spec = Some(spec_version);

        self.ensure_metadata(spec_version, transaction_version, &runtime, number)
            .await?;

        let events = at
            .events()
            .fetch()
            .await
            .map_err(|source| ChainError::Decode {
                number,
                source: Box::new(source),
            })?
            .bytes()
            .to_vec();

        let extrinsics = self.capture_extrinsics(at, &runtime).await?;

        Ok(RawBlock {
            number,
            hash: at.block_hash().0,
            spec_version,
            transaction_version,
            header: header.encode(),
            extrinsics,
            events,
        })
    }

    /// One blob per extrinsic, which is the shape `from_bytes` wants on the way back in.
    ///
    /// Away from an upgrade the block and its parent share a runtime, so subxt's own decoded
    /// view hands back the bytes and this costs nothing extra. Only at an upgrade block do
    /// the two differ, and only there is the raw body needed — subxt exposes no way to read
    /// the bytes back out of an `Extrinsics` without first decoding them, which is precisely
    /// what cannot be done there with the wrong metadata.
    async fn capture_extrinsics(&self, at: &AtBlock, runtime: &AtBlock) -> Result<Vec<Vec<u8>>> {
        let number = at.block_number();

        if runtime.spec_version() != at.spec_version() {
            let body = self
                .chain
                .rpc
                .chain_get_block(Some(at.block_hash()))
                .await
                .map_err(|source| ChainError::BlockRead {
                    number,
                    source: Box::new(source),
                })?
                .ok_or(ChainError::UpgradeBlockBodyUnavailable {
                    chain: self.info().id.clone(),
                    number,
                })?;

            return Ok(body
                .block
                .extrinsics
                .into_iter()
                .map(|bytes| bytes.0)
                .collect());
        }

        let extrinsics = at
            .extrinsics()
            .fetch()
            .await
            .map_err(|source| ChainError::Decode {
                number,
                source: Box::new(source),
            })?;

        // Collected in a loop rather than `map(...).collect::<Result<_, _>>()`: subxt's
        // extrinsic error is over 128 bytes, and clippy rightly objects to a closure
        // returning it.
        let mut bytes = Vec::with_capacity(extrinsics.len());
        for extrinsic in extrinsics.iter() {
            let extrinsic = extrinsic.map_err(|source| ChainError::Decode {
                number,
                source: Box::new(source),
            })?;
            bytes.push(extrinsic.bytes().to_vec());
        }

        Ok(bytes)
    }

    /// Archive a runtime's metadata the first time a block executed by it appears.
    ///
    /// Once per runtime, never once per block: metadata is ~400 KB and a runtime changes a
    /// handful of times a year. Losing one of these makes every block that ran under it
    /// permanently undecodable even though the block bytes are intact.
    async fn ensure_metadata(
        &mut self,
        spec_version: u32,
        transaction_version: u32,
        runtime: &AtBlock,
        first_block: u64,
    ) -> Result<()> {
        if !self.seen.insert(spec_version) {
            return Ok(());
        }

        let chain_id = self.info().id.clone();
        if self.store.get_metadata(&chain_id, spec_version)?.is_some() {
            return Ok(());
        }

        let fetched = metadata::fetch_at(
            &self.chain.rpc,
            &chain_id,
            spec_version,
            runtime.block_hash(),
        )
        .await?;

        self.store
            .put_metadata(&chain_id, spec_version, &fetched.bytes)?;

        repo::record_archived_runtime(
            self.pool,
            &chain_id,
            &ArchivedRuntime {
                spec_version: spec_version as i32,
                spec_name: self.spec_name.to_owned(),
                // The first block *this indexer saw* executing this runtime. At an upgrade
                // that is the block after the one carrying `set_code`, because the upgrade
                // block's executing runtime is the previous one — so it re-asserts the
                // previous row and this one is created by the next block.
                first_seen_block: first_block as i64,
                transaction_version: transaction_version as i32,
                metadata_version: fetched.format_version as i16,
                metadata_hash: sha2_of(&fetched.bytes),
            },
        )
        .await?;

        tracing::info!(
            chain = %chain_id,
            spec_version,
            metadata_version = fetched.format_version,
            bytes = fetched.bytes.len(),
            "archived runtime metadata"
        );
        Ok(())
    }
}

/// Fetch blocks for one chain: catch up to the finalized head, then follow it.
pub async fn run(
    pool: &PgPool,
    config: &ChainConfig,
    chain: &ChainClient,
    store: &HotStore,
    spec_name: &str,
    start: u64,
    stop_at: Option<u64>,
) -> Result<()> {
    let mut fetcher = Fetcher::new(pool, chain, store, spec_name);

    let target = chain.finalized_number().await?;
    let catch_up_end = stop_at.map(|s| s.min(target)).unwrap_or(target);
    tracing::info!(
        chain = %config.id, start, finalized_head = target, "fetch: starting catch-up"
    );

    for number in start..=catch_up_end {
        fetcher.archive(number).await?;

        if number % 100 == 0 || number == catch_up_end {
            tracing::info!(chain = %config.id, block = number, "fetched");
        }
    }

    if catch_up_end >= start {
        fetcher.publish(catch_up_end).await?;
    }

    if let Some(stop) = stop_at
        && catch_up_end >= stop
    {
        tracing::info!(chain = %config.id, stop, "fetch: reached stop_at, finishing");
        return Ok(());
    }

    follow_head(pool, config, &mut fetcher, stop_at).await
}

/// Follow the finalized head, archiving each block as it is finalized.
///
/// There is no parallelism to be had here — blocks arrive one every few seconds — and the
/// archive hop costs well under a millisecond against that. It is done anyway so there is
/// one code path rather than two, and so the head is as replayable as the history.
async fn follow_head(
    pool: &PgPool,
    config: &ChainConfig,
    fetcher: &mut Fetcher<'_>,
    stop_at: Option<u64>,
) -> Result<()> {
    loop {
        let mut blocks = match fetcher.chain.client.stream_blocks().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::warn!(chain = %config.id, error = %e, "failed to open block stream, retrying");
                tokio::time::sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        tracing::info!(chain = %config.id, "fetch: following finalized head");

        while let Some(block) = blocks.next().await {
            let block: subxt::client::Block<PolkadotConfig> = match block {
                Ok(block) => block,
                Err(e) => {
                    tracing::warn!(chain = %config.id, error = %e, "block stream error, reconnecting");
                    break;
                }
            };

            let number = block.number();

            // Re-derived from the **fetch** watermark, not the digest's. Reading the digest's
            // would make the fetcher re-fetch everything the digest has not caught up on yet,
            // which is the entire backlog on a busy chain.
            let expected = match repo::load_watermarks(pool, &config.id).await? {
                Some(marks) => (marks.fetch + 1) as u64,
                None => number,
            };

            // Already archived — a reconnect can replay the block we stopped on.
            if number < expected {
                continue;
            }

            // The stream guarantees it delivers finalized blocks, not that it never skips one
            // under load. Filling the range keeps the archive gap-free even if it jumps.
            for n in expected..number {
                fetcher.archive(n).await?;
            }

            // Archived from the reference the stream handed us rather than by number: the
            // hash is already known and pinned, which saves a lookup.
            let at = decode::at_streamed_block(&block).await?;
            fetcher.archive_at(&at).await?;
            fetcher.publish(number).await?;
            tracing::debug!(chain = %config.id, block = number, "fetched");

            if let Some(stop) = stop_at
                && number >= stop
            {
                tracing::info!(chain = %config.id, stop, "fetch: reached stop_at, finishing");
                return Ok(());
            }
        }

        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

/// Content hash of a metadata blob, for the `runtime_versions` row.
///
/// Its job is to detect a spec version served two different ways, and to notice bit rot in
/// an artifact that is kept forever — not to resist an adversary.
fn sha2_of(bytes: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes).to_vec()
}
