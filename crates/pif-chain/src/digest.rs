//! The digest stage: read archived blocks, decode them, and commit them to Postgres.
//!
//! This half never asks the network for a block. It walks block numbers from
//! `digest_watermark + 1` up to `fetch_watermark`, reads each one out of local segment
//! files, and decodes it against the metadata archived for the runtime that executed it.
//!
//! The bound is what makes the split safe. Parallel fetch means block 5000 can land before
//! 4000, so "does the key exist?" is not "is the range ready?" — a digest asking that
//! question would step straight over a hole and record a gap as success. It asks
//! `n <= fetch_watermark` instead, and only completed, synced work advances that.
//!
//! Blocks are committed a batch at a time — `pipeline.digest_batch` of them share one
//! transaction — because the cost of a block that decodes in microseconds is otherwise
//! dominated by round-trips to Postgres. A batch is never *waited* for: whatever is ready is
//! what commits, so the batch size can never hold the fetch stage against the brake.
//!
//! Handlers that read chain *state* are served from the archive too. A block archive is not
//! a state archive — `pallet_identity` emits `IdentitySet { who }` with no display name — so
//! those reads are archived separately, keyed by the block they were made at. On the first
//! digest a read misses and goes to the node; on every later one it hits, and a replay needs
//! no network at all.

use std::time::Duration;

use pif_core::ChainInfo;
use pif_db::{BlockData, repo};
use pif_store::{HotStore, StorageCache};
use sqlx::PgPool;

use crate::archive::Archive;
use crate::cache::CachedStorage;
use crate::client::ChainClient;
use crate::error::{ChainError, Result};
use crate::handlers::{BlockContext, Selected};

/// How long to wait before re-reading `fetch_watermark` when the digest has caught up.
///
/// Only ever paid at the head, where blocks arrive every few seconds anyway. During a
/// backfill the digest is never idle, so this costs nothing that matters.
const IDLE_POLL: Duration = Duration::from_millis(250);

/// Everything the digest needs that does not change from block to block.
pub struct Digest<'a> {
    pub pool: &'a PgPool,
    pub chain: &'a ChainInfo,
    pub store: &'a HotStore,
    /// Archived answers to handler storage reads, keyed by the block they were made at.
    pub reads: &'a StorageCache,
    pub handlers: &'a Selected<'a>,
    /// What the digest may reach for on a storage read the archive cannot answer.
    ///
    /// `None` means there is no node — a `pif replay`, or a `pif digest` against a chain
    /// with no reachable endpoint. Decoding still works; an unarchived storage read does
    /// not, and says so by name rather than reaching for the network behind your back.
    pub live: Option<&'a ChainClient>,
    pub spec_name: &'a str,
    /// Blocks per transaction. `1` commits each block on its own, as this stage used to.
    pub batch: u64,
}

impl Digest<'_> {
    /// Blocks per transaction, never zero.
    ///
    /// Config rejects `0`, but a `Digest` can also be built in code; `0` there would be a
    /// loop that commits nothing and reads exactly like a hang.
    fn batch_size(&self) -> u64 {
        self.batch.max(1)
    }

    /// Digest archived blocks from `start` until `stop_at`, or forever.
    pub async fn run(&self, start: u64, stop_at: Option<u64>) -> Result<()> {
        let mut archive = Archive::new(self.store, self.chain);
        let mut next = start;
        // Only set once a block has been digested in *this* run, so the linkage check
        // compares consecutive blocks it actually saw rather than trusting a stored row.
        let mut previous_hash: Option<Vec<u8>> = None;

        tracing::info!(chain = %self.chain.id, start, batch = self.batch_size(), "digest: starting");

        loop {
            if stop_at.is_some_and(|stop| next > stop) {
                return Ok(());
            }

            // The bound that keeps the split honest. Key existence is not readiness.
            let ready = match repo::load_watermarks(self.pool, &self.chain.id).await? {
                Some(marks) => marks.fetch,
                None => -1,
            };
            let last = match stop_at {
                Some(stop) => ready.min(stop as i64),
                None => ready,
            };

            while (next as i64) <= last {
                // Bounded by what is *ready*, never by what would fill a batch. A digest
                // that held blocks back until it had `batch` of them would bring back §6.3's
                // deadlock in a new costume: with `max_digest_lag` below the batch size the
                // fetcher stops at the brake, waiting for a digest that is itself waiting
                // for blocks the fetcher has just stopped producing.
                let end = (next + self.batch_size() - 1).min(last as u64);

                previous_hash = Some(
                    self.commit(&mut archive, next, end, previous_hash.as_deref())
                        .await?,
                );
                self.log_batch(next, end, "digested");
                next = end + 1;

                if stop_at.is_some_and(|stop| end >= stop) {
                    tracing::info!(
                        chain = %self.chain.id, stop = end, "digest: reached stop_at, finishing"
                    );
                    return Ok(());
                }
            }

            // Caught up. Flushing here rather than per block because a lost cache entry
            // costs one refetch on the next pass, never a hole — so paying an fsync per
            // block would buy nothing the watermarks do not already guarantee.
            self.reads.sync()?;
            tokio::time::sleep(IDLE_POLL).await;
        }
    }

    /// Digest exactly the range `from..=to`, and nothing beyond it.
    ///
    /// The re-index path. Unlike [`Digest::run`] it does not wait for more work to appear: a
    /// replay is over a range that already exists, and hanging at the end of it would look
    /// like a hang rather than a finish.
    pub async fn replay_range(&self, from: u64, to: u64) -> Result<()> {
        let mut archive = Archive::new(self.store, self.chain);
        let mut previous_hash: Option<Vec<u8>> = None;
        let mut next = from;

        while next <= to {
            let end = (next + self.batch_size() - 1).min(to);
            previous_hash = Some(
                self.commit(&mut archive, next, end, previous_hash.as_deref())
                    .await?,
            );
            self.log_batch(next, end, "replayed");
            next = end + 1;
        }

        self.reads.sync()?;
        tracing::info!(chain = %self.chain.id, from, to, "replay complete");
        Ok(())
    }

    /// Report a committed batch.
    ///
    /// One line per batch would be noise on a long run, so info is kept to batches spanning a
    /// hundred-block boundary — the cadence the per-block digest logged at, independent of
    /// how the blocks happen to be grouped.
    fn log_batch(&self, from: u64, to: u64, what: &str) {
        if (from..=to).any(|n| n.is_multiple_of(100)) {
            tracing::info!(chain = %self.chain.id, from, to, "{what}");
        } else {
            tracing::debug!(chain = %self.chain.id, from, to, "{what}");
        }
    }

    /// Decode `from..=to` out of the archive and commit them as one transaction, together
    /// with whatever their handlers derive.
    ///
    /// Returns the last block's hash, so the caller can check the next batch links to it.
    ///
    /// Every block's core rows, every handler's rows, the cursor and the digest watermark
    /// share a single transaction. A handler failure therefore rolls the whole batch back —
    /// not merely its own block — and the watermark still cannot be ahead of the data it
    /// claims to describe. The atomicity the resume cursor rests on is unchanged; only its
    /// grain is, and `digest_batch = 1` restores the old grain exactly.
    async fn commit(
        &self,
        archive: &mut Archive<'_>,
        from: u64,
        to: u64,
        previous_hash: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let chain = self.chain;

        // Decoded before the transaction opens, for two reasons. Decoding needs no database
        // at all, so doing it inside would hold locks across pure CPU work; and the linkage
        // check has to be settled before anything is written, so a spliced batch never
        // becomes rows to be rolled back.
        let mut blocks: Vec<BlockData> = Vec::with_capacity((to - from + 1) as usize);
        let mut spec_versions: Vec<u32> = Vec::with_capacity(blocks.capacity());
        let mut link: Option<Vec<u8>> = previous_hash.map(<[u8]>::to_vec);

        for number in from..=to {
            let raw = archive.get(number)?;
            let mut data = archive.decode(&raw).await?;
            data.spec_name = self.spec_name.to_owned();

            // Nothing verified this before the split, because one endpoint fetched every
            // block in order and there was nothing to disagree with. It matters most on the
            // replay path: `OfflineClient` performs no genesis check — it stores the genesis
            // hash unchecked, and a replay leaves it unset — so this is the only thing
            // standing between a replay and another chain's segments.
            if let Some(expected) = &link
                && expected.as_slice() != data.block.parent_hash.as_slice()
            {
                return Err(ChainError::ChainLinkageBroken {
                    chain: chain.id.clone(),
                    number,
                    expected: hex::encode(expected),
                    found: hex::encode(&data.block.parent_hash),
                });
            }

            link = Some(data.block.hash.clone());
            spec_versions.push(raw.spec_version);
            blocks.push(data);
        }

        // State at each block, for handlers whose pallet reports *that* something changed
        // without reporting *what* it changed to. Answered from the archive when it has been
        // read there before; only a genuine miss touches the network, and only if there is a
        // node to touch.
        //
        // Built after the decode loop rather than inside it because the archive is borrowed
        // mutably there, and each view needs the metadata that decoded its block.
        let storages: Vec<CachedStorage<'_>> = (from..=to)
            .zip(&spec_versions)
            .map(|(number, spec_version)| {
                CachedStorage::new(
                    self.reads,
                    chain,
                    number,
                    self.live.map(|client| &client.client),
                    archive.metadata_for(*spec_version),
                )
            })
            .collect();

        let mut tx = self.pool.begin().await?;
        repo::write_blocks_in_tx(&mut tx, &blocks).await?;

        for ((number, data), storage) in (from..=to).zip(&blocks).zip(&storages) {
            let ctx = BlockContext {
                chain,
                block_number: number,
                block_hash: &data.block.hash,
                storage,
            };
            self.handlers.run(&ctx, data, &mut tx).await?;
        }

        let last = blocks
            .last()
            .expect("from <= to, so a batch is never empty");
        // Kept in step with `digest_watermark` so pif-api's progress query and any external
        // consumer keep working through the transition. `pipeline_watermarks` is the
        // authority.
        repo::update_cursor(&mut tx, &chain.id, last.block.number, &last.block.hash).await?;
        repo::advance_digest_watermark(&mut tx, &chain.id, last.block.number).await?;
        tx.commit().await?;

        // After the commit, not before: the archive should record reads belonging to blocks
        // that actually landed. A block whose reads all hit writes nothing at all, so a warm
        // re-digest does not churn the cache.
        for storage in &storages {
            storage.persist()?;
        }

        Ok(last.block.hash.clone())
    }
}
