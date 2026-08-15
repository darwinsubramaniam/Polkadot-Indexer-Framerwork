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
//! Handlers that read chain *state* still reach the network in this phase: state is not in
//! the block, so archiving blocks does not archive it. That is the one thing standing
//! between here and a fully offline replay.

use std::time::Duration;

use pif_core::ChainInfo;
use pif_db::repo;
use pif_store::HotStore;
use sqlx::PgPool;

use crate::archive::Archive;
use crate::client::ChainClient;
use crate::decode;
use crate::error::{ChainError, Result};
use crate::handlers::{BlockContext, Selected};
use crate::storage::{OfflineStorage, StorageAt, SubxtStorage};

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
    pub handlers: &'a Selected<'a>,
    /// What the digest may reach for when a handler reads chain state.
    ///
    /// `None` means there is no node — a `pif replay`, or a `pif digest` against a chain
    /// with no reachable endpoint. Decoding still works; a handler's storage read does not,
    /// and says so.
    pub live: Option<&'a ChainClient>,
    pub spec_name: &'a str,
}

impl Digest<'_> {
    /// Digest archived blocks from `start` until `stop_at`, or forever.
    pub async fn run(&self, start: u64, stop_at: Option<u64>) -> Result<()> {
        let mut archive = Archive::new(self.store, self.chain);
        let mut next = start;
        // Only set once a block has been digested in *this* run, so the linkage check
        // compares consecutive blocks it actually saw rather than trusting a stored row.
        let mut previous_hash: Option<Vec<u8>> = None;

        tracing::info!(chain = %self.chain.id, start, "digest: starting");

        loop {
            if stop_at.is_some_and(|stop| next > stop) {
                return Ok(());
            }

            // The bound that keeps the split honest. Key existence is not readiness.
            let ready = match repo::load_watermarks(self.pool, &self.chain.id).await? {
                Some(marks) => marks.fetch,
                None => -1,
            };

            while (next as i64) <= ready {
                if stop_at.is_some_and(|stop| next > stop) {
                    return Ok(());
                }

                let hash = self
                    .one(&mut archive, next, previous_hash.as_deref())
                    .await?;
                previous_hash = Some(hash);

                if next.is_multiple_of(100) {
                    tracing::info!(chain = %self.chain.id, block = next, "digested");
                } else {
                    tracing::debug!(chain = %self.chain.id, block = next, "digested");
                }

                if stop_at.is_some_and(|stop| next >= stop) {
                    tracing::info!(
                        chain = %self.chain.id, stop = next, "digest: reached stop_at, finishing"
                    );
                    return Ok(());
                }

                next += 1;
            }

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

        for number in from..=to {
            let hash = self
                .one(&mut archive, number, previous_hash.as_deref())
                .await?;
            previous_hash = Some(hash);

            if number.is_multiple_of(100) || number == to {
                tracing::info!(chain = %self.chain.id, block = number, "replayed");
            }
        }

        Ok(())
    }

    /// Decode one archived block and commit it, together with whatever its handlers derive.
    ///
    /// Returns the block's hash, so the caller can check the next block links to it.
    ///
    /// The core rows, every handler's rows, the cursor and the digest watermark share a
    /// single transaction. A handler failure therefore rolls the entire block back, and no
    /// watermark can ever be ahead of the data it claims to describe.
    async fn one(
        &self,
        archive: &mut Archive<'_>,
        number: u64,
        previous_hash: Option<&[u8]>,
    ) -> Result<Vec<u8>> {
        let (pool, chain, handlers, live, spec_name) = (
            self.pool,
            self.chain,
            self.handlers,
            self.live,
            self.spec_name,
        );

        let raw = archive.get(number)?;
        let mut data = archive.decode(&raw).await?;
        data.spec_name = spec_name.to_owned();

        // Nothing verified this before the split, because one endpoint fetched every block
        // in order and there was nothing to disagree with. It matters most on the replay
        // path: `OfflineClient` performs no genesis check — it stores the genesis hash
        // unchecked, and a replay leaves it unset — so this is the only thing standing
        // between a replay and another chain's segments.
        if let Some(expected) = previous_hash
            && expected != data.block.parent_hash.as_slice()
        {
            return Err(ChainError::ChainLinkageBroken {
                chain: chain.id.clone(),
                number,
                expected: hex::encode(expected),
                found: hex::encode(&data.block.parent_hash),
            });
        }

        // State at this exact block, for handlers whose pallet reports *that* something
        // changed without reporting *what* it changed to. Resolved only when a handler might
        // ask: with no handlers the digest touches no network at all, which is the property
        // `pif replay` rests on.
        let at = match live {
            Some(client) if !handlers.is_empty() => {
                Some(decode::at_block(&client.client, chain, number).await?)
            }
            _ => None,
        };
        let live_storage = at.as_ref().map(|at| SubxtStorage::new(at, &chain.id));
        let offline_storage = OfflineStorage::new(&chain.id, number);
        let storage: &dyn StorageAt = match &live_storage {
            Some(storage) => storage,
            None => &offline_storage,
        };

        let ctx = BlockContext {
            chain,
            block_number: number,
            block_hash: &data.block.hash,
            storage,
        };

        let mut tx = pool.begin().await?;
        repo::write_block_in_tx(&mut tx, &data).await?;
        handlers.run(&ctx, &data, &mut tx).await?;
        // Kept in step with `digest_watermark` so pif-api's progress query and any external
        // consumer keep working through the transition. `pipeline_watermarks` is the
        // authority.
        repo::update_cursor(&mut tx, &chain.id, data.block.number, &data.block.hash).await?;
        repo::advance_digest_watermark(&mut tx, &chain.id, data.block.number).await?;
        tx.commit().await?;

        Ok(data.block.hash)
    }
}
