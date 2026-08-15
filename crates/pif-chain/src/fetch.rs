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
//!
//! Backfill is parallel across endpoints, through a queue of **leases** rather than
//! assignments. A worker takes the lowest claimable chunk with `FOR UPDATE SKIP LOCKED`,
//! archives it, and marks it done; a worker that dies lets its lease expire and the chunk
//! returns to the pool with nobody having to notice the death. Losing an endpoint therefore
//! costs throughput and never correctness — and never a hole.

use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;

use parity_scale_codec::Encode;
use pif_core::{ChainConfig, ChainInfo};
use pif_db::{ArchivedRuntime, repo};
use pif_store::{HotStore, RawBlock};
use sqlx::PgPool;
use subxt::PolkadotConfig;

use crate::client::{ChainClient, EndpointPool, PooledEndpoint};
use crate::decode::{self, AtBlock};
use crate::error::{ChainError, Result};
use crate::metadata;

/// How long to wait before reconnecting after the block stream drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// How long to wait before re-asking for work when there is none to be had.
const IDLE_POLL: Duration = Duration::from_millis(250);

/// How long a worker holds a chunk before it is fair game again.
///
/// Long enough that a slow endpoint is not robbed mid-chunk, short enough that a crashed
/// process does not park work for minutes.
const LEASE_SECONDS: i32 = 120;

/// Attempts before a chunk is declared failed rather than offered round again.
///
/// A chunk that has come back this many times has been tried by every healthy endpoint in
/// the pool. Offering it again would spin the workers instead of stopping with something a
/// human can read.
const MAX_CHUNK_ATTEMPTS: i32 = 5;

/// No brake at all — an archive endpoint serves state at any depth, so there is nothing for
/// the fetcher to outrun.
pub const UNBOUNDED_LAG: u64 = u64::MAX;

/// Everything the fetch stage needs that does not change from block to block.
pub struct Fetch<'a> {
    pub pool: &'a PgPool,
    pub config: &'a ChainConfig,
    pub endpoints: &'a EndpointPool,
    pub store: &'a HotStore,
    pub spec_name: &'a str,
    /// How far ahead of the digest this may get, in blocks.
    ///
    /// Not a throughput knob. The storage read cache is filled on the *first* digest of a
    /// block, from a node, so a fetcher far ahead means every one of those reads asks for
    /// state the node discarded long ago. The brake makes that structurally impossible
    /// rather than merely unlikely, and it is the same thing that stops the archive growing
    /// without bound when the digest stalls.
    pub max_lag: u64,
    /// Blocks per lease.
    ///
    /// Clamped to `max_lag` by [`Fetch::effective_chunk_size`]: the brake bounds a chunk's
    /// *end*, so a chunk larger than the window would never be claimable and the two stages
    /// would deadlock waiting for each other.
    pub chunk_size: u64,
}

/// State shared by every worker on one chain.
struct Shared {
    /// Runtimes already archived in this run. Shared so that two workers meeting a new
    /// runtime at once do not both download 400 KB of metadata for it.
    seen_runtimes: Mutex<HashSet<u32>>,
    /// Whether the brake has already been reported. Engaging it is normal; saying so four
    /// times a second is not.
    braked: Mutex<bool>,
}

impl Fetch<'_> {
    fn chain(&self) -> &ChainInfo {
        self.endpoints.info()
    }

    /// Fetch blocks for one chain: catch up to the finalized head, then follow it.
    pub async fn run(&self, start: u64, stop_at: Option<u64>) -> Result<()> {
        let target = self
            .endpoints
            .head_follower()
            .client
            .finalized_number()
            .await?;
        let catch_up_end = stop_at.map(|s| s.min(target)).unwrap_or(target);

        tracing::info!(
            chain = %self.config.id,
            start,
            finalized_head = target,
            endpoints = self.endpoints.len(),
            "fetch: starting catch-up"
        );

        if catch_up_end >= start {
            self.backfill(start, catch_up_end).await?;
        }

        if let Some(stop) = stop_at
            && catch_up_end >= stop
        {
            tracing::info!(chain = %self.config.id, stop, "fetch: reached stop_at, finishing");
            return Ok(());
        }

        self.follow_head(stop_at).await
    }

    /// Archive `start..=end` across every healthy endpoint at once.
    async fn backfill(&self, start: u64, end: u64) -> Result<()> {
        let chain_id = &self.chain().id;

        // Anything a previous run left leased is fair game again before we count the work.
        repo::reclaim_expired_chunks(self.pool, chain_id).await?;
        let seeded = repo::seed_chunks(
            self.pool,
            chain_id,
            start as i64,
            end as i64,
            self.effective_chunk_size() as i64,
        )
        .await?;
        tracing::info!(
            chain = %self.config.id,
            seeded,
            chunk_size = self.effective_chunk_size(),
            "fetch: queued chunks"
        );

        let shared = Shared {
            seen_runtimes: Mutex::new(HashSet::new()),
            braked: Mutex::new(false),
        };

        // One worker per endpoint. More would only queue behind the same limiters, since the
        // limiter is what actually paces each node.
        let workers = self
            .endpoints
            .endpoints()
            .iter()
            .map(|endpoint| self.worker(endpoint, &shared, end));

        futures::future::try_join_all(workers).await?;

        let (_, _, failed) = repo::chunk_counts(self.pool, chain_id).await?;
        if failed > 0 {
            return Err(ChainError::ChunksFailed {
                chain: chain_id.clone(),
                count: failed as u64,
            });
        }

        Ok(())
    }

    /// One endpoint's share of the backfill: claim, archive, complete, repeat.
    async fn worker(&self, endpoint: &PooledEndpoint, shared: &Shared, end: u64) -> Result<()> {
        let chain_id = &self.chain().id;

        loop {
            // Asked before claiming, not after: a chunk claimed by an endpoint whose breaker
            // is open would sit leased until it expired, which is exactly the stall having
            // several endpoints is meant to avoid.
            if !endpoint.acquire().await {
                tokio::time::sleep(IDLE_POLL).await;
                continue;
            }

            let ceiling = self.brake_ceiling().await?;
            let Some(chunk) =
                repo::claim_chunk(self.pool, chain_id, &endpoint.url, LEASE_SECONDS, ceiling)
                    .await?
            else {
                // Nothing claimable. That is either "all done", "another worker holds the
                // rest", or "the brake is holding everything back" — and only the first
                // means this worker can stop.
                let (pending, leased, _) = repo::chunk_counts(self.pool, chain_id).await?;
                if pending == 0 && leased == 0 {
                    return Ok(());
                }
                if pending > 0 {
                    self.report_brake(shared, pending, ceiling);
                }
                repo::reclaim_expired_chunks(self.pool, chain_id).await?;
                tokio::time::sleep(IDLE_POLL).await;
                continue;
            };

            match self.archive_chunk(endpoint, shared, &chunk, end).await {
                Ok(()) => {
                    endpoint.succeeded();
                    // Durable first, recorded second: the reverse would let a crash leave
                    // `fetch_watermark` pointing at blocks the disk never received.
                    self.store.sync()?;
                    repo::complete_chunk(self.pool, chain_id, chunk.from_block).await?;
                    self.advance_watermark().await?;

                    tracing::debug!(
                        chain = %self.config.id,
                        endpoint = %endpoint.url,
                        from = chunk.from_block,
                        to = chunk.to_block,
                        rate = endpoint.rate(),
                        "fetched chunk"
                    );
                }

                Err(e) => {
                    endpoint.failed();

                    // Blocks already written stay written — every insert and every archive
                    // write is idempotent, so the retry simply overwrites them.
                    if chunk.attempts >= MAX_CHUNK_ATTEMPTS {
                        tracing::error!(
                            chain = %self.config.id, from = chunk.from_block, attempts = chunk.attempts,
                            error = %e, "chunk failed on every attempt; giving up on it"
                        );
                        repo::fail_chunk(self.pool, chain_id, chunk.from_block, &e.to_string())
                            .await?;
                    } else {
                        tracing::warn!(
                            chain = %self.config.id,
                            endpoint = %endpoint.url,
                            from = chunk.from_block,
                            attempts = chunk.attempts,
                            error = %e,
                            "chunk returned to the queue for another endpoint"
                        );
                        repo::release_chunk(self.pool, chain_id, chunk.from_block, &e.to_string())
                            .await?;
                    }
                }
            }
        }
    }

    async fn archive_chunk(
        &self,
        endpoint: &PooledEndpoint,
        shared: &Shared,
        chunk: &repo::Chunk,
        end: u64,
    ) -> Result<()> {
        let last = (chunk.to_block as u64).min(end);

        for number in chunk.from_block as u64..=last {
            // One token per block. A block costs a handful of RPC calls, so the effective
            // call rate is a small multiple of `max_rps` — the useful unit to pace on is the
            // one the work is measured in.
            if !endpoint.acquire().await {
                return Err(ChainError::EndpointUnavailable {
                    endpoint: endpoint.url.clone(),
                });
            }

            let at = decode::at_block(&endpoint.client.client, self.chain(), number).await?;
            let raw = self.capture(endpoint, shared, &at).await?;
            self.store.put_block(&self.chain().id, &raw)?;
        }

        Ok(())
    }

    /// Move `fetch_watermark` to the end of the unbroken run of completed chunks.
    ///
    /// Never to the highest *completed* chunk: chunk 12800 can finish before 12700, and a
    /// digest that treated the higher number as ready would step straight over the hole.
    async fn advance_watermark(&self) -> Result<()> {
        if let Some(through) = repo::contiguous_done_end(self.pool, &self.chain().id).await? {
            repo::advance_fetch_watermark(self.pool, &self.chain().id, through).await?;
        }
        Ok(())
    }

    /// Blocks per lease, never more than the brake will let through in one go.
    fn effective_chunk_size(&self) -> u64 {
        if self.max_lag == UNBOUNDED_LAG {
            return self.chunk_size.max(1);
        }
        self.chunk_size.min(self.max_lag).max(1)
    }

    /// The highest block a chunk may *end* at right now, or `None` for no brake.
    async fn brake_ceiling(&self) -> Result<Option<i64>> {
        if self.max_lag == UNBOUNDED_LAG {
            return Ok(None);
        }

        let digested = match repo::load_watermarks(self.pool, &self.chain().id).await? {
            Some(marks) => marks.digest,
            None => -1,
        };

        Ok(Some(digested.saturating_add(self.max_lag as i64)))
    }

    /// Say once, and only once, that work exists but the brake is holding it.
    ///
    /// A `pif fetch` run with nothing digesting stalls here *visibly*, which is the honest
    /// outcome: filling a disk with blocks whose state can never be read is worse than
    /// stopping. Saying so four times a second is not.
    fn report_brake(&self, shared: &Shared, pending: i64, ceiling: Option<i64>) {
        let Some(ceiling) = ceiling else { return };

        let mut braked = shared.braked.lock().expect("brake flag poisoned");
        if *braked {
            return;
        }
        *braked = true;

        tracing::warn!(
            chain = %self.config.id,
            pending_chunks = pending,
            ceiling,
            max_digest_lag = self.max_lag,
            "fetch: holding for the digest. Archiving further ahead would leave the digest \
             asking these endpoints for state they have already pruned. If nothing is \
             digesting this chain, start `pif digest` or raise pipeline.max_digest_lag."
        );
    }

    /// Build the archive record for one block.
    async fn capture(
        &self,
        endpoint: &PooledEndpoint,
        shared: &Shared,
        at: &AtBlock,
    ) -> Result<RawBlock> {
        let number = at.block_number();
        let client = &endpoint.client;

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
            decode::executing_runtime(&client.client, self.chain(), at, header.parent_hash).await?;
        let spec_version = runtime.spec_version();
        let transaction_version = runtime.transaction_version();

        self.ensure_metadata(
            client,
            shared,
            spec_version,
            transaction_version,
            &runtime,
            number,
        )
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

        let extrinsics = self.capture_extrinsics(client, at, &runtime).await?;

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
    async fn capture_extrinsics(
        &self,
        client: &ChainClient,
        at: &AtBlock,
        runtime: &AtBlock,
    ) -> Result<Vec<Vec<u8>>> {
        let number = at.block_number();

        if runtime.spec_version() != at.spec_version() {
            let body = client
                .rpc
                .chain_get_block(Some(at.block_hash()))
                .await
                .map_err(|source| ChainError::BlockRead {
                    number,
                    source: Box::new(source),
                })?
                .ok_or(ChainError::UpgradeBlockBodyUnavailable {
                    chain: self.chain().id.clone(),
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
        &self,
        client: &ChainClient,
        shared: &Shared,
        spec_version: u32,
        transaction_version: u32,
        runtime: &AtBlock,
        first_block: u64,
    ) -> Result<()> {
        {
            let mut seen = shared.seen_runtimes.lock().expect("runtime set poisoned");
            if !seen.insert(spec_version) {
                return Ok(());
            }
            // Under parallel fetch there is no "the previous block" to compare against, so
            // an upgrade is reported as what it observably is here: the first block seen
            // running a runtime this process had not met before. Routine either way — the
            // dynamic core is compiled against no runtime at all — so `info` and no more.
            if seen.len() > 1 {
                tracing::info!(
                    chain = %self.config.id, block = first_block, spec_version, "runtime upgrade"
                );
            }
        }

        let chain_id = self.chain().id.clone();
        if self.store.get_metadata(&chain_id, spec_version)?.is_some() {
            return Ok(());
        }

        let fetched =
            metadata::fetch_at(&client.rpc, &chain_id, spec_version, runtime.block_hash()).await?;

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

    /// Follow the finalized head, archiving each block as it is finalized.
    ///
    /// **One endpoint, not the pool.** A finality subscription is per connection and blocks
    /// arrive one every few seconds, so there is no parallelism to be had here and several
    /// subscriptions would only mean several copies of the same block. The archive hop costs
    /// well under a millisecond against a six-second block time; it is paid so that there is
    /// one code path rather than two, and so the head is as replayable as the history.
    async fn follow_head(&self, stop_at: Option<u64>) -> Result<()> {
        let endpoint = self.endpoints.head_follower();
        let shared = Shared {
            seen_runtimes: Mutex::new(HashSet::new()),
            braked: Mutex::new(false),
        };
        let chain_id = &self.chain().id;

        loop {
            let mut blocks = match endpoint.client.client.stream_blocks().await {
                Ok(stream) => stream,
                Err(e) => {
                    tracing::warn!(chain = %self.config.id, error = %e, "failed to open block stream, retrying");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    continue;
                }
            };

            tracing::info!(
                chain = %self.config.id, endpoint = %endpoint.url, "fetch: following finalized head"
            );

            while let Some(block) = blocks.next().await {
                let block: subxt::client::Block<PolkadotConfig> = match block {
                    Ok(block) => block,
                    Err(e) => {
                        tracing::warn!(chain = %self.config.id, error = %e, "block stream error, reconnecting");
                        break;
                    }
                };

                let number = block.number();

                // Re-derived from the **fetch** watermark, not the digest's. Reading the
                // digest's would make the fetcher re-fetch everything the digest has not
                // caught up on yet, which is the entire backlog on a busy chain.
                let expected = match repo::load_watermarks(self.pool, chain_id).await? {
                    Some(marks) => (marks.fetch + 1) as u64,
                    None => number,
                };

                // Already archived — a reconnect can replay the block we stopped on.
                if number < expected {
                    continue;
                }

                // The stream guarantees it delivers finalized blocks, not that it never skips
                // one under load. Filling the range keeps the archive gap-free even if it
                // jumps, and it goes through the same queue so a gap is fetched in parallel
                // like any other backfill.
                if expected < number {
                    self.backfill(expected, number - 1).await?;
                }

                // Archived from the reference the stream handed us rather than by number:
                // the hash is already known and pinned, which saves a lookup.
                let at = decode::at_streamed_block(&block).await?;
                let raw = self.capture(endpoint, &shared, &at).await?;
                self.store.put_block(chain_id, &raw)?;
                self.store.sync()?;
                repo::advance_fetch_watermark(self.pool, chain_id, number as i64).await?;
                endpoint.succeeded();

                tracing::debug!(chain = %self.config.id, block = number, "fetched");

                if let Some(stop) = stop_at
                    && number >= stop
                {
                    tracing::info!(chain = %self.config.id, stop, "fetch: reached stop_at, finishing");
                    return Ok(());
                }
            }

            tokio::time::sleep(RECONNECT_DELAY).await;
        }
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
