//! The tiering task: moving digested history off the SSD, without giving up replay.
//!
//! Polkadot is ~28M blocks — on the order of 60–120 GB of zstd-compressed blocks, plus a
//! storage read cache that can exceed them on a state-heavy handler. This stage is what makes
//! that a question about an HDD rather than a question about whether to keep the archive at
//! all: a segment that has been digested, and has sat on the fast disk long enough, is copied
//! to `cold_path` and deleted from `hot_path`. Reads fall through, so it stays replayable.
//!
//! Two rules do all the work here, and both are about ordering rather than about policy:
//!
//! * **Copy, verify, record, then delete.** Never delete-then-record. A crash anywhere in the
//!   sequence leaves two copies of a segment, which costs disk and is fixed by running the
//!   pass again; the reverse order leaves none, which is fixed only by going back to the
//!   network — for history that may be years old and served by nobody.
//! * **Move a prefix, never a set.** Segments are considered in block order and the pass
//!   stops at the first one that is not eligible. That is what makes `archive_watermark`
//!   mean something: everything below it has left the hot tier, rather than "some segments
//!   somewhere have".
//!
//! What is *not* moved is `meta/` — the runtime metadata, a handful of megabytes for a
//! chain's entire history. Losing a spec version's metadata makes every block that ran under
//! it permanently undecodable even though the block bytes are intact, so it is the one part
//! of the archive no retention policy may touch.

use std::time::{Duration, SystemTime};

use pif_core::OnDigest;
use pif_db::{SegmentRecord, repo};
use pif_store::{SegmentSpan, Tierable};
use sqlx::PgPool;

use crate::error::Result;

/// How long between passes when tiering runs alongside the pipeline.
///
/// Retention is measured in hours and days, so there is nothing to gain from looking more
/// often — and a pass that finds nothing still lists two directories per chain.
const PASS_INTERVAL: Duration = Duration::from_secs(60);

/// The cold tier for one chain.
pub struct Tiering<'a> {
    pub pool: &'a PgPool,
    pub chain_id: &'a str,
    /// The block archive and the storage read cache, in that order.
    ///
    /// Both, always. Tiering only the blocks would leave a replay of old history reading its
    /// state from the disk the move was meant to free — and on a handler like `identity`,
    /// the reads are the larger half.
    pub archives: &'a [&'a dyn Tierable],
    pub on_digest: OnDigest,
    pub retention: Duration,
}

/// What one pass moved.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TieringReport {
    pub segments: u64,
    /// Bytes reclaimed from the hot tier.
    pub freed: u64,
    /// Where `archive_watermark` ended up, if the pass moved it.
    pub archive_watermark: Option<i64>,
}

impl TieringReport {
    pub fn moved_nothing(&self) -> bool {
        self.segments == 0
    }
}

impl Tiering<'_> {
    /// Whether this configuration can ever move anything.
    ///
    /// `archive` with no `cold_path` is the default a config that says nothing at all
    /// resolves to, and it means "keep everything" — so it is a quiet no-op rather than a
    /// misconfiguration.
    pub fn enabled(&self) -> bool {
        match self.on_digest {
            OnDigest::Keep => false,
            OnDigest::Delete => true,
            OnDigest::Archive => self.archives.iter().any(|a| a.cold_root().is_some()),
        }
    }

    /// Run passes until the process ends — the loop `pif index` carries alongside the
    /// pipeline.
    ///
    /// Never returns `Ok`: it is raced against fetch and digest, and returning would cancel
    /// them. A failed pass is logged and retried rather than propagated, because the hot tier
    /// filling up is an operational problem and stopping the indexer over it would turn it
    /// into an outage.
    pub async fn run_forever(&self) -> Result<()> {
        if !self.enabled() {
            std::future::pending::<()>().await;
        }

        loop {
            match self.pass().await {
                Ok(report) if !report.moved_nothing() => {
                    tracing::info!(
                        chain = %self.chain_id,
                        segments = report.segments,
                        freed = report.freed,
                        "tiering: moved digested segments off the hot tier"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(
                        chain = %self.chain_id, error = %e,
                        "tiering pass failed; the hot tier will keep growing until it succeeds"
                    );
                }
            }

            tokio::time::sleep(PASS_INTERVAL).await;
        }
    }

    /// Move everything currently eligible, once — `pif archive`.
    pub async fn pass(&self) -> Result<TieringReport> {
        let mut report = TieringReport::default();
        if !self.enabled() {
            return Ok(report);
        }

        // The digest is the bound. A segment it has not finished with is one the pipeline is
        // still reading, and moving it would be moving the floor out from under the stage
        // that is standing on it.
        let Some(marks) = repo::load_watermarks(self.pool, self.chain_id).await? else {
            return Ok(report);
        };
        if marks.digest < 0 {
            return Ok(report);
        }

        let now = SystemTime::now();
        for archive in self.archives {
            let moved = self
                .tier_archive(*archive, marks.digest, now, &mut report)
                .await?;

            // Only the block archive sets the watermark. The read cache is keyed the same way
            // but is sparse — a block whose handlers read no state has no record at all, and
            // a chain with no state-reading handler has no records whatsoever — so its
            // segment numbering answers a different question.
            if archive.kind() == pif_store::layout::BLOCKS
                && let Some(to) = moved
            {
                repo::advance_archive_watermark(self.pool, self.chain_id, to).await?;
                report.archive_watermark = Some(to);
            }
        }

        Ok(report)
    }

    /// Move the eligible prefix of one archive, returning the highest block that left the hot
    /// tier.
    async fn tier_archive(
        &self,
        archive: &dyn Tierable,
        digested: i64,
        now: SystemTime,
        report: &mut TieringReport,
    ) -> Result<Option<i64>> {
        // Nowhere to go. Reached only when one archive has a cold root and the other does
        // not, which config does not allow — but `Tiering` can also be built in code.
        if matches!(self.on_digest, OnDigest::Archive) && archive.cold_root().is_none() {
            return Ok(None);
        }

        let mut highest = None;
        for span in archive.hot_segments(self.chain_id)? {
            // Ascending, and the first ineligible segment ends the pass. Skipping it and
            // taking the next one would leave a hole in the cold tier and make
            // `archive_watermark` a claim about a prefix that is not one.
            if span.to_block as i64 > digested {
                break;
            }
            if !self.retained_long_enough(&span, now) {
                break;
            }

            match self.on_digest {
                OnDigest::Keep => break,

                OnDigest::Archive => {
                    // Verified before anything is recorded, and recorded before the hot copy
                    // is deleted.
                    let tiered = archive.copy_to_cold(self.chain_id, span.index)?;
                    repo::record_segment(
                        self.pool,
                        self.chain_id,
                        &SegmentRecord {
                            from_block: tiered.span.from_block as i64,
                            to_block: tiered.span.to_block as i64,
                            tier: "cold".to_owned(),
                            bytes: tiered.bytes as i64,
                            checksum: tiered.checksum,
                        },
                    )
                    .await?;
                    report.freed += archive.drop_hot(self.chain_id, span.index)?;
                }

                OnDigest::Delete => {
                    // The blocks are gone: no row, rather than a row naming a file nothing
                    // can read. This forfeits replay for the range, which is why it has to
                    // be asked for.
                    report.freed += archive.drop_hot(self.chain_id, span.index)?;
                    repo::forget_segment(self.pool, self.chain_id, span.from_block as i64).await?;
                }
            }

            tracing::debug!(
                chain = %self.chain_id,
                kind = archive.kind(),
                from = span.from_block,
                to = span.to_block,
                "tiering: segment left the hot tier"
            );
            report.segments += 1;
            highest = Some(span.to_block as i64);
        }

        Ok(highest)
    }

    /// Whether a segment has been on the hot tier long enough to move.
    ///
    /// Judged from the file's own modification time rather than from a recorded "sealed at":
    /// the file is the thing being moved, so the one timestamp that cannot disagree with it
    /// is its own. A filesystem that declines to answer is treated as old enough — the digest
    /// bound above has already established the segment is finished with.
    fn retained_long_enough(&self, span: &SegmentSpan, now: SystemTime) -> bool {
        if self.retention.is_zero() {
            return true;
        }
        let Some(modified) = span.modified else {
            return true;
        };
        now.duration_since(modified)
            .map(|age| age >= self.retention)
            .unwrap_or(false)
    }
}
