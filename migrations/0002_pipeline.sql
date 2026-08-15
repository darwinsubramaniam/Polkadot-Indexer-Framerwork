-- The data pipeline: fetch and digest as separate stages, with a local block store
-- between them. See proposal/IPD-002(Datapipeline).md.
--
-- Nothing here changes how an existing deployment behaves on its next start. The
-- watermarks are seeded from the cursor it already has (bottom of this file), so a
-- migrated indexer resumes where it stopped rather than re-fetching its history.

-- ---------------------------------------------------------------------------------------
-- What the archive knows about each runtime
-- ---------------------------------------------------------------------------------------
--
-- These are columns on `runtime_versions` rather than a table of their own. That table
-- already exists with exactly this grain -- one row per (chain_id, spec_version) -- and two
-- tables at the same grain would have to agree about which block a runtime started at. The
-- day they disagreed, the archive would be the one that was wrong.
--
-- All four are nullable, and NULL is not missing data: it is the queryable fact that a
-- runtime was indexed but its metadata is not in the archive. That is true of every row
-- written before the store existed, and it is the precise answer to "which ranges can I
-- actually replay?":
--
--   SELECT chain_id, spec_version, first_seen_block
--   FROM runtime_versions
--   WHERE metadata_hash IS NULL;
ALTER TABLE runtime_versions
    -- `OfflineClient::at_block` resolves (spec_version, transaction_version) as a pair and
    -- errors if either is missing. Not needed to decode; needed to build the client that
    -- decodes, which is why a replay cannot reconstruct it from the block alone.
    ADD COLUMN transaction_version  INTEGER,
    -- 14 | 15 | 16. A runtime serves several metadata formats at once and they are not
    -- interchangeable in what they describe, so which one was archived is a fact about the
    -- archive, not about the runtime.
    ADD COLUMN metadata_version     SMALLINT,
    ADD COLUMN metadata_hash        BYTEA,
    ADD COLUMN metadata_archived_at TIMESTAMPTZ;

-- A metadata format subxt cannot read is the one runtime-upgrade failure that needs a human,
-- so make the recorded value impossible to fill in wrongly rather than merely unlikely.
ALTER TABLE runtime_versions
    ADD CONSTRAINT runtime_versions_metadata_version_known
    CHECK (metadata_version IS NULL OR metadata_version BETWEEN 9 AND 32);

-- ---------------------------------------------------------------------------------------
-- Watermarks: what has been fetched, digested, and tiered
-- ---------------------------------------------------------------------------------------
--
-- Replaces the dual meaning of `indexer_state.last_indexed_block`, which meant both
-- "fetched" and "processed" because they were the same event. `indexer_state` is kept and
-- still written, mirroring `digest_watermark`, so pif-api's progress query and any external
-- consumer keep working through the transition.
CREATE TABLE pipeline_watermarks (
    chain_id          TEXT PRIMARY KEY REFERENCES chains(id) ON DELETE CASCADE,
    -- Highest *contiguous* block in the hot store, never the highest present. Parallel
    -- fetch means block 5000 can land before 4000, and a digest that treated "the key
    -- exists" as "ready" would step over the hole and record a gap as success.
    fetch_watermark   BIGINT NOT NULL,
    -- Highest block committed to Postgres. This is what indexing progress means to a
    -- reader; anything else advertises blocks whose rows do not exist yet.
    digest_watermark  BIGINT NOT NULL,
    -- Highest block moved to cold storage.
    archive_watermark BIGINT NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Last line of defence, not the first: the ordering is meant to be maintained by only
    -- ever advancing a watermark from the stage that owns it.
    CONSTRAINT pipeline_watermarks_ordered
        CHECK (archive_watermark <= digest_watermark
           AND digest_watermark  <= fetch_watermark)
);

-- ---------------------------------------------------------------------------------------
-- The fetch work queue: leases, not assignments
-- ---------------------------------------------------------------------------------------
--
-- Claimed with `FOR UPDATE SKIP LOCKED`, which is what makes several workers -- and, later,
-- several processes -- safe against each other with no coordinator. A worker that dies
-- simply lets its lease expire and the chunk returns to the pool; an assignment table would
-- need someone to notice the death and reassign.
CREATE TABLE fetch_chunks (
    chain_id         TEXT   NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    from_block       BIGINT NOT NULL,
    to_block         BIGINT NOT NULL,
    state            TEXT   NOT NULL DEFAULT 'pending',
    -- Endpoint url. Diagnostic only: the lease is held by whoever holds the row, not by a
    -- named worker, so recovery never depends on this being accurate.
    leased_by        TEXT,
    lease_expires_at TIMESTAMPTZ,
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,

    PRIMARY KEY (chain_id, from_block),
    CONSTRAINT fetch_chunks_state_known
        CHECK (state IN ('pending', 'leased', 'done', 'failed')),
    CONSTRAINT fetch_chunks_range_ordered CHECK (to_block >= from_block),
    -- A leased chunk without an expiry can never be reclaimed, which is the one way this
    -- design loses work permanently.
    CONSTRAINT fetch_chunks_lease_has_expiry
        CHECK (state <> 'leased' OR lease_expires_at IS NOT NULL)
);

-- Partial: the scheduler only ever asks for claimable work, and on a backfilled chain the
-- 'done' rows outnumber the rest by orders of magnitude.
CREATE INDEX fetch_chunks_claimable_idx ON fetch_chunks (chain_id, from_block)
    WHERE state = 'pending';

-- Reclaiming expired leases is a sweep over a handful of rows, not over history.
CREATE INDEX fetch_chunks_expiry_idx ON fetch_chunks (lease_expires_at)
    WHERE state = 'leased';

-- ---------------------------------------------------------------------------------------
-- Segments: which tier a stretch of blocks is on, and whether it is intact
-- ---------------------------------------------------------------------------------------
--
-- Deliberately NOT a path lookup. Segments are fixed-size and aligned, so a block's file is
-- computed (`number / segment_size`) against the configured hot or cold root. A location
-- that is derived cannot drift out of sync with a table recording where it was supposed to
-- be -- which is a class of bug this table would otherwise introduce.
CREATE TABLE segments (
    chain_id   TEXT   NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    from_block BIGINT NOT NULL,
    to_block   BIGINT NOT NULL,
    tier       TEXT   NOT NULL DEFAULT 'hot',
    bytes      BIGINT NOT NULL,
    -- Verified on read and before any tiering delete. Tiering is copy -> fsync -> verify ->
    -- delete, never delete-then-record, so a crash mid-move leaves two copies rather than
    -- none.
    checksum   BYTEA  NOT NULL,
    sealed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (chain_id, from_block),
    CONSTRAINT segments_tier_known CHECK (tier IN ('hot', 'cold')),
    CONSTRAINT segments_range_ordered CHECK (to_block >= from_block)
);

-- The tiering task's only question: what is still hot and old enough to move?
CREATE INDEX segments_hot_idx ON segments (chain_id, from_block)
    WHERE tier = 'hot';

-- ---------------------------------------------------------------------------------------
-- Seed the watermarks from the cursor that already exists
-- ---------------------------------------------------------------------------------------
--
-- Without this an existing deployment would start with no watermark row and re-fetch its
-- entire history on the next run.
--
-- `fetch_watermark` is set to the existing cursor even though those blocks are NOT in the
-- hot store -- they were indexed before it existed. That is the intended trade: the
-- alternative is re-downloading everything to fill an archive nobody asked for. The
-- consequence is that `pif replay` cannot reach back before this migration, which is
-- visible rather than silent, since `segments` is empty for those ranges and
-- `runtime_versions.metadata_hash` is NULL for the runtimes that produced them.
INSERT INTO pipeline_watermarks (chain_id, fetch_watermark, digest_watermark, archive_watermark)
SELECT chain_id, last_indexed_block, last_indexed_block, 0
FROM indexer_state
ON CONFLICT (chain_id) DO NOTHING;
