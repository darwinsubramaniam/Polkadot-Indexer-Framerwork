-- Core, chain-agnostic schema.
--
-- Every table is keyed by `chain_id` because one indexer process serves N chains
-- from config. `chains.id` is the human-facing config key; `chains.genesis_hash` is
-- the real cryptographic identity, so pointing a config entry at the wrong node is
-- detectable rather than silently corrupting data.

CREATE TABLE chains (
    id             TEXT PRIMARY KEY,
    -- NOT unique on purpose. Indexing one physical chain under two ids is legitimate
    -- (re-indexing under a new id, or isolating test runs). The property we actually
    -- want -- "this id must keep pointing at the same chain" -- is enforced by
    -- `guard_chain_identity` in the pipeline, which compares the stored genesis hash
    -- against the node's for that id and refuses to continue on a mismatch.
    genesis_hash   BYTEA NOT NULL,
    name           TEXT  NOT NULL,
    token_symbol   TEXT,
    token_decimals SMALLINT,
    ss58_prefix    INTEGER,
    first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per distinct runtime seen on a chain. Lets us tell, after the fact, which
-- blocks were decoded under which metadata.
CREATE TABLE runtime_versions (
    chain_id         TEXT    NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    spec_version     INTEGER NOT NULL,
    spec_name        TEXT    NOT NULL,
    first_seen_block BIGINT  NOT NULL,
    PRIMARY KEY (chain_id, spec_version)
);

CREATE TABLE blocks (
    chain_id        TEXT    NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    number          BIGINT  NOT NULL,
    hash            BYTEA   NOT NULL,
    parent_hash     BYTEA   NOT NULL,
    state_root      BYTEA   NOT NULL,
    extrinsics_root BYTEA   NOT NULL,
    spec_version    INTEGER NOT NULL,
    timestamp       TIMESTAMPTZ,
    extrinsic_count INTEGER NOT NULL,
    event_count     INTEGER NOT NULL,
    indexed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, number),
    UNIQUE (chain_id, hash)
);

CREATE TABLE extrinsics (
    chain_id     TEXT    NOT NULL,
    block_number BIGINT  NOT NULL,
    idx          INTEGER NOT NULL,
    hash         BYTEA   NOT NULL,
    pallet       TEXT    NOT NULL,
    call         TEXT    NOT NULL,
    signer       TEXT,
    is_signed    BOOLEAN NOT NULL,
    success      BOOLEAN NOT NULL,
    -- u128 does not fit any Postgres integer type; NUMERIC(39,0) covers 2^128-1.
    fee          NUMERIC(39,0),
    -- Dynamically decoded call arguments. u128 values are encoded as JSON *strings*
    -- to survive the 2^53 precision limit of JSON numbers.
    args         JSONB   NOT NULL,
    PRIMARY KEY (chain_id, block_number, idx),
    FOREIGN KEY (chain_id, block_number)
        REFERENCES blocks(chain_id, number) ON DELETE CASCADE
);

CREATE TABLE events (
    chain_id      TEXT    NOT NULL,
    block_number  BIGINT  NOT NULL,
    idx           INTEGER NOT NULL,
    pallet        TEXT    NOT NULL,
    variant       TEXT    NOT NULL,
    phase         TEXT    NOT NULL,
    extrinsic_idx INTEGER,
    fields        JSONB   NOT NULL,
    PRIMARY KEY (chain_id, block_number, idx),
    FOREIGN KEY (chain_id, block_number)
        REFERENCES blocks(chain_id, number) ON DELETE CASCADE
);

-- The resume cursor. Updated inside the same transaction that writes a block's rows,
-- so it can never run ahead of the data it points at.
CREATE TABLE indexer_state (
    chain_id           TEXT PRIMARY KEY REFERENCES chains(id) ON DELETE CASCADE,
    last_indexed_block BIGINT NOT NULL,
    last_indexed_hash  BYTEA  NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX chains_genesis_hash_idx         ON chains (genesis_hash);
CREATE INDEX blocks_chain_timestamp_idx      ON blocks (chain_id, timestamp);
CREATE INDEX extrinsics_chain_call_idx       ON extrinsics (chain_id, pallet, call);
CREATE INDEX extrinsics_chain_signer_idx     ON extrinsics (chain_id, signer)
    WHERE signer IS NOT NULL;
CREATE INDEX events_chain_variant_idx        ON events (chain_id, pallet, variant);
CREATE INDEX events_chain_extrinsic_idx      ON events (chain_id, block_number, extrinsic_idx);

-- Without these, any JSONB predicate degrades to a sequential scan of the whole table.
CREATE INDEX extrinsics_args_gin_idx  ON extrinsics USING GIN (args);
CREATE INDEX events_fields_gin_idx    ON events     USING GIN (fields);
