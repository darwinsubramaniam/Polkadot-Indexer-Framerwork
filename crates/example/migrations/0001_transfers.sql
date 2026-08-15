-- Typed-overlay projections.
--
-- These tables are written by optional, compile-time-typed `EventHandler`s rather than
-- by the dynamic core. The core `events` table already holds every transfer as JSONB;
-- this is a denormalised, strongly-typed view for the queries that matter most.
--
-- A chain with `handlers = []` simply leaves these empty.

CREATE TABLE transfers (
    id            BIGSERIAL PRIMARY KEY,
    chain_id      TEXT    NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    block_number  BIGINT  NOT NULL,
    event_idx     INTEGER NOT NULL,
    extrinsic_idx INTEGER,
    from_address  TEXT    NOT NULL,
    to_address    TEXT    NOT NULL,
    amount        NUMERIC(39,0) NOT NULL,
    -- Makes handler replay idempotent: re-indexing a block is a no-op.
    UNIQUE (chain_id, block_number, event_idx)
);

CREATE INDEX transfers_chain_from_idx ON transfers (chain_id, from_address);
CREATE INDEX transfers_chain_to_idx   ON transfers (chain_id, to_address);
CREATE INDEX transfers_chain_block_idx ON transfers (chain_id, block_number);
