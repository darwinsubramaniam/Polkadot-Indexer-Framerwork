-- Identity / alias projection for `pallet_identity`, as deployed on the Polkadot People chain.
--
-- Versions here are LOCAL to this handler. The framework tracks it in its own
-- `_sqlx_migrations_identity` table (see `pif_chain::handlers::run_migrations`), so numbering
-- never has to be coordinated with the framework or with any other handler.
--
-- Why this shape: `pallet_identity` events are notifications, not payloads --
-- `IdentitySet { who }` carries no display name, `JudgementGiven { target, registrar_index }`
-- carries no judgement. The handler therefore learns *which* account changed from the event
-- and re-reads *what* it changed to from storage at that block. These tables store the result
-- of that read, not a replay of event fields.

-- Temporal. One row per (account, change); the current row is the one with
-- `valid_to_block IS NULL`. This is what makes "did this wallet have an alias at block N"
-- answerable, which is the whole point of indexing rather than just querying RPC.
CREATE TABLE identities (
    chain_id         TEXT    NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    -- SS58, encoded with this chain's prefix, so it joins directly against
    -- `extrinsics.signer` and any handler's own address columns.
    account          TEXT    NOT NULL,
    valid_from_block BIGINT  NOT NULL,
    valid_to_block   BIGINT,

    -- `IdentityInfo` fields, flattened for querying. All optional: an identity may set any
    -- subset, and a runtime upgrade may add fields we do not model yet (see `raw`).
    display          TEXT,
    legal            TEXT,
    web              TEXT,
    email            TEXT,
    twitter          TEXT,
    matrix           TEXT,
    github           TEXT,
    discord          TEXT,
    image            TEXT,
    pgp_fingerprint  BYTEA,

    -- [{"registrar_index": 1, "judgement": "KnownGood"}, ...]
    judgements       JSONB   NOT NULL DEFAULT '[]'::jsonb,
    -- Denormalised from `judgements`: true when any judgement is Reasonable or KnownGood.
    -- The cross-check almost always wants "is this alias *trustworthy*", not merely "set",
    -- and computing it per query would mean unpacking JSONB on every row.
    is_verified      BOOLEAN NOT NULL DEFAULT false,

    -- u128 does not fit any Postgres integer type; NUMERIC(39,0) covers 2^128-1.
    deposit          NUMERIC(39,0),

    -- The full decoded `Registration`, exactly as storage returned it. Keeps this table
    -- forward-compatible: a runtime that adds an identity field is still fully recorded even
    -- though no column exists for it yet.
    raw              JSONB   NOT NULL,

    PRIMARY KEY (chain_id, account, valid_from_block)
);

-- At most one open row per account. Also makes the "close the old row, open a new one"
-- update a single-row operation rather than a scan.
CREATE UNIQUE INDEX identities_current_idx
    ON identities (chain_id, account)
    WHERE valid_to_block IS NULL;

-- Point-in-time lookup: `WHERE account = $1 AND valid_from_block <= $2
--                         AND (valid_to_block IS NULL OR valid_to_block >= $2)`
CREATE INDEX identities_temporal_idx
    ON identities (chain_id, account, valid_from_block DESC);

-- "Every verified account", the bulk query RPC cannot serve.
CREATE INDEX identities_verified_idx
    ON identities (chain_id)
    WHERE is_verified AND valid_to_block IS NULL;

-- Case-insensitive display-name lookup, e.g. reconciling an off-chain label.
CREATE INDEX identities_display_idx
    ON identities (chain_id, lower(display))
    WHERE display IS NOT NULL AND valid_to_block IS NULL;


-- Usernames (`alice.dot`) -- pallet_identity's actual alias primitive.
--
-- Keyed by username, not account: `UsernameInfoOf` is the reverse map, multiple usernames may
-- point at one account, and `UsernameOf` picks which of them is primary.
CREATE TABLE usernames (
    chain_id         TEXT    NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    username         TEXT    NOT NULL,
    -- Nullable: a pending username has an account, a dangling one may not.
    account          TEXT,
    is_primary       BOOLEAN NOT NULL DEFAULT false,
    -- active   -- in `UsernameInfoOf`, owned and usable
    -- pending  -- granted by an authority, awaiting `accept_username`
    -- unbinding-- authority started removal; `until_block` is the grace-period expiry
    -- removed  -- no longer in storage; kept so history does not silently lose rows
    status           TEXT    NOT NULL,
    -- The username provider (`Allocation` / `AuthorityDeposit` / ...), shape varies by runtime.
    provider         JSONB,
    -- Acceptance deadline for `pending`, grace-period expiry for `unbinding`.
    until_block      BIGINT,
    granted_at_block BIGINT,
    updated_at_block BIGINT  NOT NULL,

    PRIMARY KEY (chain_id, username),
    CONSTRAINT usernames_status_known
        CHECK (status IN ('active', 'pending', 'unbinding', 'removed'))
);

CREATE INDEX usernames_account_idx ON usernames (chain_id, account)
    WHERE account IS NOT NULL;

-- One primary username per account. Without this the `identity_current` join below could
-- fan out and silently duplicate rows.
CREATE UNIQUE INDEX usernames_primary_idx
    ON usernames (chain_id, account)
    WHERE is_primary AND status = 'active' AND account IS NOT NULL;


-- Sub-identities: `SuperOf` (sub -> parent + label). Lets a cross-check resolve a sub-account
-- back to its parent's verified identity -- e.g. a validator stash under an operator's name.
CREATE TABLE sub_identities (
    chain_id         TEXT   NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    sub              TEXT   NOT NULL,
    super_account    TEXT   NOT NULL,
    label            TEXT,
    updated_at_block BIGINT NOT NULL,

    PRIMARY KEY (chain_id, sub)
);

CREATE INDEX sub_identities_super_idx ON sub_identities (chain_id, super_account);


-- The registrar set, so a judgement index can be resolved to who issued it.
CREATE TABLE identity_registrars (
    chain_id         TEXT   NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    registrar_index  INTEGER NOT NULL,
    -- Nullable: `Registrars` is a `Vec<Option<RegistrarInfo>>`; a removed registrar leaves a
    -- `None` hole and the indices of the ones after it must not shift.
    account          TEXT,
    fee              NUMERIC(39,0),
    fields           JSONB,
    updated_at_block BIGINT NOT NULL,

    PRIMARY KEY (chain_id, registrar_index)
);


-- Records that the one-off storage sweep completed, so a restart does not re-sweep.
--
-- Without the sweep, every identity set before our start block is invisible: on the People
-- chain that is essentially the entire dataset, since identities arrived via the relay-chain
-- migration rather than through blocks we index.
CREATE TABLE identity_bootstrap (
    chain_id       TEXT PRIMARY KEY REFERENCES chains(id) ON DELETE CASCADE,
    snapshot_block BIGINT      NOT NULL,
    completed_at   TIMESTAMPTZ
);


-- The cheapest cross-check surface: anything sharing this database joins against it directly,
-- with no Rust dependency and no HTTP hop.
--
-- Driven off a UNION of every account we know anything about, NOT off `identities` alone.
-- A sub-identity usually has no `IdentityOf` row of its own -- its whole point is to borrow
-- the parent's -- and an account can hold a username without an identity. Selecting
-- `FROM identities` would make exactly those accounts invisible to the cross-check.
--
-- `effective_display` / `effective_verified` are the columns to ask for "who is this wallet":
-- a sub-identity inherits its parent's name and verification.
CREATE VIEW identity_current AS
WITH known AS (
    SELECT chain_id, account FROM identities WHERE valid_to_block IS NULL
    UNION
    SELECT chain_id, sub AS account FROM sub_identities
    UNION
    SELECT chain_id, account FROM usernames
     WHERE account IS NOT NULL AND status = 'active'
)
SELECT
    k.chain_id,
    k.account,
    i.display,
    COALESCE(i.is_verified, false)     AS is_verified,
    COALESCE(i.judgements, '[]'::jsonb) AS judgements,
    i.valid_from_block,
    u.username,
    s.super_account,
    s.label                            AS sub_label,
    COALESCE(i.display, p.display)     AS effective_display,
    COALESCE(i.is_verified, false) OR COALESCE(p.is_verified, false)
                                       AS effective_verified
FROM known k
LEFT JOIN identities i
       ON i.chain_id       = k.chain_id
      AND i.account        = k.account
      AND i.valid_to_block IS NULL
LEFT JOIN usernames u
       ON u.chain_id = k.chain_id
      AND u.account  = k.account
      AND u.is_primary
      AND u.status   = 'active'
LEFT JOIN sub_identities s
       ON s.chain_id = k.chain_id
      AND s.sub      = k.account
LEFT JOIN identities p
       ON p.chain_id       = k.chain_id
      AND p.account        = s.super_account
      AND p.valid_to_block IS NULL;
