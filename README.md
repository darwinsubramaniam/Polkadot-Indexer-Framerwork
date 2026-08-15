# Polkadot Indexer Framework (PIF)

A chain-agnostic indexing **framework** for Substrate-based chains, built on
[subxt](https://github.com/paritytech/subxt).

One binary indexes any number of chains listed in `config/chains.toml`. Blocks, extrinsics
and events are decoded **dynamically** from the metadata each node reports for each block —
there is no runtime compiled into the indexing path, so a new chain is a config entry rather
than a code change, and runtime upgrades are handled automatically.

Optional **typed handlers** sit on top as a per-chain projection layer, producing
denormalised tables (`transfers`) for the queries that matter most.

## Architecture

| Directory | Published as | Import as | Role |
|---|---|---|---|
| `crates/pif-core` | `polkadot-indexer-core` | `pif_core` | config, errors, SCALE→JSON codec, SS58 |
| `crates/pif-db` | `polkadot-indexer-db` | `pif_db` | Postgres persistence, migrations, repositories |
| `crates/pif-chain` | `polkadot-indexer-chain` | `pif_chain` | subxt client, dynamic decoder, handler registry, pipeline |
| `crates/pif-api` | `polkadot-indexer-api` | `pif_api` | extensible GraphQL schema + axum server |
| `crates/pif-identity` | `polkadot-indexer-identity` | `pif_identity` | People-chain identities, usernames and the alias cross-check |
| `crates/pif-cli` | `polkadot-indexer-cli` | — | reference binary, `pif` |
| `crates/example` | *(unpublished)* | — | reference handler — **copy this to start your own** |

Dependency direction: `pif-cli` → {`pif-chain`, `pif-api`} → `pif-db` → `pif-core`.

## Building your own indexer on PIF

The framework contains **no chain-specific code**. To index Hydration's omnipool, or any
other pallet, you write a handler in your own crate and register it — you never fork or
patch the framework:

```rust
use pif_chain::handlers::{BlockContext, EventHandler, HandlerRegistry};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

struct OmnipoolSwapHandler;

#[async_trait]
impl EventHandler for OmnipoolSwapHandler {
    fn name(&self) -> &'static str { "omnipool-swap" }
    fn supports(&self, _chain: &ChainInfo) -> bool { true }
    fn migrator(&self) -> Option<&'static sqlx::migrate::Migrator> { Some(&MIGRATOR) }

    async fn handle(&self, ctx: &BlockContext<'_>, block: &BlockData,
                    tx: &mut PgConnection) -> pif_chain::Result<()> {
        // `tx` is the block's own transaction: your rows commit with the block and roll
        // back with it. Read-modify-write against your own tables works here too.
        Ok(())
    }
}

let mut registry = HandlerRegistry::new();
registry.register(Box::new(OmnipoolSwapHandler));
pif_chain::run(&pool, &chain_config, &registry, options).await?;
```

You own the table, the SQL and the migrations. Each handler's migration history lives in its
own `_sqlx_migrations_<handler>` table, so your version numbers start at 1 and never need
coordinating with the framework or with other handlers.

`crates/example` is a complete working handler (`Balances::Transfer` → a `transfers` table).
Copy it as your starting point.

## Quick start

Everything is driven through [`just`](https://github.com/casey/just) — run `just` on its own
for the full list.

```sh
just up          # Postgres (host port 5433) + a rococo-dev node on :9944
just migrate
just index       # tails the finalized head, resumable
just serve       # GraphiQL at http://localhost:8000
```

Or `just demo` to do the first three in one go and index to block 20.

Inspecting what landed:

```sh
just status                        # every chain and how far it has been indexed
just gaps                          # holes in the stored chain — always 0
just transfers <chain-id>          # typed-overlay transfers
just transfer-events <chain-id>    # the same, as the dynamic core stored them
just events <chain-id>             # most common events, a quick decode sanity check
just health                        # is the node actually finalizing?
```

The equivalent cargo commands are in the `Justfile` if you prefer to run them directly.

## Cargo features

Both are **off by default**, so a plain build is just the indexing pipeline.

| Feature | Adds |
|---|---|
| `api` | the GraphQL server (`serve`). Pulls ~57 crates — async-graphql, axum — that the pipeline never needs, so it is opt-in. Running `serve` without it prints how to enable it. |
| `handler-balances` | registers the reference handler from `crates/example`, populating a `transfers` table |
| `handler-identity` | registers the `identity` handler for the Polkadot People chain: display names, registrar judgements, sub-identities and usernames. With `api`, also merges an `identity` GraphQL root into the schema. |

```sh
cargo run -p polkadot-indexer-cli --features api -- serve              # GraphiQL on :8000
cargo run -p polkadot-indexer-cli --features handler-balances -- index # populate `transfers`
cargo run -p polkadot-indexer-cli --features api,handler-balances -- serve
```

If you would rather not run the bundled API at all, the schema is plain Postgres — pointing
Hasura or PostGraphile at it works. One caveat if you do: balances are `NUMERIC(39,0)`, and
a generated API will typically emit them as JSON **numbers**, which corrupts any value above
2^53. The bundled API returns them as strings for exactly this reason.

## Querying

`just serve` (or `cargo run -p polkadot-indexer-cli --features api -- serve`) puts GraphiQL on http://localhost:8000.
Every query is chain-scoped, so start by finding the id:

```graphql
{ chains { id name tokenSymbol } }
```

### Did a transfer happen?

**Dynamic core** — always populated, on any chain, with no feature flag or codegen:

```graphql
{
  events(chainId: "dev-local", pallet: "Balances", variant: "Transfer", limit: 10) {
    blockNumber idx extrinsicIdx fields
  }
}
```

`fields` is the raw decoded JSONB, e.g.
`{"from": ["0xd435…"], "to": ["0x8eaf…"], "amount": "1234567890123"}`. Accounts appear as
hex here; converting them to SS58 is one of the things a typed handler does.

**Typed overlay** — the `transfers` table written by the example handler holds the same data
denormalised, with SS58 addresses. The framework schema deliberately does **not** expose it:
domain queries belong to the project that owns the table. Read it with `just transfers
<chain-id>`, or add a GraphQL query of your own by merging a root into the framework's:

```rust
#[derive(async_graphql::MergedObject, Default)]
struct Query(pif_api::CoreQuery, MyTransferQuery);

let schema = pif_api::build_schema_with(pool, Query::default());
```

**The extrinsic behind it**, including who signed and what it cost:

```graphql
{
  extrinsics(chainId: "dev-local", pallet: "Balances",
             call: "transfer_allow_death", limit: 5) {
    blockNumber signer success fee
  }
}
```

### Indexing progress

```graphql
{ indexerStatus { chainId firstIndexedBlock lastIndexedBlock gaps } }
```

`gaps` should always be `0`.

> `amount` and `fee` are returned as **strings**, not numbers — see the `u128` note below.
> `limit` is capped at 100 server-side.

## Aliases: who is behind a wallet

`--features handler-identity` indexes the Polkadot People chain, which is where
`pallet-identity` lives now — display names, registrar judgements, sub-identities, and
usernames (`alice.dot`). The point is the **cross-check**: an indexer running against a
*different* chain can ask whether a wallet has an alias, and whether anyone vouched for it.

```toml
[[chains]]
id          = "polkadot-people"
ws_url      = "wss://polkadot-people-rpc.polkadot.io"
start_block = 0
handlers    = ["identity"]
```

### This handler reads chain *state*, not just events

`pallet_identity`'s events are notifications, not payloads: `IdentitySet { who }` carries no
display name, and `JudgementGiven { target, registrar_index }` carries no judgement. So the
events say **which account changed** and storage says **what it changed to**. That is why
`BlockContext` now carries `storage: &dyn StorageAt`, and why the handler runs a one-off
`bootstrap` sweep — everything set before your `start_block` is otherwise invisible, which on
the People chain is nearly the entire identity set.

> **Storage reads at a historical block need an archive node.** Tailing the finalized head
> works against any node; backfilling does not, because a default node keeps only ~256 blocks
> of state. You get `ChainError::PrunedState`, which says exactly this. Either point at an
> archive endpoint or set `start_block` near the head.

### Cross-checking from your own handler

Both indexers write to the same Postgres and every table is keyed by `chain_id`, so a lookup
from another chain is an ordinary join — no XCM correlation, no second connection:

```rust
use pif_identity::{IdentityResolver, PgIdentityResolver};

// constructed with the chain that *holds* the identities, not the one you are indexing
let identities = PgIdentityResolver::new(pool.clone(), "polkadot-people");

let alias = identities.alias_of(&from_ss58).await?;
if alias.as_ref().is_some_and(|a| a.verified) {
    // a registrar vouched for this account
}

// and, because the table is temporal, as it stood at the time of the transfer
let then = identities.alias_at(&from_ss58, block_number).await?;
```

Branch on `verified`, not on `display`: a display name only means somebody paid a deposit and
typed something. Only `Reasonable` and `KnownGood` mean a registrar checked.

A sub-identity has no identity of its own, so `effective_display` and `effective_verified`
resolve up to the parent — a validator stash reads as its operator.

### Without an indexer at all

For a point lookup the RPC is enough, and that path ships too:

```rust
let resolver = pif_identity::RpcIdentityResolver::connect(&chain_config).await?;
let alias = resolver.alias_of("5Grw...").await?;   // reads state at the finalized head
```

`alias_of` works with no database. `alias_at` returns `ResolveError::NotSupported`, because a
node cannot know what an identity looked like last year — that is exactly what the index is
for. Same trade-off, stated once:

| Question | Needs |
|---|---|
| "Does this wallet have an alias **right now**?" | a node. No indexer. |
| "Did it have one **at block N**?" | the indexer — historical state is pruned |
| "Give me **every** verified account" | the indexer — a sweep per query is not a join |

From the shell:

```sh
just alias polkadot-people 5Grw...   # display, username, verified, parent
just identities polkadot-people      # verified accounts first
just verified polkadot-people        # how many of them there are
```

## Adding a chain

Append to `config/chains.toml`. Nothing else changes:

```toml
[[chains]]
id          = "para-1000"
ws_url      = "ws://127.0.0.1:9988"
start_block = 0
handlers    = []            # or ["balances-transfer"]
```

Chain name, token symbol/decimals, SS58 prefix and genesis hash are read from the node on
first connect and stored in the `chains` table.

## Design decisions worth knowing

**Handlers can read chain state, not just events.** `BlockContext::storage` exposes the
block's state through the same dynamic, metadata-driven path the decoder uses. It exists
because some pallets report *that* something changed without reporting *what* — see the
identity section above. It costs an RPC round-trip inside the block's transaction, so read
only when an event says something changed.

**Only finalized blocks are indexed.** `stream_blocks()` yields finalized blocks, which
cannot be reverted, so there is no reorg-handling code and the stored chain can never contain
an orphaned block. Anything that changes this to follow best-blocks must add rollback logic.

**One Postgres transaction per block.** The block row, its extrinsics, events, typed-overlay
rows and the resume cursor all commit together. The cursor therefore can never run ahead of
the data it describes, which is what makes restart-resume correct rather than merely likely.
Every insert is `ON CONFLICT DO NOTHING`, so replaying a block is a no-op.

**`u128` is stored and transported as a string.** Substrate balances exceed 2^53, where JSON
numbers silently lose precision. The codec renders `u128`/`i128` as JSON strings, Postgres
stores them as `NUMERIC(39,0)`, and GraphQL exposes them through a `BigInt` scalar that
serialises as a string. On the Rust side this maps through `bigdecimal` — **not**
`rust_decimal`, whose 96-bit mantissa cannot hold a `u128`.

**Byte sequences of 16+ elements collapse to hex.** After dynamic decoding, `[u8; 32]` and
`(u8, u8)` are indistinguishable — both are an unnamed composite of small integers. A length
threshold keeps `AccountId32`, hashes and signatures readable while leaving short numeric
tuples alone.

**Newtype wrappers keep their array level.** `AccountId32` decodes as `["0x…"]` rather than
`"0x…"`. Newtypes and single-element `Vec`s are indistinguishable after SCALE decoding, so
unwrapping globally would silently turn one-element lists into scalars — a shape that changes
with length is worse than a consistently extra level. Handlers unwrap where they know the
field's semantics.

**`chains.genesis_hash` is not unique.** Indexing one physical chain under two ids is
legitimate. The property actually wanted — "this id keeps pointing at the same chain" — is
enforced by `guard_chain_identity`, which compares the stored genesis hash against the node's
for that id and refuses to continue on a mismatch.

**Queries use sqlx's runtime API, not the `query!` macros.** The macros verify SQL against a
live database at compile time, which would make `cargo build` require a running Postgres or a
`.sqlx/` cache that goes stale. Keeping the build hermetic is worth more here; the schema is
exercised by the e2e tests instead.

## Testing

```sh
just test          # hermetic: no Docker, no database
just lint          # fmt + clippy across every feature combination
just ci            # both — what CI should run
```

End-to-end tests need the compose stack and are `#[ignore]`d by default:

```sh
just fetch-zombie-cli    # once, for the zombienet suites
just test-e2e            # against the compose dev node
just test-zombienet      # spawns throwaway networks
just test-all            # everything
```

`just lint` deliberately runs clippy under **three** feature combinations. A `cfg`-gated
crate can pass in one configuration and fail in another — an unused import behind a feature
flag is invisible until you build without it.

These cover resume-after-restart, dynamic decoding, and — against a **zombienet network
spawned by the test itself** — real `Balances` transfers flowing through both the dynamic
core and the typed overlay:

```
zombienet spawned alice at ws://127.0.0.1:55601
submitted: alice -> bob, 1111111111111
submitted: alice -> charlie, 2222222222222
submitted: alice -> dave, 9007199254740993
indexing 0..=7
verified: 5GrwvaEF5zXb26Fz... -> 5FHneW46xGXgs5mU..., 1111111111111    (block 1)
verified: 5GrwvaEF5zXb26Fz... -> 5FLSigC9HGRKVhB9..., 2222222222222    (block 4)
verified: 5GrwvaEF5zXb26Fz... -> 5DAAnrj7VHTznn2A..., 9007199254740993 (block 7)
all 3 transfers recorded by both the dynamic core and the typed overlay
```

That last amount is 2^53 + 1 — it round-trips exactly, which is the point of the
string-based `u128` handling described above.

See [`crates/indexer-e2e/README.md`](crates/indexer-e2e/README.md) for why zombienet is
driven through the CLI rather than the SDK, and a known multi-node peering limitation.

## Environment notes

- **Apple Silicon:** `parity/polkadot` and `parity/polkadot-parachain` are published for
  `linux/amd64` only, so nodes run under emulation. Budget generous timeouts. `zombie-cli`
  itself is a native arm64 binary.
- **Postgres 18** changed its volume convention: the volume mounts at `/var/lib/postgresql`,
  not `/var/lib/postgresql/data`. Mounting the old path makes the image refuse to start.
- **Host port 5433** is used for Postgres because 5432 is commonly occupied by an existing
  local server or SSH tunnel.

## Not yet implemented

Parallel historical backfill (`ArchiveBackend`), reorg handling for non-finalized tailing,
Prometheus metrics, custom `Config` types for Ethereum-style chains (Moonbeam), XCM
correlation across relay/parachain, and table partitioning by `chain_id`.
