//! Errors raised while reading from a chain.

pub type Result<T, E = ChainError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum ChainError {
    #[error("failed to connect to chain {chain} at {url}")]
    Connect {
        chain: String,
        url: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("rpc call failed")]
    Rpc(#[from] subxt::rpcs::Error),

    /// This binary has no light client compiled in.
    ///
    /// Smoldot is a heavy dependency that most deployments — which point at their own node
    /// — never need, so it is behind a feature rather than always on.
    #[error(
        "chain {chain} is configured with a light-client source, but this binary was built \
         without one.\n\
         Rebuild with the `light-client` feature:\n  \
           cargo build -p polkadot-indexer-cli --features light-client"
    )]
    LightClientUnavailable { chain: String },

    #[error("failed to start the light client for chain {chain}")]
    LightClient {
        chain: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("failed to read chain spec {path} for chain {chain}")]
    ChainSpecRead {
        chain: String,
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// Something asked a light client for a block by number.
    ///
    /// Smoldot cannot verify a full node's claim that a given hash is block `n`, so it
    /// refuses to answer at all. Backfill and `--from` are therefore rpc-only.
    #[error(
        "chain {chain}: cannot index block {number} with a light client.\n\
         A light client verifies what it is told against the chain's finality proofs, and \
         has no way to verify which block sits at a given height, so it can only follow the \
         chain forward from the current finalized head.\n\
         Switch this chain to an rpc source (an archive node, for history) to index it."
    )]
    LightClientCannotBackfill { chain: String, number: u64 },

    #[error("node returned no header for its own finalized head")]
    MissingFinalizedHeader,

    #[error("failed to read the current finalized block on chain {chain}")]
    CurrentBlock {
        chain: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("failed to read block {number}")]
    BlockRead {
        number: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The node no longer holds state for this block.
    ///
    /// Substrate nodes default to `--state-pruning 256`, so anything older than the last
    /// 256 blocks becomes unreadable. The underlying RPC error ("State already discarded")
    /// gives no hint about what to do, hence this dedicated variant.
    #[error(
        "block {number} on chain {chain} has been pruned by the node: its state is no longer \
         available.\n\
         An indexer needs an archive node to backfill history. Either:\n  \
           * run the node with `--state-pruning=archive --blocks-pruning=archive`, or\n  \
           * set `start_block` for this chain to a recent block (the node currently keeps \
             roughly the last 256 blocks)."
    )]
    PrunedState { chain: String, number: u64 },

    /// The block carrying a runtime upgrade could not have its body read.
    ///
    /// This block is the one case where the raw extrinsic bytes are needed rather than
    /// subxt's decoded view: it was executed by the *previous* runtime, so it must be decoded
    /// with the previous runtime's metadata, and there is no way to recover the bytes from an
    /// `Extrinsics` that failed to decode. The bytes come from `chain_getBlock`, which a
    /// light client does not serve.
    #[error(
        "chain {chain}: cannot read the body of block {number}, which carries a runtime \
         upgrade.\n\
         This block was executed by the previous runtime and must be decoded with the \
         previous runtime's metadata, which needs its raw extrinsic bytes.\n\
         A light client cannot supply them. Index this chain from an rpc source to pass \
         the upgrade, then switch back if you wish."
    )]
    UpgradeBlockBodyUnavailable { chain: String, number: u64 },

    /// A storage read at a block failed for a reason other than pruning.
    ///
    /// Named separately from [`ChainError::Decode`] because the pallet and entry are what
    /// identify the problem — a renamed storage item after a runtime upgrade looks nothing
    /// like a malformed block, and reporting it as "failed to decode block N" hides that.
    #[error("failed to read storage {pallet}.{entry} at block {number}")]
    StorageRead {
        pallet: String,
        entry: String,
        number: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("failed to decode block {number}")]
    Decode {
        number: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The configured chain id is already bound to a different genesis hash.
    ///
    /// This means a config entry was repointed at a different chain. Continuing would
    /// interleave two chains' blocks into rows keyed by the same id, so we stop instead.
    #[error(
        "chain id {chain} is already indexed with genesis 0x{stored}, but the node at {url} \
         reports genesis 0x{found}; refusing to mix two chains under one id"
    )]
    GenesisMismatch {
        chain: String,
        url: String,
        stored: String,
        found: String,
    },

    /// The digest asked the archive for a block it does not hold.
    ///
    /// Expected, and informative, for any range indexed before the archive existed: the
    /// `0002_pipeline` migration seeded the watermarks from the cursor rather than
    /// re-downloading history, so those blocks have rows in Postgres but no bytes on disk.
    #[error(
        "chain {chain}: block {number} is not in the archive at {path}.\n\
         Blocks indexed before the archive existed were never written to it, so a replay \
         cannot reach back past that point. Re-fetch the range with `pif fetch --from \
         {number}` if you need it replayable."
    )]
    BlockNotArchived {
        chain: String,
        number: u64,
        path: String,
    },

    /// A block's executing runtime has no metadata in the archive.
    ///
    /// Worse than a missing block: the bytes are intact and undecodable. Losing a
    /// `spec_version`'s metadata makes every block that ran under it permanently unreadable,
    /// which is why the metadata directory is the one part of the store a retention policy
    /// must never touch.
    #[error(
        "chain {chain}: block {number} was executed by runtime spec {spec_version}, whose \
         metadata is not in the archive.\n\
         The block's bytes are intact but nothing can decode them. Re-fetch the metadata \
         from an archive node:\n  \
           subxt metadata --url <node> --at-block <hash of a block under spec \
         {spec_version}> -o {spec_version}.scale"
    )]
    MetadataNotArchived {
        chain: String,
        number: u64,
        spec_version: u32,
    },

    /// A handler read chain state during an offline digest, and the archive does not hold it.
    ///
    /// Archiving blocks makes the *dynamic core* replayable; it does not make a handler that
    /// reads state replayable, because state is not in the block. Until storage reads are
    /// archived too, a handler like `identity` needs a node — and this says so rather than
    /// quietly reaching for one.
    #[error(
        "chain {chain}: handler storage read {pallet}.{entry} at block {number} has no \
         archived answer.\n\
         Block bytes are archived; chain *state* is not, so handlers that read storage still \
         need a node. Point this chain at an rpc endpoint to digest it, or run without the \
         handlers that read state."
    )]
    StorageNotArchived {
        chain: String,
        number: u64,
        pallet: String,
        entry: String,
    },

    /// The archived bytes are not what they claim to be.
    #[error("chain {chain}: the archived form of block {number} is unusable: {reason}")]
    ArchiveCorrupt {
        chain: String,
        number: u64,
        reason: String,
    },

    /// Block N's `parent_hash` is not block N-1's hash.
    ///
    /// Nothing verified this before the pipeline split, because one endpoint fetched every
    /// block in order and there was nothing to disagree with. It matters most on the
    /// **replay** path: `OfflineClient` validates no chain identity at all — it stores the
    /// genesis hash unchecked, and a replay leaves it unset — so `guard_chain_identity` has
    /// no offline counterpart and this check is the only thing standing between a replay and
    /// another chain's segments.
    #[error(
        "chain {chain}: block {number} claims parent 0x{found}, but block {} was stored as \
         0x{expected}.\n\
         Refusing to store a spliced history. This means either the archive holds two \
         chains' blocks under one id, or an endpoint served an inconsistent view.",
        number - 1
    )]
    ChainLinkageBroken {
        chain: String,
        number: u64,
        expected: String,
        found: String,
    },

    /// The chain moved to a metadata format this build of subxt cannot read.
    ///
    /// The one runtime-upgrade failure that genuinely needs a human, and the reason it is not
    /// flattened into [`ChainError::Decode`]: its fix is "upgrade subxt and redeploy", not
    /// "retry". The fetch stage keeps running through it — archiving raw bytes needs no
    /// metadata — so the backlog is waiting when the new build lands.
    #[error(
        "chain {chain}: runtime spec {spec_version} reports metadata format v{metadata_version}, \
         which this build cannot decode.\n\
         Upgrade subxt and redeploy, then resume: blocks already archived are not re-fetched.\n\
         Capture the metadata before anything else, or it becomes unobtainable once the node \
         prunes:\n  \
           subxt metadata --url <node> --at-block <hash> -o {spec_version}.scale"
    )]
    UnsupportedMetadataVersion {
        chain: String,
        spec_version: u32,
        metadata_version: u32,
    },

    /// The node would not hand over the metadata for a runtime.
    ///
    /// Distinct from a format this build cannot read: here nothing arrived at all, usually
    /// because the node has pruned the state at that block. Archiving cannot proceed past
    /// it, since a block whose runtime is unarchived is a block no replay can decode.
    #[error("chain {chain}: could not read the metadata for runtime spec {spec_version}")]
    MetadataUnavailable {
        chain: String,
        spec_version: u32,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("block archive error")]
    Store(#[from] pif_store::StoreError),

    #[error("database error")]
    Database(#[from] sqlx::Error),

    #[error("handler {handler} failed on block {block}")]
    Handler {
        handler: String,
        block: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("no handler registered under the name {0:?}")]
    UnknownHandler(String),

    /// `pif replay --from 200 --to 100`, which is almost certainly a swapped pair.
    #[error("replay range {from}..={to} is empty; --to must not be below --from")]
    EmptyReplayRange { from: u64, to: u64 },

    /// A handler's migrations would collide with the framework's or another handler's.
    ///
    /// sqlx keys `_sqlx_migrations` by version alone, so this is caught at startup rather
    /// than as a corrupted migration history on a live database.
    #[error("handler {handler:?} has an invalid migration version {version}: {reason}")]
    MigrationVersion {
        handler: &'static str,
        version: i64,
        reason: String,
    },
}
