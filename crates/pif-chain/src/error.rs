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

    #[error("node returned no header for its own finalized head")]
    MissingFinalizedHeader,

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
