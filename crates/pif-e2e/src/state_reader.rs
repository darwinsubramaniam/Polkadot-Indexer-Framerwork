//! A handler that exists to read chain **state**, so the storage read cache can be proved.
//!
//! The framework's real state-reading handler is `pif-identity`, but `pallet_identity` lives
//! on the People chain rather than the relay, so testing against it needs a whole zombienet
//! network. What the cache actually has to be right about — that a read made at block N is
//! archived, keyed and served back byte-identically with no node attached — needs neither a
//! parachain nor an identity.
//!
//! So this reads two entries every Substrate chain has, chosen to cover both key shapes:
//!
//! * `Timestamp::Now` — a plain value, no map keys at all;
//! * `System::Account(Alice)` — a map entry, which exercises the key rendering the cache is
//!   keyed by. Getting that wrong is how two different reads collapse into one cache entry.
//!
//! Results go into a shared log rather than a table, so the test can compare a live pass
//! against a replayed one directly and the handler needs no migration of its own.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pif_chain::error::Result;
use pif_chain::{BlockContext, EventHandler};
use pif_core::ChainInfo;
use pif_db::BlockData;
use scale_value::Value;
use sqlx::PgConnection;

/// Alice's `AccountId32`, the well-known dev account present on every `--dev` chain.
const ALICE: [u8; 32] = [
    0xd4, 0x35, 0x93, 0xc7, 0x15, 0xfd, 0xd3, 0x1c, 0x61, 0x14, 0x1a, 0xbd, 0x04, 0xa9, 0x9f, 0xd6,
    0x82, 0x2c, 0x85, 0x58, 0x85, 0x4c, 0xcd, 0xe3, 0x9a, 0x56, 0x84, 0xe7, 0xa5, 0x6d, 0xa2, 0x7d,
];

/// What one block's reads produced: `(block, entry, answer)`.
pub type ReadLog = Arc<Mutex<Vec<(u64, &'static str, String)>>>;

/// Reads chain state on every block and records what it was told.
pub struct StateReadingHandler {
    log: ReadLog,
}

impl StateReadingHandler {
    pub fn new() -> (Self, ReadLog) {
        let log = ReadLog::default();
        (Self { log: log.clone() }, log)
    }

    pub const NAME: &'static str = "e2e-state-reader";
}

#[async_trait]
impl EventHandler for StateReadingHandler {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn supports(&self, _chain: &ChainInfo) -> bool {
        true
    }

    async fn handle(
        &self,
        ctx: &BlockContext<'_>,
        _block: &BlockData,
        _tx: &mut PgConnection,
    ) -> Result<()> {
        // Read on *every* block, not only interesting ones: a cache that worked for the
        // blocks a handler happened to touch and failed elsewhere would pass a narrower test.
        let now = ctx.storage.fetch("Timestamp", "Now", Vec::new()).await?;

        let account = ctx
            .storage
            .fetch("System", "Account", vec![Value::from_bytes(ALICE)])
            .await?;

        let mut log = self.log.lock().expect("read log poisoned");
        log.push((ctx.block_number, "Timestamp::Now", render(&now)));
        log.push((ctx.block_number, "System::Account", render(&account)));
        Ok(())
    }
}

/// `None` rendered distinctly from any value, so an absent answer and a null one cannot be
/// mistaken for each other when the two passes are compared.
fn render(value: &Option<serde_json::Value>) -> String {
    match value {
        Some(json) => format!("some:{json}"),
        None => "none".to_owned(),
    }
}
