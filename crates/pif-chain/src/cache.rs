//! [`StorageAt`] answered from the local archive, falling back to the node and recording
//! what it learned.
//!
//! `StorageAt` is a trait rather than a bare `&AtBlock` so that a handler's tests can stub it
//! with canned JSON. That same property makes it *decoratable*, which is the whole mechanism
//! here: no handler changes, no new trait, and `pif-identity` compiles untouched.
//!
//! The decorator lives in this crate and not in `pif-store`, because `StorageAt` and
//! `ChainError` are defined here while `pif-chain` needs `pif-store`'s types — putting it the
//! other way round closes a dependency cycle Cargo rejects outright. `pif-store` keeps the
//! byte-level keyed store; this keeps everything that knows what a storage read *is*.

use std::sync::Mutex;

use async_trait::async_trait;
use futures::stream::BoxStream;
use pif_core::ChainInfo;
use pif_store::{BlockReads, ReadKey, StorageCache};
use scale_value::Value;
use serde_json::Value as Json;
use subxt::{OnlineClient, PolkadotConfig, metadata::ArcMetadata};
use tokio::sync::OnceCell;

use crate::decode::{self, AtBlock};
use crate::error::{ChainError, Result};
use crate::storage::{self, StorageAt};

/// A [`StorageAt`] that answers from the archive first.
///
/// On the **first** digest of a block a read misses and goes to the network; on **every
/// subsequent** digest of that block it hits, and the network is never touched. That is the
/// difference between "blocks are archived" and "a re-index costs a re-digest".
pub struct CachedStorage<'a> {
    cache: &'a StorageCache,
    chain: &'a ChainInfo,
    block: u64,

    /// The node to fall back to on a miss. `None` during a fully offline replay, where a
    /// miss must be a **loud error** rather than a silent fetch — otherwise a replay quietly
    /// becomes a re-download and nobody notices until the bill arrives.
    client: Option<&'a OnlineClient<PolkadotConfig>>,

    /// This block's metadata, from the archive. Lets `has_pallet` answer offline, which is
    /// otherwise the one `StorageAt` method that needs a live client to say anything.
    metadata: Option<ArcMetadata>,

    /// Resolved on the first miss, never before. A warm block costs no round-trip at all —
    /// resolving `at_block(n)` eagerly would put one back on the path this exists to clear.
    at: OnceCell<AtBlock>,

    /// What is archived for this block, plus whatever this pass has learned since.
    ///
    /// `None` until something actually asks. Most blocks read no state at all — the identity
    /// handler returns early unless the block carried an identity event — so loading eagerly
    /// would add a segment read per block to buy nothing.
    reads: Mutex<Option<BlockReads>>,

    /// Whether anything was learned that is not yet on disk.
    learned: Mutex<bool>,
}

impl<'a> CachedStorage<'a> {
    /// Prepare the cached view of one block's chain state. Reads no disk until asked.
    pub fn new(
        cache: &'a StorageCache,
        chain: &'a ChainInfo,
        block: u64,
        client: Option<&'a OnlineClient<PolkadotConfig>>,
        metadata: Option<ArcMetadata>,
    ) -> Self {
        Self {
            cache,
            chain,
            block,
            client,
            metadata,
            at: OnceCell::new(),
            reads: Mutex::new(None),
            learned: Mutex::new(false),
        }
    }

    /// Write back what this pass learned, if anything.
    ///
    /// Called after the block's transaction commits, so the archive records reads that
    /// belong to a block that actually landed. A block whose every read hit the cache writes
    /// nothing at all, which is what keeps a warm re-digest free of disk churn.
    pub fn persist(&self) -> Result<()> {
        let mut learned = self.learned.lock().expect("cache learned lock poisoned");
        if !*learned {
            return Ok(());
        }

        if let Some(reads) = self
            .reads
            .lock()
            .expect("cache reads lock poisoned")
            .as_ref()
        {
            self.cache.put(&self.chain.id, self.block, reads)?;
        }
        *learned = false;
        Ok(())
    }

    /// Whether every read this pass made was already archived.
    pub fn was_fully_cached(&self) -> bool {
        !*self.learned.lock().expect("cache learned lock poisoned")
    }

    /// The live client at this block, resolved on demand.
    async fn live(&self) -> Result<&AtBlock> {
        let Some(client) = self.client else {
            unreachable!("callers check `client` before reaching for the node");
        };
        self.at
            .get_or_try_init(|| decode::at_block(client, self.chain, self.block))
            .await
    }

    fn not_archived(&self, pallet: &str, entry: &str) -> ChainError {
        ChainError::StorageNotArchived {
            chain: self.chain.id.clone(),
            number: self.block,
            pallet: pallet.to_owned(),
            entry: entry.to_owned(),
        }
    }
}

#[async_trait]
impl StorageAt for CachedStorage<'_> {
    fn block_number(&self) -> u64 {
        self.block
    }

    /// Answered from the archived metadata when there is any.
    ///
    /// Without a node and without archived metadata there is nothing to consult, and `true`
    /// is the safer guess: it sends the handler on to `fetch`, which returns a precise error
    /// naming the entry, rather than letting it conclude the chain lacks the pallet and skip
    /// the block in silence.
    fn has_pallet(&self, pallet: &str) -> bool {
        match &self.metadata {
            Some(metadata) => metadata.pallet_by_name(pallet).is_some(),
            None => true,
        }
    }

    async fn fetch(&self, pallet: &str, entry: &str, keys: Vec<Value>) -> Result<Option<Json>> {
        let key = ReadKey::new(pallet, entry, render_keys(&keys));

        // Scoped so the guard is dropped before any `await` below: this is a `std` mutex, and
        // holding one across a suspension point is how a digest deadlocks itself.
        // Two levels of `Option`, and neither is redundant: the outer one is "was this read
        // archived at all", the inner is "was the key present". Collapsing them would make a
        // cached absence indistinguishable from a cache miss, which is the single mistake
        // that would quietly turn every replay back into a re-download.
        let archived: Option<Result<Option<Json>>> = {
            let mut slot = self.reads.lock().expect("cache reads lock poisoned");
            let reads = match slot.as_mut() {
                Some(reads) => reads,
                None => slot.insert(
                    self.cache
                        .get(&self.chain.id, self.block)?
                        .unwrap_or_default(),
                ),
            };

            reads.get(&key).map(|value| match value {
                Some(bytes) => serde_json::from_slice(bytes).map(Some).map_err(|source| {
                    ChainError::ArchiveCorrupt {
                        chain: self.chain.id.clone(),
                        number: self.block,
                        reason: format!(
                            "the archived answer for {pallet}.{entry} is not valid JSON: {source}"
                        ),
                    }
                }),
                // "This account has no identity" is the *common* answer, not the exceptional
                // one, so it is archived as an answer and served as a hit.
                None => Ok(None),
            })
        };

        if let Some(result) = archived {
            return result;
        }

        if self.client.is_none() {
            return Err(self.not_archived(pallet, entry));
        }

        let at = self.live().await?;
        let value = storage::fetch_at(at, &self.chain.id, pallet, entry, keys).await?;

        let encoded = match &value {
            Some(json) => {
                Some(
                    serde_json::to_vec(json).map_err(|source| ChainError::StorageRead {
                        pallet: pallet.to_owned(),
                        entry: entry.to_owned(),
                        number: self.block,
                        source: Box::new(source),
                    })?,
                )
            }
            None => None,
        };

        self.reads
            .lock()
            .expect("cache reads lock poisoned")
            .get_or_insert_default()
            .insert(key, encoded);
        *self.learned.lock().expect("cache learned lock poisoned") = true;

        Ok(value)
    }

    /// Deliberately **not** cached.
    ///
    /// `iter` is the bootstrap sweep over `Identity::IdentityOf` and friends: a one-off,
    /// already guarded by its own table, and tens of thousands of keys that would dwarf the
    /// blocks themselves. A replay re-runs bootstrap against a node, or skips it because the
    /// table says it already ran.
    async fn iter<'s>(
        &'s self,
        pallet: String,
        entry: String,
    ) -> Result<BoxStream<'s, Result<(Vec<u8>, Json)>>> {
        if self.client.is_none() {
            return Err(self.not_archived(&pallet, &entry));
        }

        // Delegated to the live path unchanged. The returned stream borrows the `AtBlock`,
        // which this struct owns for its whole life, so it can be lent for as long as the
        // stream lives.
        let at = self.live().await?;
        storage::iter_at(at, &self.chain.id, pallet, entry).await
    }
}

/// Stable identity for a key list.
///
/// The same rendering the storage stub uses, and for the same reason: `scale_value::Value`
/// renders deterministically, and this string is only ever compared against itself.
fn render_keys(keys: &[Value]) -> String {
    keys.iter()
        .map(|key| key.to_string())
        .collect::<Vec<_>>()
        .join("|")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pif_core::ss58;

    fn chain() -> ChainInfo {
        ChainInfo {
            id: "polkadot".to_owned(),
            genesis_hash: vec![0; 32],
            name: "Polkadot".to_owned(),
            token_symbol: Some("DOT".to_owned()),
            token_decimals: Some(10),
            ss58_prefix: ss58::DEFAULT_PREFIX,
        }
    }

    fn cache() -> (tempfile::TempDir, StorageCache) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = StorageCache::open(dir.path(), 10).expect("open");
        (dir, cache)
    }

    /// Seed the archive as a first, live digest would have.
    ///
    /// The key goes through `render_keys`, exactly as a real read does. Hand-writing the
    /// rendering here would let the two drift and leave every test below passing against a
    /// key shape nothing in production ever produces.
    fn seed(cache: &StorageCache, block: u64, entries: &[(&str, Option<&str>)]) {
        let mut reads = BlockReads::default();
        for (account, value) in entries {
            reads.insert(
                ReadKey::new(
                    "Identity",
                    "IdentityOf",
                    render_keys(&[Value::string(*account)]),
                ),
                value.map(|v| v.as_bytes().to_vec()),
            );
        }
        cache.put("polkadot", block, &reads).expect("put");
        cache.sync().expect("sync");
    }

    #[tokio::test]
    async fn an_archived_read_is_served_without_a_node() {
        let (_dir, cache) = cache();
        let chain = chain();
        seed(&cache, 12, &[("alice", Some(r#"{"display":"Alice"}"#))]);

        // `client: None` — there is no network to fall back to, so a hit is the only way
        // this can succeed.
        let storage = CachedStorage::new(&cache, &chain, 12, None, None);
        let found = storage
            .fetch("Identity", "IdentityOf", vec![Value::string("alice")])
            .await
            .expect("archived");

        assert_eq!(found, Some(serde_json::json!({ "display": "Alice" })));
    }

    #[tokio::test]
    async fn an_archived_absence_is_a_hit_not_a_miss() {
        // The case that decides whether the cache is worth having: most accounts have no
        // identity, so if `Ok(None)` were not archived, a replay would go to the network for
        // nearly every read.
        let (_dir, cache) = cache();
        let chain = chain();
        seed(&cache, 12, &[("bob", None)]);

        let storage = CachedStorage::new(&cache, &chain, 12, None, None);
        let found = storage
            .fetch("Identity", "IdentityOf", vec![Value::string("bob")])
            .await
            .expect("an archived absence must not be an error");

        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn a_miss_with_no_node_is_loud() {
        // Never a silent fall-through: a replay that quietly re-downloads looks exactly like
        // a replay that worked, until the bill arrives.
        let (_dir, cache) = cache();
        let chain = chain();
        seed(&cache, 12, &[("alice", None)]);

        let storage = CachedStorage::new(&cache, &chain, 12, None, None);
        let error = storage
            .fetch("Identity", "IdentityOf", vec![Value::string("carol")])
            .await
            .expect_err("an unarchived read must not be answered");

        assert!(
            matches!(error, ChainError::StorageNotArchived { .. }),
            "got {error}"
        );
        assert!(error.to_string().contains("Identity.IdentityOf"));
    }

    #[tokio::test]
    async fn a_block_with_no_archived_reads_at_all_still_errors_rather_than_answering() {
        let (_dir, cache) = cache();
        let chain = chain();

        let storage = CachedStorage::new(&cache, &chain, 999, None, None);
        let error = storage
            .fetch("Identity", "IdentityOf", vec![Value::string("alice")])
            .await
            .expect_err("nothing is archived for this block");

        assert!(matches!(error, ChainError::StorageNotArchived { .. }));
    }

    #[tokio::test]
    async fn keys_distinguish_reads_of_the_same_entry() {
        let (_dir, cache) = cache();
        let chain = chain();
        seed(
            &cache,
            5,
            &[("alice", Some(r#""a""#)), ("bob", Some(r#""b""#))],
        );

        let storage = CachedStorage::new(&cache, &chain, 5, None, None);
        for (account, expected) in [("alice", "a"), ("bob", "b")] {
            let found = storage
                .fetch("Identity", "IdentityOf", vec![Value::string(account)])
                .await
                .expect("archived");
            assert_eq!(found, Some(serde_json::json!(expected)));
        }
    }

    #[tokio::test]
    async fn a_fully_cached_block_writes_nothing_back() {
        // A warm re-digest must not churn the archive: nothing was learned, so nothing is
        // written, and the segment does not grow a duplicate record per pass.
        let (_dir, cache) = cache();
        let chain = chain();
        seed(&cache, 12, &[("alice", None)]);

        let storage = CachedStorage::new(&cache, &chain, 12, None, None);
        storage
            .fetch("Identity", "IdentityOf", vec![Value::string("alice")])
            .await
            .expect("archived");

        assert!(storage.was_fully_cached());
        storage.persist().expect("persist is a no-op here");
    }

    #[tokio::test]
    async fn iter_is_not_served_from_the_archive() {
        // Bootstrap sweeps are tens of thousands of keys and would dwarf the blocks. Excluded
        // on purpose, and the exclusion is loud rather than silently empty — an empty stream
        // would look like "this chain has no identities".
        let (_dir, cache) = cache();
        let chain = chain();

        let storage = CachedStorage::new(&cache, &chain, 12, None, None);
        let error = storage
            .iter("Identity".to_owned(), "IdentityOf".to_owned())
            .await
            .err()
            .expect("iter must not answer offline");

        assert!(matches!(error, ChainError::StorageNotArchived { .. }));
    }
}
