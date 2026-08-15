//! The storage read cache: what a handler asked chain state, and what it was told.
//!
//! **A block archive is not a state archive.** Handlers do not read only blocks; they read
//! chain state. `pallet_identity` emits `IdentitySet { who }` with no display name, so the
//! event says *which* key changed and only storage says *what* it changed to. Archiving
//! blocks alone therefore moves the RPC load rather than removing it — and moves it onto the
//! single most expensive, most rate-limited call a public endpoint offers: historical state
//! on an archive node.
//!
//! So the answers are archived too, keyed by the block they were read at. On the **first**
//! digest a read misses and goes to the network; on **every subsequent** digest of the same
//! block it hits, and the network is never touched.
//!
//! This is a byte-level keyed store and nothing more. The `StorageAt` decorator that uses it
//! lives in `pif-chain`, because that is where the trait and its error type live — see
//! IPD-002 §11.1.1 for why the split has to fall exactly here.

use std::path::{Path, PathBuf};

use parity_scale_codec::{Decode, Encode};

use crate::error::{Result, StoreError};
use crate::layout::{self, DEFAULT_SEGMENT_SIZE};
use crate::segstore::{Segments, Usage};

/// One storage read, identified the way a handler asked for it.
///
/// The key is `(pallet, entry, keys)` and not a hash of them, because a cache whose contents
/// cannot be read by a human is a cache nobody can debug. `keys` is a rendering the caller
/// chooses; it only ever has to be stable against itself.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ReadKey {
    pub pallet: String,
    pub entry: String,
    /// The map keys, rendered by the caller. Empty for a plain value.
    pub keys: String,
}

impl ReadKey {
    pub fn new(
        pallet: impl Into<String>,
        entry: impl Into<String>,
        keys: impl Into<String>,
    ) -> Self {
        Self {
            pallet: pallet.into(),
            entry: entry.into(),
            keys: keys.into(),
        }
    }
}

/// Every storage read made at one block, and its answer.
///
/// A `Vec` rather than a map because it is written once, read once, and holds a handful of
/// entries: a block touches a few accounts, not a few thousand. Linear search over five
/// items beats a hash map that has to be built on every load.
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct BlockReads {
    /// `None` is a cached **absence**, and storing it is not optional.
    ///
    /// "This account has no identity" is the *common* answer, not the exceptional one. A
    /// cache that only kept hits would go back to the network on every replay for the
    /// overwhelming majority of reads, which is the same as having no cache at all.
    entries: Vec<(ReadKey, Option<Vec<u8>>)>,
}

impl BlockReads {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The archived answer, distinguishing "not archived" from "archived as absent".
    ///
    /// `None` means this read was never made here; `Some(None)` means it was made and the
    /// key was genuinely absent. Collapsing the two is the bug this signature exists to
    /// prevent.
    pub fn get(&self, key: &ReadKey) -> Option<Option<&[u8]>> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, value)| value.as_deref())
    }

    /// Record an answer, replacing any previous one for the same key.
    pub fn insert(&mut self, key: ReadKey, value: Option<Vec<u8>>) {
        match self.entries.iter_mut().find(|(k, _)| *k == key) {
            Some(slot) => slot.1 = value,
            None => self.entries.push((key, value)),
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &ReadKey> {
        self.entries.iter().map(|(key, _)| key)
    }
}

/// Archived storage reads, one record per block.
///
/// A sibling of the block store under the same root, not a second key shape inside it. They
/// share a key — the block number — which is what lets a replay of a range read both
/// sequentially, and lets a later cold tier move both together.
pub struct StorageCache {
    segments: Segments,
}

impl StorageCache {
    pub fn open(path: impl Into<PathBuf>, segment_size: u64) -> Result<Self> {
        Ok(Self {
            segments: Segments::new(path.into(), layout::STORAGE, segment_size)?,
        })
    }

    pub fn open_default(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open(path, DEFAULT_SEGMENT_SIZE)
    }

    pub fn root(&self) -> &Path {
        self.segments.root()
    }

    /// Every read archived at this block. `Ok(None)` means the block was never digested with
    /// a handler that read state — which is not the same as "it read nothing".
    pub fn get(&self, chain: &str, block: u64) -> Result<Option<BlockReads>> {
        let Some(payload) = self.segments.get(chain, block)? else {
            return Ok(None);
        };

        BlockReads::decode(&mut &payload[..])
            .map(Some)
            .map_err(|source| {
                StoreError::corrupt(
                    self.root(),
                    format!(
                        "the archived storage reads for block {block} did not decode: {source}"
                    ),
                )
            })
    }

    /// Archive the reads made at one block.
    ///
    /// Appending a second record for the same block is how a re-digest that read *new* keys
    /// merges: segment readers take the last entry written for a number, so the caller loads
    /// what is there, adds to it, and writes the union back. The superseded record stays on
    /// disk as dead weight until the segment is rewritten, which is the ordinary cost of an
    /// append-only file and far cheaper than making the archive mutable.
    pub fn put(&self, chain: &str, block: u64, reads: &BlockReads) -> Result<()> {
        self.segments.put(chain, block, &reads.encode())
    }

    /// Flush archived reads to disk.
    pub fn sync(&self) -> Result<()> {
        self.segments.sync()
    }

    pub fn usage(&self, chain: &str) -> Result<Usage> {
        self.segments.usage(chain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache() -> (tempfile::TempDir, StorageCache) {
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = StorageCache::open(dir.path(), 10).expect("open");
        (dir, cache)
    }

    fn key(entry: &str, account: &str) -> ReadKey {
        ReadKey::new("Identity", entry, account)
    }

    #[test]
    fn reads_round_trip() {
        let (_dir, cache) = cache();

        let mut reads = BlockReads::default();
        reads.insert(
            key("IdentityOf", "alice"),
            Some(b"{\"display\":\"Alice\"}".to_vec()),
        );
        reads.insert(key("SuperOf", "alice"), None);
        cache.put("polkadot", 42, &reads).expect("put");
        cache.sync().expect("sync");

        let found = cache.get("polkadot", 42).expect("get").expect("present");
        assert_eq!(found, reads);
        assert_eq!(cache.get("polkadot", 43).expect("get"), None);
    }

    #[test]
    fn an_absent_key_is_archived_as_an_answer_not_as_a_gap() {
        // "This account has no identity" is the common case, not the exceptional one. A
        // cache that stored only hits would go back to the network for the overwhelming
        // majority of reads on every replay, which is the same as having no cache.
        let (_dir, cache) = cache();

        let mut reads = BlockReads::default();
        reads.insert(key("IdentityOf", "bob"), None);
        cache.put("polkadot", 7, &reads).expect("put");
        cache.sync().expect("sync");

        let found = cache.get("polkadot", 7).expect("get").expect("present");

        // Archived-as-absent.
        assert_eq!(found.get(&key("IdentityOf", "bob")), Some(None));
        // Never asked. The two must not collapse into one another.
        assert_eq!(found.get(&key("IdentityOf", "carol")), None);
    }

    #[test]
    fn a_later_record_supersedes_an_earlier_one_for_the_same_block() {
        // How a re-digest that reads *new* keys merges: load what is there, add, write the
        // union back. Without this a handler added later would permanently shadow the reads
        // the first pass archived.
        let (_dir, cache) = cache();

        let mut first = BlockReads::default();
        first.insert(key("IdentityOf", "alice"), Some(b"one".to_vec()));
        cache.put("polkadot", 3, &first).expect("put");

        let mut merged = cache.get("polkadot", 3).expect("get").expect("present");
        merged.insert(key("SuperOf", "alice"), None);
        cache.put("polkadot", 3, &merged).expect("put");
        cache.sync().expect("sync");

        let found = cache.get("polkadot", 3).expect("get").expect("present");
        assert_eq!(found.len(), 2);
        assert_eq!(
            found.get(&key("IdentityOf", "alice")),
            Some(Some(&b"one"[..]))
        );
        assert_eq!(found.get(&key("SuperOf", "alice")), Some(None));
    }

    #[test]
    fn insert_replaces_rather_than_duplicating() {
        let mut reads = BlockReads::default();
        reads.insert(key("IdentityOf", "alice"), None);
        reads.insert(key("IdentityOf", "alice"), Some(b"now set".to_vec()));

        assert_eq!(reads.len(), 1);
        assert_eq!(
            reads.get(&key("IdentityOf", "alice")),
            Some(Some(&b"now set"[..]))
        );
    }

    #[test]
    fn chains_do_not_share_a_key_space() {
        let (_dir, cache) = cache();
        let mut reads = BlockReads::default();
        reads.insert(key("IdentityOf", "alice"), None);
        cache.put("polkadot", 1, &reads).expect("put");
        cache.sync().expect("sync");

        assert!(cache.get("polkadot", 1).expect("get").is_some());
        assert!(cache.get("kusama", 1).expect("get").is_none());
    }

    #[test]
    fn the_cache_lives_beside_the_blocks_not_inside_them() {
        // Two stores under one root. If they ever shared a directory, a block record and a
        // read-set record for the same number would collide silently.
        let dir = tempfile::tempdir().expect("tempdir");
        let cache = StorageCache::open(dir.path(), 10).expect("open");

        let mut reads = BlockReads::default();
        reads.insert(key("IdentityOf", "alice"), None);
        cache.put("polkadot", 1, &reads).expect("put");
        cache.sync().expect("sync");

        assert!(dir.path().join("polkadot/storage").is_dir());
        assert!(!dir.path().join("polkadot/blocks").exists());
    }
}
