//! The hot store: a `u64 -> RawBlock` map on local disk, plus runtime metadata by version.

use std::path::{Path, PathBuf};

use parity_scale_codec::{Decode, Encode};

use crate::error::{Result, StoreError};
use crate::layout::{self, DEFAULT_SEGMENT_SIZE};
use crate::raw::RawBlock;
use crate::segstore::Segments;

/// A block archive on local disk.
///
/// Logically one thing — a map from block number to [`RawBlock`] — with segment files as its
/// encoding rather than as a second data model. Everything else here (metadata by spec
/// version, usage reporting) hangs off that.
///
/// The API is synchronous. Writes are one `write(2)` per block against an already-open file
/// and reads are one `pread`-shaped seek, so the calls are short enough to make an async
/// wrapper more machinery than it buys.
pub struct HotStore {
    segments: Segments,
}

/// What a chain occupies on disk. Reported by `pif store status`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreUsage {
    pub segments: u64,
    pub bytes: u64,
    pub runtimes: u64,
}

impl HotStore {
    /// Open (or create) a store rooted at `path`.
    ///
    /// The root is created on demand: config validation deliberately does not require it to
    /// pre-exist, because the store appears on first run rather than being provisioned.
    pub fn open(path: impl Into<PathBuf>, segment_size: u64) -> Result<Self> {
        Ok(Self {
            segments: Segments::new(path.into(), layout::BLOCKS, segment_size)?,
        })
    }

    /// Open a store with the default segment size.
    pub fn open_default(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open(path, DEFAULT_SEGMENT_SIZE)
    }

    pub fn root(&self) -> &Path {
        self.segments.root()
    }

    pub fn segment_size(&self) -> u64 {
        self.segments.segment_size()
    }

    /// Archive one block.
    ///
    /// Not durable on return — see [`HotStore::sync`]. The fetch stage syncs before it
    /// advances `fetch_watermark`, which is what keeps Postgres from claiming a block the
    /// disk does not hold.
    pub fn put_block(&self, chain: &str, block: &RawBlock) -> Result<()> {
        self.segments.put(chain, block.number, &block.encode())
    }

    /// Flush archived blocks to disk.
    pub fn sync(&self) -> Result<()> {
        self.segments.sync()
    }

    /// Read one block back. `Ok(None)` means it was never archived.
    pub fn get_block(&self, chain: &str, number: u64) -> Result<Option<RawBlock>> {
        let Some(payload) = self.segments.get(chain, number)? else {
            return Ok(None);
        };

        let block = RawBlock::decode(&mut &payload[..]).map_err(|source| {
            StoreError::corrupt(
                self.root(),
                format!("block {number} did not decode: {source}"),
            )
        })?;

        if block.number != number {
            return Err(StoreError::corrupt(
                self.root(),
                format!("segment holds block {} under key {number}", block.number),
            ));
        }

        Ok(Some(block))
    }

    /// Whether a block is archived, without paying to decompress it.
    pub fn has_block(&self, chain: &str, number: u64) -> Result<bool> {
        self.segments.has(chain, number)
    }

    /// Highest `n` such that every block in `from..=n` is archived.
    ///
    /// `Ok(None)` means `from` itself is missing, so there is no contiguous run at all.
    ///
    /// This is the store's own answer to the contiguity rule, and the only way to audit
    /// `fetch_watermark` against reality: Postgres records what was *reported* complete, the
    /// store knows what is actually *there*.
    pub fn contiguous_end(&self, chain: &str, from: u64) -> Result<Option<u64>> {
        self.segments.contiguous_end(chain, from)
    }

    /// The lowest block number this chain has archived, if any.
    pub fn first_block(&self, chain: &str) -> Result<Option<u64>> {
        self.segments.first_number(chain)
    }

    /// Archive a runtime's metadata under its spec version.
    ///
    /// Written to a temporary file and renamed, so a reader never sees a half-written
    /// runtime. Losing one of these makes every block that ran under that version
    /// permanently undecodable even though the block bytes are intact — which is why it is
    /// worth the extra syscall.
    pub fn put_metadata(&self, chain: &str, spec_version: u32, scale: &[u8]) -> Result<()> {
        let path = layout::metadata_path(self.root(), chain, spec_version)?;
        let dir = path.parent().expect("metadata paths always have a parent");
        std::fs::create_dir_all(dir).map_err(StoreError::io("creating metadata directory", dir))?;

        let temp = path.with_extension("scale.partial");
        std::fs::write(&temp, scale).map_err(StoreError::io("writing metadata", &temp))?;
        std::fs::rename(&temp, &path).map_err(StoreError::io("installing metadata", &path))?;
        Ok(())
    }

    /// Read a runtime's archived metadata. `Ok(None)` means this version was never archived.
    pub fn get_metadata(&self, chain: &str, spec_version: u32) -> Result<Option<Vec<u8>>> {
        let path = layout::metadata_path(self.root(), chain, spec_version)?;
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(StoreError::Io {
                operation: "reading metadata",
                path,
                source,
            }),
        }
    }

    /// Spec versions with archived metadata, ascending.
    pub fn archived_runtimes(&self, chain: &str) -> Result<Vec<u32>> {
        let dir = layout::meta_dir(self.root(), chain)?;
        let mut versions = Vec::new();

        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(versions),
            Err(source) => {
                return Err(StoreError::Io {
                    operation: "listing metadata",
                    path: dir,
                    source,
                });
            }
        };

        for entry in entries {
            let entry = entry.map_err(StoreError::io("listing metadata", &dir))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".scale")
                && let Ok(version) = stem.parse::<u32>()
            {
                versions.push(version);
            }
        }

        versions.sort_unstable();
        Ok(versions)
    }

    /// Bytes and segment counts for one chain.
    pub fn usage(&self, chain: &str) -> Result<StoreUsage> {
        let usage = self.segments.usage(chain)?;
        Ok(StoreUsage {
            segments: usage.segments,
            bytes: usage.bytes,
            runtimes: self.archived_runtimes(chain)?.len() as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(number: u64) -> RawBlock {
        RawBlock {
            number,
            hash: [number as u8; 32],
            spec_version: 1_022_002,
            transaction_version: 26,
            header: vec![number as u8; 80],
            extrinsics: vec![vec![1; 20], vec![2; 30]],
            events: vec![3; 120],
        }
    }

    fn store() -> (tempfile::TempDir, HotStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = HotStore::open(dir.path(), 10).expect("open");
        (dir, store)
    }

    #[test]
    fn blocks_round_trip_across_segment_boundaries() {
        let (_dir, store) = store();
        for n in 0..25u64 {
            store.put_block("polkadot", &sample(n)).expect("put");
        }
        store.sync().expect("sync");

        // Read out of order, so the reader slot has to reopen segments rather than ride a
        // sequential scan.
        for n in [24u64, 0, 13, 9, 10] {
            assert_eq!(
                store.get_block("polkadot", n).expect("get"),
                Some(sample(n))
            );
        }
        assert_eq!(store.get_block("polkadot", 25).expect("get"), None);
    }

    #[test]
    fn re_archiving_a_block_supersedes_the_old_record() {
        // `pif fetch --from` over ground already held rewrites blocks the store has. A
        // reader open on that segment must not keep serving what it read before.
        let (_dir, store) = store();
        store.put_block("polkadot", &sample(4)).expect("put");
        assert_eq!(
            store.get_block("polkadot", 4).expect("get"),
            Some(sample(4))
        );

        let mut corrected = sample(4);
        corrected.events = vec![0xff; 9];
        store.put_block("polkadot", &corrected).expect("put again");

        assert_eq!(
            store.get_block("polkadot", 4).expect("get"),
            Some(corrected),
            "the second write must win, not the cached reader's first view"
        );
    }

    #[test]
    fn a_reopened_store_still_holds_everything() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let store = HotStore::open(dir.path(), 10).expect("open");
            for n in 0..12u64 {
                store.put_block("polkadot", &sample(n)).expect("put");
            }
            store.sync().expect("sync");
        }

        let store = HotStore::open(dir.path(), 10).expect("reopen");
        for n in 0..12u64 {
            assert_eq!(
                store.get_block("polkadot", n).expect("get"),
                Some(sample(n))
            );
        }
    }

    #[test]
    fn chains_do_not_share_a_key_space() {
        // A replay pointed at another chain's segments decodes without complaint —
        // `OfflineClient` validates no chain identity — so path scoping is a real defence,
        // not tidiness.
        let (_dir, store) = store();
        store.put_block("polkadot", &sample(5)).expect("put");
        store.sync().expect("sync");

        assert!(store.get_block("polkadot", 5).expect("get").is_some());
        assert!(store.get_block("kusama", 5).expect("get").is_none());
    }

    #[test]
    fn contiguous_end_stops_at_a_hole() {
        // The case the digest must never step over: parallel fetch lands 12 before 10, and
        // "the key exists" is not "the range is ready".
        let (_dir, store) = store();
        for n in (0..10u64).chain(std::iter::once(12)) {
            store.put_block("polkadot", &sample(n)).expect("put");
        }
        store.sync().expect("sync");

        assert_eq!(store.contiguous_end("polkadot", 0).expect("scan"), Some(9));
        assert_eq!(
            store.contiguous_end("polkadot", 12).expect("scan"),
            Some(12)
        );
        assert_eq!(store.contiguous_end("polkadot", 10).expect("scan"), None);
    }

    #[test]
    fn contiguous_end_walks_past_a_segment_boundary() {
        let (_dir, store) = store();
        for n in 0..23u64 {
            store.put_block("polkadot", &sample(n)).expect("put");
        }
        store.sync().expect("sync");
        assert_eq!(store.contiguous_end("polkadot", 0).expect("scan"), Some(22));
    }

    #[test]
    fn the_first_archived_block_is_found_from_the_layout() {
        // A chain with `start_block` set archives from part-way into a segment, so the file
        // name is a lower bound and the index inside it is the answer.
        let (_dir, store) = store();
        for n in 14..25u64 {
            store.put_block("polkadot", &sample(n)).expect("put");
        }
        store.sync().expect("sync");

        assert_eq!(store.first_block("polkadot").expect("scan"), Some(14));
        assert_eq!(store.first_block("kusama").expect("scan"), None);
    }

    #[test]
    fn contiguous_end_on_an_empty_store_is_none() {
        let (_dir, store) = store();
        assert_eq!(store.contiguous_end("polkadot", 0).expect("scan"), None);
    }

    #[test]
    fn metadata_is_kept_per_spec_version() {
        let (_dir, store) = store();
        store
            .put_metadata("polkadot", 1_022_002, &[1, 2, 3])
            .expect("put");
        store
            .put_metadata("polkadot", 1_024_001, &[4, 5])
            .expect("put");

        assert_eq!(
            store.get_metadata("polkadot", 1_022_002).expect("get"),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            store.get_metadata("polkadot", 1_024_001).expect("get"),
            Some(vec![4, 5])
        );
        assert_eq!(store.get_metadata("polkadot", 999).expect("get"), None);
        assert_eq!(
            store.archived_runtimes("polkadot").expect("list"),
            vec![1_022_002, 1_024_001]
        );
    }

    #[test]
    fn usage_reports_what_is_on_disk() {
        let (_dir, store) = store();
        for n in 0..15u64 {
            store.put_block("polkadot", &sample(n)).expect("put");
        }
        store.put_metadata("polkadot", 1, &[0; 64]).expect("put");
        store.sync().expect("sync");

        let usage = store.usage("polkadot").expect("usage");
        assert_eq!(usage.segments, 2);
        assert_eq!(usage.runtimes, 1);
        assert!(usage.bytes > 0);
        assert_eq!(store.usage("kusama").expect("usage"), StoreUsage::default());
    }

    #[test]
    fn a_zero_segment_size_is_rejected_rather_than_dividing_by_zero() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(HotStore::open(dir.path(), 0).is_err());
    }
}
