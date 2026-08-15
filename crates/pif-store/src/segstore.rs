//! A number-keyed byte store built out of segment files.
//!
//! The shared body of both things the archive holds: blocks, and the answers to the storage
//! reads a handler made at a block. Both are keyed by block number, both are written once
//! and read many times, and both want the same crash behaviour — so they are one
//! implementation parameterised by a directory name rather than two that drift apart.
//!
//! What sits *above* this differs entirely: a block is one record per number, while a
//! block's storage reads are a set of them. That is why this layer deals in opaque payloads
//! and knows nothing about either shape.
//!
//! There are up to two roots: the hot tier, and an optional cold one. **Writes only ever go
//! to the hot tier**; reads try hot first and fall through to cold. That asymmetry is the
//! whole of tiering as far as this layer is concerned — a segment that has moved is found in
//! the other place, and nothing above here has to know which tier answered.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::cold::{self, SegmentSpan, Tiered};
use crate::error::{Result, StoreError};
use crate::layout;
use crate::segment::{SegmentReader, SegmentWriter};

/// zstd level. 3 is the library default and sits where the curve bends: most of the ratio,
/// none of the CPU that would make the fetch stage compression-bound.
const COMPRESSION_LEVEL: i32 = 3;

pub(crate) struct Segments {
    root: PathBuf,
    /// Where digested segments are moved to. `None` keeps everything hot.
    cold: Option<PathBuf>,
    /// Subdirectory under `<root>/<chain_id>` — [`layout::BLOCKS`] or [`layout::STORAGE`].
    kind: &'static str,
    segment_size: u64,
    /// The segment currently being appended to. Held open so a sequential backfill is not
    /// one `open(2)` per record.
    writer: Mutex<Option<Active<SegmentWriter>>>,
    /// The segment most recently read from. The digest walks blocks in order, so one slot
    /// serves nearly every read.
    reader: Mutex<Option<CachedReader>>,
}

struct Active<T> {
    chain: String,
    index: u64,
    inner: T,
}

impl<T> Active<T> {
    fn matches(&self, chain: &str, index: u64) -> bool {
        self.index == index && self.chain == chain
    }
}

/// The open reader, and which tier it came from.
///
/// The tier matters because appends only ever land hot: a reader open on a segment's cold
/// copy cannot be topped up from a write it never saw, so it has to be reopened instead.
struct CachedReader {
    chain: String,
    index: u64,
    inner: SegmentReader,
    hot: bool,
}

impl CachedReader {
    fn matches(&self, chain: &str, index: u64) -> bool {
        self.index == index && self.chain == chain
    }
}

/// What one kind of store occupies on disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub segments: u64,
    pub bytes: u64,
}

impl Segments {
    pub(crate) fn new(
        root: PathBuf,
        cold: Option<PathBuf>,
        kind: &'static str,
        segment_size: u64,
    ) -> Result<Self> {
        if segment_size == 0 {
            return Err(StoreError::InvalidSegmentSize);
        }
        std::fs::create_dir_all(&root).map_err(StoreError::io("creating store root", &root))?;
        if let Some(cold) = &cold {
            if *cold == root {
                return Err(StoreError::ColdPathIsHotPath(root));
            }
            std::fs::create_dir_all(cold)
                .map_err(StoreError::io("creating the cold store root", cold))?;
        }

        Ok(Self {
            root,
            cold,
            kind,
            segment_size,
            writer: Mutex::new(None),
            reader: Mutex::new(None),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn cold_root(&self) -> Option<&Path> {
        self.cold.as_deref()
    }

    pub(crate) fn segment_size(&self) -> u64 {
        self.segment_size
    }

    /// Append one record. Not durable on return — see [`Segments::sync`].
    ///
    /// Always to the hot tier. A block whose segment has already been tiered and is written
    /// again — a re-fetch over old ground — lands hot and shadows the cold copy, which is
    /// the same rule reads follow.
    pub(crate) fn put(&self, chain: &str, number: u64, payload: &[u8]) -> Result<()> {
        let index = layout::segment_index(number, self.segment_size);
        let compressed = zstd::stream::encode_all(payload, COMPRESSION_LEVEL)
            .map_err(StoreError::io("compressing record", &self.root))?;

        let mut slot = self.writer.lock().expect("store writer lock poisoned");
        if !slot.as_ref().is_some_and(|w| w.matches(chain, index)) {
            // Sealing here rather than on a timer: the previous segment is finished the
            // moment the numbers move past it, and nothing else knows that.
            if let Some(mut previous) = slot.take() {
                previous.inner.sync()?;
            }
            let (seg, idx) =
                layout::segment_paths(&self.root, chain, self.kind, index, self.segment_size)?;
            let dir = seg.parent().expect("segment paths always have a parent");
            std::fs::create_dir_all(dir)
                .map_err(StoreError::io("creating segment directory", dir))?;

            *slot = Some(Active {
                chain: chain.to_owned(),
                index,
                inner: SegmentWriter::open(&seg, &idx)?,
            });
        }

        slot.as_mut()
            .expect("just opened")
            .inner
            .append(number, &compressed)?;

        // A reader already open on this segment holds an index built before that append, so
        // it would keep serving the *superseded* record — the storage cache merges by
        // appending a second record for the same number, and re-fetching a block does the
        // same. Topping the reader up costs only the bytes just written; invalidating it
        // instead would make every digest read reopen the file it is walking through.
        //
        // A reader that is open on the *cold* copy of this segment is dropped instead: it
        // cannot be topped up from a file the write did not touch.
        let mut reader_slot = self.reader.lock().expect("store reader lock poisoned");
        let mut superseded = false;
        if let Some(reader) = reader_slot.as_mut()
            && reader.matches(chain, index)
        {
            if reader.hot {
                reader.inner.refresh()?;
            } else {
                superseded = true;
            }
        }
        if superseded {
            *reader_slot = None;
        }

        Ok(())
    }

    /// Read one record back. `Ok(None)` means it was never written.
    pub(crate) fn get(&self, chain: &str, number: u64) -> Result<Option<Vec<u8>>> {
        let index = layout::segment_index(number, self.segment_size);
        let Some(compressed) = self
            .with_reader(chain, index, |reader| reader.read(number))?
            .flatten()
        else {
            return Ok(None);
        };

        let payload = zstd::stream::decode_all(compressed.as_slice()).map_err(|source| {
            StoreError::corrupt(
                &self.root,
                format!("record {number} did not decompress: {source}"),
            )
        })?;

        Ok(Some(payload))
    }

    /// Whether a record exists, without paying to decompress it.
    pub(crate) fn has(&self, chain: &str, number: u64) -> Result<bool> {
        let index = layout::segment_index(number, self.segment_size);
        self.with_reader(chain, index, |reader| {
            if reader.contains(number) {
                return Ok(true);
            }
            reader.refresh()?;
            Ok(reader.contains(number))
        })
        .map(|found| found.unwrap_or(false))
    }

    /// Flush written records to disk.
    pub(crate) fn sync(&self) -> Result<()> {
        let mut slot = self.writer.lock().expect("store writer lock poisoned");
        match slot.as_mut() {
            Some(active) => active.inner.sync(),
            None => Ok(()),
        }
    }

    /// Highest `n` such that every number in `from..=n` is present.
    ///
    /// `Ok(None)` means `from` itself is missing, so there is no contiguous run at all.
    pub(crate) fn contiguous_end(&self, chain: &str, from: u64) -> Result<Option<u64>> {
        let mut last = None;
        let mut number = from;

        loop {
            let index = layout::segment_index(number, self.segment_size);
            let segment_end = layout::segment_end(index, self.segment_size);

            let found = self.with_reader(chain, index, |reader| {
                reader.refresh()?;
                let mut n = number;
                while n <= segment_end && reader.contains(n) {
                    n += 1;
                }
                Ok(n)
            })?;

            // A missing segment file ends the run just as a missing record does.
            let Some(next) = found else { return Ok(last) };
            if next == number {
                return Ok(last);
            }

            last = Some(next - 1);
            number = next;

            if number <= segment_end {
                return Ok(last);
            }
        }
    }

    /// The lowest number this chain holds, if any.
    ///
    /// Found from the segment file names — which are the range they cover — rather than by
    /// scanning, because the layout puts that information in the path on purpose. Both tiers
    /// count: a tiered segment is still archived, and a replay reads it back.
    pub(crate) fn first_number(&self, chain: &str) -> Result<Option<u64>> {
        let mut lowest: Option<u64> = None;
        self.for_each_segment(chain, |name, _| {
            if name.ends_with(".seg")
                && let Some(from) = layout::stem_start(name)
            {
                let index = layout::segment_index(from, self.segment_size);
                lowest = Some(lowest.map_or(index, |current: u64| current.min(index)));
            }
            Ok(())
        })?;

        let Some(index) = lowest else {
            return Ok(None);
        };

        // The file names give the segment; only the index inside it knows which number in
        // that range is actually the first, since a chain can start mid-segment.
        Ok(self
            .with_reader(chain, index, |reader| {
                reader.refresh()?;
                Ok(reader.numbers().next())
            })?
            .flatten())
    }

    /// What this chain occupies on the hot tier.
    pub(crate) fn usage(&self, chain: &str) -> Result<Usage> {
        self.usage_of(&self.root, chain)
    }

    /// What this chain occupies on the cold tier, or nothing when there is none.
    pub(crate) fn cold_usage(&self, chain: &str) -> Result<Usage> {
        match &self.cold {
            Some(cold) => self.usage_of(cold, chain),
            None => Ok(Usage::default()),
        }
    }

    /// Every segment on the hot tier, ascending — what the tiering task decides over.
    ///
    /// Read from the directory rather than from a table: the file name *is* the range it
    /// covers, so a listing cannot disagree with the store the way a record of it could.
    pub(crate) fn hot_segments(&self, chain: &str) -> Result<Vec<SegmentSpan>> {
        let dir = layout::segment_dir(&self.root, chain, self.kind)?;
        let mut spans = Vec::new();

        for_each_file(&dir, |name, entry| {
            if !name.ends_with(".seg") {
                return Ok(());
            }
            let Some(from) = layout::stem_start(name) else {
                return Ok(());
            };
            let index = layout::segment_index(from, self.segment_size);
            spans.push(SegmentSpan {
                index,
                from_block: layout::segment_start(index, self.segment_size),
                to_block: layout::segment_end(index, self.segment_size),
                modified: entry.metadata().ok().and_then(|m| m.modified().ok()),
            });
            Ok(())
        })?;

        spans.sort_by_key(|span| span.index);
        Ok(spans)
    }

    /// Copy one segment to the cold tier and verify it landed intact.
    ///
    /// The hot copy is deliberately **left in place**: the caller records the move first and
    /// calls [`Segments::drop_hot`] afterwards, so a crash between the two leaves two copies
    /// rather than none.
    pub(crate) fn copy_to_cold(&self, chain: &str, index: u64) -> Result<Tiered> {
        let Some(cold) = &self.cold else {
            return Err(StoreError::NoColdTier);
        };

        let (hot_seg, hot_idx) =
            layout::segment_paths(&self.root, chain, self.kind, index, self.segment_size)?;
        let (cold_seg, cold_idx) =
            layout::segment_paths(cold, chain, self.kind, index, self.segment_size)?;

        let (seg_bytes, checksum) = cold::copy_verified(&hot_seg, &cold_seg)?;
        // The sidecar is a rebuildable index, not data — but copying it turns a cold read
        // from a full scan into a seek, and it is a fraction of a percent of the bytes.
        let idx_bytes = if hot_idx.exists() {
            cold::copy_verified(&hot_idx, &cold_idx)?.0
        } else {
            0
        };

        Ok(Tiered {
            span: SegmentSpan {
                index,
                from_block: layout::segment_start(index, self.segment_size),
                to_block: layout::segment_end(index, self.segment_size),
                modified: None,
            },
            bytes: seg_bytes + idx_bytes,
            checksum: checksum.to_be_bytes().to_vec(),
        })
    }

    /// Delete one segment from the hot tier, returning the bytes reclaimed.
    ///
    /// Any cached handle on it is dropped first. On Unix an open descriptor keeps reading the
    /// unlinked inode, so a stale reader would keep answering from a file nobody can see —
    /// correct bytes, but it would hide the move from everything that checks a tier.
    pub(crate) fn drop_hot(&self, chain: &str, index: u64) -> Result<u64> {
        {
            let mut slot = self.writer.lock().expect("store writer lock poisoned");
            if slot.as_ref().is_some_and(|w| w.matches(chain, index)) {
                let mut active = slot.take().expect("just checked");
                active.inner.sync()?;
            }
        }
        {
            let mut slot = self.reader.lock().expect("store reader lock poisoned");
            if slot.as_ref().is_some_and(|r| r.matches(chain, index)) {
                *slot = None;
            }
        }

        let (seg, idx) =
            layout::segment_paths(&self.root, chain, self.kind, index, self.segment_size)?;
        Ok(cold::remove(&seg)? + cold::remove(&idx)?)
    }

    fn usage_of(&self, root: &Path, chain: &str) -> Result<Usage> {
        let dir = layout::segment_dir(root, chain, self.kind)?;
        let mut usage = Usage::default();
        for_each_file(&dir, |name, entry| {
            if name.ends_with(".seg") {
                usage.segments += 1;
            }
            usage.bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
            Ok(())
        })?;
        Ok(usage)
    }

    /// Visit every segment file this chain has on either tier, hot first.
    ///
    /// A name seen on both tiers is visited once. That happens for as long as it takes a
    /// tiering pass to delete the hot copy after recording the cold one — and, after a crash
    /// in that window, indefinitely.
    fn for_each_segment(
        &self,
        chain: &str,
        mut visit: impl FnMut(&str, u64) -> Result<()>,
    ) -> Result<()> {
        let mut seen: HashSet<String> = HashSet::new();

        for root in [Some(&self.root), self.cold.as_ref()].into_iter().flatten() {
            let dir = layout::segment_dir(root, chain, self.kind)?;
            for_each_file(&dir, |name, entry| {
                if !seen.insert(name.to_owned()) {
                    return Ok(());
                }
                visit(name, entry.metadata().map(|m| m.len()).unwrap_or(0))
            })?;
        }
        Ok(())
    }

    /// Run `f` against the reader for one segment, opening it if the cached slot holds a
    /// different one. `Ok(None)` means the segment is on neither tier.
    fn with_reader<T>(
        &self,
        chain: &str,
        index: u64,
        f: impl FnOnce(&mut SegmentReader) -> Result<T>,
    ) -> Result<Option<T>> {
        let mut slot = self.reader.lock().expect("store reader lock poisoned");

        if !slot.as_ref().is_some_and(|r| r.matches(chain, index)) {
            let Some((seg, idx, hot)) = self.locate(chain, index)? else {
                *slot = None;
                return Ok(None);
            };
            match SegmentReader::open(&seg, &idx)? {
                Some(reader) => {
                    *slot = Some(CachedReader {
                        chain: chain.to_owned(),
                        index,
                        inner: reader,
                        hot,
                    });
                }
                None => {
                    *slot = None;
                    return Ok(None);
                }
            }
        }

        f(&mut slot.as_mut().expect("just opened").inner).map(Some)
    }

    /// Where a segment's files are, and whether that is the hot tier.
    ///
    /// Hot wins when both hold it. They are identical while that is true — a cold copy is
    /// only made from a verified read of the hot one — so the choice is about which tier is
    /// cheaper to read, not about which is right.
    fn locate(&self, chain: &str, index: u64) -> Result<Option<(PathBuf, PathBuf, bool)>> {
        let (seg, idx) =
            layout::segment_paths(&self.root, chain, self.kind, index, self.segment_size)?;
        if seg.exists() {
            return Ok(Some((seg, idx, true)));
        }

        let Some(cold) = &self.cold else {
            return Ok(None);
        };
        let (seg, idx) = layout::segment_paths(cold, chain, self.kind, index, self.segment_size)?;
        if seg.exists() {
            Ok(Some((seg, idx, false)))
        } else {
            Ok(None)
        }
    }
}

/// Visit every file in one directory. A directory that does not exist yet is an empty store,
/// not a failure — neither tier is provisioned before something is written to it.
fn for_each_file(
    dir: &Path,
    mut visit: impl FnMut(&str, &std::fs::DirEntry) -> Result<()>,
) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(StoreError::Io {
                operation: "listing segments",
                path: dir.to_path_buf(),
                source,
            });
        }
    };

    for entry in entries {
        let entry = entry.map_err(StoreError::io("listing segments", dir))?;
        let name = entry.file_name();
        visit(&name.to_string_lossy(), &entry)?;
    }
    Ok(())
}
