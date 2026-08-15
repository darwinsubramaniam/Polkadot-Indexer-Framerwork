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

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{Result, StoreError};
use crate::layout;
use crate::segment::{SegmentReader, SegmentWriter};

/// zstd level. 3 is the library default and sits where the curve bends: most of the ratio,
/// none of the CPU that would make the fetch stage compression-bound.
const COMPRESSION_LEVEL: i32 = 3;

pub(crate) struct Segments {
    root: PathBuf,
    /// Subdirectory under `<root>/<chain_id>` — [`layout::BLOCKS`] or [`layout::STORAGE`].
    kind: &'static str,
    segment_size: u64,
    /// The segment currently being appended to. Held open so a sequential backfill is not
    /// one `open(2)` per record.
    writer: Mutex<Option<Active<SegmentWriter>>>,
    /// The segment most recently read from. The digest walks blocks in order, so one slot
    /// serves nearly every read.
    reader: Mutex<Option<Active<SegmentReader>>>,
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

/// What one kind of store occupies on disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub segments: u64,
    pub bytes: u64,
}

impl Segments {
    pub(crate) fn new(root: PathBuf, kind: &'static str, segment_size: u64) -> Result<Self> {
        if segment_size == 0 {
            return Err(StoreError::InvalidSegmentSize);
        }
        std::fs::create_dir_all(&root).map_err(StoreError::io("creating store root", &root))?;

        Ok(Self {
            root,
            kind,
            segment_size,
            writer: Mutex::new(None),
            reader: Mutex::new(None),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn segment_size(&self) -> u64 {
        self.segment_size
    }

    /// Append one record. Not durable on return — see [`Segments::sync`].
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
        if let Some(reader) = self
            .reader
            .lock()
            .expect("store reader lock poisoned")
            .as_mut()
            && reader.matches(chain, index)
        {
            reader.inner.refresh()?;
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
    /// scanning, because the layout puts that information in the path on purpose.
    pub(crate) fn first_number(&self, chain: &str) -> Result<Option<u64>> {
        let mut lowest: Option<u64> = None;
        self.for_each_segment(chain, |name, _| {
            if let Some(stem) = name.strip_suffix(".seg")
                && let Some((from, _)) = stem.split_once('-')
                && let Ok(from) = from.parse::<u64>()
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

    pub(crate) fn usage(&self, chain: &str) -> Result<Usage> {
        let mut usage = Usage::default();
        self.for_each_segment(chain, |name, len| {
            if name.ends_with(".seg") {
                usage.segments += 1;
            }
            usage.bytes += len;
            Ok(())
        })?;
        Ok(usage)
    }

    /// Visit every file in this chain's segment directory. A directory that does not exist
    /// yet is an empty store, not a failure.
    fn for_each_segment(
        &self,
        chain: &str,
        mut visit: impl FnMut(&str, u64) -> Result<()>,
    ) -> Result<()> {
        let dir = layout::segment_dir(&self.root, chain, self.kind)?;
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(StoreError::Io {
                    operation: "listing segments",
                    path: dir,
                    source,
                });
            }
        };

        for entry in entries {
            let entry = entry.map_err(StoreError::io("listing segments", &dir))?;
            let name = entry.file_name();
            let len = entry.metadata().map(|m| m.len()).unwrap_or(0);
            visit(&name.to_string_lossy(), len)?;
        }
        Ok(())
    }

    /// Run `f` against the reader for one segment, opening it if the cached slot holds a
    /// different one. `Ok(None)` means the segment file does not exist.
    fn with_reader<T>(
        &self,
        chain: &str,
        index: u64,
        f: impl FnOnce(&mut SegmentReader) -> Result<T>,
    ) -> Result<Option<T>> {
        let mut slot = self.reader.lock().expect("store reader lock poisoned");

        if !slot.as_ref().is_some_and(|r| r.matches(chain, index)) {
            let (seg, idx) =
                layout::segment_paths(&self.root, chain, self.kind, index, self.segment_size)?;
            match SegmentReader::open(&seg, &idx)? {
                Some(reader) => {
                    *slot = Some(Active {
                        chain: chain.to_owned(),
                        index,
                        inner: reader,
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
}
