//! Segment files: an append-only run of compressed block frames, plus an offset sidecar.
//!
//! ```text
//! 000012000-000012999.seg   magic, then one frame per block
//! 000012000-000012999.idx   magic, then one 20-byte entry per frame
//! ```
//!
//! A frame is `[number u64][payload_len u32][crc32 u32][payload]`, where the payload is one
//! zstd-compressed block. Compressing per block rather than per file is what keeps random
//! access possible: `get_block(n)` seeks to one frame instead of inflating a thousand.
//!
//! The `.idx` sidecar is written *after* its frame, always. That ordering is the crash
//! story: a torn write leaves the index short, so the block reads as absent and is simply
//! re-fetched — never as present-but-wrong.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Result, StoreError};

const SEG_MAGIC: &[u8; 8] = b"PIFSEG\x00\x01";
const IDX_MAGIC: &[u8; 8] = b"PIFIDX\x00\x01";

const MAGIC_LEN: u64 = 8;
/// `number` + `payload_len` + `crc32`.
const FRAME_HEADER_LEN: u64 = 8 + 4 + 4;
/// `number` + `offset` + `frame_len`.
const IDX_ENTRY_LEN: u64 = 8 + 8 + 4;

/// A frame's position within its segment: byte offset, and total length including its header.
type Located = (u64, u32);

/// Appends frames to one segment.
pub struct SegmentWriter {
    seg: File,
    idx: File,
    seg_path: PathBuf,
    idx_path: PathBuf,
    /// Byte offset the next frame will be written at.
    end: u64,
    frames: u64,
}

impl SegmentWriter {
    /// Open a segment for appending, creating it if it does not exist.
    ///
    /// Recovers from a crash mid-append by trusting the index and discarding anything past
    /// it. A frame written but not indexed is dropped rather than adopted: it may be torn,
    /// and re-fetching one block is cheaper than proving it is not.
    pub fn open(seg_path: &Path, idx_path: &Path) -> Result<Self> {
        let mut seg = open_rw(seg_path)?;
        let mut idx = open_rw(idx_path)?;

        init_or_check_magic(&mut seg, seg_path, SEG_MAGIC)?;
        init_or_check_magic(&mut idx, idx_path, IDX_MAGIC)?;

        // A torn index entry is discarded whole; a partial entry names nothing.
        let idx_len = file_len(&idx, idx_path)?;
        let frames = (idx_len - MAGIC_LEN) / IDX_ENTRY_LEN;
        let indexed_len = MAGIC_LEN + frames * IDX_ENTRY_LEN;
        if idx_len != indexed_len {
            idx.set_len(indexed_len)
                .map_err(StoreError::io("truncating index", idx_path))?;
        }

        let end = if frames == 0 {
            MAGIC_LEN
        } else {
            let (_, offset, len) = read_idx_entry(&mut idx, idx_path, frames - 1)?;
            offset + u64::from(len)
        };

        let seg_len = file_len(&seg, seg_path)?;
        if seg_len > end {
            tracing::debug!(
                segment = %seg_path.display(),
                discarded_bytes = seg_len - end,
                "discarding an unindexed tail left by an interrupted write"
            );
            seg.set_len(end)
                .map_err(StoreError::io("truncating segment", seg_path))?;
        }

        seg.seek(SeekFrom::Start(end))
            .map_err(StoreError::io("seeking segment", seg_path))?;
        idx.seek(SeekFrom::Start(indexed_len))
            .map_err(StoreError::io("seeking index", idx_path))?;

        Ok(Self {
            seg,
            idx,
            seg_path: seg_path.to_path_buf(),
            idx_path: idx_path.to_path_buf(),
            end,
            frames,
        })
    }

    /// Append one block's payload. The frame lands before its index entry, never after.
    pub fn append(&mut self, number: u64, payload: &[u8]) -> Result<()> {
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            StoreError::corrupt(&self.seg_path, format!("block {number} exceeds 4 GiB"))
        })?;
        let crc = crc32fast::hash(payload);

        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN as usize + payload.len());
        frame.extend_from_slice(&number.to_le_bytes());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(payload);

        self.seg
            .write_all(&frame)
            .map_err(StoreError::io("appending to segment", &self.seg_path))?;

        let frame_len = u32::try_from(frame.len()).expect("payload_len already bounded");
        let mut entry = Vec::with_capacity(IDX_ENTRY_LEN as usize);
        entry.extend_from_slice(&number.to_le_bytes());
        entry.extend_from_slice(&self.end.to_le_bytes());
        entry.extend_from_slice(&frame_len.to_le_bytes());

        self.idx
            .write_all(&entry)
            .map_err(StoreError::io("appending to index", &self.idx_path))?;

        self.end += frame.len() as u64;
        self.frames += 1;
        Ok(())
    }

    /// Flush both files to disk.
    ///
    /// The fetch stage calls this *before* advancing `fetch_watermark`, so the watermark can
    /// never claim a block the disk does not hold. That ordering is what removes the need to
    /// reconcile Postgres against the store on every start.
    pub fn sync(&mut self) -> Result<()> {
        self.seg
            .sync_data()
            .map_err(StoreError::io("syncing segment", &self.seg_path))?;
        self.idx
            .sync_data()
            .map_err(StoreError::io("syncing index", &self.idx_path))?;
        Ok(())
    }

    pub fn frames(&self) -> u64 {
        self.frames
    }
}

/// Reads frames back out of one segment.
///
/// Holds an in-memory offset map that it tops up on demand, because a segment is commonly
/// being appended to by the fetch stage while the digest reads it.
pub struct SegmentReader {
    seg: File,
    seg_path: PathBuf,
    idx: Option<(File, PathBuf)>,
    index: BTreeMap<u64, Located>,
    /// Bytes of the `.idx` already parsed, or — when there is no `.idx` — the `.seg` offset
    /// scanning reached.
    cursor: u64,
}

impl SegmentReader {
    /// Open a segment for reading. `Ok(None)` means the segment does not exist.
    pub fn open(seg_path: &Path, idx_path: &Path) -> Result<Option<Self>> {
        if !seg_path.exists() {
            return Ok(None);
        }

        let mut seg = open_ro(seg_path)?;
        check_magic(&mut seg, seg_path, SEG_MAGIC)?;

        // The sidecar is the fast path. Without it the frames themselves still describe the
        // segment, so a lost `.idx` costs a scan rather than the data.
        let idx = if idx_path.exists() {
            let mut file = open_ro(idx_path)?;
            check_magic(&mut file, idx_path, IDX_MAGIC)?;
            Some((file, idx_path.to_path_buf()))
        } else {
            tracing::warn!(
                segment = %seg_path.display(),
                "index sidecar missing; rebuilding it by scanning the segment"
            );
            None
        };

        let mut reader = Self {
            seg,
            seg_path: seg_path.to_path_buf(),
            idx,
            index: BTreeMap::new(),
            // Both paths start just past the magic: one counts index entries, the other
            // segment bytes.
            cursor: MAGIC_LEN,
        };
        reader.refresh()?;
        Ok(Some(reader))
    }

    /// Pick up frames appended since the last look.
    pub fn refresh(&mut self) -> Result<()> {
        match self.idx.take() {
            Some((mut file, path)) => {
                let len = file_len(&file, &path)?;
                let complete = MAGIC_LEN + (len - MAGIC_LEN) / IDX_ENTRY_LEN * IDX_ENTRY_LEN;
                if complete > self.cursor {
                    file.seek(SeekFrom::Start(self.cursor))
                        .map_err(StoreError::io("seeking index", &path))?;
                    let mut buf = vec![0u8; (complete - self.cursor) as usize];
                    file.read_exact(&mut buf)
                        .map_err(StoreError::io("reading index", &path))?;

                    for entry in buf.chunks_exact(IDX_ENTRY_LEN as usize) {
                        let number = u64::from_le_bytes(entry[0..8].try_into().expect("8 bytes"));
                        let offset = u64::from_le_bytes(entry[8..16].try_into().expect("8 bytes"));
                        let len = u32::from_le_bytes(entry[16..20].try_into().expect("4 bytes"));
                        self.index.insert(number, (offset, len));
                    }
                    self.cursor = complete;
                }
                self.idx = Some((file, path));
                Ok(())
            }
            None => self.scan_from_cursor(),
        }
    }

    /// Read one block's payload back, verifying its checksum.
    pub fn read(&mut self, number: u64) -> Result<Option<Vec<u8>>> {
        if !self.index.contains_key(&number) {
            self.refresh()?;
        }
        let Some(&(offset, len)) = self.index.get(&number) else {
            return Ok(None);
        };

        let mut frame = vec![0u8; len as usize];
        self.seg
            .seek(SeekFrom::Start(offset))
            .map_err(StoreError::io("seeking segment", &self.seg_path))?;
        self.seg.read_exact(&mut frame).map_err(|source| {
            // The index says this frame is here and it is not. A truncated or overwritten
            // file is the only way that happens, and it must be loud: silently returning
            // "absent" would let the digest step over a hole and record it as success.
            StoreError::corrupt(
                &self.seg_path,
                format!("block {number} is indexed at offset {offset} but unreadable: {source}"),
            )
        })?;

        let stored_number = u64::from_le_bytes(frame[0..8].try_into().expect("8 bytes"));
        let payload_len = u32::from_le_bytes(frame[8..12].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(frame[12..16].try_into().expect("4 bytes"));
        let payload = &frame[FRAME_HEADER_LEN as usize..];

        if stored_number != number {
            return Err(StoreError::corrupt(
                &self.seg_path,
                format!("index points at block {number} but the frame says {stored_number}"),
            ));
        }
        if payload.len() != payload_len {
            return Err(StoreError::corrupt(
                &self.seg_path,
                format!(
                    "block {number}: frame declares {payload_len} payload bytes, found {}",
                    payload.len()
                ),
            ));
        }
        if crc32fast::hash(payload) != crc {
            return Err(StoreError::corrupt(
                &self.seg_path,
                format!("block {number}: checksum mismatch"),
            ));
        }

        Ok(Some(payload.to_vec()))
    }

    /// Block numbers this segment currently holds, in order.
    pub fn numbers(&self) -> impl Iterator<Item = u64> + '_ {
        self.index.keys().copied()
    }

    pub fn contains(&self, number: u64) -> bool {
        self.index.contains_key(&number)
    }

    /// Walk the frames themselves, used when the `.idx` sidecar is gone.
    ///
    /// A torn frame at the tail stops the scan rather than failing it: that is an interrupted
    /// append, which the next fetch simply redoes.
    fn scan_from_cursor(&mut self) -> Result<()> {
        let total = file_len(&self.seg, &self.seg_path)?;
        let mut offset = self.cursor;

        while offset + FRAME_HEADER_LEN <= total {
            let mut header = [0u8; FRAME_HEADER_LEN as usize];
            self.seg
                .seek(SeekFrom::Start(offset))
                .map_err(StoreError::io("seeking segment", &self.seg_path))?;
            self.seg
                .read_exact(&mut header)
                .map_err(StoreError::io("reading segment", &self.seg_path))?;

            let number = u64::from_le_bytes(header[0..8].try_into().expect("8 bytes"));
            let payload_len = u32::from_le_bytes(header[8..12].try_into().expect("4 bytes"));
            let frame_len = FRAME_HEADER_LEN + u64::from(payload_len);

            if offset + frame_len > total {
                break;
            }

            let located = u32::try_from(frame_len).map_err(|_| {
                StoreError::corrupt(
                    &self.seg_path,
                    format!("frame for block {number} is absurd"),
                )
            })?;
            self.index.insert(number, (offset, located));
            offset += frame_len;
        }

        self.cursor = offset;
        Ok(())
    }
}

fn open_rw(path: &Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .map_err(StoreError::io("opening", path))
}

fn open_ro(path: &Path) -> Result<File> {
    File::open(path).map_err(StoreError::io("opening", path))
}

fn file_len(file: &File, path: &Path) -> Result<u64> {
    Ok(file.metadata().map_err(StoreError::io("stat", path))?.len())
}

fn init_or_check_magic(file: &mut File, path: &Path, magic: &[u8; 8]) -> Result<()> {
    if file_len(file, path)? == 0 {
        file.write_all(magic)
            .map_err(StoreError::io("writing header to", path))?;
        return Ok(());
    }
    check_magic(file, path, magic)
}

fn check_magic(file: &mut File, path: &Path, magic: &[u8; 8]) -> Result<()> {
    let mut found = [0u8; 8];
    file.seek(SeekFrom::Start(0))
        .map_err(StoreError::io("seeking", path))?;
    file.read_exact(&mut found).map_err(|source| {
        StoreError::corrupt(path, format!("file is too short to be a segment: {source}"))
    })?;

    if &found != magic {
        return Err(StoreError::corrupt(
            path,
            format!("expected magic {magic:?}, found {found:?}"),
        ));
    }
    Ok(())
}

fn read_idx_entry(idx: &mut File, path: &Path, n: u64) -> Result<(u64, u64, u32)> {
    let mut entry = [0u8; IDX_ENTRY_LEN as usize];
    idx.seek(SeekFrom::Start(MAGIC_LEN + n * IDX_ENTRY_LEN))
        .map_err(StoreError::io("seeking index", path))?;
    idx.read_exact(&mut entry)
        .map_err(StoreError::io("reading index", path))?;

    Ok((
        u64::from_le_bytes(entry[0..8].try_into().expect("8 bytes")),
        u64::from_le_bytes(entry[8..16].try_into().expect("8 bytes")),
        u32::from_le_bytes(entry[16..20].try_into().expect("4 bytes")),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(dir: &Path) -> (PathBuf, PathBuf) {
        (dir.join("s.seg"), dir.join("s.idx"))
    }

    #[test]
    fn frames_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (seg, idx) = paths(dir.path());

        let mut writer = SegmentWriter::open(&seg, &idx).expect("open");
        for n in 0..10u64 {
            writer
                .append(n, &vec![n as u8; 32 + n as usize])
                .expect("append");
        }
        writer.sync().expect("sync");
        drop(writer);

        let mut reader = SegmentReader::open(&seg, &idx)
            .expect("open")
            .expect("exists");
        for n in 0..10u64 {
            let payload = reader.read(n).expect("read").expect("present");
            assert_eq!(payload, vec![n as u8; 32 + n as usize]);
        }
        assert!(reader.read(10).expect("read").is_none());
        assert_eq!(
            reader.numbers().collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_reader_sees_blocks_appended_after_it_opened() {
        // The digest reads a segment the fetch stage is still writing; if the reader cached
        // its index once and never refreshed, the digest would stall forever one block short.
        let dir = tempfile::tempdir().expect("tempdir");
        let (seg, idx) = paths(dir.path());

        let mut writer = SegmentWriter::open(&seg, &idx).expect("open");
        writer.append(0, b"first").expect("append");

        let mut reader = SegmentReader::open(&seg, &idx)
            .expect("open")
            .expect("exists");
        assert_eq!(
            reader.read(0).expect("read").as_deref(),
            Some(&b"first"[..])
        );
        assert!(reader.read(1).expect("read").is_none());

        writer.append(1, b"second").expect("append");
        assert_eq!(
            reader.read(1).expect("read").as_deref(),
            Some(&b"second"[..])
        );
    }

    #[test]
    fn a_truncated_segment_is_an_error_not_a_panic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (seg, idx) = paths(dir.path());

        let mut writer = SegmentWriter::open(&seg, &idx).expect("open");
        writer.append(0, &[1u8; 64]).expect("append");
        writer.append(1, &[2u8; 64]).expect("append");
        writer.sync().expect("sync");
        drop(writer);

        // Lose the tail of the file while the index still promises it.
        let len = std::fs::metadata(&seg).expect("stat").len();
        let file = OpenOptions::new().write(true).open(&seg).expect("open");
        file.set_len(len - 20).expect("truncate");

        let mut reader = SegmentReader::open(&seg, &idx)
            .expect("open")
            .expect("exists");
        assert!(reader.read(0).expect("read").is_some());
        let err = reader.read(1).expect_err("truncated frame must be loud");
        assert!(
            matches!(err, StoreError::SegmentCorrupt { .. }),
            "got {err}"
        );
    }

    #[test]
    fn a_flipped_byte_fails_the_checksum() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (seg, idx) = paths(dir.path());

        let mut writer = SegmentWriter::open(&seg, &idx).expect("open");
        writer.append(7, &[0u8; 100]).expect("append");
        writer.sync().expect("sync");
        drop(writer);

        let mut bytes = std::fs::read(&seg).expect("read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&seg, &bytes).expect("write");

        let mut reader = SegmentReader::open(&seg, &idx)
            .expect("open")
            .expect("exists");
        let err = reader.read(7).expect_err("bit rot must be caught");
        assert!(err.to_string().contains("checksum"), "got {err}");
    }

    #[test]
    fn an_interrupted_append_is_discarded_on_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (seg, idx) = paths(dir.path());

        let mut writer = SegmentWriter::open(&seg, &idx).expect("open");
        writer.append(0, &[1u8; 50]).expect("append");
        writer.sync().expect("sync");
        drop(writer);

        // A frame that reached the segment but whose index entry did not: exactly what a
        // crash between the two writes leaves behind.
        let mut seg_file = OpenOptions::new().append(true).open(&seg).expect("open");
        seg_file.write_all(&[0xcc; 40]).expect("write");
        drop(seg_file);

        let mut writer = SegmentWriter::open(&seg, &idx).expect("reopen");
        assert_eq!(writer.frames(), 1);
        writer.append(1, &[2u8; 50]).expect("append");
        writer.sync().expect("sync");
        drop(writer);

        let mut reader = SegmentReader::open(&seg, &idx)
            .expect("open")
            .expect("exists");
        assert_eq!(reader.read(0).expect("read").expect("present").len(), 50);
        assert_eq!(
            reader.read(1).expect("read").expect("present"),
            vec![2u8; 50]
        );
    }

    #[test]
    fn a_lost_index_is_rebuilt_from_the_frames() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (seg, idx) = paths(dir.path());

        let mut writer = SegmentWriter::open(&seg, &idx).expect("open");
        for n in 0..5u64 {
            writer.append(n, &[n as u8; 24]).expect("append");
        }
        writer.sync().expect("sync");
        drop(writer);

        std::fs::remove_file(&idx).expect("remove index");

        let mut reader = SegmentReader::open(&seg, &idx)
            .expect("open")
            .expect("exists");
        for n in 0..5u64 {
            assert_eq!(
                reader.read(n).expect("read").expect("present"),
                vec![n as u8; 24]
            );
        }
    }
}
