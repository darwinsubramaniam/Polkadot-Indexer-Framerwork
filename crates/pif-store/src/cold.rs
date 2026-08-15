//! The cold tier: moving a segment from the SSD to the HDD without giving up replay.
//!
//! A segment is a file, so tiering is a **file copy** rather than a range scan-and-delete —
//! which is the whole reason the archive is laid out in segments (§9.1). What matters here is
//! not the copy but its order:
//!
//! ```text
//! copy -> fsync -> verify checksum -> record -> delete
//! ```
//!
//! never delete-then-record. A crash at any point in that sequence leaves **two** copies of
//! the segment, which is recoverable and costs disk; the reverse order leaves none, which is
//! not recoverable at all without going back to the network. Everything in this module is
//! written to be re-run from the start after a crash.
//!
//! This module owns the bytes. *Which* segments are eligible — digested, past their retention
//! — is policy, and lives in `pif-chain`.

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::time::SystemTime;

use crate::error::{Result, StoreError};

/// Streaming buffer for the copy. Large enough that an HDD sees sequential writes, small
/// enough that a segment of any size costs constant memory.
const COPY_BUFFER: usize = 64 * 1024;

/// One segment file, as the tiering task sees it before deciding anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentSpan {
    pub index: u64,
    pub from_block: u64,
    pub to_block: u64,
    /// When the segment file was last written — the closest thing to "sealed at" that cannot
    /// drift, because it is the file itself rather than a record of it. `None` when the
    /// filesystem declines to say.
    pub modified: Option<SystemTime>,
}

/// A segment that reached the cold tier intact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tiered {
    pub span: SegmentSpan,
    /// Bytes the segment occupies on the cold tier, `.seg` and `.idx` together.
    pub bytes: u64,
    /// CRC32 of the `.seg` file, big-endian. Verified against the copy before the hot
    /// original is deleted, and recorded so the same check can be made again later.
    pub checksum: Vec<u8>,
}

/// What a tiering pass needs of an archive, so that a block and the state read at it move
/// together rather than through two lookalike code paths.
///
/// The *policy* — which segments are eligible, and what the watermarks say about it — is not
/// here. It needs Postgres, so it lives in `pif-chain`; this crate owns bytes on disk and
/// nothing else.
/// `Send + Sync` because the tiering task is raced against fetch and digest inside one
/// chain's `tokio::select!`, and a chain runs on a spawned task.
pub trait Tierable: Send + Sync {
    /// Which archive this is, for logs and for the operator: `blocks` or `storage`.
    fn kind(&self) -> &'static str;

    /// Where tiered segments go, or `None` when everything stays hot.
    fn cold_root(&self) -> Option<&Path>;

    /// Every segment still on the hot tier, ascending by block number.
    fn hot_segments(&self, chain: &str) -> Result<Vec<SegmentSpan>>;

    /// Copy one segment to the cold tier and verify it. Leaves the hot copy in place.
    fn copy_to_cold(&self, chain: &str, index: u64) -> Result<Tiered>;

    /// Delete one segment from the hot tier, returning the bytes reclaimed.
    fn drop_hot(&self, chain: &str, index: u64) -> Result<u64>;
}

/// Copy one file to `dst`, verify the copy reads back byte-for-byte, and return its length
/// and CRC32.
///
/// The destination is written under a `.partial` name and renamed, so a reader never sees a
/// half-copied segment: an interrupted copy leaves a `.partial` file the next pass overwrites,
/// not a truncated `.seg` that would read as corruption.
pub(crate) fn copy_verified(src: &Path, dst: &Path) -> Result<(u64, u32)> {
    if let Some(dir) = dst.parent() {
        std::fs::create_dir_all(dir).map_err(StoreError::io("creating the cold directory", dir))?;
    }

    let partial = partial_path(dst);
    let (bytes, checksum) = {
        let mut source = File::open(src).map_err(StoreError::io("opening", src))?;
        let mut target = File::create(&partial).map_err(StoreError::io("creating", &partial))?;

        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = vec![0u8; COPY_BUFFER];
        let mut bytes = 0u64;

        loop {
            let read = source
                .read(&mut buffer)
                .map_err(StoreError::io("reading", src))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            target
                .write_all(&buffer[..read])
                .map_err(StoreError::io("writing", &partial))?;
            bytes += read as u64;
        }

        // Before the rename, not after: the name is what makes the file findable, so the
        // contents have to be durable first.
        target
            .sync_all()
            .map_err(StoreError::io("syncing", &partial))?;
        (bytes, hasher.finalize())
    };

    std::fs::rename(&partial, dst).map_err(StoreError::io("installing", dst))?;

    // Read the destination back rather than trusting the write. This is the whole point of
    // the order: a bad disk on the cold tier must be found while the hot copy is still there
    // to re-copy from, not years later when it is the only copy left.
    let found = checksum_of(dst)?;
    if found != checksum {
        return Err(StoreError::corrupt(
            dst,
            format!(
                "the copy does not match its source (crc {found:08x} against {checksum:08x}); \
                 the hot copy has been left in place"
            ),
        ));
    }

    Ok((bytes, checksum))
}

/// CRC32 of a whole file, streamed.
pub(crate) fn checksum_of(path: &Path) -> Result<u32> {
    let mut file = File::open(path).map_err(StoreError::io("opening", path))?;
    let mut hasher = crc32fast::Hasher::new();
    let mut buffer = vec![0u8; COPY_BUFFER];

    loop {
        let read = file
            .read(&mut buffer)
            .map_err(StoreError::io("reading", path))?;
        if read == 0 {
            return Ok(hasher.finalize());
        }
        hasher.update(&buffer[..read]);
    }
}

/// Remove a file that is expected to be there, treating "already gone" as done.
///
/// A previous pass that was interrupted between the delete and the next step is the ordinary
/// case, not an error: the archive is a cache of what the chain already holds, and the whole
/// sequence is written to be re-runnable.
pub(crate) fn remove(path: &Path) -> Result<u64> {
    let bytes = match std::fs::metadata(path) {
        Ok(meta) => meta.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(StoreError::Io {
                operation: "stat",
                path: path.to_path_buf(),
                source,
            });
        }
    };

    match std::fs::remove_file(path) {
        Ok(()) => Ok(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(source) => Err(StoreError::Io {
            operation: "removing",
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// `foo.seg` -> `foo.seg.partial`.
///
/// Appended rather than substituted, so the result never collides with a real segment name —
/// and so [`crate::layout::stem_start`] does not recognise it, which keeps a half-copied file
/// invisible to everything that lists a tier.
fn partial_path(dst: &Path) -> std::path::PathBuf {
    let mut name = dst.file_name().unwrap_or_default().to_os_string();
    name.push(".partial");
    dst.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_copy_is_verified_against_its_source() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("a.seg");
        let dst = dir.path().join("cold/a.seg");
        std::fs::write(&src, vec![7u8; 200_000]).expect("write");

        let (bytes, crc) = copy_verified(&src, &dst).expect("copy");
        assert_eq!(bytes, 200_000);
        assert_eq!(std::fs::read(&dst).expect("read"), vec![7u8; 200_000]);
        assert_eq!(crc, checksum_of(&src).expect("crc"));
    }

    #[test]
    fn nothing_half_copied_is_left_under_a_segment_name() {
        // A `.partial` that the next pass overwrites, never a short `.seg` that would read
        // as corruption on the tier that is meant to be the durable one.
        let dir = tempfile::tempdir().expect("tempdir");
        let src = dir.path().join("a.seg");
        let dst = dir.path().join("cold/a.seg");
        std::fs::write(&src, b"contents").expect("write");
        std::fs::create_dir_all(dir.path().join("cold")).expect("mkdir");
        std::fs::write(dir.path().join("cold/a.seg.partial"), b"junk from a crash").expect("write");

        copy_verified(&src, &dst).expect("copy");
        assert_eq!(std::fs::read(&dst).expect("read"), b"contents");
        assert!(!dir.path().join("cold/a.seg.partial").exists());
    }

    #[test]
    fn removing_something_already_gone_is_not_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("a.seg");
        std::fs::write(&path, b"12345").expect("write");

        assert_eq!(remove(&path).expect("remove"), 5);
        assert_eq!(remove(&path).expect("remove again"), 0);
    }
}
