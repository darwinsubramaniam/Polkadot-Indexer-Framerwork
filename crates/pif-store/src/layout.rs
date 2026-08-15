//! Where a block lives on disk — computed, never looked up.
//!
//! Segments are fixed-size and aligned, so `segment_index = number / segment_size` resolves
//! a block to a file with no database round-trip at all. That is why `segments` in Postgres
//! records tier and checksum but not a path: a location that is *derived* cannot drift out
//! of sync with a table recording where it was supposed to be.

use std::path::{Path, PathBuf};

use crate::error::{Result, StoreError};

/// Blocks per segment file.
///
/// One file per block would be 28M inodes on Polkadot; one file for everything gives up
/// random access. A thousand blocks keeps tiering a **file copy** rather than a range
/// scan-and-delete, and keeps replay a **sequential** read — the difference between usable
/// and unusable on an HDD.
pub const DEFAULT_SEGMENT_SIZE: u64 = 1000;

/// Which segment holds this block.
pub fn segment_index(number: u64, segment_size: u64) -> u64 {
    number / segment_size
}

/// First block number a segment covers.
pub fn segment_start(index: u64, segment_size: u64) -> u64 {
    index * segment_size
}

/// Last block number a segment covers, inclusive.
pub fn segment_end(index: u64, segment_size: u64) -> u64 {
    segment_start(index, segment_size) + segment_size - 1
}

/// Archived blocks: `<root>/<chain_id>/blocks`.
pub const BLOCKS: &str = "blocks";

/// Archived answers to handler storage reads: `<root>/<chain_id>/storage`.
///
/// A sibling of [`BLOCKS`] rather than a second key shape inside it. The two are keyed the
/// same way — by block number — which is what lets them share segment plumbing and, later,
/// tier to cold storage together; but a block is one record and a block's storage reads are
/// a set of them, so they are different stores holding different things.
pub const STORAGE: &str = "storage";

/// `<root>/<chain_id>/<kind>`
pub fn segment_dir(root: &Path, chain: &str, kind: &str) -> Result<PathBuf> {
    Ok(chain_dir(root, chain)?.join(kind))
}

/// `<root>/<chain_id>/meta`
pub fn meta_dir(root: &Path, chain: &str) -> Result<PathBuf> {
    Ok(chain_dir(root, chain)?.join("meta"))
}

/// `<root>/<chain_id>/meta/<spec_version>.scale`
pub fn metadata_path(root: &Path, chain: &str, spec_version: u32) -> Result<PathBuf> {
    Ok(meta_dir(root, chain)?.join(format!("{spec_version}.scale")))
}

/// The `.seg` and `.idx` pair for one segment.
pub fn segment_paths(
    root: &Path,
    chain: &str,
    kind: &str,
    index: u64,
    segment_size: u64,
) -> Result<(PathBuf, PathBuf)> {
    let dir = segment_dir(root, chain, kind)?;
    let stem = segment_stem(index, segment_size);
    Ok((
        dir.join(format!("{stem}.seg")),
        dir.join(format!("{stem}.idx")),
    ))
}

/// `000012800-000013799` — zero-padded so an `ls` sorts in block order.
pub fn segment_stem(index: u64, segment_size: u64) -> String {
    format!(
        "{:09}-{:09}",
        segment_start(index, segment_size),
        segment_end(index, segment_size)
    )
}

/// The first block a segment file name covers, or `None` for a name that is not one.
///
/// The inverse of [`segment_stem`], and the reason the range is in the file name at all: a
/// directory listing is enough to know what a tier holds, with nothing to consult and
/// nothing to keep in step.
pub fn stem_start(file_name: &str) -> Option<u64> {
    let stem = file_name
        .strip_suffix(".seg")
        .or_else(|| file_name.strip_suffix(".idx"))?;
    stem.split_once('-')?.0.parse().ok()
}

fn chain_dir(root: &Path, chain: &str) -> Result<PathBuf> {
    check_chain_id(chain)?;
    Ok(root.join(chain))
}

/// Refuse a chain id that would escape the store root.
pub fn check_chain_id(chain: &str) -> Result<()> {
    let ok = !chain.is_empty()
        && chain != "."
        && chain != ".."
        && chain
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));

    if ok {
        Ok(())
    } else {
        Err(StoreError::InvalidChainId(chain.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_resolves_to_a_file_without_a_lookup() {
        assert_eq!(segment_index(0, 1000), 0);
        assert_eq!(segment_index(999, 1000), 0);
        assert_eq!(segment_index(1000, 1000), 1);
        assert_eq!(segment_index(12_800, 1000), 12);
    }

    #[test]
    fn segment_bounds_are_inclusive_and_aligned() {
        assert_eq!(segment_start(12, 1000), 12_000);
        assert_eq!(segment_end(12, 1000), 12_999);
        assert_eq!(segment_stem(12, 1000), "000012000-000012999");
    }

    #[test]
    fn every_block_in_a_segment_maps_back_to_it() {
        for number in 0..3_000u64 {
            let index = segment_index(number, 500);
            assert!(number >= segment_start(index, 500));
            assert!(number <= segment_end(index, 500));
        }
    }

    #[test]
    fn a_file_name_names_the_range_it_covers() {
        // What the tiering task reads a directory with: no table, nothing to keep in step.
        assert_eq!(stem_start("000012000-000012999.seg"), Some(12_000));
        assert_eq!(stem_start("000012000-000012999.idx"), Some(12_000));
        assert_eq!(stem_start("000012000-000012999.seg.partial"), None);
        assert_eq!(stem_start("notes.txt"), None);
    }

    #[test]
    fn a_chain_id_cannot_escape_the_store_root() {
        assert!(check_chain_id("polkadot").is_ok());
        assert!(check_chain_id("asset-hub_2.0").is_ok());
        assert!(check_chain_id("../../etc").is_err());
        assert!(check_chain_id("..").is_err());
        assert!(check_chain_id("a/b").is_err());
        assert!(check_chain_id("").is_err());
    }
}
