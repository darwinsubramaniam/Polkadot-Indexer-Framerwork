//! Errors raised by the local block archive.

use std::path::{Path, PathBuf};

pub type Result<T, E = StoreError> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{operation} failed on {}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A segment file is not what it claims to be.
    ///
    /// Recovery is always the same — re-fetch the range it covers — which is why this is one
    /// variant rather than one per way a file can be wrong. The `reason` names the specific
    /// damage so a bad disk and a truncated write are still distinguishable in the log.
    #[error(
        "segment {} is corrupt: {reason}.\n\
         Re-fetch the blocks it covers; the archive is a cache of what the chain already \
         holds, so nothing is lost by deleting it.",
        path.display()
    )]
    SegmentCorrupt { path: PathBuf, reason: String },

    /// A chain id that would escape the store root.
    ///
    /// Chain ids are config keys and become directory names, so a `../` in one would write
    /// outside the configured store. Rejected rather than sanitised: silently renaming an
    /// operator's chain id would put the archive somewhere they cannot find it.
    #[error(
        "chain id {0:?} cannot be used as a store directory name; use only letters, digits, \
         '-', '_' and '.'"
    )]
    InvalidChainId(String),

    #[error("segment_size must be greater than zero")]
    InvalidSegmentSize,
}

impl StoreError {
    pub(crate) fn io(
        operation: &'static str,
        path: impl AsRef<Path>,
    ) -> impl FnOnce(std::io::Error) -> Self {
        let path = path.as_ref().to_path_buf();
        move |source| StoreError::Io {
            operation,
            path,
            source,
        }
    }

    pub(crate) fn corrupt(path: impl AsRef<Path>, reason: impl Into<String>) -> Self {
        StoreError::SegmentCorrupt {
            path: path.as_ref().to_path_buf(),
            reason: reason.into(),
        }
    }
}
