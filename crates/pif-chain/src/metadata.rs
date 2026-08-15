//! Getting a runtime's metadata off the wire, in the richest format the node offers.
//!
//! Metadata has to come from the node as **bytes**: `subxt_metadata::Metadata` implements
//! `Decode` but not `Encode`, so the already-decoded metadata a live client holds cannot be
//! re-serialised into the archive. It is fetched once per unseen `spec_version` — the one
//! extra RPC call the pipeline adds, amortised across every block of a runtime.
//!
//! "Amortised" is a design obligation rather than a given. Westend metadata is ~400 KB, and
//! an indexer that built a client per block would move hundreds of kilobytes to archive a
//! few kilobytes of block. [`crate::client::ChainClient`] holds one connection open for the
//! life of a chain, and this module is only reached on a runtime it has not seen before.

use parity_scale_codec::{Decode, Encode};
use subxt::{PolkadotConfig, config::HashFor, metadata::Metadata};

use crate::client::Rpc;
use crate::error::{ChainError, Result};

/// The oldest format worth archiving.
///
/// Below V14 metadata carries no type information at all, so decoding needs an external
/// legacy type registry rather than a newer indexer. That is a different problem with a
/// different fix, and it is out of scope here.
const OLDEST_USABLE_FORMAT: u32 = 14;

/// A runtime's metadata, and which format it came back as.
pub struct FetchedMetadata {
    pub bytes: Vec<u8>,
    /// 14 | 15 | 16 — recorded because it is a fact about the *archive*, not about the
    /// runtime. The same runtime serves several formats simultaneously.
    pub format_version: u16,
}

/// Fetch the metadata for the runtime at `hash`, preferring the richest format available.
///
/// The obvious implementation — call `state_getMetadata` — silently pins the archive to
/// **V14**, the oldest format the chain offers, because that is what the legacy RPC returns.
/// That is a real choice being made by accident, and it is irreversible: once the archive
/// node that served a historical block is gone, the richer metadata for that spec version
/// can never be obtained again. V14 decodes blocks perfectly well, but V15 added runtime API
/// descriptions and outer enum types and V16 more again, so a handler that later wants a
/// runtime API finds V14 cannot describe one.
///
/// So the ladder is walked from the top: ask the runtime which formats it serves, take the
/// highest this build can actually decode, and fall back to the legacy RPC only if the
/// runtime API is not there. `Metadata::decode_from` accepts V14, V15 and V16 identically,
/// so preferring the richest costs nothing at decode time.
pub async fn fetch_at(
    rpc: &Rpc,
    chain: &str,
    spec_version: u32,
    hash: HashFor<PolkadotConfig>,
) -> Result<FetchedMetadata> {
    let offered = offered_versions(rpc, hash).await;

    let mut candidates: Vec<u32> = offered
        .iter()
        .copied()
        .filter(|v| *v >= OLDEST_USABLE_FORMAT)
        .collect();
    candidates.sort_unstable_by(|a, b| b.cmp(a));

    for version in &candidates {
        let Some(bytes) = at_version(rpc, hash, *version).await? else {
            continue;
        };

        // Whether subxt can read it is the real test, not a table of version numbers this
        // build was compiled with. A format nobody has heard of yet is archived the day
        // subxt learns to decode it, with no change here.
        if Metadata::decode_from(&bytes).is_ok() {
            if Some(*version) != candidates.first().copied() {
                tracing::warn!(
                    chain = %chain,
                    spec_version,
                    archived = *version,
                    offered = ?candidates,
                    "the node offers a richer metadata format than this build can decode; \
                     the archive keeps the best readable one"
                );
            }
            return Ok(FetchedMetadata {
                format_version: format_version(&bytes).unwrap_or(*version) as u16,
                bytes,
            });
        }
    }

    // No runtime API, or nothing it offered was readable. The legacy RPC is universally
    // available and answers V14, which every subxt can decode.
    let bytes = rpc
        .state_get_metadata(Some(hash))
        .await
        .map_err(|source| ChainError::MetadataUnavailable {
            chain: chain.to_owned(),
            spec_version,
            source: Box::new(source),
        })?
        .into_raw();

    let found = format_version(&bytes).unwrap_or(0);
    if Metadata::decode_from(&bytes).is_err() {
        return Err(ChainError::UnsupportedMetadataVersion {
            chain: chain.to_owned(),
            spec_version,
            metadata_version: found,
        });
    }

    Ok(FetchedMetadata {
        bytes,
        format_version: found as u16,
    })
}

/// Which metadata formats this runtime serves, newest last. Empty if it cannot say.
async fn offered_versions(rpc: &Rpc, hash: HashFor<PolkadotConfig>) -> Vec<u32> {
    let Ok(raw) = rpc
        .state_call("Metadata_metadata_versions", None, Some(hash))
        .await
    else {
        // Pre-`Metadata_metadata_versions` runtimes simply do not have it. Not an error:
        // the legacy RPC still answers, and this is a probe, not a requirement.
        return Vec::new();
    };

    Vec::<u32>::decode(&mut &raw[..]).unwrap_or_default()
}

/// Ask for one specific metadata format. `Ok(None)` means the runtime does not serve it.
async fn at_version(
    rpc: &Rpc,
    hash: HashFor<PolkadotConfig>,
    version: u32,
) -> Result<Option<Vec<u8>>> {
    let Ok(raw) = rpc
        .state_call(
            "Metadata_metadata_at_version",
            Some(&version.encode()),
            Some(hash),
        )
        .await
    else {
        return Ok(None);
    };

    // `Option<OpaqueMetadata>`, and `OpaqueMetadata` is a newtype over `Vec<u8>` — so the
    // wire shape is exactly `Option<Vec<u8>>` and needs no frame-metadata dependency to read.
    Ok(Option::<Vec<u8>>::decode(&mut &raw[..]).unwrap_or_default())
}

/// Read the format version out of the metadata blob itself.
///
/// `RuntimeMetadataPrefixed` is `("meta", RuntimeMetadata)`, and a SCALE enum discriminant is
/// one byte — so the fifth byte *is* the version. Reading it from the bytes rather than
/// trusting what was asked for means the recorded `metadata_version` describes what is
/// actually in the archive.
fn format_version(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < 5 || &bytes[0..4] != b"meta" {
        return None;
    }
    Some(u32::from(bytes[4]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_format_version_out_of_the_blob() {
        let mut v14 = b"meta".to_vec();
        v14.push(14);
        v14.extend_from_slice(&[0; 32]);
        assert_eq!(format_version(&v14), Some(14));

        let mut v16 = b"meta".to_vec();
        v16.push(16);
        assert_eq!(format_version(&v16), Some(16));
    }

    #[test]
    fn refuses_to_guess_at_something_that_is_not_metadata() {
        // A wrong answer here would be recorded in the archive as fact, so no heuristics.
        assert_eq!(format_version(b""), None);
        assert_eq!(format_version(b"met"), None);
        assert_eq!(format_version(b"nope\x0e"), None);
    }
}
