//! The archive record: everything needed to decode one block with no network attached.

use parity_scale_codec::{Decode, Encode};

/// A block as it is archived — IPD-002 §9.1.
///
/// Raw, not decoded, on purpose. Decoding is the part most likely to have bugs and the part
/// a replay exists to redo; archiving an already-decoded block would freeze today's decoder
/// into the archive and forfeit the point of having one.
///
/// Two of these fields are easy to leave out and fatal to leave out:
///
/// * **`events`.** `System::Events` is a *storage item* read at block N, not part of the
///   block. An archive of header + extrinsics sends every replay back to an archive node for
///   events, which is a re-download wearing a replay's clothes.
/// * **`spec_version`.** It cannot be recovered from the block bytes — decoding is entirely
///   metadata-driven, so without it a stored block names no decoder.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RawBlock {
    /// The key. Also the ordering, and the digest's sequence.
    ///
    /// Keying by number is correct only because the store holds *finalized* blocks
    /// exclusively (IPD-002 §8). Under a fork, block number is not unique — block hash is.
    /// Anything that later archives unfinalized blocks gets a silent key collision rather
    /// than an error, and must re-key by hash first.
    pub number: u64,

    pub hash: [u8; 32],

    /// The spec version of the runtime that **executed** this block — the runtime at its
    /// *parent*, not the one `state_getRuntimeVersion(this block)` reports.
    ///
    /// The two differ by exactly one block, at every runtime upgrade. The block carrying
    /// `set_code` is executed by the old runtime, so its events and extrinsics are encoded
    /// against the old metadata — but its post-state already holds the new `:code`, so the
    /// node reports the *new* version for it. Archiving the reported version stores the wrong
    /// decoder for that block, and a replay fails on it forever after, long past the point
    /// where the mistake could be traced.
    pub spec_version: u32,

    /// Required by `OfflineClient::at_block`, which resolves `(spec_version,
    /// transaction_version)` as a pair and errors if either is missing. Not needed to decode;
    /// needed to build the client that decodes, which is why a replay cannot reconstruct it
    /// from the block alone.
    pub transaction_version: u32,

    /// SCALE-encoded header. Archived rather than re-derived because `block_header()` is on
    /// subxt's *online* client only.
    pub header: Vec<u8>,

    /// One blob per extrinsic, not one concatenated SCALE blob — that is the shape
    /// `ExtrinsicsClient::from_bytes` accepts on the way back in.
    pub extrinsics: Vec<Vec<u8>>,

    /// `System::Events` at this block, as the raw blob. Decoding it needs the executing
    /// runtime's metadata, which is archived separately, once per `spec_version`.
    pub events: Vec<u8>,
}

impl RawBlock {
    /// Bytes this record occupies once encoded, near enough for sizing reports.
    pub fn encoded_len(&self) -> usize {
        self.header.len()
            + self.events.len()
            + self.extrinsics.iter().map(Vec::len).sum::<usize>()
            + 48
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn sample(number: u64) -> RawBlock {
        RawBlock {
            number,
            hash: [number as u8; 32],
            spec_version: 1_022_002,
            transaction_version: 26,
            header: vec![1, 2, 3, number as u8],
            extrinsics: vec![vec![9; 12], vec![7; 3]],
            events: vec![0xab; 40],
        }
    }

    #[test]
    fn round_trips_through_scale() {
        let block = sample(42);
        let encoded = block.encode();
        let decoded = RawBlock::decode(&mut &encoded[..]).expect("decodes");
        assert_eq!(block, decoded);
    }

    #[test]
    fn extrinsics_stay_separated() {
        // Concatenating them would be a silent data loss: `from_bytes` needs one blob per
        // extrinsic and cannot recover the boundaries from a single buffer.
        let block = sample(1);
        let decoded = RawBlock::decode(&mut &block.encode()[..]).expect("decodes");
        assert_eq!(decoded.extrinsics.len(), 2);
        assert_eq!(decoded.extrinsics[0].len(), 12);
        assert_eq!(decoded.extrinsics[1].len(), 3);
    }
}
