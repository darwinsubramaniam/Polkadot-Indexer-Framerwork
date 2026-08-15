//! Decoding blocks out of the archive, with no network attached.
//!
//! This is the read side of the split: where [`crate::decode::decode_at`] resolves a block
//! over RPC, [`Archive`] resolves it from local segment files and local metadata. The
//! difference is not an optimisation — it is what makes a re-index cost a re-*digest* rather
//! than a re-download.

use std::collections::HashMap;
use std::sync::Arc;

use pif_core::ChainInfo;
use pif_db::BlockData;
use pif_store::{HotStore, RawBlock};
use subxt::{
    PolkadotConfig,
    client::OfflineClient,
    config::substrate::SpecVersionForRange,
    metadata::{ArcMetadata, Metadata},
};

use crate::decode;
use crate::error::{ChainError, Result};

/// Reads blocks back out of the archive and decodes them offline.
///
/// Holds decoded metadata per `spec_version` and one offline client, rebuilt only when the
/// runtime changes. That matters: metadata is ~400 KB per runtime and decoding it per block
/// would make the digest metadata-bound on a chain that upgrades a dozen times a year and
/// otherwise never changes.
pub struct Archive<'a> {
    store: &'a HotStore,
    chain: &'a ChainInfo,
    /// Decoded once, shared by `Arc` into every client built afterwards.
    metadata: HashMap<u32, ArcMetadata>,
    /// The client currently configured, and the runtime it is configured for.
    client: Option<(u32, u32, OfflineClient<PolkadotConfig>)>,
}

impl<'a> Archive<'a> {
    pub fn new(store: &'a HotStore, chain: &'a ChainInfo) -> Self {
        Self {
            store,
            chain,
            metadata: HashMap::new(),
            client: None,
        }
    }

    /// Read one block's archived form.
    ///
    /// A block that was indexed before the archive existed is absent here and present in
    /// Postgres — which is why this is a named error rather than a silent skip.
    pub fn get(&self, number: u64) -> Result<RawBlock> {
        self.store
            .get_block(&self.chain.id, number)?
            .ok_or_else(|| ChainError::BlockNotArchived {
                chain: self.chain.id.clone(),
                number,
                path: self.store.root().display().to_string(),
            })
    }

    /// Whether a block is archived.
    pub fn has(&self, number: u64) -> Result<bool> {
        Ok(self.store.has_block(&self.chain.id, number)?)
    }

    /// The decoded metadata for a runtime, if this archive has already loaded it.
    ///
    /// Handed to the storage cache so `has_pallet` can be answered with no node attached —
    /// otherwise it is the one `StorageAt` method with nothing to consult during a replay.
    /// Only ever a cache read: a caller that has just decoded a block is guaranteed a hit.
    pub fn metadata_for(&self, spec_version: u32) -> Option<ArcMetadata> {
        self.metadata.get(&spec_version).map(Arc::clone)
    }

    /// Decode an archived block into the rows the digest writes.
    pub async fn decode(&mut self, raw: &RawBlock) -> Result<BlockData> {
        self.ensure_client(raw)?;

        let (_, _, client) = self.client.as_ref().expect("ensure_client just set it");
        let at = client
            .at_block(raw.number)
            .map_err(|source| ChainError::ArchiveCorrupt {
                chain: self.chain.id.clone(),
                number: raw.number,
                reason: format!("no offline client could be built for it: {source}"),
            })?;

        decode::decode_stored(&at, raw, self.chain).await
    }

    /// Point the offline client at the runtime that executed this block, rebuilding it only
    /// when the runtime actually changes.
    fn ensure_client(&mut self, raw: &RawBlock) -> Result<()> {
        let wanted = (raw.spec_version, raw.transaction_version);
        if self
            .client
            .as_ref()
            .is_some_and(|(spec, tx, _)| (*spec, *tx) == wanted)
        {
            return Ok(());
        }

        let metadata = self.load_metadata(raw)?;

        let config = PolkadotConfig::builder()
            // Only the archived runtime's metadata is in play, and only one runtime is
            // current at a time, so the block range is open-ended rather than one entry per
            // block. `use_known_spec_versions` is deliberately not used: it hardcodes the
            // live Polkadot relay chain's ranges, and subxt's own docs warn it "will lead to
            // obscure errors" against anything else. The archive knows the exact spec
            // version per block, which is both correct and narrower.
            .set_metadata_for_spec_versions([(raw.spec_version, metadata)])
            .set_spec_version_for_block_ranges([SpecVersionForRange {
                block_range: 0..u64::MAX,
                spec_version: raw.spec_version,
                transaction_version: raw.transaction_version,
            }])
            // Off on purpose. The default loads `frame_decode`'s Polkadot *relay chain*
            // legacy types, which V14+ metadata never consults — so on this path they are
            // pure cost, and on a non-Polkadot chain serving pre-V14 metadata they would be
            // the wrong type registry rather than no registry.
            .use_historic_types(false)
            .build();

        self.client = Some((
            raw.spec_version,
            raw.transaction_version,
            OfflineClient::new_with_config(config),
        ));
        Ok(())
    }

    /// The decoded metadata for a block's executing runtime, from cache or from the archive.
    fn load_metadata(&mut self, raw: &RawBlock) -> Result<ArcMetadata> {
        if let Some(metadata) = self.metadata.get(&raw.spec_version) {
            return Ok(Arc::clone(metadata));
        }

        let bytes = self
            .store
            .get_metadata(&self.chain.id, raw.spec_version)?
            .ok_or_else(|| ChainError::MetadataNotArchived {
                chain: self.chain.id.clone(),
                number: raw.number,
                spec_version: raw.spec_version,
            })?;

        let metadata = Metadata::decode_from(&bytes).map_err(|source| {
            // The bytes are in the archive and unreadable. Reported against the runtime
            // rather than the block, because every block under it has the same problem.
            ChainError::ArchiveCorrupt {
                chain: self.chain.id.clone(),
                number: raw.number,
                reason: format!(
                    "the archived metadata for runtime spec {} does not decode: {source}",
                    raw.spec_version
                ),
            }
        })?;

        let metadata: ArcMetadata = Arc::new(metadata);
        self.metadata
            .insert(raw.spec_version, Arc::clone(&metadata));
        Ok(metadata)
    }
}
