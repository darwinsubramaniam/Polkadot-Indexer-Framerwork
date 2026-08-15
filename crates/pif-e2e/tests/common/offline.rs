//! Archiving a block as raw bytes, and decoding it back with no network — IPD-002 §9.1.
//!
//! This is the shape `pif-store` and `decode_stored` will take, written once here so the
//! spike (`decode_stored_spike.rs`) and the runtime-upgrade test (`upgrade_boundary.rs`)
//! exercise the *same* code rather than two lookalikes that can drift apart.

use std::collections::HashMap;

use parity_scale_codec::{Decode, Encode};
use subxt::{
    Config, OnlineClient, PolkadotConfig,
    client::{ClientAtBlock, OfflineClient, OfflineClientAtBlockT},
    config::{Hasher, RpcConfigFor, substrate::SpecVersionForRange},
    events::{Events, Phase},
    extrinsics::Extrinsics,
    metadata::{ArcMetadata, Metadata},
    rpcs::{LegacyRpcMethods, RpcClient},
};

type Header = <PolkadotConfig as Config>::Header;

/// The §9.1 archive record: everything needed to decode block N offline.
#[derive(Debug, Clone)]
pub struct RawBlock {
    pub number: u64,
    pub hash: Vec<u8>,
    /// The spec version of the runtime that **executed** this block — which is the runtime
    /// at its *parent*, not the one `state_getRuntimeVersion(this block)` reports.
    ///
    /// They differ by exactly one block, at every runtime upgrade. The block carrying
    /// `set_code` is executed by the old runtime, so its events and extrinsics are encoded
    /// against the old metadata — but its post-state already holds the new `:code`, so the
    /// node reports the *new* version for it. Archiving the reported version stores the
    /// wrong decoder for that block. See `upgrade_boundary.rs`.
    pub spec_version: u32,
    /// Required by `OfflineClient::at_block`, which resolves `(spec_version,
    /// transaction_version)` together — not needed to decode, needed to build the
    /// client that decodes.
    pub transaction_version: u32,
    pub header: Vec<u8>,
    /// One blob per extrinsic — `ExtrinsicsClient::from_bytes` takes `Vec<Vec<u8>>`,
    /// not one concatenated SCALE blob.
    pub extrinsics: Vec<Vec<u8>>,
    pub events: Vec<u8>,
    /// Archived per `spec_version` in the real store; carried per block here so a test
    /// can hand a set of blocks around as one value.
    pub metadata: Vec<u8>,
}

/// Every primitive `decode_at` (`decode.rs:105`) shapes its rows out of.
///
/// If these match, every derived field matches too — `NewBlock`/`NewEvent`/`NewExtrinsic`
/// are pure functions of exactly this projection, so comparing it keeps the test
/// independent of `decode_at`'s current shape.
#[derive(Debug, PartialEq, Eq)]
pub struct Projection {
    pub number: u64,
    pub spec_version: u32,
    pub events: Vec<EventRow>,
    pub extrinsics: Vec<ExtrinsicRow>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct EventRow {
    pub idx: u32,
    pub pallet: String,
    pub variant: String,
    pub phase: String,
    pub field_bytes: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ExtrinsicRow {
    pub idx: usize,
    pub pallet: String,
    pub call: String,
    pub hash: String,
    pub is_signed: bool,
    pub address_bytes: Option<String>,
    pub call_data_field_bytes: String,
}

fn phase_name(phase: &Phase) -> &'static str {
    match phase {
        Phase::ApplyExtrinsic(_) => "ApplyExtrinsic",
        Phase::Finalization => "Finalization",
        Phase::Initialization => "Initialization",
    }
}

/// Generic over the client, which is the point: `OnlineClientAtBlockT: OfflineClientAtBlockT`,
/// so one decode core serves the online and offline paths both.
pub fn project<C>(
    number: u64,
    spec_version: u32,
    events: &Events<PolkadotConfig>,
    extrinsics: &Extrinsics<'_, PolkadotConfig, C>,
) -> anyhow::Result<Projection>
where
    C: OfflineClientAtBlockT<PolkadotConfig>,
{
    let mut event_rows = Vec::new();
    for event in events.iter() {
        let event = event?;
        event_rows.push(EventRow {
            idx: event.index(),
            pallet: event.pallet_name().to_owned(),
            variant: event.event_name().to_owned(),
            phase: phase_name(&event.phase()).to_owned(),
            field_bytes: hex::encode(event.field_bytes()),
        });
    }

    let mut extrinsic_rows = Vec::new();
    for extrinsic in extrinsics.iter() {
        let extrinsic = extrinsic?;
        extrinsic_rows.push(ExtrinsicRow {
            idx: extrinsic.index(),
            pallet: extrinsic.pallet_name().to_owned(),
            call: extrinsic.call_name().to_owned(),
            hash: hex::encode(extrinsic.hash().0),
            is_signed: extrinsic.is_signed(),
            address_bytes: extrinsic.address_bytes().map(hex::encode),
            call_data_field_bytes: hex::encode(extrinsic.call_data_field_bytes()),
        });
    }

    Ok(Projection {
        number,
        spec_version,
        events: event_rows,
        extrinsics: extrinsic_rows,
    })
}

/// What a capture produced: the archive record, plus what subxt's *naive* online path made
/// of the same block.
///
/// `naive_online` is `Err` exactly when the block's own reported runtime cannot decode it —
/// the runtime-upgrade block. Keeping it as an error rather than smoothing it over is the
/// point: it is an independent reference everywhere it succeeds, and evidence of the defect
/// where it does not.
pub struct Captured {
    pub raw: RawBlock,
    pub naive_online: Result<Projection, String>,
}

/// One connection and one metadata cache, reused across every block captured.
///
/// This mirrors what the fetch stage has to do, and the reason is not tidiness. A fresh
/// `OnlineClient` per block downloads full runtime metadata on construction — ~400 KB on
/// westend — so capturing 40 small blocks with a per-block client moved tens of megabytes
/// to archive a few kilobytes. Metadata belongs in a cache keyed by `spec_version` (IPD-002
/// §9.1 archives it that way for exactly this reason); connections belong open.
pub struct Fetcher {
    client: OnlineClient<PolkadotConfig>,
    legacy: LegacyRpcMethods<RpcConfigFor<PolkadotConfig>>,
    /// spec_version -> raw metadata. The whole point of the struct.
    metadata_by_spec: HashMap<u32, Vec<u8>>,
    /// block number -> (spec_version, transaction_version), so resolving a block's parent
    /// runtime does not re-ask for a block we already looked at.
    runtime_at_block: HashMap<u64, (u32, u32)>,
}

impl Fetcher {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let rpc = RpcClient::from_insecure_url(url).await?;
        let legacy = LegacyRpcMethods::<RpcConfigFor<PolkadotConfig>>::new(rpc.clone());
        let client = OnlineClient::<PolkadotConfig>::from_rpc_client(rpc).await?;
        Ok(Self {
            client,
            legacy,
            metadata_by_spec: HashMap::new(),
            runtime_at_block: HashMap::new(),
        })
    }

    pub fn client(&self) -> &OnlineClient<PolkadotConfig> {
        &self.client
    }

    /// Capture a `RawBlock` the way the fetch stage would.
    pub async fn capture(&mut self, number: u64) -> anyhow::Result<Captured> {
        let at = self.client.at_block(number).await?;
        let header = at.block_header().await?;
        let block_hash = at.block_hash();

        // The runtime that executed this block is the one at its parent. Genesis has no
        // parent and executed nothing, so it stands for itself.
        let runtime_block = number.saturating_sub(1);
        let (spec_version, transaction_version) =
            match self.runtime_at_block.get(&runtime_block).copied() {
                Some(known) => known,
                None => {
                    let runtime_at = self.client.at_block(runtime_block).await?;
                    let pair = (runtime_at.spec_version(), runtime_at.transaction_version());
                    self.runtime_at_block.insert(runtime_block, pair);

                    // Raw metadata must come off the wire: `subxt_metadata::Metadata` has no
                    // `Encode` impl, so already-decoded metadata cannot be re-serialised for
                    // the archive. Fetch once per spec version, never once per block.
                    if let std::collections::hash_map::Entry::Vacant(slot) =
                        self.metadata_by_spec.entry(pair.0)
                    {
                        let bytes = self
                            .legacy
                            .state_get_metadata(Some(runtime_at.block_hash()))
                            .await?
                            .into_raw();
                        slot.insert(bytes);
                    }
                    pair
                }
            };

        let metadata = self
            .metadata_by_spec
            .get(&spec_version)
            .expect("metadata cached alongside the runtime version above")
            .clone();

        // Bytes are safe to take from the block's own handle — only their *interpretation*
        // depends on which metadata is in play.
        let events = at.events().fetch().await?;
        let extrinsics = at.extrinsics().fetch().await?;

        // Collected in a loop rather than `map(...).collect::<Result<_, _>>()`: subxt's
        // extrinsic error is >128 bytes, and clippy rightly objects to a closure returning it.
        let mut extrinsic_bytes = Vec::with_capacity(extrinsics.len());
        for extrinsic in extrinsics.iter() {
            extrinsic_bytes.push(extrinsic?.bytes().to_vec());
        }

        // Also remember this block's own runtime, so it is free when the next block asks
        // for it as its parent.
        self.runtime_at_block
            .insert(number, (at.spec_version(), at.transaction_version()));

        let raw = RawBlock {
            number,
            hash: block_hash.0.to_vec(),
            spec_version,
            transaction_version,
            header: header.encode(),
            extrinsics: extrinsic_bytes,
            events: events.bytes().to_vec(),
            metadata,
        };

        // The naive online decode: whatever metadata subxt resolves for the block itself.
        // This is what `decode_at` does today, and it is a genuine independent reference —
        // except at an upgrade block, where it decodes against the wrong runtime and fails.
        let naive_online = project(number, at.spec_version(), &events, &extrinsics)
            .map_err(|e: anyhow::Error| e.to_string());

        Ok(Captured { raw, naive_online })
    }
}

/// An offline client holding the metadata for *every* spec version in an archive.
///
/// This is the shape that matters: one store keyed by `spec_version`, and a block-number →
/// spec-version map built from the archive. Decoding a range that spans a runtime upgrade
/// is then a question of the client picking the right metadata per block, with no
/// per-block reconfiguration to paper over a mistake.
pub fn offline_client_for(blocks: &[RawBlock]) -> anyhow::Result<OfflineClient<PolkadotConfig>> {
    let mut metadata_by_spec: Vec<(u32, ArcMetadata)> = Vec::new();
    let mut ranges: Vec<SpecVersionForRange> = Vec::new();

    for block in blocks {
        if !metadata_by_spec
            .iter()
            .any(|(v, _)| *v == block.spec_version)
        {
            let metadata = Metadata::decode_from(&block.metadata)?;
            metadata_by_spec.push((block.spec_version, std::sync::Arc::new(metadata)));
        }
        ranges.push(SpecVersionForRange {
            block_range: block.number..block.number + 1,
            spec_version: block.spec_version,
            transaction_version: block.transaction_version,
        });
    }

    let config = PolkadotConfig::builder()
        .set_metadata_for_spec_versions(metadata_by_spec)
        .set_spec_version_for_block_ranges(ranges)
        .build();

    Ok(OfflineClient::new_with_config(config))
}

/// Rebuild one block from its archive record alone. No client of its own, no network.
pub async fn decode_stored(
    offline: &OfflineClient<PolkadotConfig>,
    raw: &RawBlock,
) -> anyhow::Result<Projection> {
    let at: ClientAtBlock<PolkadotConfig, _> = offline.at_block(raw.number)?;

    let events = at.events().from_bytes(raw.events.clone());
    let extrinsics = at.extrinsics().from_bytes(raw.extrinsics.clone()).await;

    // The header round-trips through SCALE, and its hash must reproduce the stored hash —
    // which is what would let §9.1 drop the explicit `hash` field.
    let header = Header::decode(&mut &raw.header[..])?;
    let rederived = at.hasher().hash(&header.encode());
    anyhow::ensure!(
        rederived.0.as_slice() == raw.hash.as_slice(),
        "block {}: header re-encode did not reproduce the block hash",
        raw.number
    );

    project(at.block_number(), at.spec_version(), &events, &extrinsics)
}

/// Convenience for a single block: build a client for it and decode it.
pub async fn decode_stored_standalone(raw: &RawBlock) -> anyhow::Result<Projection> {
    let offline = offline_client_for(std::slice::from_ref(raw))?;
    decode_stored(&offline, raw).await
}
