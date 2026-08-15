//! Reading chain *state* at a block, for handlers that need more than the block's events.
//!
//! The dynamic core stores what *happened* in a block. Some pallets do not put what changed
//! into the event at all — `pallet_identity` emits `IdentitySet { who }` with no display name,
//! and `JudgementGiven { target, registrar_index }` with no judgement. For those, the event
//! says *which key changed* and only storage says *what it changed to*.
//!
//! Everything here is resolved from the node's own metadata at the block being read, so this
//! keeps the framework's central property: no runtime is compiled into the indexing path.
//!
//! [`StorageAt`] is a trait rather than a bare `&AtBlock` for two reasons:
//!
//!   * a handler's tests can stub it with canned JSON and run with no node and no network;
//!   * values arrive as the same JSON shape handlers already parse out of `event.fields`,
//!     because both go through [`pif_core::codec`] — one decoding convention, not two.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use pif_core::codec;
use scale_value::Value;
use serde_json::Value as Json;

use crate::decode::{AtBlock, is_pruned_state};
use crate::error::{ChainError, Result};

/// Read access to one block's state.
#[async_trait]
pub trait StorageAt: Send + Sync {
    /// The block this reads state at.
    fn block_number(&self) -> u64;

    /// Whether the runtime at this block has the pallet at all.
    ///
    /// A handler configured against the wrong chain should fail loudly at startup rather than
    /// silently indexing nothing, and `EventHandler::supports` cannot tell — it only sees
    /// `ChainInfo`, which carries no metadata.
    fn has_pallet(&self, pallet: &str) -> bool;

    /// Read one storage entry.
    ///
    /// `keys` is empty for a plain value, and holds one [`Value`] per map key. `Ok(None)`
    /// means the key is genuinely absent, which is ordinary — most accounts have no identity.
    async fn fetch(&self, pallet: &str, entry: &str, keys: Vec<Value>) -> Result<Option<Json>>;

    /// Stream every `(key_bytes, value)` pair in a storage map.
    ///
    /// For one-off bootstrap sweeps. This is a *stream* rather than a `Vec` on purpose: a map
    /// like `Identity::IdentityOf` has tens of thousands of entries, and the caller should be
    /// able to batch them into the database as they arrive instead of holding the whole map
    /// in memory first.
    ///
    /// Takes owned names because the returned stream borrows only `self`.
    async fn iter<'s>(
        &'s self,
        pallet: String,
        entry: String,
    ) -> Result<BoxStream<'s, Result<(Vec<u8>, Json)>>>;
}

/// [`StorageAt`] backed by a live node, through subxt's dynamic storage API.
///
/// The digest does not use this directly — it goes through [`crate::cache::CachedStorage`],
/// which answers from the archive first and delegates here only on a miss. This stays as the
/// unconditional live path, for the handler bootstrap sweep and for anything that genuinely
/// wants the node.
pub struct SubxtStorage<'a> {
    at: &'a AtBlock,
    chain_id: &'a str,
}

impl<'a> SubxtStorage<'a> {
    pub fn new(at: &'a AtBlock, chain_id: &'a str) -> Self {
        Self { at, chain_id }
    }
}

#[async_trait]
impl StorageAt for SubxtStorage<'_> {
    fn block_number(&self) -> u64 {
        self.at.block_number()
    }

    fn has_pallet(&self, pallet: &str) -> bool {
        self.at.metadata_ref().pallet_by_name(pallet).is_some()
    }

    async fn fetch(&self, pallet: &str, entry: &str, keys: Vec<Value>) -> Result<Option<Json>> {
        fetch_at(self.at, self.chain_id, pallet, entry, keys).await
    }

    async fn iter<'s>(
        &'s self,
        pallet: String,
        entry: String,
    ) -> Result<BoxStream<'s, Result<(Vec<u8>, Json)>>> {
        iter_at(self.at, self.chain_id, pallet, entry).await
    }
}

/// Read one storage entry from a live block.
///
/// A free function rather than a method so the cache decorator can call it holding a
/// long-lived `&AtBlock` of its own. One implementation, so a cached digest and an uncached
/// one cannot decode a value differently — a divergence the archive would preserve forever.
pub(crate) async fn fetch_at(
    at: &AtBlock,
    chain_id: &str,
    pallet: &str,
    entry: &str,
    keys: Vec<Value>,
) -> Result<Option<Json>> {
    let number = at.block_number();

    // A `(pallet, entry)` tuple is subxt's dynamic address: key parts and value are both
    // `scale_value::Value`, decoded against the block's own metadata.
    let found = match at.storage().try_fetch((pallet, entry), keys).await {
        Ok(found) => found,
        // An entry this runtime does not have is "no value", not a failure. Storage items
        // come and go across runtime versions — `Identity::UsernameOf` did not exist
        // before usernames shipped — and a handler indexing history across an upgrade
        // must not die at the boundary. A missing *pallet* is still loud, via
        // `has_pallet`, because that means the handler is pointed at the wrong chain.
        Err(subxt::error::StorageError::StorageEntryNotFound { .. }) => return Ok(None),
        Err(e) => return Err(storage_error(chain_id, number, pallet, entry, e)),
    };

    let Some(value) = found else {
        return Ok(None);
    };

    let decoded = value
        .decode()
        .map_err(|e| storage_error(chain_id, number, pallet, entry, e))?;

    Ok(Some(codec::value_to_json(&decoded)))
}

/// Stream every `(key_bytes, value)` in a storage map at a live block.
///
/// The returned stream borrows `at`, which is why this takes it by reference rather than
/// being a method: the cache decorator owns its `AtBlock` and can lend it for as long as the
/// stream lives, where a temporary `SubxtStorage` could not.
pub(crate) async fn iter_at<'a>(
    at: &'a AtBlock,
    chain_id: &'a str,
    pallet: String,
    entry: String,
) -> Result<BoxStream<'a, Result<(Vec<u8>, Json)>>> {
    let number = at.block_number();

    // The address takes owned names, so nothing stays borrowed from `pallet`/`entry` and
    // they are free to move into the stream closure below. An empty key prefix means
    // "every entry in the map".
    let entries = at
        .storage()
        .iter((pallet.clone(), entry.clone()), Vec::<Value>::new())
        .await
        .map_err(|e| storage_error(chain_id, number, &pallet, &entry, e))?;

    let chain_id = chain_id.to_owned();

    let stream = entries.map(move |item| {
        let kv = item.map_err(|e| storage_error(&chain_id, number, &pallet, &entry, e))?;
        // `key_bytes` borrows from `kv`, so copy before it is dropped.
        let key = kv.key_bytes().to_vec();
        let value = kv
            .value()
            .decode()
            .map_err(|e| storage_error(&chain_id, number, &pallet, &entry, e))?;
        Ok((key, codec::value_to_json(&value)))
    });

    Ok(stream.boxed())
}

/// Turn a subxt storage failure into a `ChainError`.
///
/// Pruning is separated out because it is the one failure with an actionable fix, and the
/// raw message ("State already discarded") does not say what it is. Reuses the same detector
/// the block-read path uses, so both report it identically.
fn storage_error(
    chain_id: &str,
    number: u64,
    pallet: &str,
    entry: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> ChainError {
    if is_pruned_state(&source) {
        return ChainError::PrunedState {
            chain: chain_id.to_owned(),
            number,
        };
    }
    ChainError::StorageRead {
        pallet: pallet.to_owned(),
        entry: entry.to_owned(),
        number,
        source: Box::new(source),
    }
}

/// A [`StorageAt`] backed by canned JSON, so handlers can be tested without a node.
///
/// Lives here rather than behind `#[cfg(test)]` because the handlers that need it are in
/// *other* crates, and a `cfg(test)` item is not visible across a crate boundary.
pub mod stub {
    use std::collections::HashMap;

    use super::*;

    /// One storage entry's rows: `(key_bytes, value)`, as [`StorageAt::iter`] yields them.
    pub type StubRows = Vec<(Vec<u8>, Json)>;

    /// Keyed by `(pallet, entry, keys-as-json)`.
    #[derive(Default)]
    pub struct StubStorage {
        block_number: u64,
        pallets: Vec<String>,
        values: HashMap<(String, String, String), Json>,
        maps: HashMap<(String, String), StubRows>,
    }

    impl StubStorage {
        pub fn new(block_number: u64) -> Self {
            Self {
                block_number,
                ..Default::default()
            }
        }

        /// Declare a pallet present, so `has_pallet` reports it.
        pub fn with_pallet(mut self, pallet: &str) -> Self {
            self.pallets.push(pallet.to_owned());
            self
        }

        pub fn with_value(
            mut self,
            pallet: &str,
            entry: &str,
            keys: &[Value],
            value: Json,
        ) -> Self {
            self.values
                .insert((pallet.to_owned(), entry.to_owned(), key_id(keys)), value);
            self
        }

        pub fn with_map(mut self, pallet: &str, entry: &str, rows: StubRows) -> Self {
            self.maps
                .insert((pallet.to_owned(), entry.to_owned()), rows);
            self
        }
    }

    /// Stable identity for a key list. `scale_value::Value` renders deterministically, which
    /// is all this needs — it is only ever compared against itself.
    fn key_id(keys: &[Value]) -> String {
        keys.iter()
            .map(|k| k.to_string())
            .collect::<Vec<_>>()
            .join("|")
    }

    #[async_trait]
    impl StorageAt for StubStorage {
        fn block_number(&self) -> u64 {
            self.block_number
        }

        fn has_pallet(&self, pallet: &str) -> bool {
            self.pallets.iter().any(|p| p == pallet)
        }

        async fn fetch(&self, pallet: &str, entry: &str, keys: Vec<Value>) -> Result<Option<Json>> {
            Ok(self
                .values
                .get(&(pallet.to_owned(), entry.to_owned(), key_id(&keys)))
                .cloned())
        }

        async fn iter<'s>(
            &'s self,
            pallet: String,
            entry: String,
        ) -> Result<BoxStream<'s, Result<(Vec<u8>, Json)>>> {
            let rows = self.maps.get(&(pallet, entry)).cloned().unwrap_or_default();
            Ok(futures::stream::iter(rows.into_iter().map(Ok)).boxed())
        }
    }
}
