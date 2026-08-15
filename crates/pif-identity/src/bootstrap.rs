//! The one-off storage sweep that seeds identity state before indexing starts.
//!
//! Events only ever describe *changes*. Everything set before the first block we index is
//! therefore invisible to the block loop — and on the Polkadot People chain that is very
//! nearly the entire identity set, because identities arrived there by migration from the
//! relay chain rather than through blocks anyone indexed. Without this sweep the tables would
//! contain only accounts that happened to touch their identity since the start block.
//!
//! This runs outside any block transaction (see `EventHandler::bootstrap`), because a full
//! `IdentityOf` sweep is tens of thousands of keys and must not be held open in the
//! one-transaction-per-block path.

use futures::StreamExt;
use parity_scale_codec::Decode;
use pif_chain::error::{ChainError, Result};
use pif_chain::storage::StorageAt;
use pif_core::{ChainInfo, ss58};
use serde_json::Value as Json;
use sqlx::PgPool;

use crate::model::{parse_registration, username_to_string};
use crate::read::{self, PALLET};
use crate::store;

/// How many entries to write per transaction.
///
/// One transaction for the whole sweep would hold locks for minutes and lose everything on a
/// disconnect; one per entry would pay a round-trip per row. Batching also bounds how much
/// work a crash discards.
const BATCH: usize = 500;

/// Seed identity state, unless a completed sweep is already recorded.
pub async fn run(chain: &ChainInfo, storage: &dyn StorageAt, pool: &PgPool) -> Result<()> {
    // The wrong-chain check. `supports()` cannot do this — it only sees `ChainInfo`, which
    // carries no metadata — so this is the first point where being pointed at a chain with no
    // Identity pallet is detectable, and it should stop the run rather than index nothing.
    if !storage.has_pallet(PALLET) {
        return Err(ChainError::Handler {
            handler: crate::handler::NAME.to_string(),
            block: storage.block_number(),
            source: format!(
                "chain {:?} has no `{PALLET}` pallet at block {}; the `identity` handler is \
                 configured for a chain that cannot serve it (did you mean a People chain?)",
                chain.id,
                storage.block_number()
            )
            .into(),
        });
    }

    let block = i64::try_from(storage.block_number()).unwrap_or(i64::MAX);
    let mut conn = pool.acquire().await?;

    if let Some(done_at) = store::bootstrap_block(&mut conn, &chain.id).await? {
        tracing::info!(chain = %chain.id, snapshot_block = done_at, "identity bootstrap already done");
        return Ok(());
    }

    tracing::info!(chain = %chain.id, block, "sweeping identity storage; this takes a while");

    let identities = sweep_identities(storage, pool, &chain.id, chain.ss58_prefix, block).await?;
    let usernames = sweep_usernames(storage, pool, &chain.id, chain.ss58_prefix, block).await?;
    let subs = sweep_subs(storage, pool, &chain.id, chain.ss58_prefix, block).await?;

    // One value, so it needs no sweep.
    let mut conn = pool.acquire().await?;
    read::sync_registrars(storage, &mut conn, &chain.id, chain.ss58_prefix, block).await?;

    store::finish_bootstrap(&mut conn, &chain.id, block).await?;

    tracing::info!(
        chain = %chain.id, block, identities, usernames, sub_identities = subs,
        "identity bootstrap complete"
    );
    Ok(())
}

/// `IdentityOf` -> the `identities` table.
async fn sweep_identities(
    storage: &dyn StorageAt,
    pool: &PgPool,
    chain_id: &str,
    prefix: u16,
    block: i64,
) -> Result<usize> {
    let mut stream = storage
        .iter(PALLET.to_owned(), "IdentityOf".to_owned())
        .await?;

    let mut batch = Vec::with_capacity(BATCH);
    let mut total = 0usize;

    while let Some(item) = stream.next().await {
        let (key, value) = item?;
        let Some(account) = account_from_key(&key, prefix) else {
            continue;
        };
        let Some(row) = parse_registration(&value) else {
            continue;
        };

        batch.push((account, row));
        if batch.len() >= BATCH {
            total += flush_identities(pool, chain_id, block, &mut batch).await?;
            tracing::debug!(chain = %chain_id, identities = total, "sweeping");
        }
    }

    total += flush_identities(pool, chain_id, block, &mut batch).await?;
    Ok(total)
}

async fn flush_identities(
    pool: &PgPool,
    chain_id: &str,
    block: i64,
    batch: &mut Vec<(String, crate::model::IdentityRow)>,
) -> Result<usize> {
    if batch.is_empty() {
        return Ok(0);
    }

    let mut tx = pool.begin().await?;
    for (account, row) in batch.iter() {
        store::put_identity(&mut tx, chain_id, account, block, Some(row)).await?;
    }
    tx.commit().await?;

    let written = batch.len();
    batch.clear();
    Ok(written)
}

/// `UsernameInfoOf` -> the `usernames` table, then `UsernameOf` to mark the primaries.
///
/// Both are needed and neither is enough: `UsernameInfoOf` is keyed by username and knows the
/// owner, while `UsernameOf` is keyed by account and knows which of an account's several
/// usernames is primary.
async fn sweep_usernames(
    storage: &dyn StorageAt,
    pool: &PgPool,
    chain_id: &str,
    prefix: u16,
    block: i64,
) -> Result<usize> {
    let mut total = 0usize;

    {
        let mut stream = storage
            .iter(PALLET.to_owned(), "UsernameInfoOf".to_owned())
            .await?;
        let mut tx = pool.begin().await?;
        let mut in_tx = 0usize;

        while let Some(item) = stream.next().await {
            let (key, value) = item?;
            let Some(username) = username_from_key(&key) else {
                continue;
            };
            let owner = value
                .get("owner")
                .and_then(|v| ss58::decode_account(v, prefix));

            store::put_username(
                &mut tx,
                chain_id,
                &username,
                owner.as_deref(),
                "active",
                value.get("provider"),
                None,
                block,
            )
            .await?;

            total += 1;
            in_tx += 1;
            if in_tx >= BATCH {
                tx.commit().await?;
                tx = pool.begin().await?;
                in_tx = 0;
            }
        }
        tx.commit().await?;
    }

    // Primaries, second: `set_primary_username` demotes an account's other names, so running
    // it before the names exist would leave nothing to promote.
    let mut stream = storage
        .iter(PALLET.to_owned(), "UsernameOf".to_owned())
        .await?;
    let mut tx = pool.begin().await?;
    let mut in_tx = 0usize;

    while let Some(item) = stream.next().await {
        let (key, value) = item?;
        let (Some(account), Some(username)) =
            (account_from_key(&key, prefix), username_to_string(&value))
        else {
            continue;
        };

        store::set_primary_username(&mut tx, chain_id, &account, Some(&username), block).await?;

        in_tx += 1;
        if in_tx >= BATCH {
            tx.commit().await?;
            tx = pool.begin().await?;
            in_tx = 0;
        }
    }
    tx.commit().await?;

    Ok(total)
}

/// `SuperOf` -> the `sub_identities` table.
async fn sweep_subs(
    storage: &dyn StorageAt,
    pool: &PgPool,
    chain_id: &str,
    prefix: u16,
    block: i64,
) -> Result<usize> {
    let mut stream = storage
        .iter(PALLET.to_owned(), "SuperOf".to_owned())
        .await?;

    let mut tx = pool.begin().await?;
    let mut total = 0usize;
    let mut in_tx = 0usize;

    while let Some(item) = stream.next().await {
        let (key, value) = item?;
        let Some(sub) = account_from_key(&key, prefix) else {
            continue;
        };
        let parts = value.as_array().map(Vec::as_slice).unwrap_or_default();
        let Some(parent) = parts.first().and_then(|v| ss58::decode_account(v, prefix)) else {
            continue;
        };
        let label = crate::model::data_to_string(parts.get(1));

        store::put_sub_identity(&mut tx, chain_id, &sub, &parent, label.as_deref(), block).await?;

        total += 1;
        in_tx += 1;
        if in_tx >= BATCH {
            tx.commit().await?;
            tx = pool.begin().await?;
            in_tx = 0;
        }
    }
    tx.commit().await?;

    Ok(total)
}

/// Recover the account from a storage key.
///
/// A map key is `twox128(pallet) ++ twox128(entry) ++ hash(key) ++ key`, and both hashers
/// `pallet_identity` uses for account keys (`Twox64Concat`, `Blake2_128Concat`) append the
/// raw key after the hash. The account is therefore the final 32 bytes.
fn account_from_key(key: &[u8], prefix: u16) -> Option<String> {
    if key.len() < 32 {
        return None;
    }
    Some(ss58::encode(&key[key.len() - 32..], prefix))
}

/// Recover a username from a `Blake2_128Concat` key.
///
/// Layout: `twox128(pallet) ++ twox128(entry) ++ blake2_128(key) ++ key`, so 32 + 16 bytes of
/// prefix and then the SCALE-encoded `BoundedVec<u8>`.
///
/// The tail is decoded with the codec rather than scanned: a compact length is often printable
/// itself (9 encodes as `0x24`, `'$'`), so "skip to the first printable byte" would prepend
/// junk to every short username. `Vec<u8>::decode` reads the compact length and exactly that
/// many bytes, and errors rather than truncating if the key claims more than it carries.
fn username_from_key(key: &[u8]) -> Option<String> {
    const PREFIX_LEN: usize = 32 + 16;

    let mut tail = key.get(PREFIX_LEN..)?;
    let name = Vec::<u8>::decode(&mut tail).ok()?;

    if name.is_empty() {
        return None;
    }
    Some(String::from_utf8_lossy(&name).into_owned())
}

/// Used only to keep the `Json` import honest across cfg combinations.
#[allow(dead_code)]
fn _assert_json(_: &Json) {}

#[cfg(test)]
mod tests {
    use super::*;

    const ALICE: [u8; 32] = [
        0xd4, 0x35, 0x93, 0xc7, 0x15, 0xfd, 0xd3, 0x1c, 0x61, 0x14, 0x1a, 0xbd, 0x04, 0xa9, 0x9f,
        0xd6, 0x82, 0x2c, 0x85, 0x58, 0x85, 0x4c, 0xcd, 0xe3, 0x9a, 0x56, 0x84, 0xe7, 0xa5, 0x6d,
        0xa2, 0x7d,
    ];

    #[test]
    fn recovers_the_account_from_a_concat_storage_key() {
        // 32 bytes of module/entry prefix, 8 of Twox64, then the account itself.
        let mut key = vec![0u8; 40];
        key.extend_from_slice(&ALICE);

        assert_eq!(
            account_from_key(&key, 0).as_deref(),
            Some("15oF4uVJwmo4TdGW7VfQxNLavjCXviqxT9S1MgbjMNHr6Sp5")
        );
    }

    #[test]
    fn a_key_too_short_to_hold_an_account_is_rejected() {
        // Better to skip an entry than to invent an address out of prefix bytes.
        assert!(account_from_key(&[0u8; 16], 0).is_none());
    }

    #[test]
    fn recovers_a_username_from_its_storage_key() {
        let mut key = vec![0u8; 48];
        key.push(9 << 2); // compact length prefix for 9 bytes
        key.extend_from_slice(b"alice.dot");

        assert_eq!(username_from_key(&key).as_deref(), Some("alice.dot"));
    }

    #[test]
    fn recovers_a_long_username_using_the_two_byte_length_form() {
        // Names of 64 bytes or more switch compact mode; a single-byte reader would take the
        // low bits of the length as the first character.
        let name = "a".repeat(70);
        let mut key = vec![0u8; 48];
        key.extend_from_slice(&(((name.len() as u16) << 2) | 0b01).to_le_bytes());
        key.extend_from_slice(name.as_bytes());

        assert_eq!(username_from_key(&key).as_deref(), Some(name.as_str()));
    }

    #[test]
    fn a_key_with_no_username_payload_is_rejected() {
        assert!(username_from_key(&[0u8; 48]).is_none());
        assert!(username_from_key(&[0u8; 10]).is_none());
    }

    #[test]
    fn a_key_whose_length_overruns_the_payload_is_rejected() {
        // Claiming 40 bytes while carrying 3 means the key is not what we think it is;
        // truncating silently would store a mangled username.
        let mut key = vec![0u8; 48];
        key.push(40 << 2);
        key.extend_from_slice(b"abc");

        assert!(username_from_key(&key).is_none());
    }

    #[test]
    fn a_non_canonical_length_encoding_is_rejected() {
        // 20 fits the single-byte compact form, so spelling it in the four-byte form is not
        // valid SCALE. Delegating to the codec gets this rejection for free; a hand-rolled
        // width table would have accepted it and produced a username from a malformed key.
        let name = "a".repeat(20);
        let mut key = vec![0u8; 48];
        key.extend_from_slice(&(((name.len() as u32) << 2) | 0b10).to_le_bytes());
        key.extend_from_slice(name.as_bytes());

        assert!(username_from_key(&key).is_none());
    }
}
