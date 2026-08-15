//! Reading `pallet_identity` state and writing it into this handler's tables.
//!
//! Shared by the per-block handler and the bootstrap sweep, so a row written during the
//! initial snapshot is byte-for-byte the row the block loop would have written. Two code
//! paths producing "the same" row is exactly how a snapshot and its updates drift apart.

use pif_chain::{Result, StorageAt};
use pif_core::ss58;
use scale_value::Value;
use serde_json::Value as Json;
use sqlx::PgConnection;

use crate::model::{
    big_decimal_from_json, data_to_string, parse_registration, u64_from_json, unwrap_bounded,
    username_to_string,
};
use crate::store::{self, RegistrarEntry};

pub const PALLET: &str = "Identity";

/// An account to sync: SS58 for storage, raw bytes for the storage key.
pub type Account = (String, Vec<u8>);

/// Re-read and persist everything that depends on one account.
///
/// Returns any sub-account discovered through `SubsOf` that the caller has not already
/// scheduled. `SubIdentitiesSet` names only the parent, so a child's *label* is only
/// reachable by reading the child's own `SuperOf` — the parent's list gives us who to ask.
pub async fn sync_account(
    storage: &dyn StorageAt,
    conn: &mut PgConnection,
    chain_id: &str,
    prefix: u16,
    account: &Account,
    block: i64,
) -> Result<Vec<Account>> {
    let (address, bytes) = account;
    let key = || vec![Value::from_bytes(bytes)];

    // --- IdentityOf: display name, judgements, deposit -------------------------------
    let raw = storage.fetch(PALLET, "IdentityOf", key()).await?;
    let row = raw.as_ref().and_then(parse_registration);
    store::put_identity(conn, chain_id, address, block, row.as_ref()).await?;

    // --- UsernameOf: which username is primary ---------------------------------------
    let primary = storage
        .fetch(PALLET, "UsernameOf", key())
        .await?
        .as_ref()
        .and_then(username_to_string);
    store::set_primary_username(conn, chain_id, address, primary.as_deref(), block).await?;

    // --- SuperOf: is this account somebody's sub-identity? ----------------------------
    match storage.fetch(PALLET, "SuperOf", key()).await? {
        Some(value) => match parse_super_of(&value, prefix) {
            Some((parent, label)) => {
                store::put_sub_identity(conn, chain_id, address, &parent, label.as_deref(), block)
                    .await?;
            }
            // Present but unreadable: leave the existing row rather than delete on a guess.
            None => tracing::warn!(
                account = %address, block,
                "Identity::SuperOf has an unexpected shape; leaving the stored sub-identity alone"
            ),
        },
        None => store::delete_sub_identity(conn, chain_id, address).await?,
    }

    // --- SubsOf: which accounts are this one's subs? ----------------------------------
    let subs = storage
        .fetch(PALLET, "SubsOf", key())
        .await?
        .as_ref()
        .map(|value| parse_subs_of(value, prefix))
        .unwrap_or_default();

    let addresses: Vec<String> = subs.iter().map(|(a, _)| a.clone()).collect();
    store::retain_subs(conn, chain_id, address, &addresses).await?;

    Ok(subs)
}

/// Re-read and persist one username.
pub async fn sync_username(
    storage: &dyn StorageAt,
    conn: &mut PgConnection,
    chain_id: &str,
    prefix: u16,
    username: &str,
    block: i64,
) -> Result<()> {
    let key = || vec![Value::from_bytes(username.as_bytes())];

    // An unbinding username is still present in `UsernameInfoOf`, so this must be checked
    // alongside it rather than as an alternative to it.
    let unbinding = storage
        .fetch(PALLET, "UnbindingUsernames", key())
        .await?
        .as_ref()
        .and_then(u64_from_json)
        .map(|n| n as i64);

    if let Some(info) = storage.fetch(PALLET, "UsernameInfoOf", key()).await? {
        let owner = info
            .get("owner")
            .and_then(|v| ss58::decode_account(v, prefix));
        let provider = info.get("provider").cloned();
        let status = if unbinding.is_some() {
            "unbinding"
        } else {
            "active"
        };

        store::put_username(
            conn,
            chain_id,
            username,
            owner.as_deref(),
            status,
            provider.as_ref(),
            unbinding,
            block,
        )
        .await?;
        return Ok(());
    }

    // Granted by an authority but not yet accepted: `(AccountId, BlockNumber, Provider)`.
    if let Some(pending) = storage.fetch(PALLET, "PendingUsernames", key()).await? {
        let parts = pending.as_array().map(Vec::as_slice).unwrap_or_default();
        let owner = parts.first().and_then(|v| ss58::decode_account(v, prefix));
        let deadline = parts.get(1).and_then(u64_from_json).map(|n| n as i64);
        let provider = parts.get(2).cloned();

        store::put_username(
            conn,
            chain_id,
            username,
            owner.as_deref(),
            "pending",
            provider.as_ref(),
            deadline,
            block,
        )
        .await?;
        return Ok(());
    }

    // In no map at all: gone. Kept as a row rather than deleted so that a name which was
    // once bound to an account does not silently vanish from history.
    store::put_username(conn, chain_id, username, None, "removed", None, None, block).await?;
    Ok(())
}

/// Re-read the whole registrar set.
pub async fn sync_registrars(
    storage: &dyn StorageAt,
    conn: &mut PgConnection,
    chain_id: &str,
    prefix: u16,
    block: i64,
) -> Result<()> {
    let Some(value) = storage.fetch(PALLET, "Registrars", Vec::new()).await? else {
        return Ok(());
    };

    let registrars = parse_registrars(&value, prefix);
    store::put_registrars(conn, chain_id, &registrars, block).await?;
    Ok(())
}

/// `SuperOf` is `(AccountId, Data)`: the parent and this account's label within it.
fn parse_super_of(value: &Json, prefix: u16) -> Option<(String, Option<String>)> {
    let parts = value.as_array()?;
    let parent = ss58::decode_account(parts.first()?, prefix)?;
    let label = data_to_string(parts.get(1));
    Some((parent, label))
}

/// `SubsOf` is `(Balance, BoundedVec<AccountId>)`. The deposit is not stored — it is the
/// parent's, not the sub's, and duplicating it per child would make it look per-child.
fn parse_subs_of(value: &Json, prefix: u16) -> Vec<Account> {
    let Some(parts) = value.as_array() else {
        return Vec::new();
    };
    let Some(accounts) = parts.get(1).map(unwrap_bounded).and_then(Json::as_array) else {
        return Vec::new();
    };

    accounts
        .iter()
        .filter_map(|v| {
            let bytes = ss58::account_bytes(v)?;
            Some((ss58::encode(&bytes, prefix), bytes))
        })
        .collect()
}

/// Decode `Registrars`: `Vec<Option<RegistrarInfo>>`. A removed registrar leaves a `None`
/// hole that must be preserved, because a `RegistrarIndex` in a judgement indexes into this
/// list.
///
/// Public so the genesis-override generator can round-trip what it is about to install.
pub fn parse_registrars(value: &Json, prefix: u16) -> Vec<RegistrarEntry> {
    let Some(items) = unwrap_bounded(value).as_array() else {
        return Vec::new();
    };

    items
        .iter()
        .map(|item| {
            if item.is_null() {
                return None;
            }
            let account = item
                .get("account")
                .and_then(|v| ss58::decode_account(v, prefix));
            let fee = item.get("fee").and_then(big_decimal_from_json);
            Some((account, fee, item.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ALICE: &str = "0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d";
    const BOB: &str = "0x8eaf04151687736326c9fea17e25fc5287613693c912909cb226aa4794f26a48";

    fn alice() -> String {
        ss58::encode(&hex::decode(&ALICE[2..]).unwrap(), 0)
    }

    #[test]
    fn super_of_yields_the_parent_and_the_label() {
        let value = json!([[ALICE], { "Raw6": [118, 97, 108, 45, 48, 49] }]);
        let (parent, label) = parse_super_of(&value, 0).expect("well-formed SuperOf");

        assert_eq!(parent, alice());
        assert_eq!(label.as_deref(), Some("val-01"));
    }

    #[test]
    fn super_of_without_a_label_still_yields_the_parent() {
        // A sub can be registered with `Data::None`; the link still matters.
        let value = json!([[ALICE], { "None": [] }]);
        let (parent, label) = parse_super_of(&value, 0).unwrap();
        assert_eq!(parent, alice());
        assert_eq!(label, None);
    }

    #[test]
    fn a_malformed_super_of_is_rejected_rather_than_guessed() {
        assert!(parse_super_of(&json!("nonsense"), 0).is_none());
        assert!(parse_super_of(&json!([]), 0).is_none());
    }

    #[test]
    fn subs_of_yields_every_child() {
        let value = json!(["1000000", [[ALICE], [BOB]]]);
        let subs = parse_subs_of(&value, 0);

        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0].0, alice());
        assert_eq!(subs[0].1.len(), 32, "raw key bytes must be preserved");
    }

    #[test]
    fn subs_of_unwraps_the_bounded_vec_level() {
        // The exact shape a real People runtime returns: `(Balance, BoundedVec<AccountId>)`
        // where the BoundedVec newtype adds one array level. Reading `parts[1]` directly
        // finds a single element -- the inner Vec -- and decodes zero accounts, so every
        // sub-identity silently disappears.
        let value = json!(["1000000", [[[ALICE], [BOB]]]]);
        let subs = parse_subs_of(&value, 0);

        assert_eq!(subs.len(), 2, "both subs must survive the newtype level");
        assert_eq!(subs[0].0, alice());
    }

    #[test]
    fn an_empty_bounded_vec_of_subs_is_empty_not_one_bad_entry() {
        // Observed live: `Identity.SubsOf(Alice) = ["0", [[]]]`.
        assert!(parse_subs_of(&json!(["0", [[]]]), 0).is_empty());
    }

    #[test]
    fn registrars_unwrap_the_bounded_vec_level() {
        let value = json!([[
            { "account": [ALICE], "fee": "100", "fields": 0 },
            null,
            { "account": [BOB], "fee": "200", "fields": 0 }
        ]]);

        let registrars = parse_registrars(&value, 0);
        assert_eq!(registrars.len(), 3);
        assert!(registrars[1].is_none(), "the hole must survive too");
    }

    #[test]
    fn an_empty_registrar_set_is_empty_not_one_fabricated_registrar() {
        // Observed live on a fresh chain: `Identity.Registrars = [[]]`. Read at the outer
        // level this is a one-element list, and that element becomes a registrar that does
        // not exist -- shifting every `registrar_index` in every judgement.
        assert!(parse_registrars(&json!([[]]), 0).is_empty());
    }

    #[test]
    fn an_account_with_no_subs_yields_an_empty_list() {
        // `SubsOf` is a ValueQuery, so it always returns a value — the default is (0, []).
        // This must produce an empty keep-list, which is what deletes stale children.
        assert!(parse_subs_of(&json!(["0", []]), 0).is_empty());
    }

    #[test]
    fn registrar_holes_keep_their_index() {
        // A judgement's `registrar_index` indexes into this list. Compacting away the `None`
        // would shift every later registrar and mis-attribute judgements.
        let value = json!([
            { "account": [ALICE], "fee": "100", "fields": 0 },
            null,
            { "account": [BOB], "fee": "200", "fields": 0 }
        ]);

        let registrars = parse_registrars(&value, 0);
        assert_eq!(registrars.len(), 3);
        assert!(registrars[1].is_none(), "the hole must be preserved");

        let (account, fee, _) = registrars[2].as_ref().unwrap();
        assert!(account.is_some());
        assert_eq!(fee.as_ref().unwrap().to_string(), "200");
    }
}
