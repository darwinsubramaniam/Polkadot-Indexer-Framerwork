//! Working out *which* keys a block changed, from events that do not say *what* changed.
//!
//! `pallet_identity`'s events are notifications, not payloads: `IdentitySet { who }` carries
//! no display name, `JudgementGiven { target, registrar_index }` carries no judgement, and
//! `SubIdentitiesSet { main, number_of_subs, .. }` does not say which subs. So this module's
//! only job is to answer "whose storage do we need to re-read", and the handler then reads it.
//!
//! Extraction is by **field name**, not by matching on variant names. A runtime upgrade that
//! adds `Identity::SomethingNew { who }` is then picked up automatically, and one that adds an
//! event with no account field is ignored rather than treated as an error. Failing closed on
//! an unrecognised variant would mean silently missing changes; erroring on one would stop the
//! whole indexer for something harmless.

use std::collections::{BTreeMap, BTreeSet};

use pif_db::NewEvent;

use crate::model::username_to_string;

/// Keys a block touched, and therefore whose state must be re-read.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Touched {
    /// Accounts whose `IdentityOf` / `UsernameOf` / `SuperOf` / `SubsOf` to re-read, as
    /// SS58 (the form stored, and a natural dedup key) mapped to the raw 32 bytes (the form
    /// a storage key needs).
    pub accounts: BTreeMap<String, Vec<u8>>,
    /// Usernames whose `UsernameInfoOf` / `PendingUsernames` / `UnbindingUsernames` to re-read.
    pub usernames: BTreeSet<String>,
    /// Whether the registrar set changed.
    pub registrars: bool,
}

impl Touched {
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty() && self.usernames.is_empty() && !self.registrars
    }

    /// Just the addresses, for assertions and logging.
    pub fn account_set(&self) -> BTreeSet<String> {
        self.accounts.keys().cloned().collect()
    }
}

/// Every field name in `pallet_identity` that holds an account whose identity state changes.
///
/// `authority` is deliberately absent: `AuthorityAdded`/`AuthorityRemoved` concern who may
/// *grant* usernames, which changes no row in these tables. Including it would buy nothing
/// and cost a storage read per event.
const ACCOUNT_FIELDS: &[&str] = &["who", "target", "main", "sub", "whose"];

/// Collect the keys touched by one block's events.
pub fn collect(events: &[NewEvent], ss58_prefix: u16) -> Touched {
    let mut touched = Touched::default();

    for event in events {
        if event.pallet != "Identity" {
            continue;
        }

        // `RegistrarAdded`/`RegistrarRemoved` carry only an index, and the set is a single
        // `StorageValue`, so any change means re-reading the whole thing.
        if event.variant.starts_with("Registrar") {
            touched.registrars = true;
        }

        let Some(fields) = event.fields.as_object() else {
            // Positional (unnamed) event fields cannot be identified by name. This pallet has
            // used named fields for its whole life, so rather than guess at positions we
            // record nothing and let it be visible as missing data.
            tracing::warn!(
                block = event.block_number,
                variant = %event.variant,
                "Identity event has unnamed fields; cannot identify touched keys"
            );
            continue;
        };

        for name in ACCOUNT_FIELDS {
            if let Some(value) = fields.get(*name)
                && let Some(bytes) = pif_core::ss58::account_bytes(value)
            {
                touched
                    .accounts
                    .insert(pif_core::ss58::encode(&bytes, ss58_prefix), bytes);
            }
        }

        if let Some(value) = fields.get("username")
            && let Some(username) = username_to_string(value)
        {
            touched.usernames.insert(username);
        }
    }

    touched
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ALICE: &str = "0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d";
    const BOB: &str = "0x8eaf04151687736326c9fea17e25fc5287613693c912909cb226aa4794f26a48";

    fn alice() -> String {
        pif_core::ss58::encode(&hex::decode(&ALICE[2..]).unwrap(), 0)
    }

    fn bob() -> String {
        pif_core::ss58::encode(&hex::decode(&BOB[2..]).unwrap(), 0)
    }

    fn event(pallet: &str, variant: &str, fields: serde_json::Value) -> NewEvent {
        NewEvent {
            chain_id: "polkadot-people".into(),
            block_number: 42,
            idx: 0,
            pallet: pallet.into(),
            variant: variant.into(),
            phase: "ApplyExtrinsic".into(),
            extrinsic_idx: Some(1),
            fields,
        }
    }

    #[test]
    fn identity_set_touches_the_account() {
        let events = vec![event("Identity", "IdentitySet", json!({ "who": [ALICE] }))];
        let touched = collect(&events, 0);

        assert_eq!(touched.account_set(), BTreeSet::from([alice()]));
        assert!(touched.usernames.is_empty());
        assert!(!touched.registrars);
    }

    #[test]
    fn judgement_given_touches_the_target_not_the_registrar_index() {
        // The event names the field `target`, not `who`; missing this would mean judgements
        // never update an identity's verified flag.
        let events = vec![event(
            "Identity",
            "JudgementGiven",
            json!({ "target": [ALICE], "registrar_index": 1 }),
        )];
        assert_eq!(collect(&events, 0).account_set(), BTreeSet::from([alice()]));
    }

    #[test]
    fn sub_identity_events_touch_both_ends() {
        let events = vec![event(
            "Identity",
            "SubIdentityAdded",
            json!({ "sub": [BOB], "main": [ALICE], "deposit": "100" }),
        )];
        assert_eq!(
            collect(&events, 0).account_set(),
            BTreeSet::from([alice(), bob()])
        );
    }

    #[test]
    fn sub_identities_set_touches_the_parent_so_its_children_can_be_diffed() {
        // The event says how *many* subs there now are, never which. Re-reading `SubsOf`
        // for `main` is the only way to learn what was removed.
        let events = vec![event(
            "Identity",
            "SubIdentitiesSet",
            json!({ "main": [ALICE], "number_of_subs": 2, "new_deposit": "200" }),
        )];
        assert_eq!(collect(&events, 0).account_set(), BTreeSet::from([alice()]));
    }

    #[test]
    fn username_events_touch_both_the_account_and_the_username() {
        let events = vec![event(
            "Identity",
            "UsernameSet",
            json!({ "who": [ALICE], "username": format!("0x{}", hex::encode("alice.dot")) }),
        )];
        let touched = collect(&events, 0);

        assert_eq!(touched.account_set(), BTreeSet::from([alice()]));
        assert_eq!(touched.usernames, BTreeSet::from(["alice.dot".to_string()]));
    }

    #[test]
    fn a_username_only_event_still_touches_the_username() {
        // `UsernameUnbound`/`Removed`/`Killed` name no account; the account is found by
        // reading `UsernameInfoOf` for the username.
        let events = vec![event(
            "Identity",
            "UsernameUnbound",
            json!({ "username": format!("0x{}", hex::encode("bob.dot")) }),
        )];
        let touched = collect(&events, 0);

        assert!(touched.accounts.is_empty());
        assert_eq!(touched.usernames, BTreeSet::from(["bob.dot".to_string()]));
    }

    #[test]
    fn registrar_changes_flag_the_whole_set() {
        let events = vec![event(
            "Identity",
            "RegistrarAdded",
            json!({ "registrar_index": 3 }),
        )];
        assert!(collect(&events, 0).registrars);
    }

    #[test]
    fn events_from_other_pallets_are_ignored() {
        // The handler runs on every block of the chain, most of which are pure `Balances`
        // traffic. Touching nothing is what keeps the storage-read cost at zero.
        let events = vec![event(
            "Balances",
            "Transfer",
            json!({ "from": [ALICE], "to": [BOB], "amount": "1" }),
        )];
        assert!(collect(&events, 0).is_empty());
    }

    #[test]
    fn an_unknown_future_variant_with_a_known_field_is_still_picked_up() {
        // Name-based extraction is what makes this work without a code change.
        let events = vec![event(
            "Identity",
            "SomeFutureThing",
            json!({ "who": [ALICE], "extra": 7 }),
        )];
        assert_eq!(collect(&events, 0).account_set(), BTreeSet::from([alice()]));
    }

    #[test]
    fn an_unknown_variant_with_no_recognisable_field_is_ignored_not_an_error() {
        let events = vec![event("Identity", "SomeFutureThing", json!({ "extra": 7 }))];
        assert!(collect(&events, 0).is_empty());
    }

    #[test]
    fn a_malformed_account_field_is_skipped_rather_than_stored_wrong() {
        // Better to record nothing than to invent an address from a short hex blob.
        let events = vec![event(
            "Identity",
            "IdentitySet",
            json!({ "who": ["0xdead"] }),
        )];
        assert!(collect(&events, 0).accounts.is_empty());
    }

    #[test]
    fn accounts_are_encoded_with_the_chains_prefix() {
        let events = vec![event("Identity", "IdentitySet", json!({ "who": [ALICE] }))];

        let polkadot = collect(&events, 0).account_set();
        let generic = collect(&events, 42).account_set();

        assert_ne!(polkadot, generic);
        assert!(polkadot.iter().next().unwrap().starts_with('1'));
    }

    #[test]
    fn duplicate_touches_within_a_block_collapse() {
        // Several events for one account in one block must cost one storage read, not N.
        let events = vec![
            event("Identity", "IdentitySet", json!({ "who": [ALICE] })),
            event(
                "Identity",
                "JudgementGiven",
                json!({ "target": [ALICE], "registrar_index": 0 }),
            ),
        ];
        assert_eq!(collect(&events, 0).accounts.len(), 1);
    }
}
