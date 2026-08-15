//! The alias cross-check, end to end across two chains.
//!
//! Transfers happen on the **Asset Hub**; identities are set on the **People** chain; the
//! indexer runs over both into one Postgres; and then a single SQL join answers "who sent
//! this money" with a human name that was never present on the hub at all.
//!
//! That join is the whole point of `pif-identity`. No XCM is involved: both indexers write
//! `chain_id`-keyed rows to the same database, so resolving an address against another
//! chain's identities is an ordinary join.
//!
//! Run with:
//! ```sh
//! just zn-up          # relay + hub + people (takes a few minutes under emulation)
//! docker compose up -d postgres
//! cargo test -p pif-e2e --features handler-balances,handler-identity \
//!     --test three_chain_alias -- --ignored --nocapture
//! ```

#![cfg(all(feature = "handler-balances", feature = "handler-identity"))]

mod common;

use anyhow::{Context, Result};
use common::identity::{data_raw, identity_info, keypair};
use pif_chain::{IndexOptions, pipeline};
use pif_core::{ChainConfig, ss58};
use pif_e2e::database_url;
use sqlx::{PgPool, Row};
use subxt::config::Hasher;
use subxt::config::substrate::BlakeTwo256;
use subxt::{OnlineClient, PolkadotConfig, dynamic::Value, transactions::dynamic};

const HUB_CHAIN_ID: &str = "zn-asset-hub";
const PEOPLE_CHAIN_ID: &str = "zn-people";

fn hub_url() -> String {
    std::env::var("HUB_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:9946".to_owned())
}

fn people_url() -> String {
    std::env::var("PEOPLE_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:9955".to_owned())
}

/// Who transfers what on the hub, and the display name each gets on People.
const CAST: [(&str, &str, u128); 3] = [
    ("alice", "Alice Wonderland", 0),
    ("bob", "Bob Builder", 1_111_111_111_111),
    ("charlie", "Charlie Chaplin", 9_007_199_254_740_993), // 2^53 + 1
];

/// Accounts that get no identity of their own, only a sub-identity under Alice. Resolving
/// these is the case a naive lookup gets wrong: `IdentityOf` is empty for both.
const SUBS: [(&str, &str); 2] = [("dave", "validator-01"), ("eve", "validator-02")];

/// The suffix Alice is the username authority for, installed at genesis by the network spec.
const SUFFIX: &str = "pif";

// `data_raw`, `identity_info` and `keypair` live in `common::identity`: `IdentityInfo` is
// runtime-shaped, and two copies of it would drift into a half-encoded identity rather than
// into a compile error.

async fn connect(url: &str, what: &str) -> Option<OnlineClient<PolkadotConfig>> {
    match OnlineClient::<PolkadotConfig>::from_insecure_url(url).await {
        Ok(api) => Some(api),
        Err(e) => {
            eprintln!("skipping: no {what} node at {url} ({e}); run `just zn-up`");
            None
        }
    }
}

#[tokio::test]
#[ignore = "needs `just zn-up` and Postgres"]
async fn hub_transfers_resolve_to_people_chain_aliases() -> Result<()> {
    let (Some(hub), Some(people)) = (
        connect(&hub_url(), "Asset Hub").await,
        connect(&people_url(), "People").await,
    ) else {
        return Ok(());
    };

    let pool: PgPool = pif_db::connect(&database_url(), 5).await?;
    pif_db::migrate(&pool).await?;
    for handler in pif_e2e::registry().all() {
        pif_chain::handlers::run_migrations(&pool, handler).await?;
    }

    // A respawned network has a fresh genesis, which would correctly trip
    // `guard_chain_identity` if these ids were still bound to the previous one.
    for id in [HUB_CHAIN_ID, PEOPLE_CHAIN_ID] {
        sqlx::query("DELETE FROM chains WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await?;
    }

    let hub_config = ChainConfig::rpc(HUB_CHAIN_ID, hub_url()).with_handlers(["balances-transfer"]);
    let people_config = ChainConfig::rpc(PEOPLE_CHAIN_ID, people_url()).with_handlers(["identity"]);

    // The cross-chain join matches on SS58 text, so the two chains must render the same key
    // identically. Asset Hub and People both use prefix 42 on Westend (0 on Polkadot); a
    // mismatch here would make every join silently return nothing.
    let hub_info = pif_chain::ChainClient::connect(&hub_config).await?.info;
    let people_info = pif_chain::ChainClient::connect(&people_config).await?.info;
    assert_eq!(
        hub_info.ss58_prefix, people_info.ss58_prefix,
        "cross-chain address joins require a shared SS58 prefix"
    );
    let prefix = hub_info.ss58_prefix;
    println!(
        "hub={} people={} (ss58 prefix {prefix})",
        hub_info.name, people_info.name
    );

    // ---- People: give each account an identity ---------------------------------------
    for (name, display, _) in CAST {
        let who = keypair(name);
        let call = dynamic("Identity", "set_identity", vec![identity_info(display)]);

        people
            .at_current_block()
            .await?
            .transactions()
            .sign_and_submit_then_watch_default(&call, &who)
            .await?
            .wait_for_finalized_success()
            .await
            .with_context(|| format!("set_identity for {name} ({display})"))?;

        println!("people: {name} -> {display:?}");
    }

    // ---- People: get those identities judged -----------------------------------------
    //
    // Setting a name is not the same as anyone believing it, and `verified` is the field the
    // cross-check is really for. Alice is registrar #0, installed at genesis by the network
    // spec's `raw_spec_override` -- `add_registrar` needs a root origin, and this chain has
    // no `Sudo` and no reachable relay to send an XCM `Transact` from.
    let alice = keypair("alice");

    for (name, display, _) in CAST {
        let who = keypair(name);

        // The subject asks for a judgement (max_fee 0; registrar #0 charges nothing).
        let request = dynamic(
            "Identity",
            "request_judgement",
            vec![Value::u128(0), Value::u128(0)],
        );
        people
            .at_current_block()
            .await?
            .transactions()
            .sign_and_submit_then_watch_default(&request, &who)
            .await?
            .wait_for_finalized_success()
            .await
            .with_context(|| format!("request_judgement for {name}"))?;

        // The registrar answers. `identity` is the hash of the *exact* IdentityInfo being
        // judged, which is what stops a stale judgement being applied to a changed identity
        // -- so it is re-encoded against the runtime's own type rather than guessed.
        let at = people.at_current_block().await?;
        let md = at.metadata();
        let info_ty = md
            .pallet_by_name("Identity")
            .and_then(|p| {
                p.call_variant_by_name("set_identity")
                    .map(|c| c.fields[0].ty.id)
            })
            .context("set_identity metadata")?;

        let mut encoded = Vec::new();
        scale_value::scale::encode_as_type(
            &identity_info(display),
            info_ty,
            md.types(),
            &mut encoded,
        )
        .context("encoding IdentityInfo for the judgement hash")?;
        let info_hash = BlakeTwo256.hash(&encoded);

        let provide = dynamic(
            "Identity",
            "provide_judgement",
            vec![
                Value::u128(0),
                Value::unnamed_variant("Id", [Value::from_bytes(who.public_key().0)]),
                Value::unnamed_variant("KnownGood", []),
                Value::from_bytes(info_hash.0),
            ],
        );
        at.transactions()
            .sign_and_submit_then_watch_default(&provide, &alice)
            .await?
            .wait_for_finalized_success()
            .await
            .with_context(|| format!("provide_judgement for {name}"))?;

        println!("people: {name} judged KnownGood by registrar #0");
    }

    // ---- People: grant and accept usernames -------------------------------------------
    //
    // Alice is the username authority for `.pif`, installed at genesis alongside the
    // registrar (`add_username_authority` is root-only, same as `add_registrar`).
    //
    // Passing `signature: None` queues the name rather than granting it, so each account has
    // to accept: that walks a username through pending -> active -> primary, which is three
    // distinct rows' worth of state in the indexer rather than one.
    for (name, _, _) in CAST {
        let who = keypair(name);
        let username = format!("{name}.{SUFFIX}");

        let grant = dynamic(
            "Identity",
            "set_username_for",
            vec![
                Value::unnamed_variant("Id", [Value::from_bytes(who.public_key().0)]),
                Value::from_bytes(username.as_bytes()),
                Value::unnamed_variant("None", []),
                Value::bool(true), // spend the authority's allocation, not a deposit
            ],
        );
        people
            .at_current_block()
            .await?
            .transactions()
            .sign_and_submit_then_watch_default(&grant, &alice)
            .await?
            .wait_for_finalized_success()
            .await
            .with_context(|| format!("set_username_for {username}"))?;

        let accept = dynamic(
            "Identity",
            "accept_username",
            vec![Value::from_bytes(username.as_bytes())],
        );
        people
            .at_current_block()
            .await?
            .transactions()
            .sign_and_submit_then_watch_default(&accept, &who)
            .await?
            .wait_for_finalized_success()
            .await
            .with_context(|| format!("accept_username {username}"))?;

        println!("people: {name} accepted username {username:?}");
    }

    // ---- People: give Alice two sub-identities ----------------------------------------
    //
    // Dave and Eve get NO identity of their own -- that is the point. A sub-identity borrows
    // its parent's name and verification, so resolving one has to walk up through `SuperOf`.
    let subs = Value::unnamed_composite(SUBS.map(|(name, label)| {
        Value::unnamed_composite([
            Value::from_bytes(keypair(name).public_key().0),
            data_raw(label),
        ])
    }));
    let set_subs = dynamic("Identity", "set_subs", vec![subs]);
    people
        .at_current_block()
        .await?
        .transactions()
        .sign_and_submit_then_watch_default(&set_subs, &alice)
        .await?
        .wait_for_finalized_success()
        .await
        .context("set_subs for Alice")?;
    println!("people: alice set subs {SUBS:?}");

    // ---- Asset Hub: move money between those same accounts ---------------------------
    let alice_ss58 = ss58::encode(&alice.public_key().0, prefix);
    let mut expected = Vec::new();

    for (name, display, amount) in CAST {
        if amount == 0 {
            continue; // Alice is the sender, not a recipient
        }
        let to = keypair(name);
        let to_ss58 = ss58::encode(&to.public_key().0, prefix);

        let call = dynamic(
            "Balances",
            "transfer_allow_death",
            vec![
                Value::unnamed_variant("Id", [Value::from_bytes(to.public_key().0)]),
                Value::u128(amount),
            ],
        );

        // Waiting for finalization between submissions also keeps the nonce in order.
        hub.at_current_block()
            .await?
            .transactions()
            .sign_and_submit_then_watch_default(&call, &alice)
            .await?
            .wait_for_finalized_success()
            .await
            .with_context(|| format!("transfer of {amount} to {name}"))?;

        println!("hub: alice -> {name}, {amount}");
        expected.push((to_ss58, display, amount));
    }

    // ---- index both chains ------------------------------------------------------------
    for config in [&hub_config, &people_config] {
        let client = pif_chain::ChainClient::connect(config).await?;
        let head = client.finalized_number().await?;
        println!("indexing {} 0..={head}", config.id);

        pipeline::run(
            &pool,
            config,
            &pif_e2e::registry(),
            IndexOptions {
                from: Some(0),
                stop_at: Some(head),
            },
        )
        .await
        .with_context(|| format!("indexing {}", config.id))?;
    }

    // ---- each identity landed ---------------------------------------------------------
    for (name, display, _) in CAST {
        let who = ss58::encode(&keypair(name).public_key().0, prefix);
        let row = sqlx::query(
            "SELECT effective_display, effective_verified
               FROM identity_current WHERE chain_id = $1 AND account = $2",
        )
        .bind(PEOPLE_CHAIN_ID)
        .bind(&who)
        .fetch_optional(&pool)
        .await?
        .with_context(|| format!("no identity indexed for {name} ({who})"))?;

        let stored: Option<String> = row.get(0);
        assert_eq!(
            stored.as_deref(),
            Some(display),
            "display name for {name} did not survive the indexer"
        );

        // Registrar #0 vouched for each of these, so the indexer must say so. Setting a name
        // and having one *believed* are different things, and `verified` is the distinction
        // the whole cross-check exists to make -- previously exercised only against stubs.
        let verified: bool = row.get(1);
        assert!(
            verified,
            "{name} was judged KnownGood by registrar #0 and must read as verified"
        );
    }

    // ---- usernames landed, with the right status and primary flag ---------------------
    for (name, _, _) in CAST {
        let who = ss58::encode(&keypair(name).public_key().0, prefix);
        let username = format!("{name}.{SUFFIX}");

        let row = sqlx::query(
            "SELECT account, status, is_primary FROM usernames
              WHERE chain_id = $1 AND username = $2",
        )
        .bind(PEOPLE_CHAIN_ID)
        .bind(&username)
        .fetch_optional(&pool)
        .await?
        .with_context(|| format!("no username row for {username}"))?;

        let owner: Option<String> = row.get(0);
        let status: String = row.get(1);
        let is_primary: bool = row.get(2);

        assert_eq!(owner.as_deref(), Some(who.as_str()), "{username} owner");
        // Accepted, so no longer pending: the handler must have followed the transition.
        assert_eq!(
            status, "active",
            "{username} should be active after acceptance"
        );
        assert!(
            is_primary,
            "{username} is the account's only username, so primary"
        );

        // And it surfaces on the account's own row.
        let joined: Option<String> = sqlx::query(
            "SELECT username FROM identity_current WHERE chain_id = $1 AND account = $2",
        )
        .bind(PEOPLE_CHAIN_ID)
        .bind(&who)
        .fetch_one(&pool)
        .await?
        .get(0);
        assert_eq!(joined.as_deref(), Some(username.as_str()));
    }

    // ---- sub-identities inherit their parent's name and verification ------------------
    //
    // Dave and Eve have no `IdentityOf` entry at all. A view built over `identities` alone
    // would not return them, and a resolver that only read `IdentityOf` would report "no
    // alias" for accounts that plainly have one.
    let alice_display = "Alice Wonderland";
    for (name, label) in SUBS {
        let sub = ss58::encode(&keypair(name).public_key().0, prefix);

        let row = sqlx::query(
            "SELECT effective_display, effective_verified, super_account, sub_label, display
               FROM identity_current WHERE chain_id = $1 AND account = $2",
        )
        .bind(PEOPLE_CHAIN_ID)
        .bind(&sub)
        .fetch_optional(&pool)
        .await?
        .with_context(|| format!("{name} ({sub}) missing from identity_current"))?;

        let own_display: Option<String> = row.get(4);
        assert_eq!(own_display, None, "{name} must have no identity of its own");

        let effective: Option<String> = row.get(0);
        let verified: bool = row.get(1);
        let parent: Option<String> = row.get(2);
        let stored_label: Option<String> = row.get(3);

        assert_eq!(
            effective.as_deref(),
            Some(alice_display),
            "{name} inherits Alice's name"
        );
        assert!(verified, "{name} inherits Alice's registrar judgement");
        assert_eq!(parent.as_deref(), Some(alice_ss58.as_str()));
        assert_eq!(stored_label.as_deref(), Some(label), "{name} sub label");

        println!("people: {name} -> {effective:?} via {label:?}");
    }

    // ---- THE CROSS-CHECK: hub transfers resolved through People identities ------------
    //
    // `transfers` knows only addresses; it has never heard of the People chain. The join
    // supplies the names, keyed by `chain_id` -- which is the entire mechanism.
    //
    // Scoped to this run's amounts rather than asserting a total: the network is long-lived
    // and re-running the demo adds more transfers, so a global count would be testing how
    // many times the demo has been run.
    let amounts: Vec<String> = expected.iter().map(|(_, _, a)| a.to_string()).collect();

    let rows = sqlx::query(
        "SELECT t.amount::text,
                sender.effective_display    AS from_display,
                recipient.effective_display AS to_display
           FROM transfers t
           LEFT JOIN identity_current sender
                  ON sender.chain_id = $2 AND sender.account = t.from_address
           LEFT JOIN identity_current recipient
                  ON recipient.chain_id = $2 AND recipient.account = t.to_address
          WHERE t.chain_id = $1 AND t.amount::text = ANY($3)
          ORDER BY t.block_number, t.event_idx",
    )
    .bind(HUB_CHAIN_ID)
    .bind(PEOPLE_CHAIN_ID)
    .bind(&amounts)
    .fetch_all(&pool)
    .await?;

    assert!(
        !rows.is_empty(),
        "none of this run's transfers were indexed on the hub"
    );

    println!("\n--- cross-chain resolution ---");
    for row in &rows {
        let amount: String = row.get(0);
        let from: Option<String> = row.get(1);
        let to: Option<String> = row.get(2);
        println!(
            "  {} -> {}: {amount}",
            from.as_deref().unwrap_or("<unknown>"),
            to.as_deref().unwrap_or("<unknown>")
        );

        assert_eq!(
            from.as_deref(),
            Some("Alice Wonderland"),
            "the sender must resolve to a People-chain display name"
        );
        assert!(
            to.is_some(),
            "the recipient must resolve to a People-chain display name"
        );
    }

    // And the exact amounts, so the join did not quietly reorder or lose precision.
    for (to_ss58, display, amount) in &expected {
        let found: Option<String> = sqlx::query(
            "SELECT i.effective_display
               FROM transfers t
               JOIN identity_current i
                 ON i.chain_id = $2 AND i.account = t.to_address
              WHERE t.chain_id = $1 AND t.to_address = $3 AND t.amount::text = $4",
        )
        .bind(HUB_CHAIN_ID)
        .bind(PEOPLE_CHAIN_ID)
        .bind(to_ss58)
        .bind(amount.to_string())
        .fetch_optional(&pool)
        .await?
        .and_then(|r| r.get(0));

        assert_eq!(
            found.as_deref(),
            Some(*display),
            "transfer of {amount} to {to_ss58} should resolve to {display}"
        );
    }

    // The resolver -- what another handler would actually call -- must agree with the SQL.
    use pif_identity::{IdentityResolver, PgIdentityResolver};
    let resolver = PgIdentityResolver::new(pool.clone(), PEOPLE_CHAIN_ID);
    let alias = resolver
        .alias_of(&alice_ss58)
        .await?
        .context("resolver found no alias for Alice")?;

    assert_eq!(alias.display.as_deref(), Some("Alice Wonderland"));
    assert!(alias.verified, "registrar #0 judged Alice KnownGood");
    assert_eq!(
        alias.best_judgement,
        Some(pif_identity::Judgement::KnownGood),
        "the strongest judgement must survive to the resolver"
    );
    // The username is what a user actually types, so it must be the preferred name.
    assert_eq!(
        alias.username.as_deref(),
        Some(format!("alice.{SUFFIX}").as_str())
    );
    assert_eq!(alias.best_name(), Some(format!("alice.{SUFFIX}").as_str()));
    println!("\nresolver: {alice_ss58} -> {:?}", alias.best_name());

    // Reverse: a username resolves back to its owner.
    let owner = resolver
        .resolve_username(&format!("alice.{SUFFIX}"))
        .await?
        .context("username did not resolve to an account")?;
    assert_eq!(owner, alice_ss58);
    println!("resolver: alice.{SUFFIX} -> {owner}");

    // And a sub-identity resolves through its parent -- no identity of its own anywhere.
    let dave = ss58::encode(&keypair("dave").public_key().0, prefix);
    let dave_alias = resolver
        .alias_of(&dave)
        .await?
        .context("resolver found no alias for a sub-identity")?;

    assert_eq!(dave_alias.display.as_deref(), Some(alice_display));
    assert!(
        dave_alias.verified,
        "verification is inherited from the parent"
    );
    let (parent, label) = dave_alias
        .via_super
        .clone()
        .context("sub-identity must report its parent")?;
    assert_eq!(parent, alice_ss58);
    assert_eq!(label, "validator-01");
    println!("resolver: {dave} -> {:?} via {label:?}", dave_alias.display);

    Ok(())
}
