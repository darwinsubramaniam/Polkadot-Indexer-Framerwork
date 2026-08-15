//! The single test that proves the proposal's central claim — IPD-002 §14.
//!
//! Index a range of the **People** chain with `handler-identity`, then point the node URL at
//! a dead address and replay the same range. It must succeed, and it must produce exactly
//! the rows the live run produced.
//!
//! `pif-identity` is the handler that makes this worth asserting. It resolves almost
//! everything through chain **state** rather than event payloads — `IdentitySet { who }`
//! carries no display name, `JudgementGiven { target, registrar_index }` carries no
//! judgement — so it reads `IdentityOf`, `SuperOf`, `SubsOf`, `UsernameInfoOf`,
//! `PendingUsernames` and `Registrars` per block. If any one of those is not archived, or is
//! archived under a key that does not match on the way back, this test fails rather than
//! quietly re-downloading.
//!
//! Run with:
//! ```sh
//! just zn-up          # relay + hub + people (takes a few minutes under emulation)
//! docker compose up -d postgres
//! cargo test -p pif-e2e --features handler-identity \
//!     --test replay_offline -- --ignored --nocapture
//! ```

#![cfg(feature = "handler-identity")]

mod common;

use anyhow::{Context, Result};
use common::identity;
use pif_chain::{IndexOptions, pipeline};
use pif_core::{ChainConfig, PipelineConfig};
use pif_e2e::database_url;
use sqlx::{PgPool, Row};
use subxt::{OnlineClient, PolkadotConfig};

const CHAIN_ID: &str = "zn-people-replay";

/// An address nothing is listening on. Every offline claim below is asserted by *using* this
/// rather than by counting round-trips: if any part of the digest still reached for a node,
/// the test could not pass.
const DEAD_URL: &str = "ws://127.0.0.1:1";

/// Accounts that get a display name of their own.
const NAMED: [(&str, &str); 2] = [("alice", "Alice Wonderland"), ("bob", "Bob Builder")];

/// Accounts that get *only* a sub-identity under Alice.
///
/// The case that matters most for caching: `IdentityOf` is genuinely empty for these, so the
/// archived answer is `Ok(None)`. A cache that stored only hits would go back to the network
/// for exactly these reads — and "this account has no identity" is the common answer, not
/// the exceptional one.
const SUBS: [(&str, &str); 2] = [("dave", "validator-01"), ("eve", "validator-02")];

/// The suffix Alice is the username authority for, installed at genesis by the network spec.
///
/// The local part is deliberately not `alice`: a username can only be claimed once on a
/// chain, and `three_chain_alias` claims that one. Both tests still want a network of their
/// own — they judge the same identities — but this at least keeps the cheap collision out of
/// the way.
const USERNAME: &str = "replayer.pif";

fn people_url() -> String {
    std::env::var("PEOPLE_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:9955".to_owned())
}

async fn connect(url: &str) -> Option<OnlineClient<PolkadotConfig>> {
    match OnlineClient::<PolkadotConfig>::from_insecure_url(url).await {
        Ok(api) => Some(api),
        Err(e) => {
            eprintln!("skipping: no People node at {url} ({e}); run `just zn-up`");
            None
        }
    }
}

/// Everything the block loop derives about accounts, as one comparable value.
///
/// `identity_registrars` is deliberately absent: it is seeded by the bootstrap sweep, which
/// is `StorageAt::iter` — tens of thousands of keys on a real chain, excluded from the cache
/// on purpose and not re-run by a replay. Including it would be asserting something this
/// phase does not claim.
#[derive(Debug, PartialEq, Eq)]
struct Identities {
    identities: Vec<String>,
    usernames: Vec<String>,
    subs: Vec<String>,
    current: Vec<String>,
}

async fn snapshot(pool: &PgPool, chain_id: &str) -> Result<Identities> {
    async fn rows(pool: &PgPool, chain_id: &str, sql: &'static str) -> Result<Vec<String>> {
        Ok(sqlx::query(sql)
            .bind(chain_id)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|r| r.get::<String, _>(0))
            .collect())
    }

    Ok(Identities {
        // Concatenated into one text column per row so the comparison covers every field
        // without naming a tuple type per table — including `raw`, which is the full decoded
        // `Registration` exactly as storage returned it. If the cache changed a value even
        // slightly, that column is where it shows.
        identities: rows(
            pool,
            chain_id,
            "SELECT concat_ws('|', account, valid_from_block, valid_to_block, display, legal, \
                    web, email, twitter, matrix, github, discord, is_verified, \
                    judgements::text, deposit::text, raw::text)
               FROM identities WHERE chain_id = $1 ORDER BY account, valid_from_block",
        )
        .await?,
        usernames: rows(
            pool,
            chain_id,
            "SELECT concat_ws('|', username, account, status, is_primary::text)
               FROM usernames WHERE chain_id = $1 ORDER BY username",
        )
        .await?,
        subs: rows(
            pool,
            chain_id,
            "SELECT concat_ws('|', sub, super_account, label)
               FROM sub_identities WHERE chain_id = $1 ORDER BY sub",
        )
        .await?,
        current: rows(
            pool,
            chain_id,
            "SELECT concat_ws('|', account, effective_display, effective_verified::text, \
                    username, super_account, sub_label)
               FROM identity_current WHERE chain_id = $1 ORDER BY account",
        )
        .await?,
    })
}

#[tokio::test]
#[ignore = "needs `just zn-up` and Postgres"]
async fn the_identity_handler_replays_with_no_network() -> Result<()> {
    let Some(people) = connect(&people_url()).await else {
        return Ok(());
    };

    let pool: PgPool = pif_db::connect(&database_url(), 5).await?;
    pif_db::migrate(&pool).await?;
    for handler in pif_e2e::registry().all() {
        pif_chain::handlers::run_migrations(&pool, handler).await?;
    }

    // A respawned network has a fresh genesis, which would correctly trip
    // `guard_chain_identity` if this id were still bound to the previous one.
    sqlx::query("DELETE FROM chains WHERE id = $1")
        .bind(CHAIN_ID)
        .execute(&pool)
        .await?;

    let archive = tempfile::tempdir()?;
    let pipeline_config = PipelineConfig {
        hot_path: archive.path().to_path_buf(),
        segment_size: 64,
        chunk_size: 16,
        max_digest_lag: None,
    };

    // ---- traffic: every storage read the handler makes, exercised -----------------------
    for (who, display) in NAMED {
        identity::set_identity(&people, who, display).await?;
        println!("people: {who} -> {display:?}");
    }

    // A judgement is read from `IdentityOf`'s `judgements` field, not from the event.
    identity::judge(&people, NAMED[0].0, NAMED[0].1).await?;
    println!("people: {} judged KnownGood", NAMED[0].0);

    // Usernames walk pending -> active -> primary, across `PendingUsernames`,
    // `UsernameInfoOf` and `UsernameOf`.
    identity::grant_username(&people, NAMED[0].0, USERNAME).await?;
    println!("people: {} accepted {USERNAME:?}", NAMED[0].0);

    // The negative-cache case: these accounts have no `IdentityOf` of their own.
    identity::set_subs(&people, &SUBS).await?;
    println!("people: alice set subs {SUBS:?}");

    // ---- live pass: every read misses the cache and fills it ----------------------------
    let live_config = ChainConfig::rpc(CHAIN_ID, people_url())
        .with_handlers(["identity"])
        .with_pipeline(pipeline_config.clone());

    let head = pif_chain::ChainClient::connect(&live_config)
        .await?
        .finalized_number()
        .await?;
    println!("indexing {CHAIN_ID} 0..={head}");

    pipeline::run(
        &pool,
        &live_config,
        &pif_e2e::registry(),
        IndexOptions {
            from: Some(0),
            stop_at: Some(head),
        },
    )
    .await
    .context("live indexing pass")?;

    let live = snapshot(&pool, CHAIN_ID).await?;
    assert!(
        live.identities.len() >= NAMED.len(),
        "expected an identity row per named account, got {:?}",
        live.identities
    );
    assert_eq!(
        live.subs.len(),
        SUBS.len(),
        "expected a sub-identity row per sub, got {:?}",
        live.subs
    );
    assert!(
        live.current.iter().any(|row| row.contains(USERNAME)),
        "the username never reached identity_current: {:?}",
        live.current
    );

    // ---- wipe what the block loop derives, keep the archive -----------------------------
    //
    // `chains` stays: a replay reads the chain's identity (id, SS58 prefix) back out of
    // Postgres rather than asking a node, which is what lets it run with nothing listening.
    for sql in [
        "DELETE FROM identities WHERE chain_id = $1",
        "DELETE FROM usernames WHERE chain_id = $1",
        "DELETE FROM sub_identities WHERE chain_id = $1",
        "DELETE FROM blocks WHERE chain_id = $1",
    ] {
        sqlx::query(sql).bind(CHAIN_ID).execute(&pool).await?;
    }

    let wiped = snapshot(&pool, CHAIN_ID).await?;
    assert!(
        wiped.identities.is_empty() && wiped.subs.is_empty() && wiped.usernames.is_empty(),
        "the wipe did not actually remove the rows, so the comparison below proves nothing"
    );

    // ---- replay with nothing listening --------------------------------------------------
    let offline_config = ChainConfig::rpc(CHAIN_ID, DEAD_URL)
        .with_handlers(["identity"])
        .with_pipeline(pipeline_config);

    pipeline::replay(&pool, &offline_config, &pif_e2e::registry(), 0, head)
        .await
        .context("replaying with no reachable node")?;

    assert_eq!(
        snapshot(&pool, CHAIN_ID).await?,
        live,
        "a replayed identity projection must match the live one exactly"
    );

    println!("replayed {CHAIN_ID} 0..={head} from the archive alone");
    Ok(())
}
