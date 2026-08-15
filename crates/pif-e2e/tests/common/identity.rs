//! Submitting `pallet_identity` traffic, shared by the tests that need some.
//!
//! Written once because [`identity_info`] is **runtime-shaped**: the People chains carry
//! `matrix`/`github`/`discord` and drop the relay chain's legacy `additional`, and encoding
//! is checked against live metadata rather than guessed. Two copies of that would drift, and
//! the drift would show up as a half-encoded identity rather than as a compile error.

use anyhow::{Context, Result};
use subxt::config::Hasher;
use subxt::config::substrate::BlakeTwo256;
use subxt::{OnlineClient, PolkadotConfig, dynamic::Value, transactions::dynamic};
use subxt_signer::sr25519::{Keypair, dev};

/// `Data::RawN(bytes)` — how `pallet_identity` stores a short text field.
pub fn data_raw(text: &str) -> Value {
    Value::unnamed_variant(format!("Raw{}", text.len()), [Value::from_bytes(text)])
}

pub fn data_none() -> Value {
    Value::unnamed_variant("None", [])
}

/// `people_westend_runtime::people::IdentityInfo`.
///
/// The field list is runtime-specific. It is read from live metadata by
/// `people_metadata::print_identity_info_shape`, and encoding fails loudly if it drifts,
/// which is what we want: a silently half-encoded identity would be worse.
pub fn identity_info(display: &str) -> Value {
    Value::named_composite([
        ("display", data_raw(display)),
        ("legal", data_none()),
        ("web", data_none()),
        ("matrix", data_none()),
        ("email", data_none()),
        ("pgp_fingerprint", Value::unnamed_variant("None", [])),
        ("image", data_none()),
        ("twitter", data_none()),
        ("github", data_none()),
        ("discord", data_none()),
    ])
}

pub fn keypair(name: &str) -> Keypair {
    match name {
        "alice" => dev::alice(),
        "bob" => dev::bob(),
        "charlie" => dev::charlie(),
        "dave" => dev::dave(),
        "eve" => dev::eve(),
        other => panic!("unknown dev account {other}"),
    }
}

/// Submit one call and wait for it to be finalized.
///
/// Waiting between submissions also keeps each account's nonce in order, which is why every
/// helper here is sequential rather than concurrent.
pub async fn submit<C: subxt::transactions::Payload>(
    api: &OnlineClient<PolkadotConfig>,
    signer: &Keypair,
    call: C,
    what: &str,
) -> Result<()> {
    api.at_current_block()
        .await?
        .transactions()
        .sign_and_submit_then_watch_default(&call, signer)
        .await?
        .wait_for_finalized_success()
        .await
        .with_context(|| what.to_owned())?;
    Ok(())
}

/// Give an account a display name.
pub async fn set_identity(
    api: &OnlineClient<PolkadotConfig>,
    who: &str,
    display: &str,
) -> Result<()> {
    let call = dynamic("Identity", "set_identity", vec![identity_info(display)]);
    submit(
        api,
        &keypair(who),
        call,
        &format!("set_identity for {who} ({display})"),
    )
    .await
}

/// Have registrar #0 judge an identity `KnownGood`.
///
/// Alice is registrar #0, installed at genesis by the network spec: `add_registrar` needs a
/// root origin, and the People chain has no `Sudo` and no reachable relay to send an XCM
/// `Transact` from.
pub async fn judge(api: &OnlineClient<PolkadotConfig>, who: &str, display: &str) -> Result<()> {
    let subject = keypair(who);

    let request = dynamic(
        "Identity",
        "request_judgement",
        vec![Value::u128(0), Value::u128(0)],
    );
    submit(
        api,
        &subject,
        request,
        &format!("request_judgement for {who}"),
    )
    .await?;

    // `identity` is the hash of the *exact* `IdentityInfo` being judged, which is what stops
    // a stale judgement being applied to a changed identity — so it is re-encoded against
    // the runtime's own type rather than guessed.
    let at = api.at_current_block().await?;
    let md = at.metadata();
    let info_ty = md
        .pallet_by_name("Identity")
        .and_then(|p| {
            p.call_variant_by_name("set_identity")
                .map(|c| c.fields[0].ty.id)
        })
        .context("set_identity metadata")?;

    let mut encoded = Vec::new();
    scale_value::scale::encode_as_type(&identity_info(display), info_ty, md.types(), &mut encoded)
        .context("encoding IdentityInfo for the judgement hash")?;
    let info_hash = BlakeTwo256.hash(&encoded);

    let provide = dynamic(
        "Identity",
        "provide_judgement",
        vec![
            Value::u128(0),
            Value::unnamed_variant("Id", [Value::from_bytes(subject.public_key().0)]),
            Value::unnamed_variant("KnownGood", []),
            Value::from_bytes(info_hash.0),
        ],
    );
    submit(
        api,
        &keypair("alice"),
        provide,
        &format!("provide_judgement for {who}"),
    )
    .await
}

/// Grant a username and have the holder accept it.
///
/// Passing `signature: None` queues the name rather than granting it, so accepting walks it
/// through pending -> active -> primary — three distinct rows' worth of state in the indexer
/// rather than one.
pub async fn grant_username(
    api: &OnlineClient<PolkadotConfig>,
    who: &str,
    username: &str,
) -> Result<()> {
    let holder = keypair(who);

    let grant = dynamic(
        "Identity",
        "set_username_for",
        vec![
            Value::unnamed_variant("Id", [Value::from_bytes(holder.public_key().0)]),
            Value::from_bytes(username.as_bytes()),
            Value::unnamed_variant("None", []),
            Value::bool(true), // spend the authority's allocation, not a deposit
        ],
    );
    submit(
        api,
        &keypair("alice"),
        grant,
        &format!("set_username_for {username}"),
    )
    .await?;

    let accept = dynamic(
        "Identity",
        "accept_username",
        vec![Value::from_bytes(username.as_bytes())],
    );
    submit(api, &holder, accept, &format!("accept_username {username}")).await
}

/// Attach sub-identities to Alice.
///
/// The subs get **no identity of their own** — that is the point. A sub borrows its parent's
/// name, so resolving one walks up through `SuperOf`, and `IdentityOf` for the sub itself is
/// a genuine `Ok(None)`.
pub async fn set_subs(api: &OnlineClient<PolkadotConfig>, subs: &[(&str, &str)]) -> Result<()> {
    let encoded = Value::unnamed_composite(subs.iter().map(|(name, label)| {
        Value::unnamed_composite([
            Value::from_bytes(keypair(name).public_key().0),
            data_raw(label),
        ])
    }));

    let call = dynamic("Identity", "set_subs", vec![encoded]);
    submit(api, &keypair("alice"), call, "set_subs for Alice").await
}
