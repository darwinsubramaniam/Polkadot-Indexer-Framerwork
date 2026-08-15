//! Generate the zombienet genesis override that installs Alice as identity registrar #0
//! **and** as the username authority for the `.pif` suffix.
//!
//! ## Why a genesis override rather than a transaction
//!
//! Both `Identity::add_registrar` and `Identity::add_username_authority` need a root origin.
//! The People chain has **no `Sudo` pallet** —
//! on a real network that origin arrives from the relay as an XCM `Transact`, and this demo
//! network cannot deliver one: its parachains author alone via `--dev-block-time` and are
//! never backed by the relay, so no DMP message ever reaches them.
//!
//! Genesis is the way in. `pallet_identity` has no `GenesisConfig` for registrars, so the
//! runtime-genesis patch cannot express this either — but zombienet's `raw_spec_override`
//! deep-merges JSON into the *raw* chain spec, and the raw spec is literally a map of hex
//! storage key to hex value. Writing `Identity::Registrars` there puts Alice in the registrar
//! set from block zero.
//!
//! ## Regenerating
//!
//! Both key and value are derived from live metadata, never hand-encoded, so this stays
//! correct across runtime upgrades:
//!
//! ```sh
//! just zn-up   # any People node will do
//! cargo test -p pif-e2e --features handler-identity \
//!     --test gen_registrar_override -- --ignored --nocapture
//! ```
//!
//! It rewrites `crates/pif-e2e/networks/people-registrar.json`, which the network TOML
//! references. Commit the result.

#![cfg(feature = "handler-identity")]

use pif_chain::{ChainClient, decode};
use pif_core::ChainConfig;
use scale_value::Value;

/// The username suffix Alice is authorised to grant, as in `alice.pif`. `validate_username`
/// takes everything after the last `.` as the suffix, and requires the part before it to be
/// lowercase alphanumeric.
const USERNAME_SUFFIX: &[u8] = b"pif";

/// Alice's well-known sr25519 public key — registrar #0 and username authority.
const ALICE: [u8; 32] = [
    0xd4, 0x35, 0x93, 0xc7, 0x15, 0xfd, 0xd3, 0x1c, 0x61, 0x14, 0x1a, 0xbd, 0x04, 0xa9, 0x9f, 0xd6,
    0x82, 0x2c, 0x85, 0x58, 0x85, 0x4c, 0xcd, 0xe3, 0x9a, 0x56, 0x84, 0xe7, 0xa5, 0x6d, 0xa2, 0x7d,
];

#[tokio::test]
#[ignore = "generator; run with --ignored"]
async fn generate_people_registrar_override() {
    let url = std::env::var("PEOPLE_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:9955".to_owned());

    let config = ChainConfig {
        id: "people-dev".to_owned(),
        ws_url: url.clone(),
        start_block: 0,
        handlers: Vec::new(),
    };

    let Ok(client) = ChainClient::connect(&config).await else {
        eprintln!("skipping: no People node at {url}; run `just zn-up`");
        return;
    };

    let number = client.finalized_number().await.expect("finalized head");
    let at = decode::at_block(&client.client, &client.info, number)
        .await
        .expect("block handle");
    let md = at.metadata();

    // --- the storage key ---------------------------------------------------------------
    //
    // `Registrars` is a plain StorageValue, so its key *is* the 32-byte prefix
    // twox128("Identity") ++ twox128("Registrars"). Taken from subxt rather than hashed here,
    // so there is no second implementation to get wrong.
    let entry = at
        .storage()
        .entry(("Identity", "Registrars"))
        .expect("Identity.Registrars must exist");
    let key = format!("0x{}", hex::encode(entry.key_prefix()));

    // --- the value ---------------------------------------------------------------------
    //
    // Encoded against the runtime's own type, so the `RegistrarInfo` layout (and the width of
    // its `fields` bitflags, which is runtime-defined) cannot be guessed wrong.
    let value_ty = md
        .pallet_by_name("Identity")
        .and_then(|p| p.storage())
        .and_then(|s| s.entry_by_name("Registrars"))
        .expect("Registrars metadata")
        .value_ty();

    // BoundedVec<Option<RegistrarInfo>> with a single entry: Alice, no fee, no required
    // fields. A zero fee is what lets `request_judgement` be paid for by anyone.
    let registrars =
        Value::unnamed_composite([Value::unnamed_composite([Value::unnamed_variant(
            "Some",
            [Value::named_composite([
                ("account", Value::from_bytes(ALICE)),
                ("fee", Value::u128(0)),
                ("fields", Value::u128(0)),
            ])],
        )])]);

    let mut encoded = Vec::new();
    scale_value::scale::encode_as_type(&registrars, value_ty, md.types(), &mut encoded)
        .expect("Registrars value must encode against the runtime type");
    let value = format!("0x{}", hex::encode(&encoded));

    println!("Identity.Registrars");
    println!("  key   = {key}");
    println!("  value = {value}");

    // Round-trip it: if this does not decode back to one registrar whose account is Alice,
    // the override would silently install nothing.
    let decoded = scale_value::scale::decode_as_type(&mut &encoded[..], value_ty, md.types())
        .expect("must decode back");
    let json = pif_core::codec::value_to_json(&decoded.map_context(|_| ()));
    println!("  decodes to {json}");

    let parsed = pif_identity::read::parse_registrars(&json, client.info.ss58_prefix);
    assert_eq!(parsed.len(), 1, "exactly one registrar");
    let (account, fee, _) = parsed[0].as_ref().expect("registrar 0 present");
    assert_eq!(
        account.as_deref(),
        Some(pif_core::ss58::encode(&ALICE, client.info.ss58_prefix).as_str()),
        "registrar 0 must be Alice"
    );
    assert_eq!(fee.as_ref().map(ToString::to_string).as_deref(), Some("0"));

    // --- Alice as username authority for ".pif" -----------------------------------------
    //
    // `AuthorityOf` is a map, so unlike `Registrars` the key includes the hashed suffix.
    // `fetch_key` applies the entry's own hasher (Twox64Concat) from metadata, so the hashing
    // is not reimplemented here either.
    let authority_entry = at
        .storage()
        .entry(("Identity", "AuthorityOf"))
        .expect("Identity.AuthorityOf must exist");
    let authority_key = authority_entry
        .fetch_key(vec![Value::from_bytes(USERNAME_SUFFIX)])
        .expect("suffix must encode as a storage key");
    let authority_key = format!("0x{}", hex::encode(authority_key));

    let authority_value_ty = md
        .pallet_by_name("Identity")
        .and_then(|p| p.storage())
        .and_then(|s| s.entry_by_name("AuthorityOf"))
        .expect("AuthorityOf metadata")
        .value_ty();

    // An allocation lets the authority grant usernames without reserving a deposit per name.
    let authority = Value::named_composite([
        ("account_id", Value::from_bytes(ALICE)),
        ("allocation", Value::u128(100)),
    ]);

    let mut authority_encoded = Vec::new();
    scale_value::scale::encode_as_type(
        &authority,
        authority_value_ty,
        md.types(),
        &mut authority_encoded,
    )
    .expect("AuthorityProperties must encode against the runtime type");
    let authority_value = format!("0x{}", hex::encode(&authority_encoded));

    println!(
        "\nIdentity.AuthorityOf({})",
        String::from_utf8_lossy(USERNAME_SUFFIX)
    );
    println!("  key   = {authority_key}");
    println!("  value = {authority_value}");

    // --- write the override file --------------------------------------------------------
    let overrides = serde_json::json!({
        "genesis": { "raw": { "top": {
            key: value,
            authority_key: authority_value,
        } } }
    });

    let path = pif_e2e::repo_root().join("crates/pif-e2e/networks/people-registrar.json");
    std::fs::write(&path, serde_json::to_string_pretty(&overrides).unwrap())
        .expect("writing the override file");
    println!("\nwrote {}", path.display());
}
