//! Does the `identity` handler's picture of `pallet_identity` match a real People runtime?
//!
//! Every other identity test runs against stubs, so this is the only place the handler's
//! assumptions meet actual metadata: that the pallet is called `Identity`, that the storage
//! items it reads exist under the names it uses, and that a `Registration` decodes into a row.
//!
//! Needs a People-chain dev node:
//!
//! ```sh
//! just chains-up
//! cargo test -p pif-e2e --features handler-identity --test people_metadata -- --ignored --nocapture
//! ```

#![cfg(feature = "handler-identity")]

use pif_chain::storage::{StorageAt, SubxtStorage};
use pif_chain::{ChainClient, decode};
use pif_core::ChainConfig;
use scale_value::Value;

fn people_url() -> String {
    std::env::var("PEOPLE_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:9955".to_owned())
}

fn config(url: &str) -> ChainConfig {
    ChainConfig {
        id: "people-dev".to_owned(),
        ws_url: url.to_owned(),
        start_block: 0,
        handlers: Vec::new(),
    }
}

/// Alice's well-known sr25519 public key.
const ALICE: [u8; 32] = [
    0xd4, 0x35, 0x93, 0xc7, 0x15, 0xfd, 0xd3, 0x1c, 0x61, 0x14, 0x1a, 0xbd, 0x04, 0xa9, 0x9f, 0xd6,
    0x82, 0x2c, 0x85, 0x58, 0x85, 0x4c, 0xcd, 0xe3, 0x9a, 0x56, 0x84, 0xe7, 0xa5, 0x6d, 0xa2, 0x7d,
];

#[tokio::test]
#[ignore = "needs a People dev node; run with --ignored"]
async fn handler_assumptions_hold_against_a_real_people_runtime() {
    let url = people_url();
    let client = match ChainClient::connect(&config(&url)).await {
        Ok(client) => client,
        Err(e) => {
            eprintln!("skipping: no People node at {url} ({e})");
            return;
        }
    };

    println!(
        "connected: {} (ss58 prefix {})",
        client.info.name, client.info.ss58_prefix
    );

    let number = client.finalized_number().await.expect("finalized head");
    let at = decode::at_block(&client.client, &client.info, number)
        .await
        .expect("block handle");
    let storage = SubxtStorage::new(&at, &client.info.id);

    // 1. The pallet exists under the name the handler uses. This is what `bootstrap`'s
    //    wrong-chain guard checks, so if this is wrong the handler refuses to run at all.
    assert!(
        storage.has_pallet("Identity"),
        "People runtime must expose an `Identity` pallet"
    );

    // 2. Every storage item the handler reads resolves. `fetch` maps an entry that is missing
    //    *from metadata* to `Ok(None)`, which is indistinguishable from an absent key here --
    //    so this proves the names are not silently wrong only in combination with the
    //    iteration check below, which does fail loudly on an unknown entry.
    let alice_key = || vec![Value::from_bytes(ALICE)];
    for entry in ["IdentityOf", "UsernameOf", "SuperOf", "SubsOf"] {
        let value = storage
            .fetch("Identity", entry, alice_key())
            .await
            .unwrap_or_else(|e| panic!("Identity.{entry} read failed: {e}"));
        println!("  Identity.{entry}(Alice) = {value:?}");
    }

    for entry in ["UsernameInfoOf", "PendingUsernames", "UnbindingUsernames"] {
        let value = storage
            .fetch("Identity", entry, vec![Value::from_bytes(b"alice.people")])
            .await
            .unwrap_or_else(|e| panic!("Identity.{entry} read failed: {e}"));
        println!("  Identity.{entry}(alice.people) = {value:?}");
    }

    let registrars = storage
        .fetch("Identity", "Registrars", Vec::new())
        .await
        .expect("Registrars is a plain StorageValue");
    println!("  Identity.Registrars = {registrars:?}");

    // 3. The iteration path used by the bootstrap sweep works, and its keys decode.
    //    Unlike `fetch`, `iter` errors on an entry that is not in metadata, so reaching this
    //    point proves `IdentityOf` really is the right name on this runtime.
    let mut stream = storage
        .iter("Identity".to_owned(), "IdentityOf".to_owned())
        .await
        .expect("IdentityOf must be iterable");

    use futures::StreamExt;
    let mut seen = 0usize;
    while let Some(item) = stream.next().await {
        let (key, value) = item.expect("iterating IdentityOf");
        assert!(
            key.len() >= 32 + 16,
            "a map key must carry its prefixes and the account"
        );
        // The whole point of the sweep: storage must produce a row.
        assert!(
            pif_identity::model::parse_registration(&value).is_some(),
            "a real Registration must decode into a row, got: {value}"
        );
        seen += 1;
        if seen >= 5 {
            break;
        }
    }
    println!("  swept {seen} identities from IdentityOf");
}

/// Print the shape of `Identity::set_identity`'s argument on the live runtime.
///
/// `IdentityInfo` is runtime-defined — the People chains carry different field sets from the
/// legacy relay-chain one — so the demo builds its call from what this reports rather than
/// from a guess that silently fails to encode.
#[tokio::test]
#[ignore = "diagnostic; run with --ignored"]
async fn print_identity_info_shape() {
    let url = people_url();
    let Ok(client) = ChainClient::connect(&config(&url)).await else {
        eprintln!("skipping: no People node at {url}");
        return;
    };

    let number = client.finalized_number().await.unwrap();
    let at = decode::at_block(&client.client, &client.info, number)
        .await
        .unwrap();
    let md = at.metadata();

    let pallet = md.pallet_by_name("Identity").expect("Identity pallet");
    let call = pallet
        .call_variant_by_name("set_identity")
        .expect("set_identity call");

    println!("set_identity args:");
    for f in &call.fields {
        println!(
            "  {} : type id {}",
            f.name.as_deref().unwrap_or("?"),
            f.ty.id
        );
        let ty = md.types().resolve(f.ty.id).expect("resolvable");
        println!("    {:?}", ty.path);
        if let scale_info::TypeDef::Composite(c) = &ty.type_def {
            for sub in &c.fields {
                let sub_ty = md.types().resolve(sub.ty.id).unwrap();
                println!(
                    "      {:<16} -> {}",
                    sub.name.as_deref().unwrap_or("?"),
                    sub_ty.path.segments.join("::")
                );
            }
        }
    }
}

/// Print what is needed to add a registrar and issue judgements: whether the runtime has a
/// `Sudo` pallet at all, and the exact signatures of the identity calls involved.
#[tokio::test]
#[ignore = "diagnostic; run with --ignored"]
async fn print_registrar_path() {
    let url = people_url();
    let Ok(client) = ChainClient::connect(&config(&url)).await else {
        eprintln!("skipping: no People node at {url}");
        return;
    };

    let number = client.finalized_number().await.unwrap();
    let at = decode::at_block(&client.client, &client.info, number)
        .await
        .unwrap();
    let md = at.metadata();

    println!("pallets with a governance/root entry point:");
    for p in md.pallets() {
        if matches!(p.name(), "Sudo" | "Identity" | "Utility") {
            println!("  {}", p.name());
        }
    }
    println!("\nSudo present: {}", md.pallet_by_name("Sudo").is_some());

    let identity = md.pallet_by_name("Identity").unwrap();
    for call in [
        "add_registrar",
        "request_judgement",
        "provide_judgement",
        "set_fee",
    ] {
        match identity.call_variant_by_name(call) {
            Some(v) => {
                println!("\nIdentity::{call}");
                for f in &v.fields {
                    let ty = md.types().resolve(f.ty.id).unwrap();
                    println!(
                        "  {:<12} -> {} (id {})",
                        f.name.as_deref().unwrap_or("?"),
                        if ty.path.segments.is_empty() {
                            format!("{:?}", ty.type_def)
                                .chars()
                                .take(48)
                                .collect::<String>()
                        } else {
                            ty.path.segments.join("::")
                        },
                        f.ty.id
                    );
                }
            }
            None => println!("\nIdentity::{call} -- ABSENT"),
        }
    }
}

/// Which of the three demo chains carry `Sudo` and `Identity` together?
///
/// `Identity::add_registrar` needs a root origin. On a real network that arrives from the
/// relay over XCM, which this demo network cannot deliver -- so a registrar can only be added
/// on a chain that has a local `Sudo`.
#[tokio::test]
#[ignore = "diagnostic; run with --ignored"]
async fn print_sudo_and_identity_availability() {
    for (label, url) in [
        ("relay", "ws://127.0.0.1:9944"),
        ("hub", "ws://127.0.0.1:9946"),
        ("people", "ws://127.0.0.1:9955"),
    ] {
        let Ok(client) = ChainClient::connect(&config(url)).await else {
            println!("{label:<8} {url}  -- unreachable");
            continue;
        };
        let number = client.finalized_number().await.unwrap();
        let at = decode::at_block(&client.client, &client.info, number)
            .await
            .unwrap();
        let md = at.metadata();

        println!(
            "{label:<8} {:<28} Sudo={:<5} Identity={}",
            client.info.name,
            md.pallet_by_name("Sudo").is_some(),
            md.pallet_by_name("Identity").is_some(),
        );
    }
}

/// Print what is needed to grant usernames and register sub-identities.
#[tokio::test]
#[ignore = "diagnostic; run with --ignored"]
async fn print_username_and_subs_path() {
    let url = people_url();
    let Ok(client) = ChainClient::connect(&config(&url)).await else {
        eprintln!("skipping: no People node at {url}");
        return;
    };

    let number = client.finalized_number().await.unwrap();
    let at = decode::at_block(&client.client, &client.info, number)
        .await
        .unwrap();
    let md = at.metadata();
    let identity = md.pallet_by_name("Identity").unwrap();

    for call in [
        "add_username_authority",
        "set_username_for",
        "accept_username",
        "set_primary_username",
        "set_subs",
    ] {
        match identity.call_variant_by_name(call) {
            Some(v) => {
                println!("\nIdentity::{call}");
                for f in &v.fields {
                    let ty = md.types().resolve(f.ty.id).unwrap();
                    let name = if ty.path.segments.is_empty() {
                        format!("{:?}", ty.type_def)
                            .chars()
                            .take(60)
                            .collect::<String>()
                    } else {
                        ty.path.segments.join("::")
                    };
                    println!(
                        "  {:<16} -> {name} (id {})",
                        f.name.as_deref().unwrap_or("?"),
                        f.ty.id
                    );
                }
            }
            None => println!("\nIdentity::{call} -- ABSENT"),
        }
    }

    // `AuthorityOf` is what `add_username_authority` writes; we need its exact value shape
    // to install one at genesis instead.
    let storage = identity.storage().unwrap();
    for entry in ["AuthorityOf", "UsernameInfoOf", "UsernameOf"] {
        let Some(e) = storage.entry_by_name(entry) else {
            println!("\nIdentity::{entry} -- ABSENT");
            continue;
        };
        let ty = md.types().resolve(e.value_ty()).unwrap();
        println!(
            "\nIdentity::{entry} value = {} (id {})",
            ty.path.segments.join("::"),
            e.value_ty()
        );
        if let scale_info::TypeDef::Composite(c) = &ty.type_def {
            for f in &c.fields {
                let ft = md.types().resolve(f.ty.id).unwrap();
                println!(
                    "    {:<14} -> {}",
                    f.name.as_deref().unwrap_or("?"),
                    if ft.path.segments.is_empty() {
                        format!("{:?}", ft.type_def)
                            .chars()
                            .take(40)
                            .collect::<String>()
                    } else {
                        ft.path.segments.join("::")
                    }
                );
            }
        }
    }
}
