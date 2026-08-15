//! Reference typed-overlay handler: `Balances::Transfer` -> a `transfers` table.
//!
//! **This crate is the template.** Copy it to start a chain-specific indexer (an
//! `omnipool-swap` handler for Hydration, say). It shows the whole extension contract:
//!
//!   * implement [`EventHandler`] — the framework never learns what a transfer is;
//!   * own your migrations (`migrations/0001_transfers.sql`, numbered locally);
//!   * write through the transaction the framework hands you, so your rows commit
//!     atomically with the block and roll back with it.
//!
//! The handler reads events the dynamic core has already decoded rather than carrying its
//! own `#[subxt::subxt]` codegen. That needs no per-chain metadata artifact, and the
//! projection cannot disagree with the `events` rows it derives from — both come from one
//! decode of one block.
//!
//! If a `Balances::Transfer` does not have the expected field shape, `handle` returns an
//! error and the block is rolled back rather than a half-understood row being written.

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use pif_chain::error::{ChainError, Result};
use pif_chain::handlers::{BlockContext, EventHandler};
use pif_core::ChainInfo;
use pif_db::BlockData;
use serde_json::Value as Json;
use sqlx::PgConnection;

pub const NAME: &str = "balances-transfer";

/// Migrations owning the `transfers` table.
///
/// Versions are local to this handler — the framework tracks each handler's history in its
/// own `_sqlx_migrations_<handler>` table, so numbering never collides with anyone else's.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub struct BalancesTransferHandler;

#[async_trait]
impl EventHandler for BalancesTransferHandler {
    fn name(&self) -> &'static str {
        NAME
    }

    fn supports(&self, _chain: &ChainInfo) -> bool {
        // `Balances` is near-universal, and a chain without it simply emits no matching
        // events. Correctness is enforced per-event in `handle` instead, where a genuinely
        // unexpected shape is loud rather than silently dropped.
        true
    }

    fn migrator(&self) -> Option<&'static sqlx::migrate::Migrator> {
        Some(&MIGRATOR)
    }

    async fn handle(
        &self,
        ctx: &BlockContext<'_>,
        block: &BlockData,
        tx: &mut PgConnection,
    ) -> Result<()> {
        for event in &block.events {
            if event.pallet != "Balances" || event.variant != "Transfer" {
                continue;
            }

            let (from, to, amount) =
                extract(&event.fields, ctx.chain.ss58_prefix).ok_or_else(|| {
                    ChainError::Handler {
                        handler: NAME.to_string(),
                        block: ctx.block_number,
                        source: format!(
                            "Balances::Transfer at index {} has an unexpected field shape: {}",
                            event.idx, event.fields
                        )
                        .into(),
                    }
                })?;

            // The handler owns this SQL; `pif-db` has never heard of `transfers`.
            sqlx::query(
                r#"
                INSERT INTO transfers (
                    chain_id, block_number, event_idx, extrinsic_idx,
                    from_address, to_address, amount
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (chain_id, block_number, event_idx) DO NOTHING
                "#,
            )
            .bind(&ctx.chain.id)
            .bind(event.block_number)
            .bind(event.idx)
            .bind(event.extrinsic_idx)
            .bind(&from)
            .bind(&to)
            .bind(&amount)
            .execute(&mut *tx)
            .await
            .map_err(|e| ChainError::Handler {
                handler: NAME.to_string(),
                block: ctx.block_number,
                source: Box::new(e),
            })?;
        }

        Ok(())
    }
}

/// Pull `(from, to, amount)` out of a decoded `Balances::Transfer` event.
///
/// Handles both the named form used by modern FRAME runtimes and the positional form used
/// by older ones. Accounts are converted to SS58 so they match `extrinsics.signer`, which
/// means one address string can be used to query both tables.
fn extract(fields: &Json, ss58_prefix: u16) -> Option<(String, String, BigDecimal)> {
    let (from, to, amount) = match fields {
        Json::Object(_) => (
            fields.get("from")?,
            fields.get("to")?,
            fields.get("amount").or_else(|| fields.get("value"))?,
        ),
        Json::Array(items) if items.len() >= 3 => (&items[0], &items[1], &items[2]),
        _ => return None,
    };

    Some((
        account_to_ss58(from, ss58_prefix)?,
        account_to_ss58(to, ss58_prefix)?,
        amount.as_str()?.parse().ok()?,
    ))
}

/// Render a decoded account field as SS58.
///
/// Every typed handler needs the same `AccountId32`-newtype unwrapping, so the logic lives in
/// [`pif_core::ss58::decode_account`]; this is just the local name for it.
fn account_to_ss58(value: &Json, ss58_prefix: u16) -> Option<String> {
    pif_core::ss58::decode_account(value, ss58_prefix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Alice / Bob / Charlie dev accounts, as the codec renders them: `AccountId32` is a
    /// newtype, so it arrives wrapped in a one-element array.
    const ALICE_HEX: &str = "0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d";
    const BOB_HEX: &str = "0x8eaf04151687736326c9fea17e25fc5287613693c912909cb226aa4794f26a48";

    fn alice_ss58() -> String {
        pif_core::ss58::encode(&hex::decode(&ALICE_HEX[2..]).unwrap(), 42)
    }

    #[test]
    fn extracts_named_transfer_fields() {
        let fields = json!({ "from": [ALICE_HEX], "to": [BOB_HEX], "amount": "1000" });

        let (from, to, amount) = extract(&fields, 42).expect("named form should parse");

        // Stored as SS58 so it matches `extrinsics.signer` and one address queries both.
        assert_eq!(from, alice_ss58());
        assert!(to.starts_with('5'), "expected SS58, got {to}");
        assert_eq!(amount, "1000".parse::<BigDecimal>().unwrap());
    }

    #[test]
    fn extracts_positional_transfer_fields() {
        // Older runtimes emit unnamed fields.
        let fields = json!([ALICE_HEX, BOB_HEX, "500"]);

        let (_, _, amount) = extract(&fields, 42).expect("positional form should parse");
        assert_eq!(amount, "500".parse::<BigDecimal>().unwrap());
    }

    #[test]
    fn preserves_full_u128_precision() {
        // Past 2^53, where a JSON number would silently round.
        let huge = "340282366920938463463374607431768211455";
        let fields = json!({ "from": [ALICE_HEX], "to": [BOB_HEX], "amount": huge });

        let (_, _, amount) = extract(&fields, 42).unwrap();
        assert_eq!(amount.to_string(), huge);
    }

    #[test]
    fn honours_the_chains_ss58_prefix() {
        // One keypair renders differently per network; the handler must use the chain's.
        let fields = json!({ "from": [ALICE_HEX], "to": [BOB_HEX], "amount": "1" });

        let (generic, _, _) = extract(&fields, 42).unwrap();
        let (polkadot, _, _) = extract(&fields, 0).unwrap();

        assert_ne!(generic, polkadot);
        assert_eq!(
            polkadot,
            pif_core::ss58::encode(&hex::decode(&ALICE_HEX[2..]).unwrap(), 0)
        );
    }

    #[test]
    fn unexpected_shape_is_rejected_rather_than_guessed() {
        // A runtime upgrade that changes this event must stop the block, not quietly write
        // an empty or wrong projection. `handle` turns this None into a hard error.
        assert!(extract(&json!({ "sender": [ALICE_HEX] }), 42).is_none());
        assert!(extract(&json!("nonsense"), 42).is_none());
        assert!(
            extract(&json!([ALICE_HEX, BOB_HEX]), 42).is_none(),
            "too few fields"
        );
    }

    #[test]
    fn rejects_accounts_that_are_not_32_bytes() {
        assert!(account_to_ss58(&json!(["0xdeadbeef"]), 42).is_none());
        assert!(account_to_ss58(&json!("not-hex"), 42).is_none());
    }

    #[test]
    fn accepts_both_wrapped_and_bare_account_encodings() {
        // The codec wraps newtypes in an array; a bare string should still work.
        assert_eq!(
            account_to_ss58(&json!([ALICE_HEX]), 42),
            account_to_ss58(&json!(ALICE_HEX), 42)
        );
    }

    #[test]
    fn handler_ships_its_own_migrations() {
        // The handler owns the `transfers` table; the framework has never heard of it.
        let migrator = BalancesTransferHandler
            .migrator()
            .expect("handler owns a table");
        assert!(migrator.iter().count() > 0);
    }
}
