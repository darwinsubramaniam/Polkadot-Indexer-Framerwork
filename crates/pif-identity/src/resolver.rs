//! Looking an alias up — the cross-check surface.
//!
//! There are two honestly different questions here, and conflating them is the mistake this
//! module exists to prevent:
//!
//! | Question | Answered by |
//! |---|---|
//! | "Does this wallet have an alias *right now*?" | [`RpcIdentityResolver`] — a node, no indexer, no database |
//! | "Did it have one *at block N*?" | [`PgIdentityResolver`] — historical state is pruned on most nodes |
//! | "Give me *every* verified account" | [`PgIdentityResolver`] — a sweep per query is not a join |
//!
//! Both implement [`IdentityResolver`], so a caller picks by what it has available rather
//! than by rewriting its logic.
//!
//! ## Cross-chain
//!
//! [`PgIdentityResolver`] is constructed with the chain id that *holds* the identities
//! (`"polkadot-people"`), while the handler calling it may be indexing something else
//! entirely. That one parameter is the whole cross-chain mechanism: both indexers write to
//! the same Postgres, every table is keyed by `chain_id`, so the lookup is an ordinary join.
//!
//! ```ignore
//! // inside your own handler, on a completely different chain
//! let alias = self.identities.alias_of(&from_ss58).await?;
//! if alias.as_ref().is_some_and(|a| a.verified) {
//!     // a registrar vouched for this account
//! }
//! ```

use std::collections::HashMap;

use async_trait::async_trait;
use pif_chain::storage::StorageAt;
use pif_chain::{ChainClient, SubxtStorage, decode};
use pif_core::{ChainConfig, ss58};
use scale_value::Value;
use serde_json::Value as Json;
use sqlx::{PgPool, Row};

use crate::model::{Judgement, parse_registration, username_to_string};
use crate::read::PALLET;

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("database error")]
    Database(#[from] sqlx::Error),

    #[error("chain error")]
    Chain(#[from] pif_chain::ChainError),

    /// The resolver physically cannot answer this. Returned rather than faked, because a
    /// silently-wrong historical answer is worse than a refusal.
    #[error("{0} needs indexed history; a node keeps only recent state. Use PgIdentityResolver.")]
    NotSupported(&'static str),
}

pub type Result<T> = std::result::Result<T, ResolveError>;

/// What is known about a wallet's claimed identity.
#[derive(Debug, Clone, PartialEq)]
pub struct Alias {
    pub account: String,
    /// This account's own display name, or its parent's if it is a sub-identity.
    pub display: Option<String>,
    /// Primary username, e.g. `alice.dot`.
    pub username: Option<String>,
    /// Whether a registrar vouched — `Reasonable` or `KnownGood`.
    ///
    /// This is the field a cross-check should branch on. `display` being set means only that
    /// somebody typed a name into a form and paid a deposit.
    pub verified: bool,
    pub best_judgement: Option<Judgement>,
    /// `(parent, label)` when this account is a sub-identity.
    pub via_super: Option<(String, String)>,
    /// The block this answer describes, when it came from a point-in-time query.
    pub as_of_block: Option<u64>,
}

impl Alias {
    /// The single best human-readable name, if any.
    pub fn best_name(&self) -> Option<&str> {
        self.username.as_deref().or(self.display.as_deref())
    }
}

#[async_trait]
pub trait IdentityResolver: Send + Sync {
    /// The alias as of now.
    async fn alias_of(&self, account: &str) -> Result<Option<Alias>>;

    /// The alias as it stood at `block`.
    async fn alias_at(&self, account: &str, block: u64) -> Result<Option<Alias>>;

    /// The account that owns a username.
    async fn resolve_username(&self, username: &str) -> Result<Option<String>>;

    /// Aliases for many accounts at once.
    ///
    /// A handler processing a block of transfers wants one query, not one per address.
    /// Accounts with no identity are simply absent from the map.
    async fn aliases_of(&self, accounts: &[String]) -> Result<HashMap<String, Alias>>;
}

// ---------------------------------------------------------------------------------------
// Postgres
// ---------------------------------------------------------------------------------------

/// Resolves against the indexed tables. Answers every question, including historical ones.
pub struct PgIdentityResolver {
    pool: PgPool,
    identity_chain_id: String,
}

impl PgIdentityResolver {
    pub fn new(pool: PgPool, identity_chain_id: impl Into<String>) -> Self {
        Self {
            pool,
            identity_chain_id: identity_chain_id.into(),
        }
    }

    pub fn chain_id(&self) -> &str {
        &self.identity_chain_id
    }
}

#[async_trait]
impl IdentityResolver for PgIdentityResolver {
    async fn alias_of(&self, account: &str) -> Result<Option<Alias>> {
        let row = sqlx::query(
            "SELECT account, effective_display, username, effective_verified,
                    judgements, super_account, sub_label
               FROM identity_current
              WHERE chain_id = $1 AND account = $2",
        )
        .bind(&self.identity_chain_id)
        .bind(account)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.as_ref().map(|r| alias_from_row(r, None)))
    }

    async fn alias_at(&self, account: &str, block: u64) -> Result<Option<Alias>> {
        let at = i64::try_from(block).unwrap_or(i64::MAX);

        // Only `identities` is temporal. Usernames and sub-identities are current-state
        // tables, so a point-in-time answer reports the identity as it was and leaves the
        // rest null rather than dressing today's username up as history.
        let row = sqlx::query(
            "SELECT account, display AS effective_display, is_verified AS effective_verified,
                    judgements, NULL::text AS username,
                    NULL::text AS super_account, NULL::text AS sub_label
               FROM identities
              WHERE chain_id = $1 AND account = $2
                AND valid_from_block <= $3
                AND (valid_to_block IS NULL OR valid_to_block >= $3)
              ORDER BY valid_from_block DESC
              LIMIT 1",
        )
        .bind(&self.identity_chain_id)
        .bind(account)
        .bind(at)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.as_ref().map(|r| alias_from_row(r, Some(block))))
    }

    async fn resolve_username(&self, username: &str) -> Result<Option<String>> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT account FROM usernames
              WHERE chain_id = $1 AND username = $2 AND status = 'active'",
        )
        .bind(&self.identity_chain_id)
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|(account,)| account))
    }

    async fn aliases_of(&self, accounts: &[String]) -> Result<HashMap<String, Alias>> {
        if accounts.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = sqlx::query(
            "SELECT account, effective_display, username, effective_verified,
                    judgements, super_account, sub_label
               FROM identity_current
              WHERE chain_id = $1 AND account = ANY($2)",
        )
        .bind(&self.identity_chain_id)
        .bind(accounts)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let alias = alias_from_row(row, None);
                (alias.account.clone(), alias)
            })
            .collect())
    }
}

fn alias_from_row(row: &sqlx::postgres::PgRow, as_of_block: Option<u64>) -> Alias {
    let judgements: Json = row.try_get("judgements").unwrap_or(Json::Null);
    let super_account: Option<String> = row.try_get("super_account").ok().flatten();
    let sub_label: Option<String> = row.try_get("sub_label").ok().flatten();

    Alias {
        account: row.get("account"),
        display: row.try_get("effective_display").ok().flatten(),
        username: row.try_get("username").ok().flatten(),
        verified: row.try_get("effective_verified").unwrap_or(false),
        best_judgement: best_judgement(&judgements),
        via_super: super_account.map(|parent| (parent, sub_label.unwrap_or_default())),
        as_of_block,
    }
}

// ---------------------------------------------------------------------------------------
// Live RPC
// ---------------------------------------------------------------------------------------

/// Resolves by reading state from a node at the finalized head.
///
/// No database and no indexer — this is the answer to "can we just use the RPC". It answers
/// only "right now": [`IdentityResolver::alias_at`] returns [`ResolveError::NotSupported`]
/// rather than a plausible-looking wrong answer, because a node keeps roughly the last 256
/// blocks of state and cannot know what an identity looked like last year.
pub struct RpcIdentityResolver {
    client: ChainClient,
}

impl RpcIdentityResolver {
    /// Connect to a People chain.
    pub async fn connect(config: &ChainConfig) -> Result<Self> {
        Ok(Self {
            client: ChainClient::connect(config).await?,
        })
    }

    pub fn from_client(client: ChainClient) -> Self {
        Self { client }
    }

    /// Read one account's alias at the current finalized head.
    async fn read(&self, account: &str) -> Result<Option<Alias>> {
        let number = self.client.finalized_number().await?;
        let at = decode::at_block(&self.client.client, &self.client.info, number).await?;
        let storage = SubxtStorage::new(&at, &self.client.info.id);

        alias_from_storage(&storage, self.client.info.ss58_prefix, account).await
    }
}

/// Build an [`Alias`] from live storage.
///
/// Also usable from a handler that already holds a `BlockContext`, which is why it takes
/// `&dyn StorageAt` rather than a client.
pub async fn alias_from_storage(
    storage: &dyn StorageAt,
    prefix: u16,
    account: &str,
) -> Result<Option<Alias>> {
    let Some(bytes) = ss58::decode(account) else {
        return Ok(None);
    };
    let key = || vec![Value::from_bytes(&bytes)];

    let own = storage
        .fetch(PALLET, "IdentityOf", key())
        .await?
        .as_ref()
        .and_then(parse_registration);

    let username = storage
        .fetch(PALLET, "UsernameOf", key())
        .await?
        .as_ref()
        .and_then(username_to_string);

    // A sub-identity has no identity of its own; its name and verification are its parent's.
    let mut via_super = None;
    let mut inherited = None;

    if let Some(super_of) = storage.fetch(PALLET, "SuperOf", key()).await? {
        let parts = super_of.as_array().map(Vec::as_slice).unwrap_or_default();
        if let Some(parent_bytes) = parts.first().and_then(ss58::account_bytes) {
            let parent = ss58::encode(&parent_bytes, prefix);
            let label = crate::model::data_to_string(parts.get(1)).unwrap_or_default();

            inherited = storage
                .fetch(PALLET, "IdentityOf", vec![Value::from_bytes(&parent_bytes)])
                .await?
                .as_ref()
                .and_then(parse_registration);
            via_super = Some((parent, label));
        }
    }

    if own.is_none() && username.is_none() && via_super.is_none() {
        return Ok(None);
    }

    let effective = own.as_ref().or(inherited.as_ref());

    Ok(Some(Alias {
        account: account.to_owned(),
        display: effective.and_then(|r| r.display.clone()),
        username,
        verified: effective.is_some_and(|r| r.is_verified),
        best_judgement: effective.and_then(|r| best_judgement(&r.judgements)),
        via_super,
        as_of_block: Some(storage.block_number()),
    }))
}

#[async_trait]
impl IdentityResolver for RpcIdentityResolver {
    async fn alias_of(&self, account: &str) -> Result<Option<Alias>> {
        self.read(account).await
    }

    async fn alias_at(&self, _account: &str, _block: u64) -> Result<Option<Alias>> {
        Err(ResolveError::NotSupported("alias_at"))
    }

    async fn resolve_username(&self, username: &str) -> Result<Option<String>> {
        let number = self.client.finalized_number().await?;
        let at = decode::at_block(&self.client.client, &self.client.info, number).await?;
        let storage = SubxtStorage::new(&at, &self.client.info.id);

        let info = storage
            .fetch(
                PALLET,
                "UsernameInfoOf",
                vec![Value::from_bytes(username.as_bytes())],
            )
            .await?;

        Ok(info.and_then(|v| {
            v.get("owner")
                .and_then(|o| ss58::decode_account(o, self.client.info.ss58_prefix))
        }))
    }

    async fn aliases_of(&self, accounts: &[String]) -> Result<HashMap<String, Alias>> {
        // One connection, one block, N reads — still far better than N connections, and the
        // batch case is what `PgIdentityResolver` is for anyway.
        let number = self.client.finalized_number().await?;
        let at = decode::at_block(&self.client.client, &self.client.info, number).await?;
        let storage = SubxtStorage::new(&at, &self.client.info.id);

        let mut out = HashMap::with_capacity(accounts.len());
        for account in accounts {
            if let Some(alias) =
                alias_from_storage(&storage, self.client.info.ss58_prefix, account).await?
            {
                out.insert(account.clone(), alias);
            }
        }
        Ok(out)
    }
}

/// Pick the strongest judgement recorded, which is what "verified" summarises.
fn best_judgement(judgements: &Json) -> Option<Judgement> {
    let items = judgements.as_array()?;
    items
        .iter()
        .filter_map(|j| j.get("judgement").and_then(Json::as_str))
        .map(parse_judgement)
        // `Judgement`'s ordering is declared best-first, so the minimum is the strongest.
        .min()
}

fn parse_judgement(name: &str) -> Judgement {
    match name {
        "KnownGood" => Judgement::KnownGood,
        "Reasonable" => Judgement::Reasonable,
        "FeePaid" => Judgement::FeePaid,
        "LowQuality" => Judgement::LowQuality,
        "Erroneous" => Judgement::Erroneous,
        "OutOfDate" => Judgement::OutOfDate,
        _ => Judgement::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pif_chain::storage::stub::StubStorage;
    use serde_json::json;

    const ALICE_HEX: &str = "0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a5684e7a56da27d";
    const ALICE_SS58: &str = "15oF4uVJwmo4TdGW7VfQxNLavjCXviqxT9S1MgbjMNHr6Sp5";
    const BOB_HEX: &str = "0x8eaf04151687736326c9fea17e25fc5287613693c912909cb226aa4794f26a48";

    fn alice_bytes() -> Vec<u8> {
        hex::decode(&ALICE_HEX[2..]).unwrap()
    }

    fn alice_key() -> Vec<Value> {
        vec![Value::from_bytes(alice_bytes())]
    }

    fn bob_key() -> Vec<Value> {
        vec![Value::from_bytes(hex::decode(&BOB_HEX[2..]).unwrap())]
    }

    /// Shaped like a live People chain: the judgement list is inside its `BoundedVec`
    /// newtype level, the registrar index is a stringified integer, and a short display
    /// name is an array of stringified bytes rather than hex.
    fn registration(display: &str, judgement: &str) -> Json {
        let bytes: Vec<String> = display.bytes().map(|b| b.to_string()).collect();
        json!({
            "info": { "display": { format!("Raw{}", display.len()): [bytes] } },
            "judgements": [[[ "0", { judgement: [] } ]]],
            "deposit": "1000"
        })
    }

    #[tokio::test]
    async fn reads_a_verified_identity_straight_from_storage() {
        // The "no indexer needed" path, end to end against stubbed state.
        let storage = StubStorage::new(100)
            .with_pallet("Identity")
            .with_value(
                "Identity",
                "IdentityOf",
                &alice_key(),
                registration("Alice", "KnownGood"),
            )
            .with_value(
                "Identity",
                "UsernameOf",
                &alice_key(),
                json!(format!("0x{}", hex::encode("alice.dot"))),
            );

        let alias = alias_from_storage(&storage, 0, ALICE_SS58)
            .await
            .unwrap()
            .expect("alice has an identity");

        assert_eq!(alias.display.as_deref(), Some("Alice"));
        assert_eq!(alias.username.as_deref(), Some("alice.dot"));
        assert!(alias.verified);
        assert_eq!(alias.best_judgement, Some(Judgement::KnownGood));
        assert_eq!(alias.best_name(), Some("alice.dot"));
        assert_eq!(alias.as_of_block, Some(100));
    }

    #[tokio::test]
    async fn an_account_with_nothing_set_resolves_to_none() {
        let storage = StubStorage::new(1).with_pallet("Identity");
        assert!(
            alias_from_storage(&storage, 0, ALICE_SS58)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_sub_identity_inherits_its_parents_name_and_verification() {
        // The case a naive lookup gets wrong: the sub has no identity of its own, so reading
        // only `IdentityOf` would report "no alias" for an account that plainly has one.
        let storage = StubStorage::new(7)
            .with_pallet("Identity")
            .with_value(
                "Identity",
                "SuperOf",
                &alice_key(),
                json!([[BOB_HEX], { "Raw6": [118, 97, 108, 45, 48, 49] }]),
            )
            .with_value(
                "Identity",
                "IdentityOf",
                &bob_key(),
                registration("Acme", "Reasonable"),
            );

        let alias = alias_from_storage(&storage, 0, ALICE_SS58)
            .await
            .unwrap()
            .expect("sub-identity resolves through its parent");

        assert_eq!(alias.display.as_deref(), Some("Acme"));
        assert!(alias.verified, "verification is inherited from the parent");

        let (parent, label) = alias.via_super.expect("parent recorded");
        assert_eq!(label, "val-01");
        assert!(parent.starts_with('1'), "parent rendered with the prefix");
    }

    #[tokio::test]
    async fn fee_paid_alone_does_not_resolve_as_verified() {
        let storage = StubStorage::new(1).with_pallet("Identity").with_value(
            "Identity",
            "IdentityOf",
            &alice_key(),
            registration("Alice", "FeePaid"),
        );

        let alias = alias_from_storage(&storage, 0, ALICE_SS58)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(alias.display.as_deref(), Some("Alice"));
        assert!(!alias.verified, "a paid fee is not a judgement");
    }

    #[test]
    fn the_strongest_judgement_wins() {
        let judgements = json!([
            { "registrar_index": 0, "judgement": "FeePaid" },
            { "registrar_index": 1, "judgement": "KnownGood" },
            { "registrar_index": 2, "judgement": "Reasonable" }
        ]);
        assert_eq!(best_judgement(&judgements), Some(Judgement::KnownGood));
    }

    #[test]
    fn an_erroneous_judgement_is_reported_not_hidden() {
        let judgements = json!([{ "registrar_index": 0, "judgement": "Erroneous" }]);
        let best = best_judgement(&judgements).unwrap();

        assert_eq!(best, Judgement::Erroneous);
        assert!(!best.is_vouched());
    }

    #[test]
    fn no_judgements_means_no_verdict() {
        assert_eq!(best_judgement(&json!([])), None);
        assert_eq!(best_judgement(&Json::Null), None);
    }

    #[test]
    fn judgement_ordering_is_best_first() {
        // `min()` picks the strongest only because the enum is declared in this order.
        assert!(Judgement::KnownGood < Judgement::Reasonable);
        assert!(Judgement::Reasonable < Judgement::FeePaid);
        assert!(Judgement::FeePaid < Judgement::Erroneous);
    }
}
