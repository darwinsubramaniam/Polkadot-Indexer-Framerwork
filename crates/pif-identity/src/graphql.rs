//! A GraphQL root for identities, merged into the framework schema.
//!
//! ```ignore
//! #[derive(async_graphql::MergedObject, Default)]
//! struct Query(pif_api::CoreQuery, pif_identity::IdentityQuery);
//!
//! let schema = pif_api::build_schema_with(pool, Query::default());
//! let app = pif_api::router_with(schema);
//! ```
//!
//! Chain-scoped like every framework query, because `chainId` is the primary key everywhere
//! and a lookup that ignored it would silently mix two chains' identities.

use async_graphql::{Context, Object, Result, SimpleObject};
use sqlx::{PgPool, Row};

/// Server-side cap, matching the framework's own. Without it one query can ask for the whole
/// identity set.
const MAX_LIMIT: i32 = 100;

fn clamp_limit(limit: i32) -> i64 {
    limit.clamp(1, MAX_LIMIT) as i64
}

/// Field names are snake_case here and camelCase over the wire: async-graphql renames
/// them, matching how the framework's own types are declared.
#[derive(SimpleObject)]
pub struct Identity {
    /// SS58, the same form `extrinsics.signer` uses, so it joins across tables.
    pub account: String,
    /// This account's display name, or its parent's when it is a sub-identity.
    pub display: Option<String>,
    /// Primary username, e.g. `alice.dot`.
    pub username: Option<String>,
    /// A registrar vouched — `Reasonable` or `KnownGood`.
    ///
    /// Branch on this, not on `display`: a display name only means somebody paid a deposit
    /// and typed something.
    pub verified: bool,
    /// `[{"registrar_index": 1, "judgement": "KnownGood"}]`
    pub judgements: async_graphql::Json<serde_json::Value>,
    /// The parent account, when this is a sub-identity.
    pub super_account: Option<String>,
    /// This account's label within its parent.
    pub sub_label: Option<String>,
    /// First block this identity state was true from. Absent for an account known only
    /// through a username or a sub-identity link.
    pub valid_from_block: Option<i64>,
}

#[derive(Default)]
pub struct IdentityQuery;

#[Object]
impl IdentityQuery {
    /// One account's alias as it stands now.
    async fn identity(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        account: String,
    ) -> Result<Option<Identity>> {
        let pool = ctx.data::<PgPool>()?;

        let row = sqlx::query(
            "SELECT account, effective_display, username, effective_verified,
                    judgements, super_account, sub_label, valid_from_block
               FROM identity_current
              WHERE chain_id = $1 AND account = $2",
        )
        .bind(&chain_id)
        .bind(&account)
        .fetch_optional(pool)
        .await?;

        Ok(row.as_ref().map(row_to_identity))
    }

    /// One account's alias as it stood at a past block.
    ///
    /// Only the identity itself is historical — usernames and sub-identity links are stored
    /// as current state, so they come back null rather than being presented as though they
    /// were true then.
    async fn identity_at(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        account: String,
        block: i64,
    ) -> Result<Option<Identity>> {
        let pool = ctx.data::<PgPool>()?;

        let row = sqlx::query(
            "SELECT account, display AS effective_display, is_verified AS effective_verified,
                    judgements, valid_from_block,
                    NULL::text AS username, NULL::text AS super_account,
                    NULL::text AS sub_label
               FROM identities
              WHERE chain_id = $1 AND account = $2
                AND valid_from_block <= $3
                AND (valid_to_block IS NULL OR valid_to_block >= $3)
              ORDER BY valid_from_block DESC
              LIMIT 1",
        )
        .bind(&chain_id)
        .bind(&account)
        .bind(block)
        .fetch_optional(pool)
        .await?;

        Ok(row.as_ref().map(row_to_identity))
    }

    /// The account that owns a username.
    async fn resolve_username(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        username: String,
    ) -> Result<Option<String>> {
        let pool = ctx.data::<PgPool>()?;

        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT account FROM usernames
              WHERE chain_id = $1 AND username = $2 AND status = 'active'",
        )
        .bind(&chain_id)
        .bind(&username)
        .fetch_optional(pool)
        .await?;

        Ok(row.and_then(|(account,)| account))
    }

    /// Browse identities, optionally only verified ones.
    ///
    /// This is the query a node cannot serve: answering it over RPC means sweeping the whole
    /// `IdentityOf` map per request.
    async fn identities(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        verified: Option<bool>,
        #[graphql(default = 0)] offset: i32,
        #[graphql(default = 25)] limit: i32,
    ) -> Result<Vec<Identity>> {
        let pool = ctx.data::<PgPool>()?;

        let rows = sqlx::query(
            "SELECT account, effective_display, username, effective_verified,
                    judgements, super_account, sub_label, valid_from_block
               FROM identity_current
              WHERE chain_id = $1
                AND ($2::bool IS NULL OR effective_verified = $2)
              ORDER BY account
              LIMIT $3 OFFSET $4",
        )
        .bind(&chain_id)
        .bind(verified)
        .bind(clamp_limit(limit))
        .bind(offset.max(0) as i64)
        .fetch_all(pool)
        .await?;

        Ok(rows.iter().map(row_to_identity).collect())
    }
}

fn row_to_identity(row: &sqlx::postgres::PgRow) -> Identity {
    Identity {
        account: row.get("account"),
        display: row.try_get("effective_display").ok().flatten(),
        username: row.try_get("username").ok().flatten(),
        verified: row.try_get("effective_verified").unwrap_or(false),
        judgements: async_graphql::Json(
            row.try_get("judgements")
                .unwrap_or(serde_json::Value::Array(Vec::new())),
        ),
        super_account: row.try_get("super_account").ok().flatten(),
        sub_label: row.try_get("sub_label").ok().flatten(),
        valid_from_block: row.try_get("valid_from_block").ok().flatten(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_is_capped_server_side() {
        // A client asking for everything must not be able to.
        assert_eq!(clamp_limit(10_000), MAX_LIMIT as i64);
        assert_eq!(clamp_limit(25), 25);
    }

    #[test]
    fn a_nonsense_limit_still_returns_something() {
        assert_eq!(clamp_limit(0), 1);
        assert_eq!(clamp_limit(-5), 1);
    }
}
