//! GraphQL schema over the indexed data.
//!
//! Every query is chain-scoped: one process indexes N chains, so `chainId` is a required
//! argument almost everywhere rather than an optional filter.

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Object, Result, Schema, SimpleObject,
};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use crate::scalars::{BigInt, Hex};

/// Upper bound on any `limit` argument, so a single query cannot table-scan the chain.
const MAX_LIMIT: i32 = 100;

fn clamp_limit(limit: i32) -> i64 {
    limit.clamp(1, MAX_LIMIT) as i64
}

#[derive(SimpleObject)]
pub struct Chain {
    pub id: String,
    pub genesis_hash: Hex,
    pub name: String,
    pub token_symbol: Option<String>,
    pub token_decimals: Option<i32>,
    pub ss58_prefix: Option<i32>,
}

#[derive(SimpleObject)]
pub struct Block {
    pub chain_id: String,
    pub number: i64,
    pub hash: Hex,
    pub parent_hash: Hex,
    pub spec_version: i32,
    pub timestamp: Option<DateTime<Utc>>,
    pub extrinsic_count: i32,
    pub event_count: i32,
}

#[derive(SimpleObject)]
pub struct Extrinsic {
    pub chain_id: String,
    pub block_number: i64,
    pub idx: i32,
    pub hash: Hex,
    pub pallet: String,
    pub call: String,
    pub signer: Option<String>,
    pub is_signed: bool,
    pub success: bool,
    pub fee: Option<BigInt>,
    pub args: async_graphql::Json<serde_json::Value>,
}

#[derive(SimpleObject)]
pub struct Event {
    pub chain_id: String,
    pub block_number: i64,
    pub idx: i32,
    pub pallet: String,
    pub variant: String,
    pub phase: String,
    pub extrinsic_idx: Option<i32>,
    pub fields: async_graphql::Json<serde_json::Value>,
}

/// Indexing progress for one chain.
#[derive(SimpleObject)]
pub struct ChainStatus {
    pub chain_id: String,
    pub last_indexed_block: Option<i64>,
    /// Lowest block stored — non-zero means history was never backfilled.
    pub first_indexed_block: Option<i64>,
    /// Blocks missing between first and last. Should always be 0.
    pub gaps: i64,
}

#[derive(Default)]
pub struct CoreQuery;

#[Object]
impl CoreQuery {
    /// Every chain this indexer knows about.
    async fn chains(&self, ctx: &Context<'_>) -> Result<Vec<Chain>> {
        let pool = ctx.data::<PgPool>()?;
        let rows = sqlx::query(
            "SELECT id, genesis_hash, name, token_symbol, token_decimals, ss58_prefix
             FROM chains ORDER BY id",
        )
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| Chain {
                id: r.get("id"),
                genesis_hash: Hex(r.get("genesis_hash")),
                name: r.get("name"),
                token_symbol: r.get("token_symbol"),
                token_decimals: r.get::<Option<i16>, _>("token_decimals").map(i32::from),
                ss58_prefix: r.get("ss58_prefix"),
            })
            .collect())
    }

    /// Indexing progress per chain, including a gap check.
    async fn indexer_status(&self, ctx: &Context<'_>) -> Result<Vec<ChainStatus>> {
        let pool = ctx.data::<PgPool>()?;
        let rows = sqlx::query(
            r#"
            SELECT c.id                AS chain_id,
                   max(b.number)       AS last_block,
                   min(b.number)       AS first_block
            FROM chains c
            LEFT JOIN blocks b ON b.chain_id = c.id
            GROUP BY c.id
            ORDER BY c.id
            "#,
        )
        .fetch_all(pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let chain_id: String = row.get("chain_id");
            let gaps = pif_db::repo::count_gaps(pool, &chain_id).await?;
            out.push(ChainStatus {
                chain_id,
                last_indexed_block: row.get("last_block"),
                first_indexed_block: row.get("first_block"),
                gaps,
            });
        }
        Ok(out)
    }

    /// A single block by number.
    async fn block(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        number: i64,
    ) -> Result<Option<Block>> {
        let pool = ctx.data::<PgPool>()?;
        let row = sqlx::query(
            "SELECT chain_id, number, hash, parent_hash, spec_version, timestamp,
                    extrinsic_count, event_count
             FROM blocks WHERE chain_id = $1 AND number = $2",
        )
        .bind(&chain_id)
        .bind(number)
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| Block {
            chain_id: r.get("chain_id"),
            number: r.get("number"),
            hash: Hex(r.get("hash")),
            parent_hash: Hex(r.get("parent_hash")),
            spec_version: r.get("spec_version"),
            timestamp: r.get("timestamp"),
            extrinsic_count: r.get("extrinsic_count"),
            event_count: r.get("event_count"),
        }))
    }

    /// Most recent blocks first.
    async fn blocks(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        #[graphql(default = 0)] offset: i32,
        #[graphql(default = 25)] limit: i32,
    ) -> Result<Vec<Block>> {
        let pool = ctx.data::<PgPool>()?;
        let rows = sqlx::query(
            "SELECT chain_id, number, hash, parent_hash, spec_version, timestamp,
                    extrinsic_count, event_count
             FROM blocks WHERE chain_id = $1
             ORDER BY number DESC OFFSET $2 LIMIT $3",
        )
        .bind(&chain_id)
        .bind(offset.max(0) as i64)
        .bind(clamp_limit(limit))
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| Block {
                chain_id: r.get("chain_id"),
                number: r.get("number"),
                hash: Hex(r.get("hash")),
                parent_hash: Hex(r.get("parent_hash")),
                spec_version: r.get("spec_version"),
                timestamp: r.get("timestamp"),
                extrinsic_count: r.get("extrinsic_count"),
                event_count: r.get("event_count"),
            })
            .collect())
    }

    /// Extrinsics, optionally narrowed by pallet, call or signer.
    async fn extrinsics(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        pallet: Option<String>,
        call: Option<String>,
        signer: Option<String>,
        #[graphql(default = 25)] limit: i32,
    ) -> Result<Vec<Extrinsic>> {
        let pool = ctx.data::<PgPool>()?;
        // Passing NULL for an unused filter keeps this a single prepared statement
        // instead of string-building SQL.
        let rows = sqlx::query(
            "SELECT chain_id, block_number, idx, hash, pallet, call, signer,
                    is_signed, success, fee, args
             FROM extrinsics
             WHERE chain_id = $1
               AND ($2::text IS NULL OR pallet = $2)
               AND ($3::text IS NULL OR call   = $3)
               AND ($4::text IS NULL OR signer = $4)
             ORDER BY block_number DESC, idx DESC
             LIMIT $5",
        )
        .bind(&chain_id)
        .bind(pallet.as_deref())
        .bind(call.as_deref())
        .bind(signer.as_deref())
        .bind(clamp_limit(limit))
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| Extrinsic {
                chain_id: r.get("chain_id"),
                block_number: r.get("block_number"),
                idx: r.get("idx"),
                hash: Hex(r.get("hash")),
                pallet: r.get("pallet"),
                call: r.get("call"),
                signer: r.get("signer"),
                is_signed: r.get("is_signed"),
                success: r.get("success"),
                fee: r.get::<Option<BigDecimal>, _>("fee").map(BigInt),
                args: async_graphql::Json(r.get("args")),
            })
            .collect())
    }

    /// Events, optionally narrowed by pallet or variant.
    async fn events(
        &self,
        ctx: &Context<'_>,
        chain_id: String,
        pallet: Option<String>,
        variant: Option<String>,
        #[graphql(default = 25)] limit: i32,
    ) -> Result<Vec<Event>> {
        let pool = ctx.data::<PgPool>()?;
        let rows = sqlx::query(
            "SELECT chain_id, block_number, idx, pallet, variant, phase, extrinsic_idx, fields
             FROM events
             WHERE chain_id = $1
               AND ($2::text IS NULL OR pallet  = $2)
               AND ($3::text IS NULL OR variant = $3)
             ORDER BY block_number DESC, idx DESC
             LIMIT $4",
        )
        .bind(&chain_id)
        .bind(pallet.as_deref())
        .bind(variant.as_deref())
        .bind(clamp_limit(limit))
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| Event {
                chain_id: r.get("chain_id"),
                block_number: r.get("block_number"),
                idx: r.get("idx"),
                pallet: r.get("pallet"),
                variant: r.get("variant"),
                phase: r.get("phase"),
                extrinsic_idx: r.get("extrinsic_idx"),
                fields: async_graphql::Json(r.get("fields")),
            })
            .collect())
    }
}

pub type IndexerSchema = Schema<CoreQuery, EmptyMutation, EmptySubscription>;

/// The framework's own schema: blocks, extrinsics, events, chains, status.
pub fn build_schema(pool: PgPool) -> IndexerSchema {
    build_schema_with(pool, CoreQuery)
}

/// Build a schema whose root also exposes a downstream project's queries.
///
/// Domain queries do not belong in the framework, so a downstream indexer merges its own
/// root in with async-graphql's `MergedObject`:
///
/// ```ignore
/// #[derive(MergedObject, Default)]
/// struct Query(pif_api::CoreQuery, HydrationQuery);
///
/// let schema = pif_api::build_schema_with(pool, Query::default());
/// ```
pub fn build_schema_with<Q>(pool: PgPool, query: Q) -> Schema<Q, EmptyMutation, EmptySubscription>
where
    Q: async_graphql::ObjectType + 'static,
{
    Schema::build(query, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_is_clamped_into_range() {
        assert_eq!(clamp_limit(25), 25);
        // A client asking for a million rows gets MAX_LIMIT, not a table scan.
        assert_eq!(clamp_limit(1_000_000), MAX_LIMIT as i64);
        // Zero or negative would make LIMIT meaningless or error.
        assert_eq!(clamp_limit(0), 1);
        assert_eq!(clamp_limit(-5), 1);
    }

    #[tokio::test]
    async fn schema_builds_and_exposes_the_documented_queries() {
        // Catches accidental removal or renaming of a query field, which would otherwise
        // only surface at runtime against a live database. The pool is lazy and never
        // connects — only the schema shape is under test.
        let pool = PgPool::connect_lazy("postgres://unused/unused").unwrap();
        let sdl = build_schema(pool).sdl();

        for field in [
            "chains",
            "indexerStatus",
            "block",
            "blocks",
            "extrinsics",
            "events",
        ] {
            assert!(sdl.contains(field), "schema is missing `{field}`:\n{sdl}");
        }
        // Balances must be BigInt (string), never Int/Float.
        assert!(
            sdl.contains("scalar BigInt"),
            "BigInt scalar missing:\n{sdl}"
        );
        assert!(sdl.contains("scalar Hex"), "Hex scalar missing:\n{sdl}");

        // Domain queries must NOT be here — they belong to downstream projects, which merge
        // their own root in via `build_schema_with`.
        assert!(
            !sdl.contains("transfers"),
            "the framework schema must stay domain-free:\n{sdl}"
        );
    }

    #[tokio::test]
    async fn a_downstream_project_can_merge_its_own_queries_in() {
        // The extension claim, exercised: a crate outside the framework adds its own root
        // alongside CoreQuery and gets one schema serving both.
        #[derive(Default)]
        struct HydrationQuery;

        #[Object]
        impl HydrationQuery {
            async fn omnipool_swaps(&self, _chain_id: String) -> Vec<String> {
                vec![]
            }
        }

        #[derive(async_graphql::MergedObject, Default)]
        struct Query(CoreQuery, HydrationQuery);

        let pool = PgPool::connect_lazy("postgres://unused/unused").unwrap();
        let sdl = build_schema_with(pool, Query::default()).sdl();

        assert!(
            sdl.contains("omnipoolSwaps"),
            "downstream query missing:\n{sdl}"
        );
        assert!(sdl.contains("blocks"), "framework query lost:\n{sdl}");
    }
}
