//! The SQL this handler owns.
//!
//! Every function takes a `&mut PgConnection` rather than a pool, so the same code runs inside
//! the block's transaction (where it commits atomically with the block) and during the
//! bootstrap sweep (where it runs on its own connection).
//!
//! Following the framework's contract, all of it is **idempotent**: replaying a block must be
//! a no-op, because the pipeline re-indexes any block whose transaction did not commit.
//!
//! `identities` is temporal. Writing a change means closing the currently-open row and opening
//! a new one, except when the open row already starts at this block — two identity changes in
//! one block (a `batch` of `set_identity` + `provide_judgement` is ordinary) must collapse into
//! one row, not produce a zero-length interval.

use serde_json::Value as Json;
use sqlx::PgConnection;

use crate::model::IdentityRow;

pub type Result<T> = std::result::Result<T, sqlx::Error>;

/// Write an account's identity as of `block`, or clear it when `row` is `None`.
pub async fn put_identity(
    conn: &mut PgConnection,
    chain_id: &str,
    account: &str,
    block: i64,
    row: Option<&IdentityRow>,
) -> Result<()> {
    let Some(row) = row else {
        return clear_identity(conn, chain_id, account, block).await;
    };

    // Skip a rewrite when nothing actually changed. Without this, an event that touches an
    // account without altering its identity (a sub being renamed, say) would open a new
    // interval every time and the history would be mostly noise.
    let current: Option<(Json,)> = sqlx::query_as(
        "SELECT raw FROM identities
          WHERE chain_id = $1 AND account = $2 AND valid_to_block IS NULL",
    )
    .bind(chain_id)
    .bind(account)
    .fetch_optional(&mut *conn)
    .await?;

    if current.as_ref().is_some_and(|(raw,)| raw == &row.raw) {
        return Ok(());
    }

    // Close the open interval, but only if it began earlier: an interval that already starts
    // at this block is updated in place by the upsert below.
    sqlx::query(
        "UPDATE identities SET valid_to_block = $3 - 1
          WHERE chain_id = $1 AND account = $2
            AND valid_to_block IS NULL AND valid_from_block < $3",
    )
    .bind(chain_id)
    .bind(account)
    .bind(block)
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO identities (
            chain_id, account, valid_from_block, valid_to_block,
            display, legal, web, email, twitter, matrix, github, discord, image,
            pgp_fingerprint, judgements, is_verified, deposit, raw
        )
        VALUES ($1, $2, $3, NULL,
                $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17)
        ON CONFLICT (chain_id, account, valid_from_block) DO UPDATE SET
            valid_to_block  = NULL,
            display         = EXCLUDED.display,
            legal           = EXCLUDED.legal,
            web             = EXCLUDED.web,
            email           = EXCLUDED.email,
            twitter         = EXCLUDED.twitter,
            matrix          = EXCLUDED.matrix,
            github          = EXCLUDED.github,
            discord         = EXCLUDED.discord,
            image           = EXCLUDED.image,
            pgp_fingerprint = EXCLUDED.pgp_fingerprint,
            judgements      = EXCLUDED.judgements,
            is_verified     = EXCLUDED.is_verified,
            deposit         = EXCLUDED.deposit,
            raw             = EXCLUDED.raw
        "#,
    )
    .bind(chain_id)
    .bind(account)
    .bind(block)
    .bind(&row.display)
    .bind(&row.legal)
    .bind(&row.web)
    .bind(&row.email)
    .bind(&row.twitter)
    .bind(&row.matrix)
    .bind(&row.github)
    .bind(&row.discord)
    .bind(&row.image)
    .bind(row.pgp_fingerprint.as_deref())
    .bind(&row.judgements)
    .bind(row.is_verified)
    .bind(&row.deposit)
    .bind(&row.raw)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// `IdentityCleared` / `IdentityKilled`: the account has no identity from `block` onwards.
async fn clear_identity(
    conn: &mut PgConnection,
    chain_id: &str,
    account: &str,
    block: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE identities SET valid_to_block = $3 - 1
          WHERE chain_id = $1 AND account = $2
            AND valid_to_block IS NULL AND valid_from_block < $3",
    )
    .bind(chain_id)
    .bind(account)
    .bind(block)
    .execute(&mut *conn)
    .await?;

    // Set and cleared within the same block nets out to "no identity", so the interval that
    // opened at this block must go rather than be left open describing a state that never
    // survived the block.
    sqlx::query(
        "DELETE FROM identities
          WHERE chain_id = $1 AND account = $2 AND valid_from_block = $3",
    )
    .bind(chain_id)
    .bind(account)
    .bind(block)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Record a username's current state.
///
/// Deliberately does not touch `is_primary`: which username is primary comes from
/// `UsernameOf`, handled by [`set_primary_username`], and a status update must not silently
/// demote one.
#[allow(clippy::too_many_arguments)]
pub async fn put_username(
    conn: &mut PgConnection,
    chain_id: &str,
    username: &str,
    account: Option<&str>,
    status: &str,
    provider: Option<&Json>,
    until_block: Option<i64>,
    block: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO usernames (
            chain_id, username, account, is_primary, status, provider,
            until_block, granted_at_block, updated_at_block
        )
        VALUES ($1, $2, $3, false, $4, $5, $6, $7, $7)
        ON CONFLICT (chain_id, username) DO UPDATE SET
            account          = EXCLUDED.account,
            status           = EXCLUDED.status,
            provider         = EXCLUDED.provider,
            until_block      = EXCLUDED.until_block,
            updated_at_block = EXCLUDED.updated_at_block
        "#,
    )
    .bind(chain_id)
    .bind(username)
    .bind(account)
    .bind(status)
    .bind(provider)
    .bind(until_block)
    .bind(block)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Point an account at its primary username, or at none.
///
/// Demotes first, then promotes: the partial unique index allows only one active primary per
/// account, so promoting before demoting would violate it.
pub async fn set_primary_username(
    conn: &mut PgConnection,
    chain_id: &str,
    account: &str,
    username: Option<&str>,
    block: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE usernames SET is_primary = false, updated_at_block = $3
          WHERE chain_id = $1 AND account = $2 AND is_primary",
    )
    .bind(chain_id)
    .bind(account)
    .bind(block)
    .execute(&mut *conn)
    .await?;

    let Some(username) = username else {
        return Ok(());
    };

    // The row may not exist yet: `UsernameOf` can be read before this block's
    // `UsernameInfoOf` sweep reaches the same name, so insert rather than assume.
    sqlx::query(
        r#"
        INSERT INTO usernames (
            chain_id, username, account, is_primary, status, granted_at_block, updated_at_block
        )
        VALUES ($1, $2, $3, true, 'active', $4, $4)
        ON CONFLICT (chain_id, username) DO UPDATE SET
            account          = EXCLUDED.account,
            is_primary       = true,
            updated_at_block = EXCLUDED.updated_at_block
        "#,
    )
    .bind(chain_id)
    .bind(username)
    .bind(account)
    .bind(block)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Record that `sub` is a sub-identity of `super_account`, under `label`.
pub async fn put_sub_identity(
    conn: &mut PgConnection,
    chain_id: &str,
    sub: &str,
    super_account: &str,
    label: Option<&str>,
    block: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO sub_identities (chain_id, sub, super_account, label, updated_at_block)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (chain_id, sub) DO UPDATE SET
            super_account    = EXCLUDED.super_account,
            label            = EXCLUDED.label,
            updated_at_block = EXCLUDED.updated_at_block
        "#,
    )
    .bind(chain_id)
    .bind(sub)
    .bind(super_account)
    .bind(label)
    .bind(block)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

pub async fn delete_sub_identity(conn: &mut PgConnection, chain_id: &str, sub: &str) -> Result<()> {
    sqlx::query("DELETE FROM sub_identities WHERE chain_id = $1 AND sub = $2")
        .bind(chain_id)
        .bind(sub)
        .execute(conn)
        .await?;
    Ok(())
}

/// Drop any sub of `super_account` that is no longer in its `SubsOf` list.
///
/// `SubIdentitiesSet { main, number_of_subs }` says how many subs there now are and never
/// which, so removals are only visible as the difference against what we stored.
pub async fn retain_subs(
    conn: &mut PgConnection,
    chain_id: &str,
    super_account: &str,
    keep: &[String],
) -> Result<()> {
    sqlx::query(
        "DELETE FROM sub_identities
          WHERE chain_id = $1 AND super_account = $2 AND sub <> ALL($3)",
    )
    .bind(chain_id)
    .bind(super_account)
    .bind(keep)
    .execute(conn)
    .await?;
    Ok(())
}

/// One registrar: `(account, fee, the whole decoded RegistrarInfo)`.
///
/// `None` for the entry itself is a *removed* registrar. `None` for `account` inside it is a
/// registrar whose account could not be decoded — a different thing, kept distinct so a decode
/// problem is not silently indistinguishable from a removal.
pub type RegistrarEntry = Option<(Option<String>, Option<bigdecimal::BigDecimal>, Json)>;

/// Replace the registrar set. `registrars` is index-aligned; a `None` slot must stay so the
/// indices of later registrars do not shift.
pub async fn put_registrars(
    conn: &mut PgConnection,
    chain_id: &str,
    registrars: &[RegistrarEntry],
    block: i64,
) -> Result<()> {
    for (index, entry) in registrars.iter().enumerate() {
        let index = index as i32;
        let (account, fee, fields) = match entry {
            Some((account, fee, fields)) => (account.clone(), fee.clone(), Some(fields.clone())),
            None => (None, None, None),
        };

        sqlx::query(
            r#"
            INSERT INTO identity_registrars
                (chain_id, registrar_index, account, fee, fields, updated_at_block)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (chain_id, registrar_index) DO UPDATE SET
                account          = EXCLUDED.account,
                fee              = EXCLUDED.fee,
                fields           = EXCLUDED.fields,
                updated_at_block = EXCLUDED.updated_at_block
            "#,
        )
        .bind(chain_id)
        .bind(index)
        .bind(&account)
        .bind(&fee)
        .bind(&fields)
        .bind(block)
        .execute(&mut *conn)
        .await?;
    }

    // The set can only shrink by truncation from the end.
    sqlx::query("DELETE FROM identity_registrars WHERE chain_id = $1 AND registrar_index >= $2")
        .bind(chain_id)
        .bind(registrars.len() as i32)
        .execute(&mut *conn)
        .await?;

    Ok(())
}

/// The block a completed bootstrap sweep snapshotted, if one has run.
pub async fn bootstrap_block(conn: &mut PgConnection, chain_id: &str) -> Result<Option<i64>> {
    let row: Option<(i64,)> = sqlx::query_as(
        "SELECT snapshot_block FROM identity_bootstrap
          WHERE chain_id = $1 AND completed_at IS NOT NULL",
    )
    .bind(chain_id)
    .fetch_optional(conn)
    .await?;

    Ok(row.map(|(b,)| b))
}

/// Mark the sweep finished. Only after this does a restart skip it — a crash mid-sweep
/// leaves `completed_at` NULL and the sweep runs again.
pub async fn finish_bootstrap(conn: &mut PgConnection, chain_id: &str, block: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO identity_bootstrap (chain_id, snapshot_block, completed_at)
        VALUES ($1, $2, now())
        ON CONFLICT (chain_id) DO UPDATE SET
            snapshot_block = EXCLUDED.snapshot_block,
            completed_at   = now()
        "#,
    )
    .bind(chain_id)
    .bind(block)
    .execute(conn)
    .await?;
    Ok(())
}
