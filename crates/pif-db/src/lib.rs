//! Postgres persistence for the indexer.

pub mod models;
pub mod repo;

pub use models::{
    ArchivedRuntime, BlockData, Cursor, NewBlock, NewEvent, NewExtrinsic, SegmentRecord, Watermarks,
};
pub use repo::DbResult;

use sqlx::postgres::{PgPool, PgPoolOptions};

/// Migrations are compiled into the binary, so `indexer migrate` works from anywhere
/// without needing the `migrations/` directory alongside it.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Connect to Postgres.
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await
}

/// Apply any pending migrations.
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}
