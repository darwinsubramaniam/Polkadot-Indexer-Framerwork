//! Polkadot Indexer Framework CLI (`pif`).

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use pif_core::IndexerConfig;

#[derive(Parser)]
#[command(
    name = "pif",
    version,
    about = "Polkadot Indexer Framework — a chain-agnostic Substrate indexer"
)]
struct Cli {
    /// Postgres connection string.
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://indexer:indexer@localhost:5433/substrate_indexer",
        global = true
    )]
    database_url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply database migrations.
    Migrate,

    /// Index the chains listed in the config file.
    Index {
        #[arg(long, env = "INDEXER_CONFIG", default_value = "config/chains.toml")]
        config: PathBuf,

        /// Index only this chain id, instead of every configured chain.
        #[arg(long)]
        chain: Option<String>,

        /// Ignore the stored cursor and start from this block.
        #[arg(long)]
        from: Option<u64>,

        /// Stop once this block has been indexed, instead of following the head.
        #[arg(long)]
        to: Option<u64>,
    },

    /// Serve the GraphQL API. Requires building with `--features api`.
    ///
    /// The subcommand is always present, even when the feature is off, so that running it
    /// on a default build explains what to do instead of failing with "unrecognized
    /// subcommand".
    Serve {
        #[arg(long, env = "API_PORT", default_value_t = 8000)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,pif=debug".into()),
        )
        .init();

    let cli = Cli::parse();

    let pool = pif_db::connect(&cli.database_url, 10)
        .await
        .with_context(|| format!("connecting to Postgres at {}", redact(&cli.database_url)))?;

    match cli.command {
        Command::Migrate => {
            pif_db::migrate(&pool)
                .await
                .context("running framework migrations")?;

            // Each handler owns its tables and its own migration history.
            let registry = build_registry();
            for handler in registry.all() {
                pif_chain::handlers::run_migrations(&pool, handler)
                    .await
                    .with_context(|| format!("migrations for handler {:?}", handler.name()))?;
            }
            tracing::info!("migrations applied");
        }

        Command::Index {
            config,
            chain,
            from,
            to,
        } => {
            let config = IndexerConfig::from_path(&config)?;

            let selected: Vec<_> = match &chain {
                Some(id) => vec![
                    config
                        .chain(id)
                        .with_context(|| format!("no chain {id:?} in config"))?
                        .clone(),
                ],
                None => config.chains.clone(),
            };

            tracing::info!(chains = selected.len(), "starting indexer");

            // One task per chain, each with its own connection and cursor, so a chain that
            // goes down cannot stall the others.
            let mut tasks = tokio::task::JoinSet::new();
            for chain_config in selected {
                let pool = pool.clone();
                tasks.spawn(async move {
                    let id = chain_config.id.clone();
                    let options = pif_chain::IndexOptions { stop_at: to, from };

                    // The registry is built per task because `Selected` borrows from it.
                    let registry = build_registry();
                    if let Err(e) = pif_chain::run(&pool, &chain_config, &registry, options).await {
                        tracing::error!(chain = %id, error = %e, "chain indexer stopped");
                        return Err(e);
                    }
                    Ok(())
                });
            }

            while let Some(result) = tasks.join_next().await {
                result.context("indexer task panicked")??;
            }
        }

        #[cfg(feature = "api")]
        Command::Serve { port } => {
            let app = pif_api::router_with(build_schema(pool));
            let addr = format!("0.0.0.0:{port}");
            let listener = tokio::net::TcpListener::bind(&addr)
                .await
                .with_context(|| format!("binding {addr}"))?;

            tracing::info!("GraphiQL on http://localhost:{port}, API at /graphql");
            axum::serve(listener, app).await.context("serving API")?;
        }

        // The GraphQL layer pulls ~59 crates (async-graphql, axum) that the indexing
        // pipeline does not need, so it is opt-in.
        #[cfg(not(feature = "api"))]
        Command::Serve { .. } => {
            anyhow::bail!(
                "this binary was built without the GraphQL API.\n\
                 Rebuild with the `api` feature to use `serve`:\n  \
                   cargo run -p polkadot-indexer-cli --features api -- serve\n\
                 The indexed data is also queryable directly from Postgres."
            );
        }
    }

    Ok(())
}

/// The GraphQL schema this binary serves.
///
/// The companion to [`build_registry`]: a handler that owns tables usually wants to expose
/// them, and the framework schema deliberately knows nothing about them. Merging a root here
/// is how a project adds its own queries without forking `pif-api`.
#[cfg(all(feature = "api", feature = "handler-identity"))]
fn build_schema(
    pool: sqlx::PgPool,
) -> async_graphql::Schema<Query, async_graphql::EmptyMutation, async_graphql::EmptySubscription> {
    pif_api::build_schema_with(pool, Query::default())
}

#[cfg(all(feature = "api", feature = "handler-identity"))]
#[derive(async_graphql::MergedObject, Default)]
struct Query(pif_api::CoreQuery, pif_identity::IdentityQuery);

/// Without any table-owning handler there is nothing to merge, so this is the plain
/// framework schema.
#[cfg(all(feature = "api", not(feature = "handler-identity")))]
fn build_schema(pool: sqlx::PgPool) -> pif_api::IndexerSchema {
    pif_api::build_schema(pool)
}

/// Every handler this binary knows about.
///
/// This is the whole extension seam. A downstream indexer (`hydration-indexer`, say) writes
/// its own version of this function registering its own handlers, and reuses everything else
/// in the framework unchanged — no fork, no patch.
fn build_registry() -> pif_chain::HandlerRegistry {
    #[allow(unused_mut)]
    let mut registry = pif_chain::HandlerRegistry::new();

    #[cfg(feature = "handler-balances")]
    registry.register(Box::new(pif_example_balances::BalancesTransferHandler));

    #[cfg(feature = "handler-identity")]
    registry.register(Box::new(pif_identity::IdentityHandler));

    registry
}

/// Strip credentials before a connection string reaches the logs.
fn redact(url: &str) -> String {
    match (url.find("://"), url.rfind('@')) {
        (Some(scheme_end), Some(at)) if at > scheme_end => {
            format!("{}://***{}", &url[..scheme_end], &url[at..])
        }
        _ => url.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn credentials_never_reach_the_logs() {
        assert_eq!(
            redact("postgres://user:hunter2@localhost:5432/db"),
            "postgres://***@localhost:5432/db"
        );
        // Nothing to redact is left alone rather than mangled.
        assert_eq!(redact("postgres://localhost/db"), "postgres://localhost/db");
    }
}
