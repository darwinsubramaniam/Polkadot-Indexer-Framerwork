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

    /// Archive blocks locally without processing them.
    ///
    /// Decodes nothing, so it keeps running through a runtime the digest cannot yet read —
    /// and the backlog is waiting when a fix lands.
    Fetch {
        #[arg(long, env = "INDEXER_CONFIG", default_value = "config/chains.toml")]
        config: PathBuf,

        /// Archive only this chain id, instead of every configured chain.
        #[arg(long)]
        chain: Option<String>,

        /// Ignore the stored watermarks and start from this block.
        #[arg(long)]
        from: Option<u64>,

        /// Stop once this block has been archived, instead of following the head.
        #[arg(long)]
        to: Option<u64>,
    },

    /// Process blocks that are already archived.
    ///
    /// Connects to the chain only if a configured handler reads chain state; otherwise this
    /// runs with the node switched off.
    Digest {
        #[arg(long, env = "INDEXER_CONFIG", default_value = "config/chains.toml")]
        config: PathBuf,

        #[arg(long)]
        chain: Option<String>,

        /// Re-digest from this block, moving the digest watermark back to reach it.
        #[arg(long)]
        from: Option<u64>,

        #[arg(long)]
        to: Option<u64>,
    },

    /// Re-process a range from the local archive.
    ///
    /// The reason the archive exists: adding a handler or fixing a decode bug becomes a local
    /// operation over bytes already held, rather than a re-download. Every insert is
    /// `ON CONFLICT DO NOTHING`, so replaying an indexed range is idempotent.
    Replay {
        #[arg(long, env = "INDEXER_CONFIG", default_value = "config/chains.toml")]
        config: PathBuf,

        /// Replay only this chain id.
        #[arg(long)]
        chain: Option<String>,

        #[arg(long)]
        from: u64,

        #[arg(long)]
        to: u64,
    },

    /// Inspect the local block archive.
    Store {
        #[command(subcommand)]
        command: StoreCommand,
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

#[derive(Subcommand)]
enum StoreCommand {
    /// What the archive holds, and how far each stage has got.
    Status {
        #[arg(long, env = "INDEXER_CONFIG", default_value = "config/chains.toml")]
        config: PathBuf,

        #[arg(long)]
        chain: Option<String>,
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
            let selected = select_chains(&config, chain.as_deref())?;

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

        Command::Fetch {
            config,
            chain,
            from,
            to,
        } => {
            let config = IndexerConfig::from_path(&config)?;
            let selected = select_chains(&config, chain.as_deref())?;

            tracing::info!(chains = selected.len(), "starting fetch stage");
            run_per_chain(&pool, selected, move |pool, chain_config| async move {
                let options = pif_chain::IndexOptions { stop_at: to, from };
                pif_chain::fetch_only(&pool, &chain_config, options).await
            })
            .await?;
        }

        Command::Digest {
            config,
            chain,
            from,
            to,
        } => {
            let config = IndexerConfig::from_path(&config)?;
            let selected = select_chains(&config, chain.as_deref())?;

            tracing::info!(chains = selected.len(), "starting digest stage");
            run_per_chain(&pool, selected, move |pool, chain_config| async move {
                let options = pif_chain::IndexOptions { stop_at: to, from };
                let registry = build_registry();
                pif_chain::digest_only(&pool, &chain_config, &registry, options).await
            })
            .await?;
        }

        Command::Replay {
            config,
            chain,
            from,
            to,
        } => {
            let config = IndexerConfig::from_path(&config)?;
            let selected = select_chains(&config, chain.as_deref())?;

            run_per_chain(&pool, selected, move |pool, chain_config| async move {
                let registry = build_registry();
                pif_chain::replay(&pool, &chain_config, &registry, from, to).await
            })
            .await?;
            tracing::info!(from, to, "replay complete");
        }

        Command::Store {
            command: StoreCommand::Status { config, chain },
        } => {
            let config = IndexerConfig::from_path(&config)?;
            for chain_config in select_chains(&config, chain.as_deref())? {
                let status = pif_chain::store_status(&pool, &chain_config).await?;
                print_store_status(&status);
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

/// The chains a subcommand should act on: one named chain, or every configured one.
fn select_chains(
    config: &IndexerConfig,
    chain: Option<&str>,
) -> Result<Vec<pif_core::ChainConfig>> {
    match chain {
        Some(id) => Ok(vec![
            config
                .chain(id)
                .with_context(|| format!("no chain {id:?} in config"))?
                .clone(),
        ]),
        None => Ok(config.chains.clone()),
    }
}

/// Run one task per chain, so a chain that goes down cannot stall the others.
///
/// The registry is rebuilt inside each task rather than shared, because `Selected` borrows
/// from it and a spawned task needs everything it holds to be `'static`.
async fn run_per_chain<F, Fut>(
    pool: &sqlx::PgPool,
    chains: Vec<pif_core::ChainConfig>,
    work: F,
) -> Result<()>
where
    F: Fn(sqlx::PgPool, pif_core::ChainConfig) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = pif_chain::Result<()>> + Send,
{
    let mut tasks = tokio::task::JoinSet::new();
    for chain_config in chains {
        let pool = pool.clone();
        let work = work.clone();
        tasks.spawn(async move {
            let id = chain_config.id.clone();
            if let Err(e) = work(pool, chain_config).await {
                tracing::error!(chain = %id, error = %e, "chain task stopped");
                return Err(e);
            }
            Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.context("chain task panicked")??;
    }
    Ok(())
}

/// Print one chain's archive status, as lines rather than a table: it is read in a terminal
/// next to a failing replay, and the interesting number is usually the last one.
fn print_store_status(status: &pif_chain::pipeline::StoreStatus) {
    println!("chain            {}", status.chain_id);
    println!("archive          {}", status.path);

    match status.watermarks {
        Some(marks) => {
            println!("fetch watermark  {}", marks.fetch);
            println!("digest watermark {}", marks.digest);
            println!("archive tier     {}", marks.archive);
            println!("digest backlog   {}", marks.fetch - marks.digest);
        }
        None => println!("watermarks       none — this chain has not been indexed here"),
    }

    println!("segments         {}", status.usage.segments);
    println!("runtimes         {}", status.usage.runtimes);
    println!("hot bytes        {}", human_bytes(status.usage.bytes));

    match status.replayable {
        Some((from, to)) => println!("replayable       {from}..={to}"),
        None => println!("replayable       nothing is archived for this chain"),
    }

    if !status.runtimes_without_metadata.is_empty() {
        // Expected for every range indexed before the archive existed, and the precise
        // answer to "which ranges can I actually replay?".
        println!("runtimes with no archived metadata (these ranges cannot be replayed):");
        for (spec_version, first_block) in &status.runtimes_without_metadata {
            println!("  spec {spec_version} from block {first_block}");
        }
    }

    println!();
}

/// Size in whichever unit keeps it readable.
///
/// A fixed unit is wrong at both ends of this range: an archive is kilobytes on a dev chain
/// the day it is set up, and hundreds of gigabytes on Polkadot. "0.0 MiB" answers neither
/// question.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [(&str, f64); 4] = [
        ("TiB", 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("GiB", 1024.0 * 1024.0 * 1024.0),
        ("MiB", 1024.0 * 1024.0),
        ("KiB", 1024.0),
    ];

    for (unit, scale) in UNITS {
        if bytes as f64 >= scale {
            return format!("{:.1} {unit}", bytes as f64 / scale);
        }
    }
    format!("{bytes} B")
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
    fn archive_sizes_are_readable_at_both_ends_of_the_range() {
        // A dev chain's archive and Polkadot's differ by nine orders of magnitude, and the
        // status line has to be useful for both.
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.0 KiB");
        assert_eq!(human_bytes(5 * 1024 * 1024), "5.0 MiB");
        assert_eq!(human_bytes(120 * 1024 * 1024 * 1024), "120.0 GiB");
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
