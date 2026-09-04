use std::time::Duration;

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use sqlx::postgres::PgPoolOptions;
use sync_server::{hash_secret, router, Admin, AppState, Config, Metrics, Repository};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Parser)]
#[command(about = "Run and administer the local sync server")]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Serve,
    WorkspaceCreate {
        workspace_id: String,
    },
    ActorCreate {
        workspace_id: String,
        actor_id: String,
        #[arg(long)]
        expires_in_days: Option<u32>,
    },
    ActorRotate {
        workspace_id: String,
        actor_id: String,
        #[arg(long)]
        expires_in_days: Option<u32>,
    },
    ActorRevoke {
        workspace_id: String,
        actor_id: String,
    },
    ActorList {
        workspace_id: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match Args::parse().command.unwrap_or(Command::Serve) {
        Command::Serve => serve().await,
        command => administer(command).await,
    }
}

async fn serve() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let pool = connect(&config.database_url, config.request_timeout).await?;
    bootstrap(&pool, &config).await?;
    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .context("bind server")?;
    tracing::info!(address = %config.listen_addr, "sync server listening");
    let state = AppState {
        repository: Repository::new(pool),
        config,
        metrics: Metrics::new(),
    };
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown())
        .await
        .context("serve requests")?;
    Ok(())
}

async fn administer(command: Command) -> anyhow::Result<()> {
    let database_url = std::env::var("SYNC_DATABASE_URL")
        .context("SYNC_DATABASE_URL is required for administration")?;
    let pool = connect(&database_url, Duration::from_secs(30)).await?;
    let admin = Admin::new(pool);
    match command {
        Command::WorkspaceCreate { workspace_id } => {
            admin.create_workspace(&workspace_id).await?;
            println!("created workspace {workspace_id}");
        }
        Command::ActorCreate {
            workspace_id,
            actor_id,
            expires_in_days,
        } => {
            let issued = admin
                .create_actor(&workspace_id, &actor_id, expiry(expires_in_days)?)
                .await?;
            print_credential(&issued);
        }
        Command::ActorRotate {
            workspace_id,
            actor_id,
            expires_in_days,
        } => {
            let issued = admin
                .rotate_actor(&workspace_id, &actor_id, expiry(expires_in_days)?)
                .await?;
            print_credential(&issued);
        }
        Command::ActorRevoke {
            workspace_id,
            actor_id,
        } => {
            admin.revoke_actor(&workspace_id, &actor_id).await?;
            println!("revoked actor {actor_id} in workspace {workspace_id}");
        }
        Command::ActorList { workspace_id } => {
            for actor in admin.actors(&workspace_id).await? {
                println!(
                    "{} credential_id={} revoked={} expires_at={}",
                    actor.actor_id,
                    actor.credential_id,
                    actor.revoked,
                    actor.expires_at.as_deref().unwrap_or("never")
                );
            }
        }
        Command::Serve => unreachable!("serve command is handled before administration"),
    }
    Ok(())
}

async fn connect(database_url: &str, timeout: Duration) -> anyhow::Result<sqlx::PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .acquire_timeout(timeout)
        .connect(database_url)
        .await
        .context("connect to PostgreSQL")?;
    sqlx::migrate!()
        .run(&pool)
        .await
        .context("run PostgreSQL migrations")?;
    Ok(pool)
}

fn expiry(days: Option<u32>) -> anyhow::Result<Option<Duration>> {
    let Some(days) = days else {
        return Ok(None);
    };
    let seconds = u64::from(days)
        .checked_mul(86_400)
        .context("expiry duration overflow")?;
    Ok(Some(Duration::from_secs(seconds)))
}

fn print_credential(issued: &sync_server::IssuedCredential) {
    println!("workspace_id={}", issued.workspace_id);
    println!("actor_id={}", issued.actor_id);
    println!("credential_id={}", issued.credential_id);
    println!("bearer_token={}", issued.bearer_token());
    eprintln!("Store the bearer token now; the server retains only its secure hash.");
}

async fn bootstrap(pool: &sqlx::PgPool, config: &Config) -> anyhow::Result<()> {
    match (
        &config.bootstrap_workspace,
        &config.bootstrap_actor,
        &config.bootstrap_token,
    ) {
        (None, None, None) => Ok(()),
        (Some(workspace), Some(actor), Some(token)) => {
            let (credential_id, secret) = token
                .split_once('.')
                .context("SYNC_BOOTSTRAP_TOKEN must be <uuid>.<secret>")?;
            let credential_id = Uuid::parse_str(credential_id)
                .context("bootstrap credential ID must be a UUID")?;
            let hash = hash_secret(secret).map_err(anyhow::Error::from)?;
            let mut transaction = pool.begin().await?;
            sqlx::query(
                "INSERT INTO workspaces (workspace_id) VALUES ($1) ON CONFLICT DO NOTHING",
            )
            .bind(workspace)
            .execute(&mut *transaction)
            .await?;
            sqlx::query("INSERT INTO actors (workspace_id, actor_id, credential_id, credential_hash) VALUES ($1,$2,$3,$4) ON CONFLICT (workspace_id, actor_id) DO UPDATE SET credential_id=EXCLUDED.credential_id,credential_hash=EXCLUDED.credential_hash,expires_at=NULL,revoked_at=NULL")
                .bind(workspace).bind(actor).bind(credential_id).bind(hash).execute(&mut *transaction).await?;
            transaction.commit().await?;
            tracing::info!(workspace_id = workspace, actor_id = actor, "bootstrap actor configured");
            Ok(())
        }
        _ => bail!(
            "SYNC_BOOTSTRAP_WORKSPACE, SYNC_BOOTSTRAP_ACTOR, and SYNC_BOOTSTRAP_TOKEN must be set together"
        ),
    }
}

async fn shutdown() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install ctrl-c handler");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => tracing::error!(%error, "failed to install terminate handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { () = ctrl_c => {}, () = terminate => {} }
    tracing::info!("graceful shutdown requested");
}
