use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use sync_client::{HttpTransport, LocalStore, SyncClient};
use sync_protocol::{ActorId, WorkspaceId};

#[derive(Parser)]
#[command(about = "Inspect and synchronize a local-first database")]
struct Args {
    #[arg(long, env = "SYNCCTL_DATABASE", default_value = "sync.db")]
    database: PathBuf,
    #[arg(long, env = "SYNCCTL_SERVER", default_value = "http://127.0.0.1:8080/")]
    server: String,
    #[arg(long, env = "SYNCCTL_TOKEN")]
    token: Option<String>,
    #[arg(long, env = "SYNCCTL_WORKSPACE")]
    workspace: String,
    #[arg(long, env = "SYNCCTL_ACTOR")]
    actor: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init,
    Set {
        collection: String,
        record_id: String,
        field: String,
        json_value: String,
    },
    Delete {
        collection: String,
        record_id: String,
    },
    Get {
        collection: String,
        record_id: String,
    },
    Outbox,
    Rejected,
    Retry {
        mutation_id: uuid::Uuid,
    },
    Discard {
        mutation_id: uuid::Uuid,
    },
    Sync,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let workspace = WorkspaceId::new(args.workspace);
    let actor = ActorId::new(args.actor);
    let store = LocalStore::open(&args.database)
        .with_context(|| format!("open {}", args.database.display()))?;
    match args.command {
        Command::Init => println!("initialized {}", args.database.display()),
        Command::Set {
            collection,
            record_id,
            field,
            json_value,
        } => {
            let value = serde_json::from_str(&json_value)
                .context("json_value must be valid JSON (quote strings)")?;
            let mutation = store.local_mutation(
                &workspace,
                &actor,
                &collection,
                &record_id,
                BTreeMap::from([(field, value)]),
                false,
                now_ms()?,
            )?;
            println!("queued {}", mutation.mutation_id);
        }
        Command::Delete {
            collection,
            record_id,
        } => {
            let mutation = store.local_mutation(
                &workspace,
                &actor,
                &collection,
                &record_id,
                BTreeMap::new(),
                true,
                now_ms()?,
            )?;
            println!("queued delete {}", mutation.mutation_id);
        }
        Command::Get {
            collection,
            record_id,
        } => match store.record(&workspace, &collection, &record_id)? {
            Some(record) => println!("{}", serde_json::to_string_pretty(&record)?),
            None => println!("null"),
        },
        Command::Outbox => {
            for mutation in store.pending(&workspace, usize::MAX)? {
                println!("{}", serde_json::to_string(&mutation)?);
            }
        }
        Command::Rejected => {
            for rejected in store.rejected(&workspace)? {
                println!(
                    "{} code={:?} rejected_at_ms={} message={}",
                    rejected.mutation.mutation_id,
                    rejected.error.code,
                    rejected.rejected_at_ms,
                    rejected.error.message
                );
            }
        }
        Command::Retry { mutation_id } => {
            store.retry_rejected(&workspace, mutation_id)?;
            println!("requeued {mutation_id}");
        }
        Command::Discard { mutation_id } => {
            store.discard_rejected(&workspace, mutation_id)?;
            println!("discarded {mutation_id}");
        }
        Command::Sync => {
            let Some(token) = args.token else {
                bail!("--token or SYNCCTL_TOKEN is required for sync");
            };
            let transport = HttpTransport::new(&args.server, token, Duration::from_secs(30))?;
            let report = SyncClient::new(store, transport, workspace, actor)
                .sync()
                .await?;
            println!(
                "pushed={} pulled={} pages={} newly_rejected={} quarantined={}",
                report.pushed, report.pulled, report.pages, report.rejected, report.quarantined
            );
            if report.quarantined > 0 {
                bail!(
                    "{} mutation(s) require attention; run `syncctl rejected`",
                    report.quarantined
                );
            }
        }
    }
    Ok(())
}

fn now_ms() -> anyhow::Result<u64> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    u64::try_from(millis).context("system time does not fit u64")
}
