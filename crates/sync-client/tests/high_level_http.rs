use std::time::Duration;

use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sync_client::{DeviceCredentials, SyncDatabase, SyncDatabaseConfig};
use sync_server::{router, Admin, AppState, Config, Metrics, Repository};
use uuid::Uuid;

#[tokio::test]
async fn high_level_clients_converge_through_the_http_server() -> anyhow::Result<()> {
    let Ok(database_url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL not set; skipping HTTP/PostgreSQL integration test");
        return Ok(());
    };
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("../sync-server/migrations")
        .run(&pool)
        .await?;
    let workspace = format!("http-{}", Uuid::new_v4().simple());
    let admin = Admin::new(pool.clone());
    admin.create_workspace(&workspace).await?;
    let alice_credential = admin.create_actor(&workspace, "alice", None).await?;
    let bob_credential = admin.create_actor(&workspace, "bob", None).await?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let state = AppState {
        repository: Repository::new(pool.clone()),
        config: Config {
            database_url: database_url.clone(),
            listen_addr: address,
            max_batch_size: 100,
            max_pull_limit: 100,
            max_body_bytes: 2_097_152,
            max_future_skew: Duration::from_secs(300),
            request_timeout: Duration::from_secs(10),
            bootstrap_workspace: None,
            bootstrap_actor: None,
            bootstrap_token: None,
        },
        metrics: Metrics::new(),
    };
    let server = tokio::spawn(async move { axum::serve(listener, router(state)).await });

    let temporary = tempfile::tempdir()?;
    let base_url = format!("http://{address}/");
    let alice = SyncDatabase::open(SyncDatabaseConfig::new(
        temporary.path().join("alice.db"),
        &base_url,
        DeviceCredentials::new(&workspace, "alice", alice_credential.bearer_token()),
    ))?;
    let bob = SyncDatabase::open(SyncDatabaseConfig::new(
        temporary.path().join("bob.db"),
        &base_url,
        DeviceCredentials::new(&workspace, "bob", bob_credential.bearer_token()),
    ))?;

    alice.patch("todos", "one", json!({"title": "Ship beta"}))?;
    bob.patch("todos", "one", json!({"done": true}))?;
    alice.sync().await?;
    bob.sync().await?;
    alice.sync().await?;

    let expected = Some(json!({"done": true, "title": "Ship beta"}));
    assert_eq!(alice.get("todos", "one")?, expected);
    assert_eq!(bob.get("todos", "one")?, expected);

    server.abort();
    let _stopped = server.await;
    sqlx::query("DELETE FROM workspaces WHERE workspace_id=$1")
        .bind(&workspace)
        .execute(&pool)
        .await?;
    Ok(())
}
