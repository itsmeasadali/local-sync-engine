use std::{collections::BTreeMap, time::Duration};

use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sync_protocol::{
    ActorId, CollectionName, FieldChange, HlcTimestamp, Mutation, PushStatus, RecordId, Version,
    WorkspaceId,
};
use sync_server::{hash_secret, Admin, Repository, ServerError};
use uuid::Uuid;

fn mutation(workspace: &str, actor: &str, time: u64) -> Mutation {
    Mutation {
        mutation_id: Uuid::new_v4(),
        workspace_id: WorkspaceId::new(workspace),
        actor_id: ActorId::new(actor),
        collection: CollectionName::new("todos"),
        record_id: RecordId::new("one"),
        fields: BTreeMap::from([(
            "title".into(),
            FieldChange {
                value: json!("test"),
            },
        )]),
        delete: false,
        version: Version {
            timestamp: HlcTimestamp {
                physical_ms: time,
                logical: 0,
            },
            actor_id: ActorId::new(actor),
        },
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn repository_is_idempotent_paginated_isolated_and_checks_auth_and_clock(
) -> anyhow::Result<()> {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!("TEST_DATABASE_URL not set; skipping PostgreSQL integration test");
        return Ok(());
    };
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    sqlx::migrate!().run(&pool).await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let workspace = format!("test-{suffix}");
    let other_workspace = format!("other-{suffix}");
    let actor = "actor";
    let credential_id = Uuid::new_v4();
    let other_credential_id = Uuid::new_v4();
    let expired_credential_id = Uuid::new_v4();
    let hash = hash_secret("secret")?;
    for value in [&workspace, &other_workspace] {
        sqlx::query("INSERT INTO workspaces (workspace_id) VALUES ($1)")
            .bind(value)
            .execute(&pool)
            .await?;
    }
    sqlx::query("INSERT INTO actors (workspace_id,actor_id,credential_id,credential_hash) VALUES ($1,$2,$3,$4)").bind(&workspace).bind(actor).bind(credential_id).bind(&hash).execute(&pool).await?;
    sqlx::query("INSERT INTO actors (workspace_id,actor_id,credential_id,credential_hash) VALUES ($1,$2,$3,$4)").bind(&other_workspace).bind(actor).bind(other_credential_id).bind(&hash).execute(&pool).await?;
    sqlx::query("INSERT INTO actors (workspace_id,actor_id,credential_id,credential_hash,expires_at) VALUES ($1,$2,$3,$4,now()-interval '1 second')").bind(&workspace).bind("expired").bind(expired_credential_id).bind(&hash).execute(&pool).await?;

    let repository = Repository::new(pool.clone());
    let auth = repository
        .authenticate(&format!("{credential_id}.secret"))
        .await?;
    assert!(matches!(
        repository
            .authenticate(&format!("{credential_id}.wrong"))
            .await,
        Err(ServerError::Unauthorized)
    ));
    assert!(matches!(
        repository
            .authenticate(&format!("{expired_credential_id}.secret"))
            .await,
        Err(ServerError::CredentialExpired)
    ));

    let admin = Admin::new(pool.clone());
    let issued = admin
        .create_actor(&workspace, "managed", Some(Duration::from_secs(60)))
        .await?;
    repository.authenticate(issued.bearer_token()).await?;
    admin.revoke_actor(&workspace, "managed").await?;
    assert!(matches!(
        repository.authenticate(issued.bearer_token()).await,
        Err(ServerError::CredentialRevoked)
    ));
    let rotated = admin.rotate_actor(&workspace, "managed", None).await?;
    repository.authenticate(rotated.bearer_token()).await?;
    assert_eq!(admin.actors(&workspace).await?.len(), 3);

    let first = mutation(&workspace, actor, 1);
    let second = mutation(&workspace, actor, 2);
    assert_eq!(
        repository
            .push(&auth, std::slice::from_ref(&first), Duration::from_secs(0))
            .await?[0]
            .status,
        PushStatus::Accepted
    );
    assert_eq!(
        repository
            .push(&auth, std::slice::from_ref(&first), Duration::from_secs(0))
            .await?[0]
            .status,
        PushStatus::AlreadyAccepted
    );
    assert_eq!(
        repository
            .push(&auth, std::slice::from_ref(&second), Duration::from_secs(0))
            .await?[0]
            .status,
        PushStatus::Accepted
    );
    let concurrent = mutation(&workspace, actor, 3);
    let (left, right) = tokio::join!(
        repository.push(
            &auth,
            std::slice::from_ref(&concurrent),
            Duration::from_secs(0)
        ),
        repository.push(
            &auth,
            std::slice::from_ref(&concurrent),
            Duration::from_secs(0)
        )
    );
    let statuses = [left?[0].status.clone(), right?[0].status.clone()];
    assert!(statuses.contains(&PushStatus::Accepted));
    assert!(statuses.contains(&PushStatus::AlreadyAccepted));
    let page_one = repository.pull(&WorkspaceId::new(&workspace), 0, 1).await?;
    assert_eq!(page_one.mutations.len(), 1);
    assert!(page_one.has_more);
    let page_two = repository
        .pull(&WorkspaceId::new(&workspace), page_one.cursor, 1)
        .await?;
    assert_eq!(page_two.mutations.len(), 1);
    assert_ne!(
        page_one.mutations[0].mutation.mutation_id,
        page_two.mutations[0].mutation.mutation_id
    );
    assert!(repository
        .pull(&WorkspaceId::new(&other_workspace), 0, 10)
        .await?
        .mutations
        .is_empty());

    let future = mutation(&workspace, actor, u64::MAX - 1);
    let rejected = repository
        .push(&auth, &[future], Duration::from_secs(0))
        .await?;
    assert_eq!(rejected[0].status, PushStatus::Rejected);

    sqlx::query("DELETE FROM workspaces WHERE workspace_id = ANY($1)")
        .bind(vec![workspace, other_workspace])
        .execute(&pool)
        .await?;
    Ok(())
}
