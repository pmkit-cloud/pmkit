use std::path::{Path, PathBuf};

use pmkit_core::{PortfolioId, RunId};
use serde_json::json;

use crate::{
    OwnerScope, PmEnvelope, ReplayItem, StoreError, TapeStore, TursoTapeStore,
    migrations::{Migration, apply},
};

const OLD_PM_ACCOUNT_ENVELOPE: &str = include_str!("../tests/fixtures/pm-account-envelope-v1.json");
const NEW_PM_ACCOUNT_ENVELOPE: &str = include_str!("../tests/fixtures/pm-account-envelope-v2.json");

fn database_path(name: &str) -> Result<(tempfile::TempDir, PathBuf), std::io::Error> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(format!("pmkit-store-{name}.db"));
    Ok((dir, path))
}

async fn open_connection(
    path: &Path,
) -> Result<(turso::Database, turso::Connection), turso::Error> {
    let database = turso::Builder::new_local(&path.to_string_lossy())
        .build()
        .await?;
    let connection = database.connect()?;
    Ok((database, connection))
}

async fn migration_rows(
    connection: &turso::Connection,
) -> Result<Vec<(i64, String)>, turso::Error> {
    let mut rows = connection
        .query(
            "SELECT version, applied_at FROM schema_migrations ORDER BY version",
            (),
        )
        .await?;
    let mut migrations = Vec::new();
    while let Some(row) = rows.next().await? {
        migrations.push((row.get(0)?, row.get(1)?));
    }
    Ok(migrations)
}

#[tokio::test]
async fn migration_applies_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a fresh local database.
    let (_dir, path) = database_path("migration-idempotent")?;

    // When: the store opens twice.
    let first_rows = {
        let store = TursoTapeStore::open_local(&path).await?;
        migration_rows(&store.connection).await?
    };
    let reopened_rows = {
        let store = TursoTapeStore::open_local(&path).await?;
        let rows = migration_rows(&store.connection).await?;
        store.delete_database()?;
        rows
    };

    // Then: all current migrations are recorded once with timestamps.
    assert!(matches!(
        first_rows.as_slice(),
        [
            (1, first_applied_at),
            (2, second_applied_at),
            (3, third_applied_at),
            (4, fourth_applied_at)
        ]
            if !first_applied_at.is_empty()
                && !second_applied_at.is_empty()
                && !third_applied_at.is_empty()
                && !fourth_applied_at.is_empty()
    ));
    assert_eq!(reopened_rows, first_rows);
    Ok(())
}

#[tokio::test]
async fn migration_rejects_newer_version() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a database stamped one version newer than this binary supports.
    let (_dir, path) = database_path("migration-newer")?;
    let store = TursoTapeStore::open_local(&path).await?;
    drop(store);
    let (database, connection) = open_connection(&path).await?;
    connection
        .execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (5, CURRENT_TIMESTAMP)",
            (),
        )
        .await?;
    drop(connection);
    drop(database);

    // When: the older binary reopens it.
    // Then: opening fails closed instead of attempting a downgrade.
    assert!(matches!(
        TursoTapeStore::open_local(&path).await,
        Err(StoreError::DatabaseSchemaTooNew {
            database_version: 5,
            max_supported_version: 4,
        })
    ));
    Ok(())
}

#[tokio::test]
async fn migration_rolls_back_on_failure() -> Result<(), Box<dyn std::error::Error>> {
    const FAILING_MIGRATION: Migration = Migration::new(
        5,
        &[
            "CREATE TABLE migration_partial_change (value INTEGER NOT NULL)",
            "CREATE TABLE migration_partial_change (",
        ],
    );

    // Given: a current database and a migration whose second statement is invalid.
    let (_dir, path) = database_path("migration-rollback")?;

    // When: the failing migration is applied.
    let (result, migrations, partial_table_count) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let result = apply(&store.connection, &[FAILING_MIGRATION]).await;
        let migrations = migration_rows(&store.connection).await?;
        let partial_table_count = {
            let mut rows = store
                .connection
                .query(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'migration_partial_change'",
                    (),
                )
                .await?;
            let row = rows.next().await?.ok_or("table-count row")?;
            row.get::<i64>(0)?
        };
        drop(store);
        (result, migrations, partial_table_count)
    };

    // Then: its version and first schema change are both rolled back.
    assert!(matches!(result, Err(StoreError::Storage { .. })));
    assert!(matches!(
        migrations.as_slice(),
        [(1, _), (2, _), (3, _), (4, _)]
    ));
    assert_eq!(partial_table_count, 0);
    Ok(())
}

fn pm_account_fixture(
    fixture: &str,
    scope: OwnerScope,
    frame_sequence: i64,
) -> Result<PmEnvelope, Box<dyn std::error::Error>> {
    let fixture: serde_json::Value = serde_json::from_str(fixture)?;
    let schema_version = fixture["schema_version"]
        .as_u64()
        .ok_or("fixture schema_version")?;
    let normalized = fixture
        .get("normalized")
        .cloned()
        .ok_or("fixture normalized")?;
    Ok(PmEnvelope {
        schema_version: u16::try_from(schema_version)?,
        scope,
        venue_id: "polymarket".into(),
        config_hash: "fixture".into(),
        source_id: "polymarket:user-ws".into(),
        connection_id: "account-1".into(),
        source_timestamp_ms: 1_000 + frame_sequence,
        canonical_source_rank: 0,
        connection_epoch: 1,
        frame_sequence,
        receipt_timestamp_ms: 1_001 + frame_sequence,
        ingest_sequence: frame_sequence,
        raw_frame: format!("fixture-{frame_sequence}").into_bytes(),
        normalized,
    })
}

#[tokio::test]
async fn pm_account_envelope_version_migrates_old_and_reads_new_fixtures()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a database at the pre-bump version containing a v1 account envelope.
    let (_dir, path) = database_path("pm-account-envelope-version")?;
    let scope = OwnerScope::new(PortfolioId::new("paper")?, RunId::new("run")?);
    let old = pm_account_fixture(OLD_PM_ACCOUNT_ENVELOPE, scope.clone(), 1)?;
    let store = TursoTapeStore::open_local(&path).await?;
    store.store_envelope(&old).await?;
    store
        .connection
        .execute("DELETE FROM schema_migrations WHERE version = 4", ())
        .await?;
    drop(store);

    // When: the current store migrates the database and appends a v2 settlement envelope.
    let (items, migrations, new) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let new = pm_account_fixture(NEW_PM_ACCOUNT_ENVELOPE, scope.clone(), 2)?;
        store.store_envelope(&new).await?;
        let page = store
            .read_envelopes(
                &scope,
                None,
                std::num::NonZeroUsize::new(2).ok_or("fixture page size")?,
            )
            .await?;
        let migrations = migration_rows(&store.connection).await?;
        let items = page.items;
        drop(store);
        (items, migrations, new)
    };

    // Then: both fixtures replay under the current envelope version without losing JSON evidence.
    let mut migrated_old = old;
    migrated_old.schema_version = 2;
    assert_eq!(
        items,
        vec![
            ReplayItem::Envelope(migrated_old),
            ReplayItem::Envelope(new)
        ]
    );
    assert!(matches!(
        migrations.as_slice(),
        [(1, _), (2, _), (3, _), (4, _)]
    ));
    Ok(())
}

#[tokio::test]
async fn decision_version_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a checked-in fixture created by the old bootstrap with no migration table.
    let (_dir, path) = database_path("migration-legacy")?;
    std::fs::write(
        &path,
        include_bytes!("../tests/fixtures/legacy-v0.db").as_slice(),
    )?;
    let (database, connection) = open_connection(&path).await?;
    connection
        .execute(
            "INSERT INTO causal_decisions (
                portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            ("legacy", "run", "legacy-decision", 1_i64, 1_i64, r#"{"kind":"legacy-decision"}"#),
        )
        .await?;
    connection
        .execute(
            "INSERT INTO durable_intents (
                portfolio_id, run_id, correlation_id, source_timestamp_ms, ingest_sequence, state, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6)",
            ("legacy", "run", "legacy-intent", 2_i64, 2_i64, r#"{"kind":"legacy-intent"}"#),
        )
        .await?;
    drop(connection);
    drop(database);

    // When: the current store opens the legacy fixture.
    let (migrations, killed, decision_version, intent_version, decisions, intents) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let migrations = migration_rows(&store.connection).await?;
        let killed = store.kill_state(&PortfolioId::new("legacy")?).await?;
        let decision_version = {
            let mut rows = store.connection.query(
                "SELECT schema_version FROM causal_decisions WHERE correlation_id = 'legacy-decision'",
                (),
            ).await?;
            rows.next()
                .await?
                .ok_or("legacy decision row")?
                .get::<i64>(0)?
        };
        let intent_version = {
            let mut rows = store.connection.query(
                "SELECT schema_version FROM durable_intents WHERE correlation_id = 'legacy-intent'",
                (),
            ).await?;
            rows.next()
                .await?
                .ok_or("legacy intent row")?
                .get::<i64>(0)?
        };
        let scope = OwnerScope::new(PortfolioId::new("legacy")?, RunId::new("run")?);
        let decisions = store.read_decisions(&scope).await?;
        let intents = store.read_pending_intents(&scope).await?;
        drop(store);
        (
            migrations,
            killed,
            decision_version,
            intent_version,
            decisions,
            intents,
        )
    };

    // Then: legacy records become version 1 without changing their JSON payloads.
    assert!(matches!(
        migrations.as_slice(),
        [(1, _), (2, _), (3, _), (4, _)]
    ));
    assert!(killed);
    assert_eq!(decision_version, 1);
    assert_eq!(intent_version, 1);
    assert_eq!(decisions[0].payload, json!({"kind": "legacy-decision"}));
    assert_eq!(intents[0].payload, json!({"kind": "legacy-intent"}));
    Ok(())
}
