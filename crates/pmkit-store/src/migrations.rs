use crate::{
    StoreError,
    schema::{CREATE_SCHEMA_MIGRATIONS, CURRENT_SCHEMA_VERSION, RECORD_SCHEMA_MIGRATION, SCHEMA},
};

#[derive(Clone, Copy)]
pub struct Migration {
    version: i64,
    statements: &'static [&'static str],
}

impl Migration {
    pub const fn new(version: i64, statements: &'static [&'static str]) -> Self {
        Self {
            version,
            statements,
        }
    }
}

const INITIAL_MIGRATION: Migration = Migration::new(1, &[CREATE_SCHEMA_MIGRATIONS, SCHEMA]);

const VERSIONED_CAUSAL_RECORDS_MIGRATION: Migration = Migration::new(
    2,
    &[
        "ALTER TABLE causal_decisions ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE durable_intents ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1",
    ],
);

const _: () = assert!(CURRENT_SCHEMA_VERSION == VERSIONED_CAUSAL_RECORDS_MIGRATION.version);

pub const MIGRATIONS: &[Migration] = &[INITIAL_MIGRATION, VERSIONED_CAUSAL_RECORDS_MIGRATION];

pub async fn apply(
    connection: &turso::Connection,
    migrations: &[Migration],
) -> Result<(), StoreError> {
    let database_version = database_version(connection).await?;
    let max_supported_version = migrations.last().map_or(0, |migration| migration.version);
    if database_version > max_supported_version {
        return Err(StoreError::DatabaseSchemaTooNew {
            database_version,
            max_supported_version,
        });
    }

    for migration in migrations
        .iter()
        .filter(|migration| migration.version > database_version)
    {
        apply_one(connection, migration).await?;
    }
    Ok(())
}

async fn database_version(connection: &turso::Connection) -> Result<i64, StoreError> {
    let mut tables = connection
        .query(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'schema_migrations' LIMIT 1",
            (),
        )
        .await?;
    if tables.next().await?.is_none() {
        return Ok(0);
    }

    let mut rows = connection
        .query("SELECT MAX(version) FROM schema_migrations", ())
        .await?;
    let row = rows.next().await?.ok_or_else(|| StoreError::Storage {
        message: "schema_migrations did not return a version row".into(),
    })?;
    Ok(row.get::<Option<i64>>(0)?.unwrap_or(0))
}

async fn apply_one(
    connection: &turso::Connection,
    migration: &Migration,
) -> Result<(), StoreError> {
    let transaction = connection.unchecked_transaction().await?;
    let result = async {
        for statement in migration.statements {
            transaction.execute_batch(statement).await?;
        }
        transaction
            .execute(RECORD_SCHEMA_MIGRATION, [migration.version])
            .await?;
        Ok::<(), turso::Error>(())
    }
    .await;

    match result {
        Ok(()) => transaction.commit().await?,
        Err(error) => {
            transaction.rollback().await?;
            return Err(error.into());
        }
    }
    Ok(())
}
