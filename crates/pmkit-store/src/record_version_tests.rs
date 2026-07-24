use std::path::PathBuf;

use pmkit_core::{PortfolioId, RunId};
use serde_json::json;

use crate::{
    CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, StoreError, TapeStore,
    TursoTapeStore,
};

fn fixture(
    name: &str,
    correlation_id: &str,
    ingest_sequence: i64,
) -> Result<(tempfile::TempDir, PathBuf, CausalIdentity), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(format!("pmkit-store-{name}.db"));
    let identity = CausalIdentity {
        scope: OwnerScope::new(PortfolioId::new("legacy")?, RunId::new("run")?),
        correlation_id: correlation_id.into(),
        source_timestamp_ms: ingest_sequence,
        ingest_sequence,
    };
    Ok((dir, path, identity))
}

#[tokio::test]
async fn decision_unsupported_version_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a decision row carrying a schema version newer than this reader supports.
    let (_dir, path, identity) = fixture("decision-unsupported-version", "decision", 1)?;
    let store = TursoTapeStore::open_local(&path).await?;
    store
        .store_decision(&CausalDecision {
            identity: identity.clone(),
            payload: json!({"kind": "decision"}),
        })
        .await?;
    let updated = store
        .connection
        .execute(
            "UPDATE causal_decisions SET schema_version = 2
             WHERE correlation_id = ?1 AND schema_version = 1",
            [identity.correlation_id.as_str()],
        )
        .await?;
    assert_eq!(updated, 1);

    // When: decisions are read through the public store API.
    let result = store.read_decisions(&identity.scope).await;
    drop(store);

    // Then: the unsupported row fails closed with a typed error.
    assert!(matches!(
        result,
        Err(StoreError::UnsupportedRecordSchemaVersion {
            record_type: "causal_decisions",
            schema_version: 2,
            max_supported_version: 1,
        })
    ));
    Ok(())
}

#[tokio::test]
async fn intent_unsupported_versions_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // Given: pending and unknown intent rows carrying an unsupported schema version.
    let (_dir, path, pending) = fixture("intent-unsupported-version", "pending", 1)?;
    let unknown = CausalIdentity {
        correlation_id: "unknown".into(),
        source_timestamp_ms: 2,
        ingest_sequence: 2,
        ..pending.clone()
    };
    let store = TursoTapeStore::open_local(&path).await?;
    store.store_intent_pending(&pending, &json!({})).await?;
    store.store_intent_pending(&unknown, &json!({})).await?;
    store
        .transition_intent(&unknown, IntentOutcome::Unknown)
        .await?;
    let updated = store
        .connection
        .execute(
            "UPDATE durable_intents SET schema_version = 2 WHERE schema_version = 1",
            (),
        )
        .await?;
    assert_eq!(updated, 2);

    // When: each intent state is read through the public store API.
    let pending_result = store.read_pending_intents(&pending.scope).await;
    let unknown_result = store.read_unknown_intents(&pending.scope).await;
    drop(store);

    // Then: neither reader skips its unsupported row.
    assert!(matches!(
        pending_result,
        Err(StoreError::UnsupportedRecordSchemaVersion {
            record_type: "durable_intents",
            schema_version: 2,
            max_supported_version: 1,
        })
    ));
    assert!(matches!(
        unknown_result,
        Err(StoreError::UnsupportedRecordSchemaVersion {
            record_type: "durable_intents",
            schema_version: 2,
            max_supported_version: 1,
        })
    ));
    Ok(())
}
