use std::{
    num::NonZeroUsize,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use pmkit_core::{PortfolioId, RunId};
use serde_json::json;

use crate::{
    CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, PmEnvelope, ReplayCursor,
    ReplayGapReason, ReplayItem, StoreError, TapeStore, TursoTapeStore,
};

fn database_path(name: &str) -> Result<PathBuf, std::time::SystemTimeError> {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!("pmkit-store-{name}-{suffix}.db")))
}

fn owner_scope(name: &str) -> Result<OwnerScope, Box<dyn std::error::Error>> {
    Ok(OwnerScope::new(PortfolioId::new(name)?, RunId::new("run")?))
}

fn envelope(scope: OwnerScope, ingest_sequence: i64) -> PmEnvelope {
    PmEnvelope {
        schema_version: 1,
        scope,
        venue_id: "polymarket".into(),
        config_hash: "config-sha256".into(),
        source_id: "market-channel".into(),
        connection_id: "connection-7".into(),
        source_timestamp_ms: 1_000,
        canonical_source_rank: 0,
        connection_epoch: 0,
        frame_sequence: 0,
        receipt_timestamp_ms: 1_001,
        ingest_sequence,
        raw_frame: br#"{\"event_type\":\"price_change\",\"price\":\"0.42\"}"#.to_vec(),
        normalized: json!({"kind": "market_price", "price": "0.42"}),
    }
}

#[tokio::test]
async fn lossless_pm_envelope_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a file-backed PM envelope owned by one portfolio/run.
    let path = database_path("round-trip")?;
    let scope = owner_scope("paper")?;
    let mut first = envelope(scope.clone(), 99);
    first.connection_epoch = 3;
    first.frame_sequence = 1;
    let mut second = envelope(scope.clone(), 1);
    second.connection_epoch = 4;
    second.frame_sequence = 1;
    let mut third = envelope(scope.clone(), 2);
    third.canonical_source_rank = 1;
    third.connection_epoch = 1;
    third.frame_sequence = 1;
    let store = TursoTapeStore::open_local(&path).await?;

    // When: the envelope is persisted then replayed from its owner scope.
    store.store_envelope(&third).await?;
    store.store_envelope(&second).await?;
    store.store_envelope(&first).await?;
    let first_page = store
        .read_envelopes(&scope, None, NonZeroUsize::new(2).ok_or("limit")?)
        .await?;
    let second_page = store
        .read_envelopes(
            &scope,
            Some(first_page.next_cursor.clone().ok_or("cursor")?),
            NonZeroUsize::MIN,
        )
        .await?;

    // Then: canonical keys, not ingest sequence, order and continue replay.
    assert_eq!(
        first_page.items,
        vec![ReplayItem::Envelope(first), ReplayItem::Envelope(second)]
    );
    assert_eq!(second_page.items, vec![ReplayItem::Envelope(third)]);
    store.delete_database()?;
    drop(store);
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn duplicate_or_cross_owner_read_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one envelope and a cursor for its owner scope.
    let path = database_path("ownership")?;
    let scope = owner_scope("paper")?;
    let other_scope = owner_scope("other")?;
    let stored = envelope(scope.clone(), 7);
    let store = TursoTapeStore::open_local(&path).await?;
    store.store_envelope(&stored).await?;

    // When: its source identity is duplicated or its cursor is used cross-owner.
    let duplicate = store.store_envelope(&stored).await;
    let cross_owner = store
        .read_envelopes(
            &other_scope,
            Some(ReplayCursor::from_envelope(&stored)),
            NonZeroUsize::new(10).ok_or("limit")?,
        )
        .await;

    // Then: neither operation can cross the durable owner/source boundary.
    assert!(matches!(
        duplicate,
        Err(StoreError::DuplicateSourceIdentity)
    ));
    assert!(matches!(cross_owner, Err(StoreError::ScopeMismatch)));
    store.delete_database()?;
    drop(store);
    Ok(())
}

#[tokio::test]
async fn corrupt_pm_envelope_is_replay_gap_and_cursor_continues_canonically()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: two canonically ordered PM envelopes in a fresh file-backed database.
    let path = database_path("integrity")?;
    let scope = owner_scope("paper")?;
    let mut corrupt = envelope(scope.clone(), 99);
    corrupt.connection_epoch = 1;
    corrupt.frame_sequence = 1;
    let mut intact = envelope(scope.clone(), 1);
    intact.source_id = "account-channel".into();
    intact.canonical_source_rank = 1;
    intact.connection_epoch = 1;
    intact.frame_sequence = 1;
    let store = TursoTapeStore::open_local(&path).await?;
    store.store_envelope(&intact).await?;
    store.store_envelope(&corrupt).await?;
    drop(store);

    // When: the earlier row's raw-frame digest is corrupted outside the store.
    let database = turso::Builder::new_local(&path.to_string_lossy())
        .build()
        .await?;
    let connection = database.connect()?;
    connection
        .execute(
            "UPDATE pm_envelopes SET raw_sha256 = ?1 WHERE ingest_sequence = ?2",
            ("corrupt", corrupt.ingest_sequence),
        )
        .await?;
    drop(connection);
    drop(database);
    let store = TursoTapeStore::open_local(&path).await?;
    let first_page = store
        .read_envelopes(&scope, None, NonZeroUsize::MIN)
        .await?;
    let second_page = store
        .read_envelopes(&scope, first_page.next_cursor.clone(), NonZeroUsize::MIN)
        .await?;

    // Then: validation returns a typed gap and its canonical cursor reaches the intact row.
    assert!(matches!(
        first_page.items.as_slice(),
        [ReplayItem::Gap(gap)]
            if gap.reason == ReplayGapReason::RawIntegrityMismatch
                && gap.ingest_sequence == corrupt.ingest_sequence
    ));
    assert_eq!(second_page.items, vec![ReplayItem::Envelope(intact)]);
    store.delete_database()?;
    drop(store);
    Ok(())
}

#[tokio::test]
async fn causal_decision_and_pending_intent_are_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one causal identity in a portfolio/run scope.
    let path = database_path("causal")?;
    let identity = CausalIdentity {
        scope: owner_scope("paper")?,
        correlation_id: "intent-1".into(),
        source_timestamp_ms: 1_000,
        ingest_sequence: 7,
    };
    let store = TursoTapeStore::open_local(&path).await?;

    // When: its decision and pending intent are recorded, then accepted.
    store
        .store_decision(&CausalDecision {
            identity: identity.clone(),
            payload: json!({"kind": "quote"}),
        })
        .await?;
    store
        .store_intent_pending(&identity, &json!({"kind": "place"}))
        .await?;
    store
        .transition_intent(&identity, IntentOutcome::Accepted)
        .await?;

    // Then: duplicate writes and a second terminal transition are rejected.
    assert!(matches!(
        store
            .store_decision(&CausalDecision {
                identity: identity.clone(),
                payload: json!({"kind": "quote"}),
            })
            .await,
        Err(StoreError::DuplicateCausalIdentity)
    ));
    assert!(matches!(
        store.store_intent_pending(&identity, &json!({})).await,
        Err(StoreError::DuplicateCausalIdentity)
    ));
    assert!(matches!(
        store
            .transition_intent(&identity, IntentOutcome::Unknown)
            .await,
        Err(StoreError::PendingIntentNotFound)
    ));
    store.delete_database()?;
    drop(store);
    Ok(())
}
