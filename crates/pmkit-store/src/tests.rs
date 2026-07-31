#![allow(clippy::significant_drop_tightening)]
use std::{num::NonZeroUsize, path::PathBuf};

use chrono::NaiveDate;
use pmkit_core::{PortfolioId, RunId};
use serde_json::json;

use crate::sealed_manifest::decode_sealed_closed_day_manifest_at;
use crate::{
    CacheChecksum, CausalDecision, CausalIdentity, CloudMaterializationState, IntentOutcome,
    OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, ReplayCursor, ReplayGapInterval, ReplayGapReason,
    ReplayItem, StoreError, TapeStore, TursoTapeStore, cloud_materialization_from_inbox,
    decode_sealed_closed_day_manifest, export_market_segments,
    export_market_segments_with_artifacts, export_replay_bundle, reconcile_materialization,
    terminalize_operational_gap,
};

fn database_path(name: &str) -> Result<(tempfile::TempDir, PathBuf), std::io::Error> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join(format!("pmkit-store-{name}.db"));
    Ok((dir, path))
}

fn owner_scope(name: &str) -> Result<OwnerScope, Box<dyn std::error::Error>> {
    Ok(OwnerScope::new(PortfolioId::new(name)?, RunId::new("run")?))
}

fn envelope(scope: OwnerScope, ingest_sequence: i64) -> PmEnvelope {
    PmEnvelope {
        schema_version: PM_ENVELOPE_VERSION,
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

fn sealed_manifest(day: &str) -> Result<crate::SealedClosedDayManifest, StoreError> {
    decode_sealed_closed_day_manifest(json!({
        "version": 2,
        "day": day,
        "day_seal": "sealed",
    }))
}

#[test]
fn sealed_manifest_rejects_current_and_future_utc_days() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a fixed UTC today, independent of the machine clock.
    let today = NaiveDate::from_ymd_opt(2026, 7, 30).ok_or("fixed UTC date")?;
    let tomorrow = today.succ_opt().ok_or("next UTC date")?;

    // When: a manifest claims the current day or any later UTC day is closed.
    for day in [today, tomorrow] {
        let result = decode_sealed_closed_day_manifest_at(
            json!({
                "version": 2,
                "day": day.format("%Y-%m-%d").to_string(),
                "day_seal": "sealed",
            }),
            today,
        );

        // Then: neither claim can produce a materializable sealed manifest.
        assert!(matches!(result, Err(StoreError::Storage { .. })));
    }
    Ok(())
}

#[tokio::test]
async fn lossless_pm_envelope_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a file-backed PM envelope owned by one portfolio/run.
    let (_dir, path) = database_path("round-trip")?;
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

    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn cross_outcome_envelopes_survive_restart_and_limit_one_cursor_replay()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one market's outcomes with identical transport and market identity.
    let (_dir, path) = database_path("cross-outcome-identity")?;
    let scope = owner_scope("paper")?;
    let mut up = envelope(scope.clone(), 1);
    up.normalized = json!({"payload": {"market": "btc-5m", "outcome": "up"}});
    let mut down = envelope(scope.clone(), 2);
    down.normalized = json!({"payload": {"market": "btc-5m", "outcome": "down"}});
    let store = TursoTapeStore::open_local(&path).await?;

    // When: both streams persist, the store restarts, and replay pages one row at a time.
    store.store_envelope(&up).await?;
    store.store_envelope(&down).await?;
    drop(store);
    let store = TursoTapeStore::open_local(&path).await?;
    let first_page = store
        .read_envelopes(&scope, None, NonZeroUsize::MIN)
        .await?;
    let second_page = store
        .read_envelopes(&scope, first_page.next_cursor.clone(), NonZeroUsize::MIN)
        .await?;

    // Then: both outcome streams are reached exactly once through the durable cursor.
    assert_eq!(first_page.items, vec![ReplayItem::Envelope(down)]);
    assert_eq!(second_page.items, vec![ReplayItem::Envelope(up)]);
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn duplicate_or_cross_owner_read_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one envelope and a cursor for its owner scope.
    let (_dir, path) = database_path("ownership")?;
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

    Ok(())
}

#[tokio::test]
async fn corrupt_pm_envelope_is_replay_gap_and_cursor_continues_canonically()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: two canonically ordered PM envelopes in a fresh file-backed database.
    let (_dir, path) = database_path("integrity")?;
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

    Ok(())
}

#[tokio::test]
async fn unknown_frame_is_gap_not_ignored() -> Result<(), Box<dyn std::error::Error>> {
    // Given: an envelope claiming a schema version newer than the reader supports.
    let (_dir, path) = database_path("unknown-frame")?;
    let scope = owner_scope("paper")?;
    let mut unknown = envelope(scope.clone(), 7);
    unknown.schema_version = unknown
        .schema_version
        .checked_add(1)
        .ok_or("schema version overflow")?;
    let store = TursoTapeStore::open_local(&path).await?;
    store.store_envelope(&unknown).await?;

    // When: the owner replays the stored frame.
    let page = store
        .read_envelopes(&scope, None, NonZeroUsize::MIN)
        .await?;

    // Then: the unsupported frame is retained as one typed gap, never dropped.
    assert!(matches!(
        page.items.as_slice(),
        [ReplayItem::Gap(gap)]
            if gap.reason == ReplayGapReason::UnsupportedSchemaVersion
                && gap.ingest_sequence == unknown.ingest_sequence
    ));
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn causal_decision_and_pending_intent_are_idempotent()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one causal identity in a portfolio/run scope.
    let (_dir, path) = database_path("causal")?;
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
    assert!(!path.exists());

    Ok(())
}

#[tokio::test]
async fn pending_unknown_and_accepted_intents_are_enumerated()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: two pending intents and one intent in each recoverable terminal state.
    let (_dir, path) = database_path("intents")?;
    let scope = owner_scope("paper")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let pending_a = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "pending-a".into(),
        source_timestamp_ms: 1_000,
        ingest_sequence: 1,
    };
    let pending_b = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "pending-b".into(),
        source_timestamp_ms: 1_001,
        ingest_sequence: 2,
    };
    let unknown = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "unknown".into(),
        source_timestamp_ms: 1_002,
        ingest_sequence: 3,
    };
    let accepted = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "accepted".into(),
        source_timestamp_ms: 1_003,
        ingest_sequence: 4,
    };
    store
        .store_decision(&CausalDecision {
            identity: pending_a.clone(),
            payload: json!({"a": 1}),
        })
        .await?;
    store
        .store_intent_pending(&pending_a, &json!({"a": 1}))
        .await?;
    store
        .store_decision(&CausalDecision {
            identity: pending_b.clone(),
            payload: json!({"b": 2}),
        })
        .await?;
    store
        .store_intent_pending(&pending_b, &json!({"b": 2}))
        .await?;
    store
        .store_decision(&CausalDecision {
            identity: unknown.clone(),
            payload: json!({"u": 3}),
        })
        .await?;
    store
        .store_intent_pending(&unknown, &json!({"u": 3}))
        .await?;
    store
        .transition_intent_with_order(&unknown, IntentOutcome::Unknown, Some("venue-unknown"))
        .await?;
    store
        .store_intent_pending(&accepted, &json!({"a": 4}))
        .await?;
    store
        .transition_intent_with_order(&accepted, IntentOutcome::Accepted, Some("venue-accepted"))
        .await?;
    // When: pending, unknown, and accepted intents are enumerated.
    let pending = store.read_pending_intents(&scope).await?;
    let unknowns = store.read_unknown_intents(&scope).await?;
    let accepted = store.read_accepted_intents(&scope).await?;

    // Then: pending contains the two still-pending intents; unknown contains the terminal one.
    assert_eq!(pending.len(), 2);
    assert!(
        pending
            .iter()
            .any(|intent| intent.identity.correlation_id == "pending-a")
    );
    assert!(
        pending
            .iter()
            .any(|intent| intent.identity.correlation_id == "pending-b")
    );
    assert!(
        pending
            .iter()
            .all(|intent| intent.identity.correlation_id != "unknown")
    );
    assert_eq!(unknowns.len(), 1);
    assert_eq!(unknowns[0].identity.correlation_id, "unknown");
    assert_eq!(unknowns[0].payload["venue_order_id"], "venue-unknown");
    assert_eq!(accepted.len(), 1);
    assert_eq!(accepted[0].identity.correlation_id, "accepted");
    assert_eq!(accepted[0].payload["venue_order_id"], "venue-accepted");

    store.delete_database()?;
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn rejected_intents_are_enumerated() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one durable intent rejected by the venue.
    let (_dir, path) = database_path("rejected-intents")?;
    let scope = owner_scope("paper")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let rejected = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "rejected".into(),
        source_timestamp_ms: 1_000,
        ingest_sequence: 1,
    };
    store
        .store_intent_pending(&rejected, &json!({"submitted_ms": 1_000}))
        .await?;
    store
        .transition_intent(&rejected, IntentOutcome::Rejected)
        .await?;

    // When: rejected intents are read for restart rate reconstruction.
    let intents = store.read_rejected_intents(&scope).await?;

    // Then: the terminal intent remains available exactly once.
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].identity.correlation_id, "rejected");
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn decisions_are_read_in_canonical_order_and_owner_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: two decisions stored out of order in one owner scope.
    let (_dir, path) = database_path("decisions")?;
    let scope = owner_scope("paper")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let later = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "b".into(),
        source_timestamp_ms: 2_000,
        ingest_sequence: 2,
    };
    let earlier = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "a".into(),
        source_timestamp_ms: 1_000,
        ingest_sequence: 1,
    };
    store
        .store_decision(&CausalDecision {
            identity: later,
            payload: json!({"n": 2}),
        })
        .await?;
    store
        .store_decision(&CausalDecision {
            identity: earlier,
            payload: json!({"n": 1}),
        })
        .await?;

    // When: decisions are read for the owner scope and a foreign scope.
    let decisions = store.read_decisions(&scope).await?;
    let foreign = store.read_decisions(&owner_scope("other")?).await?;

    // Then: they come back in canonical order and never cross owner scopes.
    assert_eq!(decisions.len(), 2);
    assert_eq!(decisions[0].identity.correlation_id, "a");
    assert_eq!(decisions[1].identity.correlation_id, "b");
    assert!(foreign.is_empty());
    store.delete_database()?;
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn portfolio_kill_state_survives_restart() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, path) = database_path("kill-state")?;
    let portfolio = PortfolioId::new("live")?;
    let store = TursoTapeStore::open_local(&path).await?;
    assert!(!store.kill_state(&portfolio).await?);
    store.set_kill_state(&portfolio, true).await?;
    drop(store);

    let store = TursoTapeStore::open_local(&path).await?;
    assert!(store.kill_state(&portfolio).await?);
    store.set_kill_state(&portfolio, false).await?;
    assert!(!store.kill_state(&portfolio).await?);
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn replay_bundle_gathers_manifest_evidence_and_decisions()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, path) = database_path("bundle")?;
    let scope = owner_scope("paper")?;
    let stored = envelope(scope.clone(), 1);
    let identity = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "intent-1".into(),
        source_timestamp_ms: 1_000,
        ingest_sequence: 1,
    };
    let store = TursoTapeStore::open_local(&path).await?;
    store.store_envelope(&stored).await?;
    store
        .store_decision(&CausalDecision {
            identity,
            payload: json!({"kind": "quote"}),
        })
        .await?;

    let manifest = json!({"mode": "backtest", "run": "run"});
    let checksums = [CacheChecksum {
        key: "BTCUSDT-aggTrades-2026-01-01.zip".into(),
        sha256_hex: "abc123".into(),
    }];
    let bundle = export_replay_bundle(&store, &scope, &manifest, &checksums).await?;

    assert_eq!(bundle["schema_version"], 1);
    assert_eq!(bundle["coverage"], "observed");
    assert_eq!(bundle["manifest"], manifest);
    assert_eq!(bundle["scope"]["run"], "run");
    let evidence = bundle["pm_evidence"]
        .as_array()
        .ok_or("pm_evidence array")?;
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0]["source_id"], "market-channel");
    assert_eq!(evidence[0]["normalized"]["price"], "0.42");
    let decisions = bundle["decisions"].as_array().ok_or("decisions array")?;
    assert_eq!(decisions.len(), 1);
    assert_eq!(decisions[0]["correlation_id"], "intent-1");
    let cache = bundle["cache_checksums"].as_array().ok_or("cache array")?;
    assert_eq!(cache.len(), 1);
    assert_eq!(cache[0]["sha256_hex"], "abc123");
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn market_segment_export_is_cloud_compatible() -> Result<(), Box<dyn std::error::Error>> {
    let (_dir, path) = database_path("market-segments")?;
    let scope = owner_scope("run")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let mut envelope = envelope(scope.clone(), 1);
    envelope.normalized = json!({"canonical_market_id": "token-1", "payload": {"price": "0.42"}});
    envelope.source_timestamp_ms = 1_000;
    store.store_envelope(&envelope).await?;

    let bundle = export_market_segments(&store, &scope, &sealed_manifest("1970-01-01")?).await?;

    assert_eq!(bundle["schema_version"], 1);
    assert_eq!(bundle["segments"][0]["market_id"], "token-1");
    assert!(bundle["segments"][0]["token_id"].is_null());
    assert_eq!(bundle["segments"][0]["from_ts_ms"], 1_000);
    assert_eq!(bundle["segments"][0]["to_ts_ms"], 1_000);
    assert_eq!(bundle["segments"][0]["rows"], 1);
    assert!(bundle["segments"][0]["sha256"].as_str().is_some());
    assert!(
        bundle["segments"][0]["source_manifest_sha256"]
            .as_str()
            .is_some()
    );
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn market_segment_export_returns_uploadable_utc_day_bodies()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, path) = database_path("market-segment-bodies")?;
    let scope = owner_scope("run")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let mut first = envelope(scope.clone(), 1);
    first.normalized = json!({"canonical_market_id": "token-1", "payload": {"price": "0.42"}});
    first.source_timestamp_ms = 86_400_000;
    let mut second = envelope(scope.clone(), 2);
    second.normalized = first.normalized.clone();
    second.source_timestamp_ms = 86_400_001;
    store.store_envelope(&first).await?;
    store.store_envelope(&second).await?;

    let output =
        export_market_segments_with_artifacts(&store, &scope, &sealed_manifest("1970-01-02")?)
            .await?;

    assert_eq!(output.segments.len(), 1);
    let segment = &output.segments[0];
    assert_eq!(segment.declaration["from_ts_ms"], 86_400_000);
    assert_eq!(segment.declaration["to_ts_ms"], 86_400_001);
    assert_eq!(segment.declaration["rows"], 2);
    assert_eq!(segment.declaration["bytes"], segment.bytes.len());
    assert_eq!(output.manifest["segments"][0], segment.declaration);
    assert_eq!(segment.bytes.split(|byte| *byte == b'\n').count() - 1, 2);
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn market_segment_export_rejects_a_record_outside_the_sealed_day()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a public-market record immediately after the sealed UTC day.
    let (_dir, path) = database_path("segment-outside-sealed-day")?;
    let scope = owner_scope("run")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let mut stored = envelope(scope.clone(), 1);
    stored.normalized = json!({"canonical_market_id": "token-1", "payload": {"price": "0.42"}});
    stored.source_timestamp_ms = 86_400_000;
    store.store_envelope(&stored).await?;

    // When: the prior day is materialized for Cloud publication.
    let result = export_market_segments(&store, &scope, &sealed_manifest("1970-01-01")?).await;

    // Then: future/out-of-day evidence fails closed instead of being omitted.
    assert!(matches!(result, Err(StoreError::Storage { .. })));
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn market_segment_export_rejects_intersecting_persisted_replay_gap()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a materializable market record and a recorder gap in its interval.
    let (_dir, path) = database_path("segment-gap")?;
    let scope = owner_scope("run")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let mut stored = envelope(scope.clone(), 1);
    stored.normalized = json!({"canonical_market_id": "token-1", "payload": {"price": "0.42"}});
    stored.source_timestamp_ms = 1_000;
    store.store_envelope(&stored).await?;
    store
        .store_replay_gap(&ReplayGapInterval {
            scope: scope.clone(),
            partition: "token-1:0".into(),
            start_time_ms: 999,
            end_time_ms: Some(1_001),
            reason: "malformed_data".into(),
        })
        .await?;

    // When: the market segment is materialized.
    let result = export_market_segments(&store, &scope, &sealed_manifest("1970-01-01")?).await;

    // Then: a gap intersection prevents export rather than publishing partial evidence.
    assert!(matches!(result, Err(StoreError::Storage { .. })));
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn market_segment_export_withholds_the_full_day_for_a_late_partition_gap()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: the only observed row is early in the day, but the same partition later has a gap.
    let (_dir, path) = database_path("segment-late-gap")?;
    let scope = owner_scope("run")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let mut stored = envelope(scope.clone(), 1);
    stored.normalized = json!({"canonical_market_id": "token-1", "payload": {"price": "0.42"}});
    stored.source_timestamp_ms = 1_000;
    store.store_envelope(&stored).await?;
    store
        .store_replay_gap(&ReplayGapInterval {
            scope: scope.clone(),
            partition: "token-1:0".into(),
            start_time_ms: 86_399_000,
            end_time_ms: Some(86_399_999),
            reason: "late_disconnect".into(),
        })
        .await?;

    // When: the market/day partition is materialized.
    let result = export_market_segments(&store, &scope, &sealed_manifest("1970-01-01")?).await;

    // Then: it is withheld rather than publishing the early-day partial rows.
    assert!(matches!(result, Err(StoreError::Storage { .. })));
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn market_segment_export_ignores_overlapping_gap_in_another_partition()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a materializable market record and a same-time gap for another market/day.
    let (_dir, path) = database_path("segment-unrelated-gap")?;
    let scope = owner_scope("run")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let mut stored = envelope(scope.clone(), 1);
    stored.normalized = json!({"canonical_market_id": "token-1", "payload": {"price": "0.42"}});
    stored.source_timestamp_ms = 1_000;
    store.store_envelope(&stored).await?;
    store
        .store_replay_gap(&ReplayGapInterval {
            scope: scope.clone(),
            partition: "token-2:0".into(),
            start_time_ms: 999,
            end_time_ms: Some(1_001),
            reason: "malformed_data".into(),
        })
        .await?;

    // When: the first market/day segment is materialized.
    let result = export_market_segments(&store, &scope, &sealed_manifest("1970-01-01")?).await;

    // Then: unrelated partitions do not withhold valid evidence.
    assert!(result.is_ok());
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn cloud_materialization_reconciles_a_lost_finalize_response()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, path) = database_path("cloud-materialization")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let inbox = json!({"version": 2, "day": "1970-01-02", "day_seal": "sealed"});
    let materialization =
        cloud_materialization_from_inbox(&inbox, "token-1:86400000", &"a".repeat(64))?;

    let pending = reconcile_materialization(&store, &materialization, |_| async {
        Err(StoreError::Storage {
            message: "lost_finalize_response".into(),
        })
    })
    .await?;
    let finalized = reconcile_materialization(&store, &materialization, |_| async {
        Ok("release-2".into())
    })
    .await?;

    assert_eq!(pending.state, CloudMaterializationState::Pending);
    assert_eq!(finalized.state, CloudMaterializationState::Finalized);
    assert_eq!(finalized.release_id.as_deref(), Some("release-2"));
    store.delete_database()?;
    Ok(())
}

#[test]
fn cloud_materialization_rejects_an_unsealed_closed_day_manifest() {
    // Given: JSON can name a day without certifying that the day is closed.
    let inbox = json!({"version": 2, "day": "1970-01-02"});

    // When: it is offered to the Cloud materialization boundary.
    let result = cloud_materialization_from_inbox(&inbox, "token-1:86400000", &"a".repeat(64));

    // Then: unsealed JSON cannot create a publishable identity.
    assert!(matches!(result, Err(StoreError::Storage { .. })));
}

#[tokio::test]
async fn cloud_materialization_terminalizes_a_gapped_day_without_release()
-> Result<(), Box<dyn std::error::Error>> {
    let (_dir, path) = database_path("cloud-materialization-gap")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let materialization = cloud_materialization_from_inbox(
        &json!({"version": 2, "day": "1970-01-02", "day_seal": "sealed"}),
        "token-1:86400000",
        &"b".repeat(64),
    )?;

    let terminal = terminalize_operational_gap(&store, &materialization).await?;

    assert_eq!(terminal.state, CloudMaterializationState::Terminal);
    assert_eq!(terminal.release_id, None);
    assert_eq!(terminal.terminal_reason.as_deref(), Some("operational_gap"));
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn replay_bundle_fails_closed_on_corrupt_evidence() -> Result<(), Box<dyn std::error::Error>>
{
    let (_dir, path) = database_path("bundle-corrupt")?;
    let scope = owner_scope("paper")?;
    let corrupt = envelope(scope.clone(), 1);
    let store = TursoTapeStore::open_local(&path).await?;
    store.store_envelope(&corrupt).await?;
    drop(store);

    // Corrupt the stored raw-frame digest outside the store.
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
    let manifest = json!({"mode": "backtest"});
    let result = export_replay_bundle(&store, &scope, &manifest, &[]).await;
    assert!(matches!(result, Err(StoreError::Storage { .. })));
    store.delete_database()?;
    Ok(())
}
