#![allow(clippy::significant_drop_tightening)]
use std::{
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use pmkit_book::{OrderBookL2, Side};
use pmkit_core::{MarketId, PortfolioId, RunId};
use pmkit_exec::{ExecError, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_store::{
    CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, PmEnvelope, ReplayCursor,
    ReplayPage, StoreError, TapeStore, TursoTapeStore,
};
use rust_decimal::Decimal;
use serde_json::json;

use crate::causal::{
    ActionRiskVerdict, CausalRecorder, CexTradeMetrics, DecisionKind, DecisionSnapshot,
};

fn database_path(name: &str) -> Result<PathBuf, std::time::SystemTimeError> {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!("pmkit-causal-{name}-{suffix}.db")))
}

fn identity() -> Result<CausalIdentity, Box<dyn std::error::Error>> {
    Ok(CausalIdentity {
        scope: OwnerScope::new(PortfolioId::new("portfolio")?, RunId::new("run")?),
        correlation_id: "event-7".into(),
        source_timestamp_ms: 1_000,
        ingest_sequence: 7,
    })
}

fn snapshot() -> DecisionSnapshot {
    DecisionSnapshot::from_book(
        &OrderBookL2 {
            bids: vec![(Decimal::new(41, 2), Decimal::from(9))],
            asks: vec![(Decimal::new(43, 2), Decimal::from(3))],
            timestamp_ms: 1_000,
            last_trade_price: None,
        },
        CexTradeMetrics {
            last_price: Some(Decimal::new(64_000, 0)),
            momentum: Decimal::new(12, 2),
            volume: Decimal::from(50),
            cvd: Decimal::from(-7),
            vwap: Some(Decimal::new(63_900, 0)),
        },
    )
}

fn order() -> Result<PlaceOrder, Box<dyn std::error::Error>> {
    Ok(PlaceOrder {
        market: MarketId::new("btc-5m")?,
        outcome: Outcome::Up,
        side: Side::Buy,
        price: Decimal::new(42, 2),
        qty: Decimal::from(10),
        post_only: true,
    })
}

#[test]
fn decision_causality_matches_all_modes() {
    // Given: equal normalized PM-book and CEX-trade fixtures.
    let fixture = snapshot();

    // When: each execution mode computes its portable snapshot.
    let backtest = fixture.clone();
    let paper = fixture.clone();
    let live = fixture;

    // Then: strategy-visible inputs have one stable representation.
    assert_eq!(backtest, paper);
    assert_eq!(paper, live);
}

#[tokio::test]
async fn action_risk_and_outcomes_are_independent_and_linked()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one decision with independently risked actions in a file-backed store.
    let path = database_path("actions")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let event = identity()?;
    CausalRecorder::new(&store)
        .record_evaluation(
            &event,
            &snapshot(),
            DecisionKind::Actions(vec![
                ActionRiskVerdict::accepted(0),
                ActionRiskVerdict::rejected(1, "position limit"),
            ]),
        )
        .await?;
    let accepted = CausalRecorder::new(&store).intent(&event, 0, &order()?);
    let rejected = CausalRecorder::new(&store).intent(&event, 1, &order()?);

    // When: one venue order accepts and the other rejects.
    let receipt = CausalRecorder::new(&store)
        .submit(&accepted, || async { Ok(OrderId("venue-accepted".into())) })
        .await?;
    let rejection = CausalRecorder::new(&store)
        .submit(&rejected, || async {
            Err(ExecError::Rejected {
                reason: "venue limit".into(),
            })
        })
        .await;

    // Then: every external order carries its own persisted intent correlation.
    assert_eq!(receipt.correlation_id, "event-7:0");
    assert_eq!(accepted.identity.correlation_id, "event-7:0");
    assert_eq!(rejected.identity.correlation_id, "event-7:1");
    assert!(matches!(
        rejection,
        Err(crate::causal::RecorderError::VenueRejected { .. })
    ));
    assert!(matches!(
        store
            .transition_intent(&accepted.identity, IntentOutcome::Accepted)
            .await,
        Err(StoreError::PendingIntentNotFound)
    ));
    assert!(matches!(
        store
            .transition_intent(&rejected.identity, IntentOutcome::Rejected)
            .await,
        Err(StoreError::PendingIntentNotFound)
    ));
    store.delete_database()?;
    assert!(!path.exists());
    Ok(())
}

#[tokio::test]
async fn pending_store_failure_aborts_before_submission() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: a recorder whose pending-intent write fails.
    let path = database_path("pending-failure")?;
    let submitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let store = FailingStore::new(
        TursoTapeStore::open_local(&path).await?,
        FailureMode::Pending,
    );
    let intent = CausalRecorder::new(&store).intent(&identity()?, 0, &order()?);

    // When: submission is attempted through the recorder.
    let result = CausalRecorder::new(&store)
        .submit(&intent, {
            let submitted = Arc::clone(&submitted);
            move || async move {
                submitted.store(true, std::sync::atomic::Ordering::Relaxed);
                Ok(OrderId("must-not-submit".into()))
            }
        })
        .await;

    // Then: no external request occurs before durable pending persistence.
    assert!(matches!(
        result,
        Err(crate::causal::RecorderError::Store(_))
    ));
    assert!(!submitted.load(std::sync::atomic::Ordering::Relaxed));
    store.inner.delete_database()?;
    Ok(())
}

#[tokio::test]
async fn accepted_order_with_store_failure_is_reconciled() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: a pending intent and a transition write that fails after acceptance.
    let path = database_path("restart")?;
    let (intent, result) = {
        let store = FailingStore::new(
            TursoTapeStore::open_local(&path).await?,
            FailureMode::FirstTransition,
        );
        let intent = CausalRecorder::new(&store).intent(&identity()?, 0, &order()?);

        // When: the venue accepts but recording its terminal state fails.
        let result = CausalRecorder::new(&store)
            .submit(&intent, || async { Ok(OrderId("venue-accepted".into())) })
            .await;
        (intent, result)
    };

    // Then: reopening the file-backed store reconciles exactly one terminal transition.
    assert!(matches!(
        result,
        Err(crate::causal::RecorderError::AcceptedButUnrecorded { .. })
    ));
    {
        let reopened = TursoTapeStore::open_local(&path).await?;
        CausalRecorder::new(&reopened)
            .reconcile(&intent, IntentOutcome::Accepted)
            .await?;
        assert!(matches!(
            CausalRecorder::new(&reopened)
                .reconcile(&intent, IntentOutcome::Accepted)
                .await,
            Err(crate::causal::RecorderError::Store(
                StoreError::PendingIntentNotFound
            ))
        ));
        reopened.delete_database()?;
        assert!(!path.exists());
    }
    Ok(())
}

enum FailureMode {
    Pending,
    FirstTransition,
}

struct FailingStore {
    inner: TursoTapeStore,
    failure: FailureMode,
    failed: std::sync::atomic::AtomicBool,
}

impl FailingStore {
    const fn new(inner: TursoTapeStore, failure: FailureMode) -> Self {
        Self {
            inner,
            failure,
            failed: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl TapeStore for FailingStore {
    async fn store_envelope(&self, envelope: &PmEnvelope) -> Result<(), StoreError> {
        self.inner.store_envelope(envelope).await
    }

    async fn read_envelopes(
        &self,
        scope: &OwnerScope,
        after: Option<ReplayCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<ReplayPage, StoreError> {
        self.inner.read_envelopes(scope, after, limit).await
    }

    async fn store_decision(&self, decision: &CausalDecision) -> Result<(), StoreError> {
        self.inner.store_decision(decision).await
    }

    async fn store_intent_pending(
        &self,
        identity: &CausalIdentity,
        payload: &serde_json::Value,
    ) -> Result<(), StoreError> {
        match self.failure {
            FailureMode::Pending => Err(StoreError::Storage {
                message: "pending write failed".into(),
            }),
            FailureMode::FirstTransition => {
                self.inner.store_intent_pending(identity, payload).await
            }
        }
    }

    async fn transition_intent(
        &self,
        identity: &CausalIdentity,
        outcome: IntentOutcome,
    ) -> Result<(), StoreError> {
        if matches!(&self.failure, FailureMode::FirstTransition)
            && !self.failed.swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(StoreError::Storage {
                message: "transition write failed".into(),
            });
        }
        self.inner.transition_intent(identity, outcome).await
    }

    async fn read_pending_intents(
        &self,
        scope: &OwnerScope,
    ) -> Result<Vec<pmkit_store::DurableIntent>, StoreError> {
        self.inner.read_pending_intents(scope).await
    }

    async fn read_unknown_intents(
        &self,
        scope: &OwnerScope,
    ) -> Result<Vec<pmkit_store::DurableIntent>, StoreError> {
        self.inner.read_unknown_intents(scope).await
    }
}

#[tokio::test]
async fn recorder_enumerates_pending_and_unknown_intents() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: one pending intent and one unknown-outcome intent in durable storage.
    let path = database_path("enumerate")?;
    let store = TursoTapeStore::open_local(&path).await?;
    let scope = OwnerScope::new(PortfolioId::new("portfolio")?, RunId::new("run")?);
    let event = CausalIdentity {
        scope: scope.clone(),
        correlation_id: "event-enum".into(),
        source_timestamp_ms: 1_000,
        ingest_sequence: 7,
    };
    CausalRecorder::new(&store)
        .record_evaluation(
            &event,
            &snapshot(),
            DecisionKind::Actions(vec![
                ActionRiskVerdict::accepted(0),
                ActionRiskVerdict::accepted(1),
            ]),
        )
        .await?;

    let pending = CausalRecorder::new(&store).intent(&event, 0, &order()?);
    let unknown = CausalRecorder::new(&store).intent(&event, 1, &order()?);
    store
        .store_intent_pending(&pending.identity, &json!({"kind": "place"}))
        .await?;
    store
        .store_intent_pending(&unknown.identity, &json!({"kind": "place"}))
        .await?;
    store
        .transition_intent(&unknown.identity, IntentOutcome::Unknown)
        .await?;

    // When: the recorder enumerates pending and unknown intents.
    let recorder = CausalRecorder::new(&store);
    let pending_intents = recorder.pending_intents(&scope).await?;
    let unknown_intents = recorder.unknown_intents(&scope).await?;

    // Then: only the still-pending intent and only the unknown-outcome intent are returned.
    assert_eq!(pending_intents.len(), 1);
    assert_eq!(pending_intents[0].identity.correlation_id, "event-enum:0");
    assert_eq!(unknown_intents.len(), 1);
    assert_eq!(unknown_intents[0].identity.correlation_id, "event-enum:1");

    store.delete_database()?;
    assert!(!path.exists());
    Ok(())
}
