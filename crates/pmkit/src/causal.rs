//! Portable decision snapshots and durable execution recording.

use std::future::Future;

use pmkit_book::OrderBookL2;
use pmkit_exec::{ExecError, OrderId, PlaceOrder};
use pmkit_store::{
    CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, StoreError, TapeStore,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use thiserror::Error;

/// The CEX-derived trade metrics permitted in a portable decision snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CexTradeMetrics {
    /// The most recent CEX trade price, when one is available.
    pub last_price: Option<Decimal>,
    /// The CEX trade-price momentum.
    pub momentum: Decimal,
    /// The CEX trade volume.
    pub volume: Decimal,
    /// The cumulative volume delta.
    pub cvd: Decimal,
    /// The volume-weighted average trade price, when one is available.
    pub vwap: Option<Decimal>,
}

/// The PM-book fields permitted in a portable decision snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PmBookSnapshot {
    /// The PM best bid price, when the book has bids.
    pub best_bid: Option<Decimal>,
    /// The PM best ask price, when the book has asks.
    pub best_ask: Option<Decimal>,
    /// The PM best-bid/best-ask midpoint, when both sides exist.
    pub mid: Option<Decimal>,
    /// The PM top-of-book volume imbalance.
    pub imbalance: Decimal,
}

/// The complete strategy-visible snapshot for one deterministic merged event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionSnapshot {
    /// The permitted PM-book values.
    pub pm_book: PmBookSnapshot,
    /// The permitted CEX-trade values.
    pub cex_trade: CexTradeMetrics,
}

impl DecisionSnapshot {
    /// Builds a snapshot from the PM order book and normalized CEX-trade metrics.
    #[must_use]
    pub fn from_book(book: &OrderBookL2, cex_trade: CexTradeMetrics) -> Self {
        Self {
            pm_book: PmBookSnapshot {
                best_bid: book.best_bid().map(|(price, _)| price),
                best_ask: book.best_ask().map(|(price, _)| price),
                mid: book.mid_price(),
                imbalance: book.obi(),
            },
            cex_trade,
        }
    }
}

/// The risk verdict for one strategy action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskVerdict {
    /// The action passed the risk gate.
    Accepted,
    /// The action did not reach the venue because risk rejected it.
    Rejected {
        /// The stable reason supplied by the risk gate.
        reason: String,
    },
}

/// A risk verdict linked to the action's index in its strategy evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionRiskVerdict {
    /// The zero-based action index returned by the strategy.
    pub action_index: u32,
    /// The decision made by the risk gate.
    pub verdict: RiskVerdict,
}

impl ActionRiskVerdict {
    /// Creates an accepted risk verdict for one action.
    #[must_use]
    pub const fn accepted(action_index: u32) -> Self {
        Self {
            action_index,
            verdict: RiskVerdict::Accepted,
        }
    }

    /// Creates a rejected risk verdict for one action.
    #[must_use]
    pub fn rejected(action_index: u32, reason: impl Into<String>) -> Self {
        Self {
            action_index,
            verdict: RiskVerdict::Rejected {
                reason: reason.into(),
            },
        }
    }
}

/// The strategy result persisted with an event snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecisionKind {
    /// The strategy produced no actions.
    NoAction,
    /// The strategy could not evaluate the event.
    StrategyError {
        /// The error detail returned by the strategy.
        message: String,
    },
    /// The strategy produced actions and each was evaluated independently.
    Actions(Vec<ActionRiskVerdict>),
}

/// A pre-submission order intent with its durable action correlation identity.
#[derive(Debug, Clone)]
pub struct OrderIntent {
    /// The durable identity that links the external order to its decision.
    pub identity: CausalIdentity,
    payload: Value,
}

impl OrderIntent {
    /// Reconstructs an intent from its durable identity and payload.
    #[must_use]
    pub const fn from_parts(identity: CausalIdentity, payload: Value) -> Self {
        Self { identity, payload }
    }
}

/// The result of an accepted venue submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionReceipt {
    /// The venue-assigned external order identifier.
    pub order_id: OrderId,
    /// The persisted intent correlation identifier.
    pub correlation_id: String,
}

/// A recorder failure that preserves the distinction between venue and store state.
#[derive(Debug, Error)]
pub enum RecorderError {
    /// Durable recording failed before or during a non-accepted outcome.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The venue rejected an order after its pending intent was persisted.
    #[error("venue rejected order: {source}")]
    VenueRejected {
        /// The venue rejection detail.
        #[source]
        source: ExecError,
    },
    /// The venue outcome is unknown after a transport error.
    #[error("venue outcome is unknown: {source}")]
    VenueUnknown {
        /// The transport failure detail.
        #[source]
        source: ExecError,
    },
    /// The venue accepted an order but the terminal store transition failed.
    #[error("venue accepted order but outcome was not persisted: {source}")]
    AcceptedButUnrecorded {
        /// The failed terminal transition.
        #[source]
        source: StoreError,
    },
}

/// Records portable strategy decisions and durable execution intent transitions.
#[derive(Debug)]
pub struct CausalRecorder<'a, S: TapeStore + ?Sized> {
    store: &'a S,
}

impl<'a, S: TapeStore + ?Sized> CausalRecorder<'a, S> {
    /// Creates a recorder backed by the configured durable store.
    #[must_use]
    pub const fn new(store: &'a S) -> Self {
        Self { store }
    }

    /// Persists the one decision made for a deterministic merged event.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the decision cannot be persisted.
    pub async fn record_evaluation(
        &self,
        identity: &CausalIdentity,
        snapshot: &DecisionSnapshot,
        decision: DecisionKind,
    ) -> Result<(), StoreError> {
        self.store
            .store_decision(&CausalDecision {
                identity: identity.clone(),
                payload: decision_payload(snapshot, decision),
            })
            .await?;
        Ok(())
    }

    /// Derives one durable order intent identity from a decision correlation and action index.
    #[must_use]
    pub fn intent(
        &self,
        decision: &CausalIdentity,
        action_index: u32,
        order: &PlaceOrder,
    ) -> OrderIntent {
        OrderIntent {
            identity: CausalIdentity {
                scope: decision.scope.clone(),
                correlation_id: format!("{}:{action_index}", decision.correlation_id),
                source_timestamp_ms: decision.source_timestamp_ms,
                ingest_sequence: decision.ingest_sequence,
            },
            payload: order_payload(action_index, order),
        }
    }

    /// Persists pending intent before calling the venue, then records its terminal outcome.
    ///
    /// # Errors
    ///
    /// Returns [`RecorderError::Store`] without calling `submit` when pending persistence fails.
    /// Returns [`RecorderError::AcceptedButUnrecorded`] when reconciliation must resume after an
    /// accepted venue order cannot transition in storage.
    pub async fn submit<F, Fut>(
        &self,
        intent: &OrderIntent,
        submit: F,
    ) -> Result<SubmissionReceipt, RecorderError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<OrderId, ExecError>>,
    {
        self.store
            .store_intent_pending(&intent.identity, &intent.payload)
            .await?;
        match submit().await {
            Ok(order_id) => {
                self.store
                    .transition_intent(&intent.identity, IntentOutcome::Accepted)
                    .await
                    .map_err(|source| RecorderError::AcceptedButUnrecorded { source })?;
                Ok(SubmissionReceipt {
                    order_id,
                    correlation_id: intent.identity.correlation_id.clone(),
                })
            }
            Err(source @ (ExecError::Rejected { .. } | ExecError::NotFound { .. })) => {
                self.store
                    .transition_intent(&intent.identity, IntentOutcome::Rejected)
                    .await?;
                Err(RecorderError::VenueRejected { source })
            }
            Err(source @ ExecError::Transport { .. }) => {
                Err(RecorderError::VenueUnknown { source })
            }
        }
    }

    /// Applies the terminal venue result to a durable pending intent after restart.
    ///
    /// # Errors
    ///
    /// Returns [`RecorderError::Store`] when no pending intent remains or the transition fails.
    pub async fn reconcile(
        &self,
        intent: &OrderIntent,
        outcome: IntentOutcome,
    ) -> Result<(), RecorderError> {
        self.store
            .transition_intent(&intent.identity, outcome)
            .await?;
        Ok(())
    }

    /// Lists durable intents still pending for one owner scope.
    ///
    /// # Errors
    ///
    /// Returns [`RecorderError::Store`] when the durable query fails.
    pub async fn pending_intents(
        &self,
        scope: &OwnerScope,
    ) -> Result<Vec<OrderIntent>, RecorderError> {
        self.store
            .read_pending_intents(scope)
            .await
            .map(|intents| {
                intents
                    .into_iter()
                    .map(|intent| OrderIntent::from_parts(intent.identity, intent.payload))
                    .collect()
            })
            .map_err(RecorderError::Store)
    }

    /// Lists durable intents whose terminal outcome is unknown for one owner scope.
    ///
    /// # Errors
    ///
    /// Returns [`RecorderError::Store`] when the durable query fails.
    pub async fn unknown_intents(
        &self,
        scope: &OwnerScope,
    ) -> Result<Vec<OrderIntent>, RecorderError> {
        self.store
            .read_unknown_intents(scope)
            .await
            .map(|intents| {
                intents
                    .into_iter()
                    .map(|intent| OrderIntent::from_parts(intent.identity, intent.payload))
                    .collect()
            })
            .map_err(RecorderError::Store)
    }
}

fn decision_payload(snapshot: &DecisionSnapshot, decision: DecisionKind) -> Value {
    json!({
        "snapshot": snapshot_payload(snapshot),
        "decision": match decision {
            DecisionKind::NoAction => json!({"kind": "no_action"}),
            DecisionKind::StrategyError { message } => json!({"kind": "strategy_error", "message": message}),
            DecisionKind::Actions(actions) => json!({
                "kind": "actions",
                "risk": actions.into_iter().map(risk_payload).collect::<Vec<_>>(),
            }),
        },
    })
}

fn snapshot_payload(snapshot: &DecisionSnapshot) -> Value {
    json!({
        "pm_book": {
            "best_bid": decimal_option(snapshot.pm_book.best_bid),
            "best_ask": decimal_option(snapshot.pm_book.best_ask),
            "mid": decimal_option(snapshot.pm_book.mid),
            "imbalance": snapshot.pm_book.imbalance.to_string(),
        },
        "cex_trade": {
            "last_price": decimal_option(snapshot.cex_trade.last_price),
            "momentum": snapshot.cex_trade.momentum.to_string(),
            "volume": snapshot.cex_trade.volume.to_string(),
            "cvd": snapshot.cex_trade.cvd.to_string(),
            "vwap": decimal_option(snapshot.cex_trade.vwap),
        },
    })
}

fn risk_payload(verdict: ActionRiskVerdict) -> Value {
    let ActionRiskVerdict {
        action_index,
        verdict,
    } = verdict;
    json!({
        "action_index": action_index,
        "verdict": match verdict {
            RiskVerdict::Accepted => json!({"kind": "accepted"}),
            RiskVerdict::Rejected { reason } => json!({"kind": "rejected", "reason": reason}),
        },
    })
}

fn order_payload(action_index: u32, order: &PlaceOrder) -> Value {
    json!({
        "action_index": action_index,
        "order": {
            "market": order.market.to_string(),
            "outcome": format!("{:?}", order.outcome),
            "side": match order.side {
                pmkit_book::Side::Buy => "buy",
                pmkit_book::Side::Sell => "sell",
            },
            "price": order.price.to_string(),
            "qty": order.qty.to_string(),
            "post_only": order.post_only,
        },
    })
}

fn decimal_option(value: Option<Decimal>) -> Option<String> {
    value.map(|decimal| decimal.to_string())
}
