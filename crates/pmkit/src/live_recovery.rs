use super::{
    LiveRiskState, Reservation, StartError, correlation_strategy, query_order_status,
    risk_storage_error,
};
use pmkit_event::{FillIdentity, Liquidity, PmAccountEvent};
use pmkit_exec::{ExecError, OrderId, OrderStatus, OrderStatusDetails};
use pmkit_market::Outcome;
use pmkit_runtime::{RuntimeConfig, StrategyRegistration};
use pmkit_spec::LiveRun;
use pmkit_store::{DurableIntent, OwnerScope, StoreError, TapeStore};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

fn corrupt_rate_history(message: impl Into<String>) -> StoreError {
    StoreError::Storage {
        message: format!("invalid durable order-rate history: {}", message.into()),
    }
}

pub(super) async fn accepted_submissions(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    registrations: &[StrategyRegistration],
) -> Result<Vec<(Option<pmkit_core::StrategyId>, i64)>, StoreError> {
    let mut submissions = Vec::new();
    let mut accepted_intent_ids = HashSet::new();
    for decision in store.read_decisions(scope).await? {
        let decision_kind = decision.payload["decision"]["kind"]
            .as_str()
            .ok_or_else(|| corrupt_rate_history("decision kind is missing"))?;
        let risk = match decision_kind {
            "no_action" | "strategy_error" => continue,
            "actions" => decision.payload["decision"]["risk"]
                .as_array()
                .ok_or_else(|| corrupt_rate_history("action risk list is missing"))?,
            other => {
                return Err(corrupt_rate_history(format!(
                    "unsupported decision kind {other}"
                )));
            }
        };
        let timestamp_ms = decision.payload["snapshot"]["timing"]["decision_ms"]
            .as_i64()
            .ok_or_else(|| corrupt_rate_history("logical decision timestamp is missing"))?;
        let strategy = correlation_strategy(&decision.identity.correlation_id, registrations);
        for verdict in risk {
            let action_index = verdict["action_index"]
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| corrupt_rate_history("action index is invalid"))?;
            match verdict["verdict"]["kind"]
                .as_str()
                .ok_or_else(|| corrupt_rate_history("risk verdict kind is missing"))?
            {
                "accepted" => {
                    accepted_intent_ids.insert(format!(
                        "{}:{action_index}",
                        decision.identity.correlation_id
                    ));
                    submissions.push((strategy.clone(), timestamp_ms));
                }
                "rejected" => {}
                other => {
                    return Err(corrupt_rate_history(format!(
                        "unsupported risk verdict {other}"
                    )));
                }
            }
        }
    }

    let mut intents = store.read_pending_intents(scope).await?;
    intents.extend(store.read_unknown_intents(scope).await?);
    intents.extend(store.read_accepted_intents(scope).await?);
    for intent in intents {
        if accepted_intent_ids.contains(&intent.identity.correlation_id) {
            continue;
        }
        let timestamp_ms = intent.payload["submitted_ms"]
            .as_i64()
            .ok_or_else(|| corrupt_rate_history("intent logical timestamp is missing"))?;
        submissions.push((
            correlation_strategy(&intent.identity.correlation_id, registrations),
            timestamp_ms,
        ));
    }
    Ok(submissions)
}

pub(super) fn corrupt_order(message: impl Into<String>) -> StoreError {
    StoreError::Storage {
        message: format!("invalid durable order recovery: {}", message.into()),
    }
}

#[derive(Deserialize)]
struct IntentPayload {
    action_index: u32,
    submitted_ms: i64,
    order: IntentOrder,
    venue_order_id: String,
}

#[derive(Deserialize)]
struct IntentOrder {
    market: String,
    outcome: String,
    side: String,
    price: Decimal,
    qty: Decimal,
    post_only: bool,
}

pub(super) struct DurableOrder {
    pub(super) order_id: OrderId,
    pub(super) strategy: pmkit_core::StrategyId,
    pub(super) market: pmkit_core::MarketId,
    pub(super) outcome: Outcome,
    pub(super) side: pmkit_book::Side,
    pub(super) price: Decimal,
    pub(super) qty: Decimal,
    pub(super) post_only: bool,
    pub(super) submitted_ms: i64,
}

fn durable_order(
    intent: DurableIntent,
    registrations: &[StrategyRegistration],
) -> Result<DurableOrder, StoreError> {
    let payload: IntentPayload = serde_json::from_value(intent.payload)
        .map_err(|error| corrupt_order(format!("intent payload is invalid: {error}")))?;
    if payload.venue_order_id.is_empty() {
        return Err(corrupt_order("venue order id is missing"));
    }
    let (decision_correlation, action_index) = intent
        .identity
        .correlation_id
        .rsplit_once(':')
        .ok_or_else(|| corrupt_order("intent correlation has no action index"))?;
    if action_index.parse::<u32>().ok() != Some(payload.action_index) {
        return Err(corrupt_order(
            "intent correlation and payload action index differ",
        ));
    }
    let strategy = correlation_strategy(decision_correlation, registrations)
        .ok_or_else(|| corrupt_order("intent strategy is missing or ambiguous"))?;
    let market = pmkit_core::MarketId::new(payload.order.market)
        .map_err(|error| corrupt_order(error.to_string()))?;
    if registrations
        .iter()
        .find(|registration| registration.id() == &strategy)
        .is_none_or(|registration| registration.market() != &market)
    {
        return Err(corrupt_order(
            "intent market does not match its strategy registration",
        ));
    }
    if payload.order.price.is_sign_negative() || payload.order.qty <= Decimal::ZERO {
        return Err(corrupt_order(
            "order price or quantity is outside its valid range",
        ));
    }
    Ok(DurableOrder {
        order_id: OrderId(payload.venue_order_id),
        strategy,
        market,
        outcome: match payload.order.outcome.as_str() {
            "Up" => Outcome::Up,
            "Down" => Outcome::Down,
            outcome => {
                return Err(corrupt_order(format!(
                    "unsupported order outcome {outcome}"
                )));
            }
        },
        side: match payload.order.side.as_str() {
            "buy" => pmkit_book::Side::Buy,
            "sell" => pmkit_book::Side::Sell,
            side => return Err(corrupt_order(format!("unsupported order side {side}"))),
        },
        price: payload.order.price,
        qty: payload.order.qty,
        post_only: payload.order.post_only,
        submitted_ms: payload.submitted_ms,
    })
}

pub(super) fn apply_status_fill(
    order: &DurableOrder,
    details: &OrderStatusDetails,
    risk_state: &mut LiveRiskState,
    limits: &pmkit_runtime::RiskLimits,
) -> Result<Decimal, StoreError> {
    let filled_qty = details
        .filled_qty
        .ok_or_else(|| corrupt_order("venue filled quantity is missing"))?;
    if filled_qty.is_sign_negative() || filled_qty > order.qty {
        return Err(corrupt_order(
            "venue filled quantity is outside the durable order quantity",
        ));
    }
    let durable_qty = risk_state.filled_qty(&order.order_id.0);
    if durable_qty > filled_qty {
        return Err(corrupt_order(
            "venue filled quantity regressed behind durable fills",
        ));
    }
    let missing_qty = filled_qty - durable_qty;
    if missing_qty.is_zero() {
        return Ok(filled_qty);
    }
    let price = details
        .price
        .filter(|price| !price.is_sign_negative())
        .ok_or_else(|| corrupt_order("venue fill price is missing or invalid"))?;
    let durable_fee = risk_state.fees_for_order(&order.order_id.0);
    let total_fee = details
        .fee
        .filter(|fee| *fee >= durable_fee)
        .ok_or_else(|| corrupt_order("venue fill fee is missing or regressed"))?;
    if !risk_state
        .apply_account_event(
            &PmAccountEvent::Fill {
                identity: FillIdentity::Venue(format!("recovered-order:{}", order.order_id.0)),
                strategy: Some(order.strategy.clone()),
                order_id: order.order_id.0.clone(),
                market: order.market.clone(),
                outcome: order.outcome,
                price,
                size: missing_qty,
                side: order.side,
                fee: total_fee - durable_fee,
                liquidity: if order.post_only {
                    Liquidity::Maker
                } else {
                    Liquidity::Taker
                },
                timestamp_ms: order.submitted_ms,
            },
            limits,
        )
        .map_err(|source| risk_storage_error(&source))?
    {
        return Err(corrupt_order(
            "recovered fill identity conflicts with durable ledger state",
        ));
    }
    Ok(filled_qty)
}

pub(super) struct RecoveredOrders {
    pub(super) reservations: HashMap<String, Reservation>,
    pub(super) open_orders: HashSet<OrderId>,
}

pub(super) async fn reconstruct_accepted_orders(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    store: &dyn TapeStore,
    risk_state: &mut LiveRiskState,
) -> Result<RecoveredOrders, StartError> {
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
    let intents = store
        .read_accepted_intents(&scope)
        .await
        .map_err(|source| StartError::Storage {
            run: run.id().clone(),
            source,
        })?;
    let mut seen_order_ids = HashSet::new();
    let mut recovered = RecoveredOrders {
        reservations: HashMap::new(),
        open_orders: HashSet::new(),
    };
    for intent in intents {
        let order =
            durable_order(intent, run.strategies()).map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?;
        if !seen_order_ids.insert(order.order_id.clone()) {
            return Err(StartError::Storage {
                run: run.id().clone(),
                source: corrupt_order(format!(
                    "venue order {} has multiple durable intents",
                    order.order_id.0
                )),
            });
        }
        match query_order_status(run, runtime, &order.order_id).await? {
            OrderStatus::Open(details) => {
                let filled_qty = apply_status_fill(&order, &details, risk_state, run.risk())
                    .map_err(|source| StartError::Storage {
                        run: run.id().clone(),
                        source,
                    })?;
                let remaining_qty = order.qty - filled_qty;
                if remaining_qty.is_zero() {
                    return Err(StartError::Storage {
                        run: run.id().clone(),
                        source: corrupt_order("open order has no remaining quantity"),
                    });
                }
                recovered.open_orders.insert(order.order_id.clone());
                recovered.reservations.insert(
                    order.order_id.0,
                    Reservation {
                        strategy: order.strategy,
                        market: order.market,
                        price: order.price,
                        remaining_qty,
                    },
                );
            }
            OrderStatus::Accepted(details) => {
                if apply_status_fill(&order, &details, risk_state, run.risk()).map_err(
                    |source| StartError::Storage {
                        run: run.id().clone(),
                        source,
                    },
                )? != order.qty
                {
                    return Err(StartError::Storage {
                        run: run.id().clone(),
                        source: corrupt_order(
                            "matched order filled quantity differs from durable order quantity",
                        ),
                    });
                }
            }
            OrderStatus::Rejected(_) | OrderStatus::Cancelled(_) => {}
            OrderStatus::Unknown(_) => {
                return Err(StartError::ExecutionState {
                    run: run.id().clone(),
                    source: ExecError::Transport {
                        message: format!("venue status is unknown for order {}", order.order_id.0),
                    },
                });
            }
        }
    }
    Ok(recovered)
}
