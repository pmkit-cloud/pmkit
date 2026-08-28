use super::{
    LiveReport, RunControl, RunLifecycleEvent, StartError, StrategyInstance,
    instantiate_strategies, observe_reconnect, store_signal as persist_signal,
    validate_account_owner,
};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_accounting::{ExposureReservation, aggregate_exposure};
use pmkit_book::OrderBookL2;
use pmkit_event::{FillIdentity, MarketEvent, PmAccountEvent, SourceEnvelope, StrategyFact};
use pmkit_exec::{ExecError, Executor, OrderId, OrderStatus, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_runtime::{LiveOrderPolicy, RuntimeConfig, StrategyRegistration};
use pmkit_spec::LiveRun;
use pmkit_store::{CausalIdentity, OwnerScope, ReplayItem, StoreError, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DurableFillAuthority {
    Account,
    Market,
}

impl DurableFillAuthority {
    fn for_run(run: &LiveRun) -> Self {
        if run.account_data_ref().is_some() {
            Self::Account
        } else {
            Self::Market
        }
    }
}

#[path = "live_recovery.rs"]
mod live_recovery;
#[path = "live_risk.rs"]
mod live_risk;
#[path = "live_tape.rs"]
mod live_tape;
#[cfg(test)]
use live_recovery::{DurableOrder, apply_status_fill};
use live_recovery::{accepted_submissions, corrupt_order, reconstruct_accepted_orders};
#[cfg(test)]
pub use live_risk::mark_positions;
use live_risk::{
    LiveRiskState, OrderRateLimits, OrderRateState, PortfolioRiskExposure, RiskStateError,
    passes_aggregated_risk,
};
#[cfg(test)]
pub use live_risk::{
    PortfolioRiskExposure as TestRiskExposure,
    passes_aggregated_risk as test_passes_aggregated_risk, passes_risk,
};
use live_tape::LiveTape;

async fn initial_open_orders(
    run: &LiveRun,
    runtime: &RuntimeConfig,
) -> Result<HashSet<OrderId>, StartError> {
    run.executor()
        .preflight()
        .await
        .map_err(|source| StartError::ExecutionState {
            run: run.id().clone(),
            source,
        })?;
    reconcile_open_orders(run, runtime).await
}

async fn reconcile_open_orders(
    run: &LiveRun,
    runtime: &RuntimeConfig,
) -> Result<HashSet<OrderId>, StartError> {
    let snapshot = tokio::time::timeout(
        runtime.shutdown.reconciliation_timeout,
        run.executor().reconcile(),
    )
    .await
    .map_err(|_| StartError::ExecutionState {
        run: run.id().clone(),
        source: ExecError::Transport {
            message: "reconciliation timed out".to_owned(),
        },
    })?
    .map_err(|source| StartError::ExecutionState {
        run: run.id().clone(),
        source,
    })?;
    Ok(snapshot.open_orders.into_iter().collect())
}

async fn query_order_status(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    order_id: &OrderId,
) -> Result<OrderStatus, StartError> {
    tokio::time::timeout(
        runtime.shutdown.reconciliation_timeout,
        run.executor().query_status(order_id),
    )
    .await
    .map_err(|_| StartError::ExecutionState {
        run: run.id().clone(),
        source: ExecError::Transport {
            message: format!("status query timed out for order {}", order_id.0),
        },
    })?
    .map_err(|source| StartError::ExecutionState {
        run: run.id().clone(),
        source,
    })
}

async fn recover_intents(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    store: &dyn TapeStore,
    scope: &OwnerScope,
) -> Result<(), StartError> {
    let recorder = crate::causal::CausalRecorder::new(store);
    let mut intents =
        recorder
            .pending_intents(scope)
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source: match source {
                    crate::causal::RecorderError::Store(source) => source,
                    _ => StoreError::Storage {
                        message: source.to_string(),
                    },
                },
            })?;
    intents.extend(recorder.unknown_intents(scope).await.map_err(|source| {
        StartError::Storage {
            run: run.id().clone(),
            source: match source {
                crate::causal::RecorderError::Store(source) => source,
                _ => StoreError::Storage {
                    message: source.to_string(),
                },
            },
        }
    })?);
    for intent in intents {
        let order_id = intent
            .venue_order_id()
            .ok_or_else(|| StartError::ExecutionState {
                run: run.id().clone(),
                source: ExecError::Transport {
                    message: format!(
                        "intent {} has no venue order id",
                        intent.identity.correlation_id
                    ),
                },
            })?;
        let status = query_order_status(run, runtime, &order_id).await?;
        let outcome = match status {
            OrderStatus::Open(_) | OrderStatus::Accepted(_) => pmkit_store::IntentOutcome::Accepted,
            OrderStatus::Rejected(_) | OrderStatus::Cancelled(_) => {
                pmkit_store::IntentOutcome::Rejected
            }
            OrderStatus::Unknown(_) => {
                return Err(StartError::ExecutionState {
                    run: run.id().clone(),
                    source: ExecError::Transport {
                        message: format!("venue status is unknown for order {}", order_id.0),
                    },
                });
            }
        };
        recorder
            .reconcile(&intent, outcome)
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source: match source {
                    crate::causal::RecorderError::Store(source) => source,
                    _ => StoreError::Storage {
                        message: source.to_string(),
                    },
                },
            })?;
    }
    Ok(())
}

async fn shutdown_orders(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    open_orders: &HashSet<OrderId>,
) -> Result<(), StartError> {
    match runtime.shutdown.live_orders {
        LiveOrderPolicy::Leave => Ok(()),
        LiveOrderPolicy::CancelOwned => run
            .executor()
            .cancel_batch(&open_orders.iter().cloned().collect::<Vec<_>>())
            .await
            .map_err(|source| StartError::ExecutionState {
                run: run.id().clone(),
                source,
            }),
        LiveOrderPolicy::CancelAllExplicit => {
            run.executor()
                .cancel_all()
                .await
                .map_err(|source| StartError::ExecutionState {
                    run: run.id().clone(),
                    source,
                })
        }
    }
}

fn sources(run: &LiveRun, strategies: &[StrategyInstance]) -> Vec<SourceTaskDefinition> {
    let mut subscribed = HashSet::new();
    let mut sources = Vec::new();
    for instance in strategies {
        if !subscribed.insert(instance.market.clone()) {
            continue;
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let source = run.market_data().clone();
            let market = instance.market.clone();
            let name = format!("pm:{market:?}:{outcome:?}");
            sources.push(SourceTaskDefinition::new(name, move |sink| async move {
                source.subscribe(market, outcome, sink).await
            }));
        }
    }
    if let Some(reference) = run.reference_data_ref() {
        let reference = reference.clone();
        sources.push(SourceTaskDefinition::new("cex", move |sink| async move {
            reference.subscribe_reference(sink).await
        }));
    }
    if let Some(account) = run.account_data_ref() {
        let account = account.clone();
        let portfolio = run.portfolio().clone();
        sources.push(SourceTaskDefinition::new(
            "pm-account",
            move |sink| async move { account.subscribe_account(portfolio, sink).await },
        ));
    }
    sources
}

fn report(
    run: &LiveRun,
    metrics: crate::RunMetricsSnapshot,
    exposure: pmkit_accounting::PortfolioExposure,
) -> LiveReport {
    LiveReport {
        run: run.id().clone(),
        events_processed: metrics.events_processed,
        fills: metrics.fills,
        rejected: metrics.rejected,
        metrics,
        exposure,
    }
}

fn strategy_correlation_id(
    strategy: &pmkit_core::StrategyId,
    market: &pmkit_core::MarketId,
    timestamp_ms: i64,
) -> String {
    let strategy = strategy.to_string();
    let market = market.to_string();
    format!(
        "live-strategy:{}:{strategy}:market:{}:{market}:{timestamp_ms}",
        strategy.len(),
        market.len()
    )
}

fn correlation_strategy(
    correlation_id: &str,
    registrations: &[StrategyRegistration],
) -> Option<pmkit_core::StrategyId> {
    registrations.iter().find_map(|registration| {
        let strategy = registration.id().to_string();
        let prefix = format!("live-strategy:{}:{strategy}:", strategy.len());
        correlation_id
            .starts_with(&prefix)
            .then(|| registration.id().clone())
    })
}

fn risk_storage_error(source: &RiskStateError) -> StoreError {
    StoreError::Storage {
        message: source.to_string(),
    }
}

async fn reconstruct_risk_state(
    store: &dyn TapeStore,
    scope: &OwnerScope,
    limits: &pmkit_runtime::RiskLimits,
    authority: DurableFillAuthority,
) -> Result<LiveRiskState, StoreError> {
    let page_size = NonZeroUsize::new(256).unwrap_or(NonZeroUsize::MIN);
    let mut cursor = None;
    let mut state = LiveRiskState::default();
    loop {
        let page = store.read_envelopes(scope, cursor, page_size).await?;
        let next_cursor = page.next_cursor;
        for item in page.items {
            match item {
                ReplayItem::Envelope(envelope) => {
                    match authority {
                        DurableFillAuthority::Account => state.apply_durable_account_record(
                            &envelope,
                            &scope.portfolio_id,
                            limits,
                        ),
                        DurableFillAuthority::Market => {
                            state.apply_durable_market_record(&envelope, limits)
                        }
                    }
                    .map_err(|source| risk_storage_error(&source))?;
                }
                ReplayItem::Gap(gap) => {
                    return Err(StoreError::Storage {
                        message: format!(
                            "durable risk history has a replay gap at ingest {}: {:?}",
                            gap.ingest_sequence, gap.reason
                        ),
                    });
                }
            }
        }
        let Some(next_cursor) = next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(state)
}

#[cfg(test)]
pub async fn drive(run: &LiveRun, runtime: &RuntimeConfig) -> Result<LiveReport, StartError> {
    drive_with_store(run, runtime, None).await
}

async fn store_signal(
    run: &LiveRun,
    store: Option<&dyn TapeStore>,
    scope: &OwnerScope,
    signal: &pmkit_data::SourceSignal,
) -> Result<(), StartError> {
    persist_signal(store, scope, signal)
        .await
        .map_err(|source| StartError::Storage {
            run: run.id().clone(),
            source,
        })
}

/// A failed order placement that must abort the live run after cleanup.
enum PlaceFailure {
    /// The venue outcome is unknown; the durable intent stays pending for recovery.
    Transport(ExecError),
    /// Durable storage failed before or after the venue call.
    Storage(StoreError),
}

struct Reservation {
    strategy: pmkit_core::StrategyId,
    market: pmkit_core::MarketId,
    price: rust_decimal::Decimal,
    remaining_qty: rust_decimal::Decimal,
}

impl Reservation {
    fn notional(&self) -> rust_decimal::Decimal {
        self.remaining_qty * self.price
    }
}

fn apply_reservation_fill(
    reservations: &mut HashMap<String, Reservation>,
    open_orders: &mut HashSet<OrderId>,
    order_id: &str,
    size: rust_decimal::Decimal,
) -> Result<(), RiskStateError> {
    if let Some(reservation) = reservations.get_mut(order_id) {
        if size > reservation.remaining_qty {
            return Err(RiskStateError::corrupt("fill exceeds known reservation"));
        }
        reservation.remaining_qty -= size;
        if reservation.remaining_qty.is_zero() {
            reservations.remove(order_id);
            open_orders.remove(&OrderId(order_id.to_owned()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod reservation_tests {
    use super::{Reservation, apply_reservation_fill};
    use pmkit_core::{MarketId, StrategyId};
    use pmkit_exec::OrderId;
    use rust_decimal::Decimal;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn partial_fill_keeps_only_remaining_reservation() -> Result<(), Box<dyn std::error::Error>> {
        // Given: one ten-share live reservation at a fifty-cent limit.
        let mut reservations = HashMap::from([(
            "order-1".to_owned(),
            Reservation {
                strategy: StrategyId::new("maker")?,
                market: MarketId::new("btc-5m")?,
                price: Decimal::new(5, 1),
                remaining_qty: Decimal::from(10),
            },
        )]);
        let mut open_orders = HashSet::from([OrderId("order-1".to_owned())]);

        // When: a partial three-share fill arrives before the terminal fill.
        apply_reservation_fill(
            &mut reservations,
            &mut open_orders,
            "order-1",
            Decimal::from(3),
        )?;

        // Then: the seven unfilled shares remain reserved at their limit price.
        assert_eq!(reservations["order-1"].notional(), Decimal::new(35, 1));
        assert!(open_orders.contains(&OrderId("order-1".to_owned())));
        apply_reservation_fill(
            &mut reservations,
            &mut open_orders,
            "order-1",
            Decimal::from(7),
        )?;
        assert!(reservations.is_empty());
        assert!(open_orders.is_empty());
        Ok(())
    }

    #[test]
    fn known_order_overfill_fails_without_mutating_reservation()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: one five-share reservation.
        let mut reservations = HashMap::from([(
            "order-1".to_owned(),
            Reservation {
                strategy: StrategyId::new("maker")?,
                market: MarketId::new("btc-5m")?,
                price: Decimal::new(5, 1),
                remaining_qty: Decimal::from(5),
            },
        )]);
        let mut open_orders = HashSet::from([OrderId("order-1".to_owned())]);

        // When: a fill exceeds the known remaining quantity.
        let result = apply_reservation_fill(
            &mut reservations,
            &mut open_orders,
            "order-1",
            Decimal::from(6),
        );

        // Then: the reservation is unchanged and the driver can fail closed.
        assert!(result.is_err());
        assert_eq!(reservations["order-1"].remaining_qty, Decimal::from(5));
        assert!(open_orders.contains(&OrderId("order-1".to_owned())));
        Ok(())
    }
}

/// Places one order, routing through durable causal recording when storage is configured.
async fn place_order(
    store: Option<&dyn TapeStore>,
    executor: &dyn Executor,
    order: &PlaceOrder,
    now_ms: i64,
    decision: &CausalIdentity,
    action_index: u32,
) -> Result<Option<OrderId>, PlaceFailure> {
    let Some(store) = store else {
        return match executor.submit(order, now_ms).await {
            Ok(order_id) => Ok(Some(order_id)),
            Err(source @ ExecError::Transport { .. }) => Err(PlaceFailure::Transport(source)),
            Err(ExecError::Rejected { .. } | ExecError::NotFound { .. }) => Ok(None),
        };
    };
    let recorder = crate::causal::CausalRecorder::new(store);
    let intent = recorder.intent(decision, action_index, now_ms, order);
    match recorder
        .submit(&intent, || executor.submit(order, now_ms))
        .await
    {
        Ok(receipt) => Ok(Some(receipt.order_id)),
        Err(crate::causal::RecorderError::VenueRejected { .. }) => Ok(None),
        Err(crate::causal::RecorderError::VenueUnknown { source }) => {
            Err(PlaceFailure::Transport(source))
        }
        Err(
            crate::causal::RecorderError::AcceptedButUnrecorded { source }
            | crate::causal::RecorderError::Store(source),
        ) => Err(PlaceFailure::Storage(source)),
    }
}

#[cfg(test)]
pub async fn drive_with_store(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    store: Option<&dyn TapeStore>,
) -> Result<LiveReport, StartError> {
    drive_with_control(run, runtime, store, &RunControl::default()).await
}

#[cfg(test)]
async fn drive_with_rate_limits(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    store: Option<&dyn TapeStore>,
    rate_limits: OrderRateLimits,
) -> Result<LiveReport, StartError> {
    drive_with_control_and_rate_limits(run, runtime, store, &RunControl::default(), rate_limits)
        .await
}

pub async fn drive_with_control(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    store: Option<&dyn TapeStore>,
    control: &RunControl,
) -> Result<LiveReport, StartError> {
    drive_with_control_and_rate_limits(run, runtime, store, control, OrderRateLimits::default())
        .await
}

#[expect(
    clippy::too_many_lines,
    reason = "the live run owns one ordered risk, tape, storage, and shutdown lifecycle"
)]
async fn drive_with_control_and_rate_limits(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    store: Option<&dyn TapeStore>,
    control: &RunControl,
    rate_limits: OrderRateLimits,
) -> Result<LiveReport, StartError> {
    let metrics = control.metrics_for(run.id());
    let effective_limits_by_strategy: HashMap<_, _> = run
        .strategies()
        .iter()
        .map(|registration| {
            (
                registration.id().clone(),
                registration.risk_overrides_ref().effective_limits(
                    run.risk(),
                    registration.market(),
                    registration.id(),
                ),
            )
        })
        .collect();
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;
    let executor = run.executor().clone();
    let limits = run.risk().clone();
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
    if let Some(store) = store
        && store
            .kill_state(run.portfolio())
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?
    {
        return Err(StartError::KillSwitchActive(run.id().clone()));
    }
    let mut open_orders = initial_open_orders(run, runtime).await?;
    let fill_authority = DurableFillAuthority::for_run(run);
    let max_open_orders = usize::try_from(limits.max_open_orders.get()).unwrap_or(usize::MAX);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1024);
    let feed = MergedFeed::from_tasks(FeedMode::Live, sources(run, &strategies), None)
        .with_metrics(metrics.clone());
    let merge = feed.spawn(event_tx, control.cancellation());
    let mut tape = LiveTape::open(run, runtime)?;
    let mut order_rate_state = OrderRateState::default();
    let mut risk_state = LiveRiskState::default();
    let mut reservations: HashMap<String, Reservation> = HashMap::new();
    if let Some(store) = store {
        risk_state = reconstruct_risk_state(store, &scope, &limits, fill_authority)
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?;
        let submissions = accepted_submissions(store, &scope, run.strategies())
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?;
        order_rate_state.restore(rate_limits, submissions);
        recover_intents(run, runtime, store, &scope).await?;
        let recovered = reconstruct_accepted_orders(run, runtime, store, &risk_state).await?;
        if recovered.open_orders != open_orders {
            return Err(StartError::Storage {
                run: run.id().clone(),
                source: corrupt_order(
                    "executor open-order snapshot differs from durable accepted intents",
                ),
            });
        }
        reservations = recovered.reservations;
        metrics.set_fills(risk_state.fill_count());
    }

    let mut connection_epochs = HashMap::new();
    let mut cex_metrics = crate::causal::CexTradeMetricsState::default();
    control.emit(RunLifecycleEvent::Started {
        run: run.id().clone(),
    });
    if control.is_cancelled() {
        merge.abort().await;
        control.emit(RunLifecycleEvent::Cancelled {
            run: run.id().clone(),
        });
        return finish(
            run,
            runtime,
            &open_orders,
            &mut tape,
            &metrics,
            &risk_state,
            &mut reservations,
        )
        .await;
    }
    while let Some(merged) = event_rx.recv().await {
        if control.is_cancelled() {
            merge.abort().await;
            control.emit(RunLifecycleEvent::Cancelled {
                run: run.id().clone(),
            });
            return finish(
                run,
                runtime,
                &open_orders,
                &mut tape,
                &metrics,
                &risk_state,
                &mut reservations,
            )
            .await;
        }
        validate_account_owner(run.id(), run.portfolio(), &merged.source)?;
        if observe_reconnect(&merged.source, &mut connection_epochs) {
            metrics.reconnect();
        }
        store_signal(
            run,
            store,
            &scope,
            &pmkit_data::SourceSignal::Data(Box::new(merged.source.clone())),
        )
        .await?;
        if let SourceEnvelope::CexReference(envelope) = &merged.source {
            cex_metrics.observe(&envelope.fact);
            continue;
        }
        if let SourceEnvelope::PmAccount(envelope) = &merged.source {
            tape.append_account(run, envelope)?;
            let fill_was_applied = risk_state
                .apply_account_event(&envelope.fact, &limits)
                .map_err(|source| StartError::Storage {
                    run: run.id().clone(),
                    source: risk_storage_error(&source),
                })?;
            metrics.set_fills(risk_state.fill_count());
            match &envelope.fact {
                PmAccountEvent::Fill { order_id, size, .. } => {
                    if fill_was_applied {
                        apply_reservation_fill(
                            &mut reservations,
                            &mut open_orders,
                            order_id,
                            *size,
                        )
                        .map_err(|source| StartError::Storage {
                            run: run.id().clone(),
                            source: risk_storage_error(&source),
                        })?;
                    }
                }
                PmAccountEvent::OrderCancelled { order_id, .. }
                | PmAccountEvent::OrderRejected { order_id, .. } => {
                    reservations.remove(order_id);
                    open_orders.remove(&OrderId(order_id.clone()));
                }
                PmAccountEvent::OrderAck { .. }
                | PmAccountEvent::OrderStatus { .. }
                | PmAccountEvent::Settlement { .. } => {}
            }
            continue;
        }
        let SourceEnvelope::PmMarket(envelope) = merged.source else {
            continue;
        };
        let event = envelope.fact;
        tape.append(run, &event)?;
        metrics.event();
        match &event {
            MarketEvent::BookUpdate {
                market,
                outcome,
                bids,
                asks,
                timestamp_ms,
                ..
            } => {
                let book = OrderBookL2 {
                    bids: bids.clone(),
                    asks: asks.clone(),
                    timestamp_ms: *timestamp_ms,
                    last_trade_price: None,
                };
                let fact = StrategyFact::Market(event.clone());
                let portfolio_daily_pnl = risk_state.update_book(market, *outcome, &book, &limits);
                if risk_state.loss_breached
                    && let Some(store) = store
                {
                    store
                        .set_kill_state(run.portfolio(), true)
                        .await
                        .map_err(|source| StartError::Storage {
                            run: run.id().clone(),
                            source,
                        })?;
                }
                for instance in &mut *strategies {
                    if instance.market != *market {
                        continue;
                    }
                    let identity = CausalIdentity {
                        scope: scope.clone(),
                        correlation_id: strategy_correlation_id(
                            &instance.id,
                            market,
                            *timestamp_ms,
                        ),
                        source_timestamp_ms: *timestamp_ms,
                        ingest_sequence: i64::try_from(envelope.metadata.ingest_sequence)
                            .unwrap_or(i64::MAX),
                    };
                    let mut verdicts: Vec<crate::causal::ActionRiskVerdict> = Vec::new();
                    let effective_limits = effective_limits_by_strategy
                        .get(&instance.id)
                        .map_or(&limits, |effective_limits| effective_limits);
                    let market_positions = risk_state.positions(market);
                    let context = StrategyContext {
                        fact: &fact,
                        market,
                        book: &book,
                        positions: market_positions,
                        now: LogicalTimestamp::from_millis(*timestamp_ms),
                    };
                    metrics.decision();
                    if let Ok(actions) = instance.strategy.on_event(context) {
                        for (action_index, action) in actions.as_slice().iter().enumerate() {
                            if let Action::Place(order) = action {
                                let action_index = u32::try_from(action_index).unwrap_or(u32::MAX);
                                if open_orders.len() >= max_open_orders {
                                    open_orders = reconcile_open_orders(run, runtime).await?;
                                }
                                if open_orders.len() >= max_open_orders {
                                    verdicts.push(crate::causal::ActionRiskVerdict::rejected(
                                        action_index,
                                        "open order capacity",
                                    ));
                                    metrics.reject();
                                    continue;
                                }
                                let reserved_portfolio: rust_decimal::Decimal =
                                    reservations.values().map(Reservation::notional).sum();
                                let reserved_market: rust_decimal::Decimal = reservations
                                    .values()
                                    .filter(|reservation| reservation.market == *market)
                                    .map(Reservation::notional)
                                    .sum();
                                let reserved_strategy: rust_decimal::Decimal = reservations
                                    .values()
                                    .filter(|reservation| reservation.strategy == instance.id)
                                    .map(Reservation::notional)
                                    .sum();
                                let exposure =
                                    portfolio_daily_pnl.map(|daily_pnl| PortfolioRiskExposure {
                                        portfolio_notional: risk_state.portfolio_notional()
                                            + reserved_portfolio,
                                        market_notional: risk_state.market_notional(market)
                                            + reserved_market,
                                        strategy_notional: reserved_strategy,
                                        daily_pnl,
                                        open_orders: open_orders.len(),
                                    });
                                if risk_state.loss_breached
                                    || exposure.is_none_or(|exposure| {
                                        !passes_aggregated_risk(
                                            order,
                                            effective_limits,
                                            market_positions,
                                            exposure,
                                        )
                                    })
                                {
                                    verdicts.push(crate::causal::ActionRiskVerdict::rejected(
                                        action_index,
                                        "risk gate",
                                    ));
                                    metrics.reject();
                                    continue;
                                }
                                if !order_rate_state.try_accept(
                                    &instance.id,
                                    *timestamp_ms,
                                    rate_limits,
                                ) {
                                    verdicts.push(crate::causal::ActionRiskVerdict::rejected(
                                        action_index,
                                        "order submission rate limit",
                                    ));
                                    metrics.reject();
                                    continue;
                                }
                                verdicts
                                    .push(crate::causal::ActionRiskVerdict::accepted(action_index));
                                let placement = place_order(
                                    store,
                                    executor.as_ref(),
                                    order,
                                    *timestamp_ms,
                                    &identity,
                                    action_index,
                                )
                                .await;
                                match placement {
                                    Ok(Some(order_id)) => {
                                        reservations.insert(
                                            order_id.0.clone(),
                                            Reservation {
                                                strategy: instance.id.clone(),
                                                market: market.clone(),
                                                price: order.price,
                                                remaining_qty: order.qty,
                                            },
                                        );
                                        open_orders.insert(order_id);
                                    }
                                    Ok(None) => metrics.reject(),
                                    Err(failure) => {
                                        reconcile_open_orders(run, runtime).await?;
                                        tape.flush(run)?;
                                        return Err(match failure {
                                            PlaceFailure::Transport(source) => {
                                                StartError::ExecutionState {
                                                    run: run.id().clone(),
                                                    source,
                                                }
                                            }
                                            PlaceFailure::Storage(source) => StartError::Storage {
                                                run: run.id().clone(),
                                                source,
                                            },
                                        });
                                    }
                                }
                            }
                        }
                    }
                    if let Some(store) = store {
                        let snapshot = crate::causal::DecisionSnapshot::from_book(
                            &book,
                            cex_metrics.snapshot(),
                        );
                        let decision = if verdicts.is_empty() {
                            crate::causal::DecisionKind::NoAction
                        } else {
                            crate::causal::DecisionKind::Actions(verdicts)
                        };
                        crate::causal::CausalRecorder::new(store)
                            .record_evaluation(&identity, &snapshot, decision)
                            .await
                            .map_err(|source| StartError::Storage {
                                run: run.id().clone(),
                                source,
                            })?;
                    }
                }
            }
            MarketEvent::Fill { order_id, size, .. }
                if fill_authority == DurableFillAuthority::Market
                    && risk_state.apply_fill(
                        &event,
                        &FillIdentity::transport(&envelope.metadata),
                        &limits,
                    ) =>
            {
                metrics.set_fills(risk_state.fill_count());
                apply_reservation_fill(&mut reservations, &mut open_orders, order_id, *size)
                    .map_err(|source| StartError::Storage {
                        run: run.id().clone(),
                        source: risk_storage_error(&source),
                    })?;
            }
            _ => {}
        }
    }

    merge
        .join()
        .await
        .map_err(|error| StartError::Source {
            run: run.id().clone(),
            source: pmkit_data::DataSourceError::ReplayGap {
                message: format!("merged feed task failed: {error}"),
            },
        })?
        .map_err(|source| StartError::Source {
            run: run.id().clone(),
            source,
        })?;

    let report = finish(
        run,
        runtime,
        &open_orders,
        &mut tape,
        &metrics,
        &risk_state,
        &mut reservations,
    )
    .await?;
    control.emit(if control.is_cancelled() {
        RunLifecycleEvent::Cancelled {
            run: run.id().clone(),
        }
    } else {
        RunLifecycleEvent::Completed {
            run: run.id().clone(),
        }
    });
    Ok(report)
}

async fn finish(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    open_orders: &HashSet<OrderId>,
    tape: &mut LiveTape,
    metrics: &crate::RunMetrics,
    risk_state: &LiveRiskState,
    reservations: &mut HashMap<String, Reservation>,
) -> Result<LiveReport, StartError> {
    tape.finish(run, runtime, open_orders).await?;
    if runtime.shutdown.live_orders != LiveOrderPolicy::Leave {
        reservations.clear();
    }
    let exposure = aggregate_exposure(
        &risk_state.position_exposures(),
        &reservations
            .values()
            .map(|reservation| ExposureReservation {
                market: reservation.market.clone(),
                strategy: reservation.strategy.clone(),
                notional: reservation.notional(),
            })
            .collect::<Vec<_>>(),
    );
    Ok(report(run, metrics.snapshot(), exposure))
}

#[cfg(test)]
mod recovery_tests {
    use super::{
        DurableOrder, LiveRiskState, StartError, apply_status_fill, drive_with_store,
        reconstruct_accepted_orders, recover_intents, strategy_correlation_id,
    };
    use crate::test_support::{BuyFactory, config, risk};
    use async_trait::async_trait;
    use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
    use pmkit_data::{DataSourceError, LiveDataSource, SourceSignal};
    use pmkit_exec::{
        ExecError, ExecutionSnapshot, Executor, OrderId, OrderStatus, OrderStatusDetails,
        PlaceOrder,
    };
    use pmkit_market::Outcome;
    use pmkit_runtime::{LiveOrderPolicy, StrategyRegistration};
    use pmkit_spec::LiveRun;
    use pmkit_store::{
        CausalIdentity, IntentOutcome, OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, TapeStore,
        TursoTapeStore,
    };
    use rust_decimal::Decimal;
    use serde_json::json;
    use std::{sync::Arc, time::Duration};
    use tokio::sync::mpsc::Sender;

    #[derive(Clone, Copy)]
    enum RecoveryResponse {
        Accepted,
        Unknown,
        Failure,
        Timeout,
    }

    struct RecoveryExecutor(RecoveryResponse);

    struct RestartExecutor {
        open_orders: Vec<OrderId>,
        status: OrderStatus,
    }

    #[async_trait]
    impl Executor for RecoveryExecutor {
        async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
            Ok(ExecutionSnapshot::default())
        }

        async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
            Ok(ExecutionSnapshot::default())
        }

        async fn query_status(&self, order_id: &OrderId) -> Result<OrderStatus, ExecError> {
            match self.0 {
                RecoveryResponse::Accepted => {
                    Ok(OrderStatus::Accepted(OrderStatusDetails::default()))
                }
                RecoveryResponse::Unknown => {
                    Ok(OrderStatus::Unknown(OrderStatusDetails::default()))
                }
                RecoveryResponse::Failure => Err(ExecError::NotFound {
                    order_id: order_id.0.clone(),
                }),
                RecoveryResponse::Timeout => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(OrderStatus::Accepted(OrderStatusDetails::default()))
                }
            }
        }

        async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
            Ok(OrderId("unused".into()))
        }

        async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
            Ok(())
        }

        async fn cancel_all(&self) -> Result<(), ExecError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Executor for RestartExecutor {
        async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
            Ok(ExecutionSnapshot {
                open_orders: self.open_orders.clone(),
            })
        }

        async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
            Ok(ExecutionSnapshot {
                open_orders: self.open_orders.clone(),
            })
        }

        async fn query_status(&self, _order_id: &OrderId) -> Result<OrderStatus, ExecError> {
            Ok(self.status.clone())
        }

        async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
            Ok(OrderId("unused".into()))
        }

        async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
            Ok(())
        }

        async fn cancel_all(&self) -> Result<(), ExecError> {
            Ok(())
        }
    }

    struct RecoverySource;

    #[async_trait]
    impl LiveDataSource for RecoverySource {
        async fn subscribe(
            &self,
            _market: pmkit_core::MarketId,
            _outcome: Outcome,
            sink: Sender<SourceSignal>,
        ) -> Result<(), DataSourceError> {
            sink.send(SourceSignal::Watermark(i64::MAX))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            sink.send(SourceSignal::Eof)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            Ok(())
        }
    }

    fn recovery_run(
        run_id: &str,
        response: RecoveryResponse,
    ) -> Result<LiveRun, Box<dyn std::error::Error>> {
        Ok(LiveRun::new(
            RunId::new(run_id)?,
            PortfolioId::new("recovery")?,
            Arc::new(RecoveryExecutor(response)),
            Arc::new(RecoverySource),
            risk()?,
        ))
    }

    fn recovery_identity(scope: &OwnerScope, correlation_id: &str) -> CausalIdentity {
        CausalIdentity {
            scope: scope.clone(),
            correlation_id: correlation_id.into(),
            source_timestamp_ms: 1_000,
            ingest_sequence: 1,
        }
    }

    fn restart_run(
        run_id: &str,
        executor: RestartExecutor,
    ) -> Result<LiveRun, Box<dyn std::error::Error>> {
        Ok(LiveRun::new(
            RunId::new(run_id)?,
            PortfolioId::new("recovery")?,
            Arc::new(executor),
            Arc::new(RecoverySource),
            risk()?,
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("maker")?,
            MarketId::new("btc-5m")?,
            Arc::new(BuyFactory),
        )))
    }

    fn accepted_intent_payload() -> serde_json::Value {
        json!({
            "action_index": 0,
            "submitted_ms": 1_000,
            "order": {
                "market": "btc-5m",
                "outcome": "Up",
                "side": "buy",
                "price": "0.50",
                "qty": "10",
                "post_only": true
            }
        })
    }

    fn market_fill_envelope(scope: &OwnerScope, order_id: &str, size: Decimal) -> PmEnvelope {
        PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope: scope.clone(),
            venue_id: "polymarket".into(),
            config_hash: "runtime".into(),
            source_id: "polymarket:market-ws".into(),
            connection_id: "market-1".into(),
            source_timestamp_ms: 1_001,
            canonical_source_rank: 0,
            connection_epoch: 1,
            frame_sequence: 1,
            receipt_timestamp_ms: 1_001,
            ingest_sequence: 1,
            raw_frame: Vec::new(),
            normalized: json!({
                "schema_version": 1,
                "payload": {
                    "kind": "fill",
                    "ts": 1_001,
                    "strategy": "maker",
                    "order_id": order_id,
                    "market": "btc-5m",
                    "outcome": "up",
                    "price": "0.41",
                    "size": size.to_string(),
                    "side": "buy",
                    "fee": "0.03",
                    "liquidity": "taker"
                }
            }),
        }
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn recover_terminal_intent() -> Result<(), Box<dyn std::error::Error>> {
        // Given: an unresolved durable intent whose venue reports acceptance.
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("recover-terminal.db");
        let store = TursoTapeStore::open_local(&path).await?;
        let run = recovery_run("recover-terminal", RecoveryResponse::Accepted)?;
        let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
        let identity = recovery_identity(&scope, "terminal");
        store
            .store_intent_pending(&identity, &json!({"kind": "place"}))
            .await?;
        store
            .transition_intent_with_order(&identity, IntentOutcome::Unknown, Some("venue-terminal"))
            .await?;

        // When: restart recovery queries the authoritative venue status.
        recover_intents(&run, &config()?, &store, &scope).await?;

        // Then: the intent leaves the unresolved set exactly as before.
        assert!(store.read_unknown_intents(&scope).await?.is_empty());
        store.delete_database()?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn recover_unknown_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let mut runtime = config()?;
        runtime.shutdown.reconciliation_timeout = Duration::from_millis(1);
        let cases = [
            ("unknown", RecoveryResponse::Unknown, true),
            ("timeout", RecoveryResponse::Timeout, true),
            ("failure", RecoveryResponse::Failure, true),
            ("missing-id", RecoveryResponse::Accepted, false),
        ];

        for (name, response, has_venue_id) in cases {
            // Given: one unresolved durable intent with an unsafe recovery outcome.
            let dir = tempfile::tempdir()?;
            let path = dir.path().join(format!("recover-{name}.db"));
            let store = TursoTapeStore::open_local(&path).await?;
            let run = recovery_run(name, response)?;
            let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
            let identity = recovery_identity(&scope, name);
            store
                .store_intent_pending(&identity, &json!({"kind": "place"}))
                .await?;
            if has_venue_id {
                store
                    .transition_intent_with_order(
                        &identity,
                        IntentOutcome::Unknown,
                        Some("venue-unresolved"),
                    )
                    .await?;
            }

            // When: restart recovery cannot establish an authoritative status.
            let result = recover_intents(&run, &runtime, &store, &scope).await;

            // Then: startup aborts and the intent remains explicitly unresolved.
            assert!(matches!(result, Err(StartError::ExecutionState { .. })));
            let unresolved = store.read_pending_intents(&scope).await?.len()
                + store.read_unknown_intents(&scope).await?.len();
            assert_eq!(unresolved, 1, "case: {name}");
            store.delete_database()?;
        }
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn restart_restores_exact_open_reservation() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a durable accepted ten-share maker order with three shares filled at the venue.
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("recover-open-reservation.db");
        let store = TursoTapeStore::open_local(&path).await?;
        let run = restart_run(
            "recover-open-reservation",
            RestartExecutor {
                open_orders: vec![OrderId("venue-open".into())],
                status: OrderStatus::Open(OrderStatusDetails {
                    filled_qty: Some(Decimal::from(3)),
                    price: Some(Decimal::new(50, 2)),
                    fee: Some(Decimal::ZERO),
                    settlement_reference: None,
                }),
            },
        )?;
        let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
        let correlation =
            strategy_correlation_id(&StrategyId::new("maker")?, &MarketId::new("btc-5m")?, 1_000);
        let identity = recovery_identity(&scope, &format!("{correlation}:0"));
        store
            .store_intent_pending(&identity, &accepted_intent_payload())
            .await?;
        store
            .transition_intent_with_order(&identity, IntentOutcome::Accepted, Some("venue-open"))
            .await?;
        store
            .store_envelope(&market_fill_envelope(
                &scope,
                "venue-open",
                Decimal::from(3),
            ))
            .await?;
        let mut runtime = config()?;
        runtime.shutdown.live_orders = LiveOrderPolicy::Leave;

        // When: the live driver restarts from durable intent and venue status authorities.
        let report = drive_with_store(&run, &runtime, Some(&store)).await?;

        // Then: exact remaining notional is attributed to its durable market and strategy.
        assert_eq!(report.exposure.portfolio_notional, Decimal::new(350, 2));
        assert_eq!(report.exposure.market_notionals.len(), 1);
        assert_eq!(
            report.exposure.market_notionals[0].market,
            MarketId::new("btc-5m")?
        );
        assert_eq!(
            report.exposure.market_notionals[0].notional,
            Decimal::new(350, 2)
        );
        assert_eq!(report.exposure.strategy_notionals.len(), 1);
        assert_eq!(
            report.exposure.strategy_notionals[0].strategy,
            StrategyId::new("maker")?
        );
        assert_eq!(
            report.exposure.strategy_notionals[0].notional,
            Decimal::new(350, 2)
        );
        store.delete_database()?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn restart_applies_matched_status_fill_once() -> Result<(), Box<dyn std::error::Error>> {
        // Given: an accepted order whose authoritative venue status is fully matched.
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("recover-matched-fill.db");
        let store = TursoTapeStore::open_local(&path).await?;
        let scope = OwnerScope::new(
            PortfolioId::new("recovery")?,
            RunId::new("recover-matched-fill")?,
        );
        let correlation =
            strategy_correlation_id(&StrategyId::new("maker")?, &MarketId::new("btc-5m")?, 1_000);
        let identity = recovery_identity(&scope, &format!("{correlation}:0"));
        store
            .store_intent_pending(&identity, &accepted_intent_payload())
            .await?;
        store
            .transition_intent_with_order(&identity, IntentOutcome::Accepted, Some("venue-matched"))
            .await?;
        store
            .store_envelope(&market_fill_envelope(
                &scope,
                "venue-matched",
                Decimal::from(10),
            ))
            .await?;
        let mut runtime = config()?;
        runtime.shutdown.live_orders = LiveOrderPolicy::Leave;

        // When: the same durable restart reconstruction is performed twice.
        for _ in 0..2 {
            let run = restart_run(
                "recover-matched-fill",
                RestartExecutor {
                    open_orders: Vec::new(),
                    status: OrderStatus::Accepted(OrderStatusDetails {
                        filled_qty: Some(Decimal::from(10)),
                        price: Some(Decimal::new(52, 2)),
                        fee: None,
                        settlement_reference: None,
                    }),
                },
            )?;
            let report = drive_with_store(&run, &runtime, Some(&store)).await?;

            // Then: the recovered aggregate fill has one stable identity per reconstruction.
            assert_eq!(report.fills, 1);
        }
        store.delete_database()?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn restart_rejects_missing_or_ambiguous_order_detail()
    -> Result<(), Box<dyn std::error::Error>> {
        for ambiguous in [false, true] {
            // Given: an open venue order whose durable detail is missing or duplicated.
            let dir = tempfile::tempdir()?;
            let path = dir.path().join(format!("recover-invalid-{ambiguous}.db"));
            let store = TursoTapeStore::open_local(&path).await?;
            let run_id = format!("recover-invalid-{ambiguous}");
            let run = restart_run(
                &run_id,
                RestartExecutor {
                    open_orders: vec![OrderId("venue-invalid".into())],
                    status: OrderStatus::Open(OrderStatusDetails {
                        filled_qty: Some(Decimal::ZERO),
                        price: Some(Decimal::new(50, 2)),
                        fee: Some(Decimal::ZERO),
                        settlement_reference: None,
                    }),
                },
            )?;
            let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
            let correlation = strategy_correlation_id(
                &StrategyId::new("maker")?,
                &MarketId::new("btc-5m")?,
                1_000,
            );
            let identity = recovery_identity(&scope, &format!("{correlation}:0"));
            let mut payload = accepted_intent_payload();
            if !ambiguous {
                payload["order"]
                    .as_object_mut()
                    .ok_or("order object")?
                    .remove("price");
            }
            store.store_intent_pending(&identity, &payload).await?;
            store
                .transition_intent_with_order(
                    &identity,
                    IntentOutcome::Accepted,
                    Some("venue-invalid"),
                )
                .await?;
            if ambiguous {
                let duplicate = CausalIdentity {
                    correlation_id: format!("{correlation}:1"),
                    ingest_sequence: 2,
                    ..identity.clone()
                };
                let mut duplicate_payload = accepted_intent_payload();
                duplicate_payload["action_index"] = json!(1);
                store
                    .store_intent_pending(&duplicate, &duplicate_payload)
                    .await?;
                store
                    .transition_intent_with_order(
                        &duplicate,
                        IntentOutcome::Accepted,
                        Some("venue-invalid"),
                    )
                    .await?;
            }
            let mut runtime = config()?;
            runtime.shutdown.live_orders = LiveOrderPolicy::Leave;

            // When: startup cannot identify one complete durable authority for the open order.
            let result = drive_with_store(&run, &runtime, Some(&store)).await;

            // Then: recovery aborts instead of inferring price, size, or ownership.
            assert!(matches!(result, Err(StartError::Storage { .. })));
            store.delete_database()?;
        }
        Ok(())
    }

    #[test]
    fn unmatched_status_total_does_not_synthesize_missing_fill_economics()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: four durable shares and a matched venue aggregate of ten shares for one order.
        let limits = risk()?;
        let market = MarketId::new("btc-5m")?;
        let strategy = StrategyId::new("maker")?;
        let order = DurableOrder {
            order_id: OrderId("venue-delta".into()),
            strategy: strategy.clone(),
            market: market.clone(),
            price: Decimal::new(50, 2),
            qty: Decimal::from(10),
        };
        let mut state = LiveRiskState::default();
        state.apply_account_event(
            &pmkit_event::PmAccountEvent::Fill {
                identity: pmkit_event::FillIdentity::Venue("trade-1".into()),
                strategy: Some(strategy),
                order_id: order.order_id.0.clone(),
                market: market.clone(),
                outcome: Outcome::Up,
                price: Decimal::new(51, 2),
                size: Decimal::from(4),
                side: pmkit_book::Side::Buy,
                fee: Decimal::new(4, 2),
                liquidity: pmkit_event::Liquidity::Maker,
                timestamp_ms: 1_001,
            },
            &limits,
        )?;
        let details = OrderStatusDetails {
            filled_qty: Some(Decimal::from(10)),
            price: Some(Decimal::new(52, 2)),
            fee: Some(Decimal::new(10, 2)),
            settlement_reference: None,
        };

        let exact = OrderStatusDetails {
            filled_qty: Some(Decimal::from(4)),
            price: Some(Decimal::new(99, 2)),
            fee: None,
            settlement_reference: None,
        };

        // When: exact status quantity has no fee, then a later total has no durable economics.
        assert_eq!(apply_status_fill(&order, &exact, &state)?, Decimal::from(4));
        let result = apply_status_fill(&order, &details, &state);

        // Then: recovery fails closed without inventing price, fee, or trade identity.
        assert!(result.is_err());
        assert_eq!(state.filled_qty("venue-delta"), Decimal::from(4));
        assert_eq!(state.fees_for_order("venue-delta"), Decimal::new(4, 2));
        assert_eq!(state.fill_count(), 1);
        assert_eq!(state.positions(&market)[0].qty, Decimal::from(4));
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn cancelled_status_with_unmatched_fill_total_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: three exact durable shares for an accepted order later cancelled at four filled.
        let dir = tempfile::tempdir()?;
        let store = TursoTapeStore::open_local(dir.path().join("cancelled-unmatched.db")).await?;
        let run = restart_run(
            "cancelled-unmatched",
            RestartExecutor {
                open_orders: Vec::new(),
                status: OrderStatus::Cancelled(OrderStatusDetails {
                    filled_qty: Some(Decimal::from(4)),
                    price: Some(Decimal::new(52, 2)),
                    fee: None,
                    settlement_reference: None,
                }),
            },
        )?;
        let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
        let correlation =
            strategy_correlation_id(&StrategyId::new("maker")?, &MarketId::new("btc-5m")?, 1_000);
        let intent = recovery_identity(&scope, &format!("{correlation}:0"));
        store
            .store_intent_pending(&intent, &accepted_intent_payload())
            .await?;
        store
            .transition_intent_with_order(&intent, IntentOutcome::Accepted, Some("venue-cancelled"))
            .await?;
        let limits = risk()?;
        let mut state = LiveRiskState::default();
        state.apply_account_event(
            &pmkit_event::PmAccountEvent::Fill {
                identity: pmkit_event::FillIdentity::Venue("trade-1".into()),
                strategy: Some(StrategyId::new("maker")?),
                order_id: "venue-cancelled".into(),
                market: MarketId::new("btc-5m")?,
                outcome: Outcome::Up,
                price: Decimal::new(41, 2),
                size: Decimal::from(3),
                side: pmkit_book::Side::Buy,
                fee: Decimal::new(3, 2),
                liquidity: pmkit_event::Liquidity::Taker,
                timestamp_ms: 1_001,
            },
            &limits,
        )?;

        // When: accepted-order reconstruction checks the terminal venue status.
        let result = reconstruct_accepted_orders(&run, &config()?, &store, &state).await;

        // Then: startup aborts instead of closing over one unmatched synthetic share.
        assert!(matches!(result, Err(StartError::Storage { .. })));
        assert_eq!(state.filled_qty("venue-cancelled"), Decimal::from(3));
        store.delete_database()?;
        Ok(())
    }
}

#[cfg(test)]
mod rate_limit_tests {
    use super::{
        OrderRateLimits, accepted_submissions, drive_with_rate_limits, strategy_correlation_id,
    };
    use crate::test_support::{config, risk};
    use async_trait::async_trait;
    use pmkit_book::Side;
    use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
    use pmkit_data::{DataSourceError, LiveDataSource, SourceSignal};
    use pmkit_event::MarketEvent;
    use pmkit_exec::{
        ExecError, ExecutionSnapshot, Executor, OrderId, OrderStatus, OrderStatusDetails,
        PlaceOrder, TimeInForce,
    };
    use pmkit_market::Outcome;
    use pmkit_runtime::StrategyRegistration;
    use pmkit_spec::LiveRun;
    use pmkit_store::{
        CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, TapeStore, TursoTapeStore,
    };
    use pmkit_strategy::{
        Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
    };
    use rust_decimal::Decimal;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc::Sender;

    #[derive(Default)]
    struct RateExecutor {
        submissions: AtomicUsize,
    }

    #[async_trait]
    impl Executor for RateExecutor {
        async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
            Ok(ExecutionSnapshot::default())
        }

        async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
            Ok(ExecutionSnapshot::default())
        }

        async fn query_status(&self, _order_id: &OrderId) -> Result<OrderStatus, ExecError> {
            Ok(OrderStatus::Cancelled(OrderStatusDetails {
                filled_qty: Some(Decimal::ZERO),
                price: None,
                fee: None,
                settlement_reference: None,
            }))
        }

        async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
            let sequence = self.submissions.fetch_add(1, Ordering::Relaxed);
            Ok(OrderId(format!("rate-{sequence}")))
        }

        async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
            Ok(())
        }

        async fn cancel_all(&self) -> Result<(), ExecError> {
            Ok(())
        }
    }

    struct RateSource {
        timestamps_ms: Vec<i64>,
    }

    #[async_trait]
    impl LiveDataSource for RateSource {
        async fn subscribe(
            &self,
            market: MarketId,
            outcome: Outcome,
            sink: Sender<SourceSignal>,
        ) -> Result<(), DataSourceError> {
            if outcome == Outcome::Up {
                for &timestamp_ms in &self.timestamps_ms {
                    sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                        market: market.clone(),
                        outcome,
                        bids: vec![(Decimal::new(49, 2), Decimal::from(50))],
                        asks: vec![(Decimal::new(51, 2), Decimal::from(50))],
                        timestamp_ms,
                    }))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                }
            }
            sink.send(SourceSignal::Watermark(i64::MAX))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            sink.send(SourceSignal::Eof)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            Ok(())
        }
    }

    struct RateStrategy;

    impl Strategy for RateStrategy {
        fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
            Ok(Actions::place(PlaceOrder {
                market: context.market.clone(),
                outcome: Outcome::Up,
                side: Side::Buy,
                price: Decimal::new(50, 2),
                qty: Decimal::from(10),
                post_only: false,
                tif: TimeInForce::Gtc,
            }))
        }
    }

    struct RateFactory;

    impl StrategyFactory for RateFactory {
        fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
            Ok(Box::new(RateStrategy))
        }
    }

    fn rate_run(
        run_id: &str,
        timestamps_ms: Vec<i64>,
        executor: Arc<RateExecutor>,
    ) -> Result<LiveRun, Box<dyn std::error::Error>> {
        Ok(LiveRun::new(
            RunId::new(run_id)?,
            PortfolioId::new("rate-portfolio")?,
            executor,
            Arc::new(RateSource { timestamps_ms }),
            risk()?,
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("rate-strategy")?,
            MarketId::new("btc-5m")?,
            Arc::new(RateFactory),
        )))
    }

    #[tokio::test]
    async fn rate_limit_allows_within_window() -> Result<(), Box<dyn std::error::Error>> {
        // Given: two orders inside a logical-time window whose limit is two.
        let executor = Arc::new(RateExecutor::default());
        let run = rate_run("rate-burst", vec![1_000, 1_050], Arc::clone(&executor))?;
        let rate_limits = OrderRateLimits::new(2, 2, 100).ok_or("invalid rate limits")?;

        // When: the live risk gate evaluates the burst.
        let report = drive_with_rate_limits(&run, &config()?, None, rate_limits).await?;

        // Then: every order within the configured allowance reaches the executor.
        assert_eq!(executor.submissions.load(Ordering::Relaxed), 2);
        assert_eq!(report.rejected, 0);
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn rate_limit_rejects_over_and_survives_restart() -> Result<(), Box<dyn std::error::Error>>
    {
        // Given: two accepted submissions durably recorded in [1_000, 1_100].
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("live-rate-restart.db");
        let store = TursoTapeStore::open_local(&path).await?;
        let rate_limits = OrderRateLimits::new(2, 2, 100).ok_or("invalid rate limits")?;
        let first_executor = Arc::new(RateExecutor::default());
        let first_run = rate_run(
            "rate-restart",
            vec![1_000, 1_050],
            Arc::clone(&first_executor),
        )?;
        drive_with_rate_limits(&first_run, &config()?, Some(&store), rate_limits).await?;

        // When: a restarted driver sees one order at the inclusive end and one after it.
        let restarted_executor = Arc::new(RateExecutor::default());
        let restarted_run = rate_run(
            "rate-restart",
            vec![1_100, 1_101],
            Arc::clone(&restarted_executor),
        )?;
        let report =
            drive_with_rate_limits(&restarted_run, &config()?, Some(&store), rate_limits).await?;

        // Then: N+1 is rejected and counted; the next logical millisecond resets the window.
        assert_eq!(first_executor.submissions.load(Ordering::Relaxed), 2);
        assert_eq!(restarted_executor.submissions.load(Ordering::Relaxed), 1);
        assert_eq!(report.rejected, 1);
        store.delete_database()?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn accepted_intents_fill_decision_crash_window_without_double_counting()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: accepted and rejected intents on both sides of decision persistence.
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("live-rate-accepted-intents.db");
        let store = TursoTapeStore::open_local(&path).await?;
        let run = rate_run(
            "rate-accepted-intents",
            Vec::new(),
            Arc::new(RateExecutor::default()),
        )?;
        let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
        let strategy = StrategyId::new("rate-strategy")?;
        let market = MarketId::new("btc-5m")?;
        let first_correlation = strategy_correlation_id(&strategy, &market, 1_000);
        let rejected_correlation = strategy_correlation_id(&strategy, &market, 1_075);
        for (correlation_id, timestamp_ms, ingest_sequence) in [
            (first_correlation.clone(), 1_000, 1),
            (rejected_correlation.clone(), 1_075, 3),
        ] {
            store
                .store_decision(&CausalDecision {
                    identity: CausalIdentity {
                        scope: scope.clone(),
                        correlation_id,
                        source_timestamp_ms: timestamp_ms,
                        ingest_sequence,
                    },
                    payload: json!({
                        "snapshot": {"timing": {"decision_ms": timestamp_ms}},
                        "decision": {
                            "kind": "actions",
                            "risk": [{"action_index": 0, "verdict": {"kind": "accepted"}}]
                        }
                    }),
                })
                .await?;
        }
        for (correlation_id, timestamp_ms, ingest_sequence, outcome) in [
            (
                format!("{first_correlation}:0"),
                1_000,
                1,
                IntentOutcome::Accepted,
            ),
            (
                format!("{}:0", strategy_correlation_id(&strategy, &market, 1_050)),
                1_050,
                2,
                IntentOutcome::Accepted,
            ),
            (
                format!("{rejected_correlation}:0"),
                1_075,
                3,
                IntentOutcome::Rejected,
            ),
            (
                format!("{}:0", strategy_correlation_id(&strategy, &market, 1_090)),
                1_090,
                4,
                IntentOutcome::Rejected,
            ),
        ] {
            let intent = CausalIdentity {
                scope: scope.clone(),
                correlation_id,
                source_timestamp_ms: timestamp_ms,
                ingest_sequence,
            };
            store
                .store_intent_pending(&intent, &json!({"submitted_ms": timestamp_ms}))
                .await?;
            let venue_order_id = format!("venue-{ingest_sequence}");
            store
                .transition_intent_with_order(&intent, outcome, Some(&venue_order_id))
                .await?;
        }

        // When: accepted submission history is reconstructed after the crash window.
        let submissions = accepted_submissions(&store, &scope, run.strategies()).await?;

        // Then: every admitted intent contributes, and decision-backed states contribute once.
        assert_eq!(submissions.len(), 4);
        assert!(submissions.contains(&(Some(strategy.clone()), 1_000)));
        assert!(submissions.contains(&(Some(strategy.clone()), 1_050)));
        assert!(submissions.contains(&(Some(strategy.clone()), 1_075)));
        assert!(submissions.contains(&(Some(strategy), 1_090)));
        store.delete_database()?;
        Ok(())
    }
}

#[cfg(test)]
mod risk_reconstruction_tests {
    use super::{DurableFillAuthority, reconstruct_risk_state};
    use crate::test_support::risk;
    use pmkit_book::OrderBookL2;
    use pmkit_core::{MarketId, PortfolioId, RunId};
    use pmkit_market::Outcome;
    use pmkit_store::{
        OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, StoreError, TapeStore, TursoTapeStore,
    };
    use rust_decimal::Decimal;
    use serde_json::{Value, json};

    fn account_envelope(scope: &OwnerScope, sequence: i64, payload: &Value) -> PmEnvelope {
        PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope: scope.clone(),
            venue_id: "polymarket".into(),
            config_hash: "runtime".into(),
            source_id: "account".into(),
            connection_id: "account-1".into(),
            source_timestamp_ms: sequence * 1_000,
            canonical_source_rank: 0,
            connection_epoch: 1,
            frame_sequence: sequence,
            receipt_timestamp_ms: sequence * 1_000,
            ingest_sequence: sequence,
            raw_frame: Vec::new(),
            normalized: json!({
                "schema_version": 1,
                "portfolio": scope.portfolio_id.to_string(),
                "payload": payload,
            }),
        }
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn risk_state_reconstructs_from_durable() -> Result<(), Box<dyn std::error::Error>> {
        // Given: one owner-scoped fill and partial settlement in durable canonical order.
        let dir = tempfile::tempdir()?;
        let store = TursoTapeStore::open_local(dir.path().join("risk-restart.db")).await?;
        let scope = OwnerScope::new(
            PortfolioId::new("risk-portfolio")?,
            RunId::new("risk-restart")?,
        );
        let market = MarketId::new("btc-5m")?;
        store
            .store_envelope(&account_envelope(
                &scope,
                1,
                &json!({
                    "kind": "fill",
                    "ts": 1_000,
                    "strategy": null,
                    "order_id": "venue-1",
                    "market": market.to_string(),
                    "outcome": "up",
                    "price": "0.4",
                    "size": "10",
                    "side": "buy",
                    "fee": "0.1",
                    "liquidity": "taker",
                }),
            ))
            .await?;
        store
            .store_envelope(&account_envelope(
                &scope,
                2,
                &json!({
                    "kind": "settlement",
                    "ts": 2_000,
                    "market": market.to_string(),
                    "outcome": "up",
                    "settled_size": "4",
                    "proceeds": "4",
                }),
            ))
            .await?;
        let limits = risk()?;

        // When: startup reconstructs a fresh ledger and the first book marks it.
        let mut state =
            reconstruct_risk_state(&store, &scope, &limits, DurableFillAuthority::Account).await?;
        state.update_book(
            &market,
            Outcome::Up,
            &OrderBookL2 {
                bids: vec![(Decimal::new(5, 1), Decimal::ONE)],
                asks: vec![(Decimal::new(5, 1), Decimal::ONE)],
                timestamp_ms: 3_000,
                last_trade_price: None,
            },
            &limits,
        );

        // Then: position, exposure, accumulated PnL, and fill count equal fresh replay.
        let position = state
            .positions(&market)
            .first()
            .ok_or("missing reconstructed position")?;
        assert_eq!(position.qty, Decimal::from(6));
        assert_eq!(state.portfolio_notional(), Decimal::from(3));
        assert_eq!(state.market_notional(&market), Decimal::from(3));
        assert_eq!(state.realized_pnl(), Decimal::new(24, 1));
        assert_eq!(state.daily_pnl(), Some(Decimal::new(29, 1)));
        assert_eq!(state.fill_count(), 1);
        store.delete_database()?;
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the store must remain open through replay assertions and database deletion"
    )]
    async fn risk_state_replays_only_exact_fill_identities()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: two distinct same-value fills and one exact identity replay in durable order.
        let dir = tempfile::tempdir()?;
        let store = TursoTapeStore::open_local(dir.path().join("fill-identity-replay.db")).await?;
        let scope = OwnerScope::new(
            PortfolioId::new("risk-portfolio")?,
            RunId::new("fill-identity-replay")?,
        );
        let market = MarketId::new("btc-5m")?;
        for (sequence, fill_id) in [(1, "fill-1"), (2, "fill-2"), (3, "fill-1")] {
            let mut envelope = account_envelope(
                &scope,
                sequence,
                &json!({
                    "kind": "fill",
                    "ts": 1_000,
                    "identity": { "source": "venue", "id": fill_id },
                    "strategy": null,
                    "order_id": "venue-1",
                    "market": market.to_string(),
                    "outcome": "up",
                    "price": "0.4",
                    "size": "10",
                    "side": "buy",
                    "fee": "0.1",
                    "liquidity": "taker",
                }),
            );
            envelope.normalized["schema_version"] = json!(3);
            store.store_envelope(&envelope).await?;
        }

        // When: startup reconstructs the authoritative ledger.
        let state =
            reconstruct_risk_state(&store, &scope, &risk()?, DurableFillAuthority::Account).await?;

        // Then: the repeated identity applies once and the distinct identity applies separately.
        let position = state
            .positions(&market)
            .first()
            .ok_or("missing reconstructed position")?;
        assert_eq!(position.qty, Decimal::from(20));
        assert_eq!(state.fill_count(), 2);
        store.delete_database()?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn risk_state_replays_market_fill_without_account_authority()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: one exact PM market fill durably recorded without an account owner field.
        let dir = tempfile::tempdir()?;
        let store = TursoTapeStore::open_local(dir.path().join("market-fill-restart.db")).await?;
        let scope = OwnerScope::new(
            PortfolioId::new("risk-portfolio")?,
            RunId::new("market-fill-restart")?,
        );
        let market = MarketId::new("btc-5m")?;
        let mut envelope = account_envelope(
            &scope,
            1,
            &json!({
                "kind": "fill",
                "ts": 1_000,
                "strategy": null,
                "order_id": "venue-1",
                "market": market.to_string(),
                "outcome": "up",
                "price": "0.4",
                "size": "3",
                "side": "buy",
                "fee": "0.1",
                "liquidity": "taker",
            }),
        );
        envelope.source_id = "polymarket:market-ws".into();
        envelope
            .normalized
            .as_object_mut()
            .ok_or("normalized object")?
            .remove("portfolio");
        store.store_envelope(&envelope).await?;

        // When: startup reconstructs a run with no authoritative account source.
        let state =
            reconstruct_risk_state(&store, &scope, &risk()?, DurableFillAuthority::Market).await?;

        // Then: the exact durable market fill is the restart authority too.
        assert_eq!(state.fill_count(), 1);
        assert_eq!(state.positions(&market)[0].qty, Decimal::from(3));
        store.delete_database()?;
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::significant_drop_tightening)]
    async fn risk_state_corrupt_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        // Given: an owner-scoped settlement with no matching durable position context.
        let dir = tempfile::tempdir()?;
        let store = TursoTapeStore::open_local(dir.path().join("risk-corrupt.db")).await?;
        let scope = OwnerScope::new(
            PortfolioId::new("risk-portfolio")?,
            RunId::new("risk-corrupt")?,
        );
        store
            .store_envelope(&account_envelope(
                &scope,
                1,
                &json!({
                    "kind": "settlement",
                    "ts": 1_000,
                    "market": "btc-5m",
                    "outcome": "up",
                    "settled_size": "1",
                    "proceeds": "1",
                }),
            ))
            .await?;

        // When: startup attempts authoritative reconstruction.
        let result =
            reconstruct_risk_state(&store, &scope, &risk()?, DurableFillAuthority::Account).await;

        // Then: startup fails closed through the typed storage error path.
        assert!(matches!(result, Err(StoreError::Storage { .. })));
        store.delete_database()?;
        Ok(())
    }
}
