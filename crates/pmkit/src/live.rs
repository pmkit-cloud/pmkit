use super::{
    LiveReport, RunControl, RunLifecycleEvent, StartError, StrategyInstance,
    instantiate_strategies, store_signal as persist_signal,
};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_book::OrderBookL2;
use pmkit_event::{MarketEvent, PmAccountEvent, SourceEnvelope, StrategyFact};
use pmkit_exec::{ExecError, Executor, OrderId, OrderStatus, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_runtime::{LiveOrderPolicy, RuntimeConfig};
use pmkit_spec::LiveRun;
use pmkit_store::{CausalIdentity, OwnerScope, StoreError, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use std::collections::{HashMap, HashSet};

#[path = "live_risk.rs"]
mod live_risk;
#[path = "live_tape.rs"]
mod live_tape;
#[cfg(test)]
pub use live_risk::mark_positions;
use live_risk::{LiveRiskState, PortfolioRiskExposure, passes_aggregated_risk};
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
        let Some(order_id) = intent.venue_order_id() else {
            continue;
        };
        let Ok(status) = tokio::time::timeout(
            runtime.shutdown.reconciliation_timeout,
            run.executor().query_status(&order_id),
        )
        .await
        else {
            continue;
        };
        let Ok(status) = status else {
            continue;
        };
        let outcome = match status {
            OrderStatus::Open(_) | OrderStatus::Accepted(_) => {
                Some(pmkit_store::IntentOutcome::Accepted)
            }
            OrderStatus::Rejected(_) | OrderStatus::Cancelled(_) => {
                Some(pmkit_store::IntentOutcome::Rejected)
            }
            OrderStatus::Unknown(_) => None,
        };
        if let Some(outcome) = outcome {
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

fn report(run: &LiveRun, counts: [usize; 3]) -> LiveReport {
    LiveReport {
        run: run.id().clone(),
        events_processed: counts[0],
        fills: counts[1],
        rejected: counts[2],
    }
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
    notional: rust_decimal::Decimal,
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

#[expect(
    clippy::too_many_lines,
    reason = "the live run owns one ordered risk, tape, storage, and shutdown lifecycle"
)]
pub async fn drive_with_control(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    store: Option<&dyn TapeStore>,
    control: &RunControl,
) -> Result<LiveReport, StartError> {
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
    let max_open_orders = usize::try_from(limits.max_open_orders.get()).unwrap_or(usize::MAX);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1024);
    let feed = MergedFeed::from_tasks(FeedMode::Live, sources(run, &strategies), None);
    let merge = tokio::spawn(async move { feed.forward(event_tx).await });
    let mut tape = LiveTape::open(run, runtime)?;
    if let Some(store) = store {
        recover_intents(run, runtime, store, &scope).await?;
    }

    let mut risk_state = LiveRiskState::default();
    let mut reservations: HashMap<String, Reservation> = HashMap::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let mut rejected = 0_usize;
    let mut cex_metrics = crate::causal::CexTradeMetricsState::default();
    control.emit(RunLifecycleEvent::Started {
        run: run.id().clone(),
    });
    if control.is_cancelled() {
        control.emit(RunLifecycleEvent::Cancelled {
            run: run.id().clone(),
        });
        return finish(
            run,
            runtime,
            &open_orders,
            &mut tape,
            [events_processed, fills, rejected],
        )
        .await;
    }
    while let Some(merged) = event_rx.recv().await {
        if control.is_cancelled() {
            control.emit(RunLifecycleEvent::Cancelled {
                run: run.id().clone(),
            });
            return finish(
                run,
                runtime,
                &open_orders,
                &mut tape,
                [events_processed, fills, rejected],
            )
            .await;
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
            match &envelope.fact {
                PmAccountEvent::Fill { order_id, .. }
                | PmAccountEvent::OrderCancelled { order_id, .. }
                | PmAccountEvent::OrderRejected { order_id, .. } => {
                    reservations.remove(order_id);
                    open_orders.remove(&OrderId(order_id.clone()));
                }
                PmAccountEvent::OrderAck { .. } | PmAccountEvent::OrderStatus { .. } => {}
            }
            continue;
        }
        let SourceEnvelope::PmMarket(envelope) = merged.source else {
            continue;
        };
        let event = envelope.fact;
        tape.append(run, &event)?;
        events_processed += 1;
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
                let portfolio_unrealized_pnl =
                    risk_state.update_book(market, *outcome, &book, &limits);
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
                let mut verdicts: Vec<crate::causal::ActionRiskVerdict> = Vec::new();
                for instance in &mut *strategies {
                    if instance.market != *market {
                        continue;
                    }
                    let market_positions = risk_state.positions(market);
                    let context = StrategyContext {
                        fact: &fact,
                        market,
                        book: &book,
                        positions: market_positions,
                        now: LogicalTimestamp::from_millis(*timestamp_ms),
                    };
                    if let Ok(actions) = instance.strategy.on_event(context) {
                        for (action_index, action) in actions.as_slice().iter().enumerate() {
                            if let Action::Place(order) = action {
                                if open_orders.len() >= max_open_orders {
                                    open_orders = reconcile_open_orders(run, runtime).await?;
                                }
                                if open_orders.len() >= max_open_orders {
                                    verdicts.push(crate::causal::ActionRiskVerdict::rejected(
                                        u32::try_from(action_index).unwrap_or(u32::MAX),
                                        "open order capacity",
                                    ));
                                    rejected += 1;
                                    continue;
                                }
                                let reserved_portfolio: rust_decimal::Decimal = reservations
                                    .values()
                                    .map(|reservation| reservation.notional)
                                    .sum();
                                let reserved_market: rust_decimal::Decimal = reservations
                                    .values()
                                    .filter(|reservation| reservation.market == *market)
                                    .map(|reservation| reservation.notional)
                                    .sum();
                                let reserved_strategy: rust_decimal::Decimal = reservations
                                    .values()
                                    .filter(|reservation| reservation.strategy == instance.id)
                                    .map(|reservation| reservation.notional)
                                    .sum();
                                let exposure = portfolio_unrealized_pnl.map(|daily_pnl| {
                                    PortfolioRiskExposure {
                                        portfolio_notional: risk_state.portfolio_notional()
                                            + reserved_portfolio,
                                        market_notional: risk_state.market_notional(market)
                                            + reserved_market,
                                        strategy_notional: reserved_strategy,
                                        daily_pnl,
                                        open_orders: open_orders.len(),
                                    }
                                });
                                if risk_state.loss_breached
                                    || exposure.is_none_or(|exposure| {
                                        !passes_aggregated_risk(
                                            order,
                                            &limits,
                                            market_positions,
                                            exposure,
                                        )
                                    })
                                {
                                    verdicts.push(crate::causal::ActionRiskVerdict::rejected(
                                        u32::try_from(action_index).unwrap_or(u32::MAX),
                                        "risk gate",
                                    ));
                                    rejected += 1;
                                    continue;
                                }
                                let identity = CausalIdentity {
                                    scope: scope.clone(),
                                    correlation_id: format!("{market:?}:{timestamp_ms}"),
                                    source_timestamp_ms: envelope.metadata.source_time_ms,
                                    ingest_sequence: i64::try_from(
                                        envelope.metadata.ingest_sequence,
                                    )
                                    .unwrap_or(i64::MAX),
                                };
                                verdicts.push(crate::causal::ActionRiskVerdict::accepted(
                                    u32::try_from(action_index).unwrap_or(u32::MAX),
                                ));
                                let placement = place_order(
                                    store,
                                    executor.as_ref(),
                                    order,
                                    *timestamp_ms,
                                    &identity,
                                    u32::try_from(action_index).unwrap_or(u32::MAX),
                                )
                                .await;
                                match placement {
                                    Ok(Some(order_id)) => {
                                        reservations.insert(
                                            order_id.0.clone(),
                                            Reservation {
                                                strategy: instance.id.clone(),
                                                market: market.clone(),
                                                notional: order.qty * order.price,
                                            },
                                        );
                                        open_orders.insert(order_id);
                                    }
                                    Ok(None) => {}
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
                }
                if let Some(store) = store {
                    let identity = CausalIdentity {
                        scope: scope.clone(),
                        correlation_id: format!("{market:?}:{timestamp_ms}"),
                        source_timestamp_ms: envelope.metadata.source_time_ms,
                        ingest_sequence: i64::try_from(envelope.metadata.ingest_sequence)
                            .unwrap_or(i64::MAX),
                    };
                    let snapshot =
                        crate::causal::DecisionSnapshot::from_book(&book, cex_metrics.snapshot());
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
            MarketEvent::Fill { .. } => {
                risk_state.apply_fill(&event, &limits);
                fills += 1;
            }
            _ => {}
        }
    }

    merge
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
        [events_processed, fills, rejected],
    )
    .await?;
    control.emit(RunLifecycleEvent::Completed {
        run: run.id().clone(),
    });
    Ok(report)
}

async fn finish(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    open_orders: &HashSet<OrderId>,
    tape: &mut LiveTape,
    counts: [usize; 3],
) -> Result<LiveReport, StartError> {
    tape.finish(run, runtime, open_orders).await?;
    Ok(report(run, counts))
}
