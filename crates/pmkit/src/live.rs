use super::{
    LiveReport, StartError, StrategyInstance, instantiate_strategies,
    store_signal as persist_signal,
};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_book::OrderBookL2;
use pmkit_event::{MarketEvent, SourceEnvelope, StrategyFact};
use pmkit_exec::{ExecError, Executor, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_runtime::{LiveOrderPolicy, RuntimeConfig};
use pmkit_spec::LiveRun;
use pmkit_store::{CausalIdentity, OwnerScope, StoreError, TapeStore};
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use std::collections::HashSet;

#[path = "live_risk.rs"]
mod live_risk;
#[path = "live_tape.rs"]
mod live_tape;
use live_risk::LiveRiskState;
#[cfg(test)]
pub use live_risk::mark_positions;
pub use live_risk::passes_risk;
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
    for (market, _) in strategies {
        if !subscribed.insert(market.clone()) {
            continue;
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let source = run.market_data().clone();
            let market = market.clone();
            let name = format!("pm:{market:?}:{outcome:?}");
            sources.push(SourceTaskDefinition::new(name, move |sink| async move {
                source.subscribe(market, outcome, sink).await
            }));
        }
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
    let intent = recorder.intent(decision, action_index, order);
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

#[expect(
    clippy::too_many_lines,
    reason = "the live run owns one ordered risk, tape, storage, and shutdown lifecycle"
)]
pub async fn drive_with_store(
    run: &LiveRun,
    runtime: &RuntimeConfig,
    store: Option<&dyn TapeStore>,
) -> Result<LiveReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;
    let executor = run.executor().clone();
    let limits = run.risk().clone();
    let mut open_orders = initial_open_orders(run, runtime).await?;
    let max_open_orders = usize::try_from(limits.max_open_orders.get()).unwrap_or(usize::MAX);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1024);
    let feed = MergedFeed::from_tasks(FeedMode::Live, sources(run, &strategies), None);
    let merge = tokio::spawn(async move { feed.forward(event_tx).await });
    let mut tape = LiveTape::open(run, runtime)?;
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
    if let Some(store) = store {
        store
            .read_pending_intents(&scope)
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?;
        store
            .read_unknown_intents(&scope)
            .await
            .map_err(|source| StartError::Storage {
                run: run.id().clone(),
                source,
            })?;
    }

    let mut risk_state = LiveRiskState::default();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let mut rejected = 0_usize;
    while let Some(merged) = event_rx.recv().await {
        store_signal(
            run,
            store,
            &scope,
            &pmkit_data::SourceSignal::Data(Box::new(merged.source.clone())),
        )
        .await?;
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
                for (registered_market, strategy) in &mut *strategies {
                    if *registered_market != *market {
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
                    if let Ok(actions) = strategy.on_event(context) {
                        for (action_index, action) in actions.as_slice().iter().enumerate() {
                            if let Action::Place(order) = action {
                                if open_orders.len() >= max_open_orders {
                                    open_orders = reconcile_open_orders(run, runtime).await?;
                                }
                                if open_orders.len() >= max_open_orders {
                                    rejected += 1;
                                    continue;
                                }
                                if risk_state.loss_breached
                                    || !passes_risk(
                                        order,
                                        &limits,
                                        market_positions,
                                        portfolio_unrealized_pnl,
                                    )
                                {
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

    finish(
        run,
        runtime,
        &open_orders,
        &mut tape,
        [events_processed, fills, rejected],
    )
    .await
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
