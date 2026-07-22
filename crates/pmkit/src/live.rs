use super::{
    LiveReport, StartError, StrategyInstance, instantiate_strategies,
    store_signal as persist_signal,
};
use pmkit_book::OrderBookL2;
use pmkit_event::{MarketEvent, SourceEnvelope, StrategyFact};
use pmkit_exec::{ExecError, OrderId};
use pmkit_market::Outcome;
use pmkit_runtime::{LiveOrderPolicy, RuntimeConfig};
use pmkit_spec::LiveRun;
use pmkit_store::{OwnerScope, TapeStore};
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

fn subscribe(
    run: &LiveRun,
    strategies: &[StrategyInstance],
) -> tokio::sync::mpsc::Receiver<pmkit_data::SourceSignal> {
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(1024);
    let mut subscribed = HashSet::new();
    for (market, _) in strategies {
        if !subscribed.insert(market.clone()) {
            continue;
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let source = run.market_data().clone();
            let sink = event_tx.clone();
            let market = market.clone();
            tokio::spawn(async move { source.subscribe(market, outcome, sink).await });
        }
    }
    drop(event_tx);
    event_rx
}

fn market_event(signal: pmkit_data::SourceSignal) -> Option<MarketEvent> {
    let pmkit_data::SourceSignal::Data(envelope) = signal else {
        return None;
    };
    let SourceEnvelope::PmMarket(envelope) = *envelope else {
        return None;
    };
    Some(envelope.fact)
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
    let mut event_rx = subscribe(run, &strategies);
    let mut tape = LiveTape::open(run, runtime)?;
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());

    let mut risk_state = LiveRiskState::default();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let mut rejected = 0_usize;
    while let Some(signal) = event_rx.recv().await {
        store_signal(run, store, &scope, &signal).await?;
        let Some(event) = market_event(signal) else {
            continue;
        };
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
                        for action in actions.as_slice() {
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
                                match executor.submit(order, *timestamp_ms).await {
                                    Ok(order_id) => {
                                        open_orders.insert(order_id);
                                    }
                                    Err(source @ ExecError::Transport { .. }) => {
                                        reconcile_open_orders(run, runtime).await?;
                                        tape.flush(run)?;
                                        return Err(StartError::ExecutionState {
                                            run: run.id().clone(),
                                            source,
                                        });
                                    }
                                    Err(
                                        ExecError::Rejected { .. } | ExecError::NotFound { .. },
                                    ) => {}
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
