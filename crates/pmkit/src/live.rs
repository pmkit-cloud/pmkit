use super::{LiveReport, StartError, StrategyInstance, instantiate_strategies};
use pmkit_book::OrderBookL2;
use pmkit_event::MarketEvent;
use pmkit_exec::{ExecError, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_runtime::RiskLimits;
use pmkit_spec::LiveRun;
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use std::collections::HashSet;

#[must_use]
pub fn passes_risk(
    order: &PlaceOrder,
    limits: &RiskLimits,
    positions: &[pmkit_book::Position],
) -> bool {
    if order.qty * order.price > limits.max_order_notional.as_decimal() {
        return false;
    }
    let signed = match order.side {
        pmkit_book::Side::Buy => order.qty,
        pmkit_book::Side::Sell => -order.qty,
    };
    let held = positions
        .iter()
        .find(|position| position.outcome == order.outcome)
        .map(|position| position.qty)
        .unwrap_or_default();
    (held + signed).abs() * order.price <= limits.max_position_notional.as_decimal()
}

async fn initial_open_orders(run: &LiveRun) -> Result<HashSet<OrderId>, StartError> {
    run.executor()
        .preflight()
        .await
        .map_err(|source| StartError::ExecutionState {
            run: run.id().clone(),
            source,
        })?;
    reconcile_open_orders(run).await
}

async fn reconcile_open_orders(run: &LiveRun) -> Result<HashSet<OrderId>, StartError> {
    let snapshot =
        run.executor()
            .reconcile()
            .await
            .map_err(|source| StartError::ExecutionState {
                run: run.id().clone(),
                source,
            })?;
    Ok(snapshot.open_orders.into_iter().collect())
}

fn subscribe(
    run: &LiveRun,
    strategies: &[StrategyInstance],
) -> tokio::sync::mpsc::Receiver<MarketEvent> {
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

pub async fn drive(run: &LiveRun) -> Result<LiveReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;
    let executor = run.executor().clone();
    let limits = run.risk().clone();
    let mut open_orders = initial_open_orders(run).await?;
    let max_open_orders = usize::try_from(limits.max_open_orders.get()).unwrap_or(usize::MAX);
    let mut event_rx = subscribe(run, &strategies);

    // ponytail: v0 risk gate omits loss limits and tape.
    let mut positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let mut rejected = 0_usize;

    while let Some(event) = event_rx.recv().await {
        events_processed += 1;
        match &event {
            MarketEvent::BookUpdate {
                market,
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
                for (registered_market, strategy) in &mut *strategies {
                    if *registered_market != *market {
                        continue;
                    }
                    let context = StrategyContext {
                        market,
                        book: &book,
                        positions: &positions,
                        now: LogicalTimestamp::from_millis(*timestamp_ms),
                    };
                    if let Ok(actions) = strategy.on_event(context) {
                        for action in actions.as_slice() {
                            if let Action::Place(order) = action {
                                if open_orders.len() >= max_open_orders {
                                    open_orders = reconcile_open_orders(run).await?;
                                }
                                if open_orders.len() >= max_open_orders {
                                    rejected += 1;
                                    continue;
                                }
                                if !passes_risk(order, &limits, &positions) {
                                    rejected += 1;
                                    continue;
                                }
                                match executor.submit(order, *timestamp_ms).await {
                                    Ok(order_id) => {
                                        open_orders.insert(order_id);
                                    }
                                    Err(source @ ExecError::Transport { .. }) => {
                                        reconcile_open_orders(run).await?;
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
            MarketEvent::Fill {
                outcome,
                side,
                price,
                size,
                ..
            } => {
                pmkit_book::book::apply_fill(&mut positions, *outcome, *side, *price, *size);
                fills += 1;
            }
            _ => {}
        }
    }

    Ok(LiveReport {
        run: run.id().clone(),
        events_processed,
        fills,
        rejected,
    })
}
