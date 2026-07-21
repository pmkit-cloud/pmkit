use super::{LiveReport, StartError, StrategyInstance, instantiate_strategies};
use pmkit_book::{OrderBookL2, Position};
use pmkit_core::MarketId;
use pmkit_event::MarketEvent;
use pmkit_exec::{ExecError, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_runtime::RiskLimits;
use pmkit_spec::LiveRun;
use pmkit_strategy::{Action, LogicalTimestamp, StrategyContext};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};

#[must_use]
pub fn passes_risk(
    order: &PlaceOrder,
    limits: &RiskLimits,
    positions: &[pmkit_book::Position],
    portfolio_unrealized_pnl: Option<Decimal>,
) -> bool {
    let Some(portfolio_unrealized_pnl) = portfolio_unrealized_pnl else {
        return false;
    };
    if portfolio_unrealized_pnl <= -limits.max_loss.as_decimal() {
        return false;
    }
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

#[must_use]
pub fn mark_positions(
    positions_by_market: &mut HashMap<MarketId, Vec<Position>>,
    marks: &HashMap<(MarketId, Outcome), Decimal>,
) -> Option<Decimal> {
    let mut portfolio_unrealized_pnl = Decimal::ZERO;
    for (market, positions) in positions_by_market {
        for position in positions {
            if position.qty.is_zero() {
                position.unrealized_pnl = Decimal::ZERO;
                continue;
            }
            let mark = *marks.get(&(market.clone(), position.outcome))?;
            position.unrealized_pnl = position.qty * (mark - position.avg_entry);
            portfolio_unrealized_pnl += position.unrealized_pnl;
        }
    }
    Some(portfolio_unrealized_pnl)
}

#[derive(Default)]
struct LiveRiskState {
    positions_by_market: HashMap<MarketId, Vec<Position>>,
    marks: HashMap<(MarketId, Outcome), Decimal>,
    loss_breached: bool,
}

impl LiveRiskState {
    fn positions(&self, market: &MarketId) -> &[Position] {
        self.positions_by_market
            .get(market)
            .map_or(&[][..], Vec::as_slice)
    }

    fn update_book(
        &mut self,
        market: &MarketId,
        outcome: Outcome,
        book: &OrderBookL2,
        limits: &RiskLimits,
    ) -> Option<Decimal> {
        match book.mid_price() {
            Some(mark) => {
                self.marks.insert((market.clone(), outcome), mark);
            }
            None => {
                self.marks.remove(&(market.clone(), outcome));
            }
        }
        self.refresh_marks(limits)
    }

    fn apply_fill(&mut self, event: &MarketEvent, limits: &RiskLimits) {
        let MarketEvent::Fill {
            market,
            outcome,
            side,
            price,
            size,
            ..
        } = event
        else {
            return;
        };
        pmkit_book::book::apply_fill(
            self.positions_by_market.entry(market.clone()).or_default(),
            *outcome,
            *side,
            *price,
            *size,
        );
        self.refresh_marks(limits);
    }

    fn refresh_marks(&mut self, limits: &RiskLimits) -> Option<Decimal> {
        let portfolio_unrealized_pnl = mark_positions(&mut self.positions_by_market, &self.marks);
        if portfolio_unrealized_pnl.is_some_and(|pnl| pnl <= -limits.max_loss.as_decimal()) {
            self.loss_breached = true;
        }
        portfolio_unrealized_pnl
    }
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

    // ponytail: v0 live execution omits tape and realized PnL accounting.
    let mut risk_state = LiveRiskState::default();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let mut rejected = 0_usize;

    while let Some(event) = event_rx.recv().await {
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
                let portfolio_unrealized_pnl =
                    risk_state.update_book(market, *outcome, &book, &limits);
                for (registered_market, strategy) in &mut *strategies {
                    if *registered_market != *market {
                        continue;
                    }
                    let market_positions = risk_state.positions(market);
                    let context = StrategyContext {
                        market,
                        book: &book,
                        positions: market_positions,
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
            MarketEvent::Fill { .. } => {
                risk_state.apply_fill(&event, &limits);
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
