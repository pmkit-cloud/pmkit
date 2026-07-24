use pmkit_book::Position;
use pmkit_core::MarketId;
use pmkit_event::MarketEvent;
use pmkit_exec::PlaceOrder;
use pmkit_market::Outcome;
use pmkit_runtime::RiskLimits;
use rust_decimal::Decimal;
use std::collections::HashMap;

#[must_use]
pub fn passes_risk(
    order: &PlaceOrder,
    limits: &RiskLimits,
    positions: &[Position],
    portfolio_unrealized_pnl: Option<Decimal>,
) -> bool {
    let Some(portfolio_unrealized_pnl) = portfolio_unrealized_pnl else {
        return false;
    };
    if portfolio_unrealized_pnl <= -limits.max_loss.as_decimal()
        || order.qty * order.price > limits.max_order_notional.as_decimal()
    {
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

/// Aggregated exposure checked before one live venue submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortfolioRiskExposure {
    /// Current marked position and reserved-order notional.
    pub portfolio_notional: Decimal,
    /// Current marked position and reserved-order notional for the order market.
    pub market_notional: Decimal,
    /// Current reserved notional attributable to the strategy.
    pub strategy_notional: Decimal,
    /// Current daily portfolio profit and loss.
    pub daily_pnl: Decimal,
    /// Current open order count.
    pub open_orders: usize,
}

#[must_use]
pub fn passes_aggregated_risk(
    order: &PlaceOrder,
    limits: &RiskLimits,
    positions: &[Position],
    exposure: PortfolioRiskExposure,
) -> bool {
    let order_notional = order.qty * order.price;
    passes_risk(order, limits, positions, Some(exposure.daily_pnl))
        && exposure.portfolio_notional + order_notional
            <= limits.max_portfolio_notional.as_decimal()
        && exposure.market_notional + order_notional <= limits.max_market_notional.as_decimal()
        && exposure.strategy_notional + order_notional <= limits.max_strategy_notional.as_decimal()
        && exposure.daily_pnl > -limits.max_daily_loss.as_decimal()
        && exposure.open_orders
            < usize::try_from(limits.max_open_orders.get()).unwrap_or(usize::MAX)
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
pub(super) struct LiveRiskState {
    positions_by_market: HashMap<MarketId, Vec<Position>>,
    marks: HashMap<(MarketId, Outcome), Decimal>,
    pub(super) loss_breached: bool,
}

impl LiveRiskState {
    pub(super) fn positions(&self, market: &MarketId) -> &[Position] {
        self.positions_by_market
            .get(market)
            .map_or(&[][..], Vec::as_slice)
    }

    pub(super) fn update_book(
        &mut self,
        market: &MarketId,
        outcome: Outcome,
        book: &pmkit_book::OrderBookL2,
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

    pub(super) fn apply_fill(&mut self, event: &MarketEvent, limits: &RiskLimits) {
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

    pub(super) fn portfolio_notional(&self) -> Decimal {
        self.positions_by_market
            .iter()
            .map(|(market, positions)| self.marked_notional(market, positions))
            .sum()
    }

    pub(super) fn market_notional(&self, market: &MarketId) -> Decimal {
        self.positions_by_market
            .get(market)
            .map_or(Decimal::ZERO, |positions| {
                self.marked_notional(market, positions)
            })
    }

    fn marked_notional(&self, market: &MarketId, positions: &[Position]) -> Decimal {
        positions
            .iter()
            .map(|position| {
                self.marks
                    .get(&(market.clone(), position.outcome))
                    .map_or(Decimal::ZERO, |mark| position.qty.abs() * *mark)
            })
            .sum()
    }

    fn refresh_marks(&mut self, limits: &RiskLimits) -> Option<Decimal> {
        let portfolio_unrealized_pnl = mark_positions(&mut self.positions_by_market, &self.marks);
        if portfolio_unrealized_pnl.is_some_and(|pnl| pnl <= -limits.max_loss.as_decimal()) {
            self.loss_breached = true;
        }
        portfolio_unrealized_pnl
    }
}
