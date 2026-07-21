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

    fn refresh_marks(&mut self, limits: &RiskLimits) -> Option<Decimal> {
        let portfolio_unrealized_pnl = mark_positions(&mut self.positions_by_market, &self.marks);
        if portfolio_unrealized_pnl.is_some_and(|pnl| pnl <= -limits.max_loss.as_decimal()) {
            self.loss_breached = true;
        }
        portfolio_unrealized_pnl
    }
}
