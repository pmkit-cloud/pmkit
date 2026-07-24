use pmkit_book::Position;
use pmkit_core::MarketId;
use pmkit_event::{MarketEvent, PmAccountEvent};
use pmkit_exec::PlaceOrder;
use pmkit_market::Outcome;
use pmkit_runtime::RiskLimits;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};

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

type FillIdentity = (
    String,
    MarketId,
    Outcome,
    pmkit_book::Side,
    Decimal,
    Decimal,
    i64,
);
type SettlementIdentity = (MarketId, Outcome, Decimal, Decimal, i64);

#[derive(Default)]
pub(super) struct LiveRiskState {
    positions_by_market: HashMap<MarketId, Vec<Position>>,
    marks: HashMap<(MarketId, Outcome), Decimal>,
    applied_fills: HashSet<FillIdentity>,
    applied_settlements: HashSet<SettlementIdentity>,
    realized_pnl: Decimal,
    fees: Decimal,
    fill_count: usize,
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
            order_id,
            market,
            outcome,
            side,
            price,
            size,
            fee,
            timestamp_ms,
            ..
        } = event
        else {
            return;
        };
        if !self.applied_fills.insert((
            order_id.clone(),
            market.clone(),
            *outcome,
            *side,
            *price,
            *size,
            *timestamp_ms,
        )) {
            return;
        }
        let positions = self.positions_by_market.entry(market.clone()).or_default();
        if let Some(position) = positions
            .iter()
            .find(|position| position.outcome == *outcome)
        {
            let closing_size = if (position.qty > Decimal::ZERO && *side == pmkit_book::Side::Sell)
                || (position.qty < Decimal::ZERO && *side == pmkit_book::Side::Buy)
            {
                position.qty.abs().min(*size)
            } else {
                Decimal::ZERO
            };
            self.realized_pnl += if position.qty > Decimal::ZERO {
                (*price - position.avg_entry) * closing_size
            } else {
                (position.avg_entry - *price) * closing_size
            };
        }
        pmkit_book::book::apply_fill(positions, *outcome, *side, *price, *size);
        self.fees += *fee;
        self.fill_count += 1;
        self.refresh_marks(limits);
    }

    pub(super) fn apply_account_event(&mut self, event: &PmAccountEvent, limits: &RiskLimits) {
        match event {
            PmAccountEvent::Fill {
                strategy,
                order_id,
                market,
                outcome,
                price,
                size,
                side,
                fee,
                liquidity,
                timestamp_ms,
            } => self.apply_fill(
                &MarketEvent::Fill {
                    strategy: strategy.clone(),
                    order_id: order_id.clone(),
                    market: market.clone(),
                    outcome: *outcome,
                    price: *price,
                    size: *size,
                    side: *side,
                    fee: *fee,
                    liquidity: *liquidity,
                    timestamp_ms: *timestamp_ms,
                },
                limits,
            ),
            PmAccountEvent::Settlement {
                market,
                outcome,
                settled_size,
                proceeds,
                timestamp_ms,
            } => self.apply_settlement(
                (
                    market.clone(),
                    *outcome,
                    *settled_size,
                    *proceeds,
                    *timestamp_ms,
                ),
                limits,
            ),
            PmAccountEvent::OrderAck { .. }
            | PmAccountEvent::OrderCancelled { .. }
            | PmAccountEvent::OrderRejected { .. }
            | PmAccountEvent::OrderStatus { .. } => {}
        }
    }

    fn apply_settlement(&mut self, settlement: SettlementIdentity, limits: &RiskLimits) {
        if !self.applied_settlements.insert(settlement.clone()) {
            return;
        }
        let (market, outcome, settled_size, proceeds, _) = settlement;
        let positions = self.positions_by_market.entry(market).or_default();
        let average_entry = positions
            .iter()
            .find(|position| position.outcome == outcome)
            .map_or(Decimal::ZERO, |position| position.avg_entry);
        self.realized_pnl += proceeds - average_entry * settled_size;
        if let Some(position) = positions
            .iter_mut()
            .find(|position| position.outcome == outcome)
        {
            position.qty -= settled_size;
        }
        positions.retain(|position| !position.qty.is_zero());
        self.refresh_marks(limits);
    }

    pub(super) const fn fill_count(&self) -> usize {
        self.fill_count
    }

    #[cfg(test)]
    pub(super) const fn realized_pnl(&self) -> Decimal {
        self.realized_pnl
    }

    #[cfg(test)]
    pub(super) fn daily_pnl(&self) -> Option<Decimal> {
        let mut unrealized_pnl = Decimal::ZERO;
        for (market, positions) in &self.positions_by_market {
            for position in positions {
                if position.qty.is_zero() {
                    continue;
                }
                if !self.marks.contains_key(&(market.clone(), position.outcome)) {
                    return None;
                }
                unrealized_pnl += position.unrealized_pnl;
            }
        }
        Some(self.realized_pnl - self.fees + unrealized_pnl)
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
        let daily_pnl = mark_positions(&mut self.positions_by_market, &self.marks)
            .map(|unrealized_pnl| self.realized_pnl - self.fees + unrealized_pnl);
        if daily_pnl.is_some_and(|pnl| pnl <= -limits.max_loss.as_decimal()) {
            self.loss_breached = true;
        }
        daily_pnl
    }
}

#[cfg(test)]
mod ledger_tests {
    use super::LiveRiskState;
    use crate::test_support::risk;
    use pmkit_book::{OrderBookL2, Side};
    use pmkit_core::MarketId;
    use pmkit_event::{Liquidity, PmAccountEvent};
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;

    fn fill(market: MarketId) -> PmAccountEvent {
        PmAccountEvent::Fill {
            strategy: None,
            order_id: "venue-1".into(),
            market,
            outcome: Outcome::Up,
            price: Decimal::new(4, 1),
            size: Decimal::from(10),
            side: Side::Buy,
            fee: Decimal::new(1, 1),
            liquidity: Liquidity::Taker,
            timestamp_ms: 1_000,
        }
    }

    #[test]
    fn account_fill_updates_ledger_once() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a marked live ledger and one durable account fill replayed twice.
        let market = MarketId::new("btc-5m")?;
        let limits = risk()?;
        let mut state = LiveRiskState::default();
        state.update_book(
            &market,
            Outcome::Up,
            &OrderBookL2 {
                bids: vec![(Decimal::new(5, 1), Decimal::ONE)],
                asks: vec![(Decimal::new(5, 1), Decimal::ONE)],
                timestamp_ms: 900,
                last_trade_price: None,
            },
            &limits,
        );
        let event = fill(market.clone());

        // When: both deliveries pass through the authoritative account ledger.
        state.apply_account_event(&event, &limits);
        state.apply_account_event(&event, &limits);

        // Then: position, marked exposure, PnL, and fill count change exactly once.
        let position = state
            .positions(&market)
            .first()
            .ok_or("missing account-fill position")?;
        assert_eq!(position.qty, Decimal::from(10));
        assert_eq!(state.portfolio_notional(), Decimal::from(5));
        assert_eq!(state.market_notional(&market), Decimal::from(5));
        assert_eq!(state.daily_pnl(), Some(Decimal::new(9, 1)));
        assert_eq!(state.fill_count(), 1);
        Ok(())
    }

    #[test]
    fn restart_does_not_double_count() -> Result<(), Box<dyn std::error::Error>> {
        // Given: duplicate durable fill and settlement records during restart replay.
        let market = MarketId::new("btc-5m")?;
        let limits = risk()?;
        let fill = fill(market.clone());
        let settlement = PmAccountEvent::Settlement {
            market: market.clone(),
            outcome: Outcome::Up,
            settled_size: Decimal::from(10),
            proceeds: Decimal::from(10),
            timestamp_ms: 2_000,
        };
        let mut state = LiveRiskState::default();

        // When: reconstruction sees each durable record more than once.
        for event in [&fill, &settlement, &fill, &settlement] {
            state.apply_account_event(event, &limits);
        }

        // Then: only one fill and settlement affect the reconstructed ledger.
        assert!(state.positions(&market).is_empty());
        assert_eq!(state.realized_pnl(), Decimal::from(6));
        assert_eq!(state.daily_pnl(), Some(Decimal::new(59, 1)));
        assert_eq!(state.fill_count(), 1);
        Ok(())
    }
}
