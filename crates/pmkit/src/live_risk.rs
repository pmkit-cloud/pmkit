use pmkit_accounting::PositionExposure;
use pmkit_book::Position;
use pmkit_core::{MarketId, PortfolioId, StrategyId};
use pmkit_event::{FillIdentity, MarketEvent, PmAccountEvent};
use pmkit_exec::PlaceOrder;
use pmkit_market::Outcome;
use pmkit_runtime::RiskLimits;
use pmkit_store::PmEnvelope;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

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

type SettlementIdentity = (MarketId, Outcome, Decimal, Decimal, i64);

#[derive(Debug, thiserror::Error)]
pub(super) enum RiskStateError {
    #[error("durable risk history is corrupt or inconsistent: {message}")]
    CorruptRecord { message: String },
}

impl RiskStateError {
    pub(super) fn corrupt(message: impl Into<String>) -> Self {
        Self::CorruptRecord {
            message: message.into(),
        }
    }
}

fn string_field<'a>(payload: &'a Value, field: &str) -> Result<&'a str, RiskStateError> {
    payload[field]
        .as_str()
        .ok_or_else(|| RiskStateError::corrupt(format!("{field} is missing or invalid")))
}

fn decimal_field(payload: &Value, field: &str) -> Result<Decimal, RiskStateError> {
    Decimal::from_str(string_field(payload, field)?)
        .map_err(|error| RiskStateError::corrupt(format!("{field} is invalid: {error}")))
}

fn outcome_field(payload: &Value) -> Result<Outcome, RiskStateError> {
    match string_field(payload, "outcome")? {
        "up" => Ok(Outcome::Up),
        "down" => Ok(Outcome::Down),
        outcome => Err(RiskStateError::corrupt(format!(
            "unsupported outcome {outcome}"
        ))),
    }
}

fn durable_fill_identity(
    record: &PmEnvelope,
    payload: &Value,
    account_schema_version: u64,
) -> Result<FillIdentity, RiskStateError> {
    let Some(identity) = payload.get("identity") else {
        return match account_schema_version {
            1 | 2 => Ok(FillIdentity::Transport {
                source_id: record.source_id.clone(),
                connection_id: record.connection_id.clone(),
                connection_epoch: record.connection_epoch,
                frame_sequence: record.frame_sequence,
            }),
            3 => Err(RiskStateError::corrupt("fill identity is missing")),
            _ => Err(RiskStateError::corrupt(format!(
                "unsupported account schema version {account_schema_version}"
            ))),
        };
    };
    match string_field(identity, "source")? {
        "venue" => Ok(FillIdentity::Venue(
            string_field(identity, "id")?.to_owned(),
        )),
        "transport" => Ok(FillIdentity::Transport {
            source_id: string_field(identity, "source_id")?.to_owned(),
            connection_id: string_field(identity, "connection_id")?.to_owned(),
            connection_epoch: identity["connection_epoch"]
                .as_i64()
                .ok_or_else(|| RiskStateError::corrupt("connection_epoch is missing or invalid"))?,
            frame_sequence: identity["frame_sequence"]
                .as_i64()
                .ok_or_else(|| RiskStateError::corrupt("frame_sequence is missing or invalid"))?,
        }),
        source => Err(RiskStateError::corrupt(format!(
            "unsupported fill identity source {source}"
        ))),
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OrderRateLimits {
    max_per_strategy: u32,
    max_per_portfolio: u32,
    window_duration_ms: i64,
}

impl OrderRateLimits {
    #[cfg(test)]
    pub(super) const fn new(
        max_per_strategy: u32,
        max_per_portfolio: u32,
        window_duration_ms: i64,
    ) -> Option<Self> {
        if max_per_strategy == 0 || max_per_portfolio == 0 || window_duration_ms <= 0 {
            return None;
        }
        Some(Self {
            max_per_strategy,
            max_per_portfolio,
            window_duration_ms,
        })
    }
}

impl Default for OrderRateLimits {
    fn default() -> Self {
        Self {
            max_per_strategy: 100,
            max_per_portfolio: 1_000,
            window_duration_ms: 60_000,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FixedOrderWindow {
    started_at_ms: i64,
    accepted: u32,
}

impl FixedOrderWindow {
    const fn has_capacity(self, timestamp_ms: i64, duration_ms: i64, maximum: u32) -> bool {
        timestamp_ms > self.started_at_ms.saturating_add(duration_ms) || self.accepted < maximum
    }

    const fn record(&mut self, timestamp_ms: i64, duration_ms: i64) {
        if timestamp_ms > self.started_at_ms.saturating_add(duration_ms) {
            self.started_at_ms = timestamp_ms;
            self.accepted = 1;
        } else {
            self.accepted = self.accepted.saturating_add(1);
        }
    }
}

#[derive(Default)]
pub(super) struct OrderRateState {
    portfolio: Option<FixedOrderWindow>,
    per_strategy: HashMap<StrategyId, FixedOrderWindow>,
}

impl OrderRateState {
    pub(super) fn restore(
        &mut self,
        limits: OrderRateLimits,
        mut accepted: Vec<(Option<StrategyId>, i64)>,
    ) {
        accepted.sort_by_key(|(_, timestamp_ms)| *timestamp_ms);
        for (strategy, timestamp_ms) in accepted {
            self.record_portfolio(timestamp_ms, limits.window_duration_ms);
            if let Some(strategy) = strategy {
                self.record_strategy(strategy, timestamp_ms, limits.window_duration_ms);
            }
        }
    }

    pub(super) fn try_accept(
        &mut self,
        strategy: &StrategyId,
        timestamp_ms: i64,
        limits: OrderRateLimits,
    ) -> bool {
        let portfolio_has_capacity = self.portfolio.is_none_or(|window| {
            window.has_capacity(
                timestamp_ms,
                limits.window_duration_ms,
                limits.max_per_portfolio,
            )
        });
        let strategy_has_capacity = self.per_strategy.get(strategy).is_none_or(|window| {
            window.has_capacity(
                timestamp_ms,
                limits.window_duration_ms,
                limits.max_per_strategy,
            )
        });
        if !portfolio_has_capacity || !strategy_has_capacity {
            return false;
        }
        self.record_portfolio(timestamp_ms, limits.window_duration_ms);
        self.record_strategy(strategy.clone(), timestamp_ms, limits.window_duration_ms);
        true
    }

    const fn record_portfolio(&mut self, timestamp_ms: i64, duration_ms: i64) {
        match &mut self.portfolio {
            Some(window) => window.record(timestamp_ms, duration_ms),
            None => {
                self.portfolio = Some(FixedOrderWindow {
                    started_at_ms: timestamp_ms,
                    accepted: 1,
                });
            }
        }
    }

    fn record_strategy(&mut self, strategy: StrategyId, timestamp_ms: i64, duration_ms: i64) {
        match self.per_strategy.get_mut(&strategy) {
            Some(window) => window.record(timestamp_ms, duration_ms),
            None => {
                self.per_strategy.insert(
                    strategy,
                    FixedOrderWindow {
                        started_at_ms: timestamp_ms,
                        accepted: 1,
                    },
                );
            }
        }
    }
}

#[derive(Default)]
pub(super) struct LiveRiskState {
    positions_by_market: HashMap<MarketId, Vec<Position>>,
    marks: HashMap<(MarketId, Outcome), Decimal>,
    applied_fills: HashSet<FillIdentity>,
    filled_qty_by_order: HashMap<String, Decimal>,
    fees_by_order: HashMap<String, Decimal>,
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

    pub(super) fn apply_fill(
        &mut self,
        event: &MarketEvent,
        identity: &FillIdentity,
        limits: &RiskLimits,
    ) -> bool {
        let MarketEvent::Fill {
            order_id,
            market,
            outcome,
            side,
            price,
            size,
            fee,
            ..
        } = event
        else {
            return false;
        };
        if !self.applied_fills.insert(identity.clone()) {
            return false;
        }
        *self
            .filled_qty_by_order
            .entry(order_id.clone())
            .or_default() += *size;
        *self.fees_by_order.entry(order_id.clone()).or_default() += *fee;
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
        true
    }

    pub(super) fn apply_durable_account_record(
        &mut self,
        record: &PmEnvelope,
        portfolio: &PortfolioId,
        limits: &RiskLimits,
    ) -> Result<(), RiskStateError> {
        let normalized = &record.normalized;
        let Some(record_portfolio) = normalized.get("portfolio") else {
            return Ok(());
        };
        let record_portfolio = record_portfolio
            .as_str()
            .ok_or_else(|| RiskStateError::corrupt("portfolio is invalid"))?;
        if record_portfolio != portfolio.to_string() {
            return Err(RiskStateError::corrupt(format!(
                "record owner {record_portfolio} does not match {portfolio}"
            )));
        }
        let account_schema_version = normalized["schema_version"].as_u64().ok_or_else(|| {
            RiskStateError::corrupt("account schema version is missing or invalid")
        })?;
        if !matches!(account_schema_version, 1..=3) {
            return Err(RiskStateError::corrupt(format!(
                "unsupported account schema version {account_schema_version}"
            )));
        }
        let payload = normalized
            .get("payload")
            .ok_or_else(|| RiskStateError::corrupt("account payload is missing"))?;
        let timestamp_ms = payload["ts"]
            .as_i64()
            .ok_or_else(|| RiskStateError::corrupt("event timestamp is missing or invalid"))?;
        let event = match string_field(payload, "kind")? {
            "fill" => {
                let strategy = match payload.get("strategy") {
                    Some(Value::Null) => None,
                    Some(Value::String(strategy)) => Some(
                        StrategyId::new(strategy)
                            .map_err(|error| RiskStateError::corrupt(error.to_string()))?,
                    ),
                    Some(_) | None => {
                        return Err(RiskStateError::corrupt("strategy is missing or invalid"));
                    }
                };
                let market = MarketId::new(string_field(payload, "market")?)
                    .map_err(|error| RiskStateError::corrupt(error.to_string()))?;
                let price = decimal_field(payload, "price")?;
                let size = decimal_field(payload, "size")?;
                let fee = decimal_field(payload, "fee")?;
                if price < Decimal::ZERO || size <= Decimal::ZERO || fee < Decimal::ZERO {
                    return Err(RiskStateError::corrupt(
                        "fill price, size, or fee is outside its valid range",
                    ));
                }
                PmAccountEvent::Fill {
                    identity: durable_fill_identity(record, payload, account_schema_version)?,
                    strategy,
                    order_id: string_field(payload, "order_id")?.to_owned(),
                    market,
                    outcome: outcome_field(payload)?,
                    price,
                    size,
                    side: match string_field(payload, "side")? {
                        "buy" => pmkit_book::Side::Buy,
                        "sell" => pmkit_book::Side::Sell,
                        side_value => {
                            return Err(RiskStateError::corrupt(format!(
                                "unsupported side {side_value}"
                            )));
                        }
                    },
                    fee,
                    liquidity: match string_field(payload, "liquidity")? {
                        "maker" => pmkit_event::Liquidity::Maker,
                        "taker" => pmkit_event::Liquidity::Taker,
                        liquidity => {
                            return Err(RiskStateError::corrupt(format!(
                                "unsupported liquidity {liquidity}"
                            )));
                        }
                    },
                    timestamp_ms,
                }
            }
            "settlement" => PmAccountEvent::Settlement {
                market: MarketId::new(string_field(payload, "market")?)
                    .map_err(|error| RiskStateError::corrupt(error.to_string()))?,
                outcome: outcome_field(payload)?,
                settled_size: decimal_field(payload, "settled_size")?,
                proceeds: decimal_field(payload, "proceeds")?,
                timestamp_ms,
            },
            "order_ack" | "order_cancelled" | "order_rejected" | "order_status" => {
                return Ok(());
            }
            kind => {
                return Err(RiskStateError::corrupt(format!(
                    "unsupported account event kind {kind}"
                )));
            }
        };
        self.apply_account_event(&event, limits).map(|_| ())
    }

    pub(super) fn apply_account_event(
        &mut self,
        event: &PmAccountEvent,
        limits: &RiskLimits,
    ) -> Result<bool, RiskStateError> {
        match event {
            PmAccountEvent::Fill {
                identity,
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
            } => Ok(self.apply_fill(
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
                identity,
                limits,
            )),
            PmAccountEvent::Settlement {
                market,
                outcome,
                settled_size,
                proceeds,
                timestamp_ms,
            } => self
                .apply_settlement(
                    (
                        market.clone(),
                        *outcome,
                        *settled_size,
                        *proceeds,
                        *timestamp_ms,
                    ),
                    limits,
                )
                .map(|()| false),
            PmAccountEvent::OrderAck { .. }
            | PmAccountEvent::OrderCancelled { .. }
            | PmAccountEvent::OrderRejected { .. }
            | PmAccountEvent::OrderStatus { .. } => Ok(false),
        }
    }

    fn apply_settlement(
        &mut self,
        settlement: SettlementIdentity,
        limits: &RiskLimits,
    ) -> Result<(), RiskStateError> {
        if self.applied_settlements.contains(&settlement) {
            return Ok(());
        }
        let (market, outcome, settled_size, proceeds, _) = &settlement;
        if *settled_size <= Decimal::ZERO || *proceeds < Decimal::ZERO {
            return Err(RiskStateError::corrupt(
                "settlement size or proceeds is outside its valid range",
            ));
        }
        let position = self
            .positions_by_market
            .get(market)
            .and_then(|positions| {
                positions
                    .iter()
                    .find(|position| position.outcome == *outcome)
            })
            .ok_or_else(|| RiskStateError::corrupt("settlement has no matching position"))?;
        if position.qty < *settled_size {
            return Err(RiskStateError::corrupt(
                "settlement exceeds the matching position",
            ));
        }
        let average_entry = position.avg_entry;
        self.applied_settlements.insert(settlement.clone());
        let (market, outcome, settled_size, proceeds, _) = settlement;
        let positions = self.positions_by_market.entry(market).or_default();
        self.realized_pnl += proceeds - average_entry * settled_size;
        if let Some(position) = positions
            .iter_mut()
            .find(|position| position.outcome == outcome)
        {
            position.qty -= settled_size;
        }
        positions.retain(|position| !position.qty.is_zero());
        self.refresh_marks(limits);
        Ok(())
    }

    pub(super) const fn fill_count(&self) -> usize {
        self.fill_count
    }

    pub(super) fn filled_qty(&self, order_id: &str) -> Decimal {
        self.filled_qty_by_order
            .get(order_id)
            .copied()
            .unwrap_or_default()
    }

    pub(super) fn fees_for_order(&self, order_id: &str) -> Decimal {
        self.fees_by_order
            .get(order_id)
            .copied()
            .unwrap_or_default()
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

    pub(super) fn position_exposures(&self) -> Vec<PositionExposure> {
        self.positions_by_market
            .iter()
            .map(|(market, positions)| PositionExposure {
                market: market.clone(),
                notional: self.marked_notional(market, positions),
            })
            .collect()
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
    use pmkit_event::{FillIdentity, Liquidity, PmAccountEvent};
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;

    fn fill(fill_id: &str, market: MarketId) -> PmAccountEvent {
        PmAccountEvent::Fill {
            identity: FillIdentity::Venue(fill_id.into()),
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
        let event = fill("fill-1", market.clone());

        // When: both deliveries pass through the authoritative account ledger.
        state.apply_account_event(&event, &limits)?;
        state.apply_account_event(&event, &limits)?;

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
    fn distinct_fill_identities_apply_same_value_and_time_twice()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: two venue fills with identical economics and timestamps but distinct identities.
        let market = MarketId::new("btc-5m")?;
        let limits = risk()?;
        let mut state = LiveRiskState::default();

        // When: both fills pass through the authoritative account ledger.
        state.apply_account_event(&fill("fill-1", market.clone()), &limits)?;
        state.apply_account_event(&fill("fill-2", market.clone()), &limits)?;

        // Then: both identities affect the position and fill count.
        let position = state
            .positions(&market)
            .first()
            .ok_or("missing account-fill position")?;
        assert_eq!(position.qty, Decimal::from(20));
        assert_eq!(state.fill_count(), 2);
        Ok(())
    }

    #[test]
    fn restart_does_not_double_count() -> Result<(), Box<dyn std::error::Error>> {
        // Given: duplicate durable fill and settlement records during restart replay.
        let market = MarketId::new("btc-5m")?;
        let limits = risk()?;
        let fill = fill("fill-1", market.clone());
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
            state.apply_account_event(event, &limits)?;
        }

        // Then: only one fill and settlement affect the reconstructed ledger.
        assert!(state.positions(&market).is_empty());
        assert_eq!(state.realized_pnl(), Decimal::from(6));
        assert_eq!(state.daily_pnl(), Some(Decimal::new(59, 1)));
        assert_eq!(state.fill_count(), 1);
        Ok(())
    }
}

#[cfg(test)]
mod override_tests {
    use super::{PortfolioRiskExposure, passes_aggregated_risk};
    use crate::test_support::{risk, risk_overrides};
    use pmkit_book::Side;
    use pmkit_core::{MarketId, StrategyId};
    use pmkit_exec::PlaceOrder;
    use pmkit_market::Outcome;
    use pmkit_money::Money;
    use pmkit_runtime::PartialRiskLimits;
    use rust_decimal::Decimal;

    fn order(market: MarketId) -> PlaceOrder {
        PlaceOrder {
            market,
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::ONE,
            qty: Decimal::from(10),
            post_only: false,
        }
    }

    fn empty_exposure() -> PortfolioRiskExposure {
        PortfolioRiskExposure {
            portfolio_notional: Decimal::ZERO,
            market_notional: Decimal::ZERO,
            strategy_notional: Decimal::ZERO,
            daily_pnl: Decimal::ZERO,
            open_orders: 0,
        }
    }

    #[test]
    fn override_tightens() -> Result<(), Box<dyn std::error::Error>> {
        // Given: a globally valid order and a tighter strategy notional override.
        let limits = risk()?;
        let market = MarketId::new("btc-5m")?;
        let strategy = StrategyId::new("maker")?;
        let order = order(market.clone());
        let mut overrides = risk_overrides();
        overrides.per_strategy.insert(
            strategy.clone(),
            PartialRiskLimits {
                max_strategy_notional: Some(Money::usdc(5)),
                ..PartialRiskLimits::default()
            },
        );

        // When: the scoped effective limits are applied.
        let effective = overrides.effective_limits(&limits, &market, &strategy);

        // Then: the override rejects what the global limit alone allows.
        assert!(passes_aggregated_risk(
            &order,
            &limits,
            &[],
            empty_exposure(),
        ));
        assert!(!passes_aggregated_risk(
            &order,
            &effective,
            &[],
            empty_exposure(),
        ));
        Ok(())
    }

    #[test]
    fn override_cannot_loosen() -> Result<(), Box<dyn std::error::Error>> {
        // Given: tight global market and strategy limits with looser overrides.
        let mut limits = risk()?;
        limits.max_market_notional = Money::usdc(5);
        limits.max_strategy_notional = Money::usdc(5);
        let market = MarketId::new("btc-5m")?;
        let strategy = StrategyId::new("maker")?;
        let mut overrides = risk_overrides();
        overrides.per_market.insert(
            market.clone(),
            PartialRiskLimits {
                max_market_notional: Some(Money::usdc(50)),
                ..PartialRiskLimits::default()
            },
        );
        overrides.per_strategy.insert(
            strategy.clone(),
            PartialRiskLimits {
                max_strategy_notional: Some(Money::usdc(50)),
                ..PartialRiskLimits::default()
            },
        );

        // When: the looser scoped values are combined with the globals.
        let effective = overrides.effective_limits(&limits, &market, &strategy);

        // Then: both effective values remain global and the order stays blocked.
        assert_eq!(effective.max_market_notional, Money::usdc(5));
        assert_eq!(effective.max_strategy_notional, Money::usdc(5));
        assert!(!passes_aggregated_risk(
            &order(market),
            &effective,
            &[],
            empty_exposure(),
        ));
        Ok(())
    }
}
