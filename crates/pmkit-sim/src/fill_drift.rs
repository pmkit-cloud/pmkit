//! Deterministic, read-only comparison of simulated and venue-observed fills.
//!
//! Exact `order_id`, `market`, and `outcome` values are the match key. Callers
//! must explicitly map simulator order ids to their venue ids; this module does
//! not infer identity. Quantities are cumulative and prices are quantity-weighted
//! averages. All timestamp deltas are raw observed-minus-predicted milliseconds:
//! fill-event timestamps come from the fill facts, source and receipt times from
//! the account envelope. They are assumed to be epoch milliseconds; no clock
//! correction, latency assumption, or tolerance is applied.

use std::{collections::HashMap, error::Error, fmt};

use pmkit_core::MarketId;
use pmkit_event::{MarketEvent, PmAccountEnvelope, PmAccountEvent};
use pmkit_market::Outcome;
use rust_decimal::Decimal;

/// Classification based only on whether either side contains a fill for the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillDriftStatus {
    /// A simulated fill has no observed venue fill.
    MissingObserved,
    /// An observed venue fill has no simulated fill.
    ExtraObserved,
    /// Both sides contain fills; compare the reported deltas for drift.
    Compared,
}

/// One cumulative comparison for an exact order, market, and outcome identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FillDrift {
    /// Exact order identity used for matching.
    pub order_id: String,
    /// Exact market identity used for matching.
    pub market: MarketId,
    /// Exact outcome identity used for matching.
    pub outcome: Outcome,
    /// Whether one side is absent or both sides are present.
    pub status: FillDriftStatus,
    /// Total simulated quantity.
    pub predicted_quantity: Decimal,
    /// Total venue-observed quantity.
    pub observed_quantity: Decimal,
    /// Observed quantity minus predicted quantity.
    pub quantity_delta: Decimal,
    /// Quantity-weighted simulated average fill price, if nonzero quantity exists.
    pub predicted_vwap: Option<Decimal>,
    /// Quantity-weighted observed average fill price, if nonzero quantity exists.
    pub observed_vwap: Option<Decimal>,
    /// Observed VWAP minus predicted VWAP, when both exist.
    pub price_delta: Option<Decimal>,
    /// Latest simulated fill-event timestamp.
    pub predicted_timestamp_ms: Option<i64>,
    /// Latest venue fill-event timestamp.
    pub observed_timestamp_ms: Option<i64>,
    /// Latest account-envelope source timestamp associated with the latest fill.
    pub observed_source_time_ms: Option<i64>,
    /// Latest local receipt timestamp associated with the latest fill.
    pub observed_receipt_time_ms: Option<i64>,
    /// Venue fill-event time minus simulated fill-event time.
    pub fill_time_delta_ms: Option<i64>,
    /// Account source time minus simulated fill-event time.
    pub source_time_delta_ms: Option<i64>,
    /// Local receipt time minus simulated fill-event time.
    pub receipt_time_delta_ms: Option<i64>,
    /// Local receipt time minus account source time for the latest observed fill.
    pub observed_receipt_latency_ms: Option<i64>,
}

/// Stable, sorted fill comparisons.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FillDriftReport {
    /// Comparisons sorted by order id, market id, then outcome (`Up` before `Down`).
    pub fills: Vec<FillDrift>,
}

/// A checked arithmetic operation exceeded Decimal or timestamp range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillDriftError;

impl fmt::Display for FillDriftError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("fill drift arithmetic overflow")
    }
}

impl Error for FillDriftError {}

/// Compares `SimEngine` fill events with venue account envelopes.
///
/// Non-fill events are ignored. Exact order ids are required, so callers should
/// apply their explicit simulator-to-venue order-id mapping before comparing.
///
/// # Errors
///
/// Returns [`FillDriftError`] if cumulative Decimal arithmetic or a timestamp
/// delta exceeds its representable range.
pub fn compare_fill_drift(
    predicted: &[MarketEvent],
    observed: &[PmAccountEnvelope],
) -> Result<FillDriftReport, FillDriftError> {
    let mut comparisons = HashMap::<FillKey, FillComparison>::new();

    for event in predicted {
        if let MarketEvent::Fill {
            order_id,
            market,
            outcome,
            price,
            size,
            timestamp_ms,
            ..
        } = event
        {
            comparisons
                .entry(FillKey::new(order_id, market, *outcome))
                .or_default()
                .add_predicted(*price, *size, *timestamp_ms)?;
        }
    }

    for envelope in observed {
        if let PmAccountEvent::Fill {
            order_id,
            market,
            outcome,
            price,
            size,
            timestamp_ms,
            ..
        } = &envelope.fact
        {
            comparisons
                .entry(FillKey::new(order_id, market, *outcome))
                .or_default()
                .add_observed(
                    *price,
                    *size,
                    *timestamp_ms,
                    envelope.metadata.source_time_ms,
                    envelope.metadata.receipt_time_ms,
                )?;
        }
    }

    let mut fills = comparisons
        .into_iter()
        .map(|(key, comparison)| comparison.into_drift(key))
        .collect::<Result<Vec<_>, _>>()?;
    fills.sort_by(|left, right| {
        left.order_id
            .cmp(&right.order_id)
            .then_with(|| left.market.to_string().cmp(&right.market.to_string()))
            .then_with(|| outcome_sort_key(left.outcome).cmp(&outcome_sort_key(right.outcome)))
    });
    Ok(FillDriftReport { fills })
}

const fn outcome_sort_key(outcome: Outcome) -> u8 {
    match outcome {
        Outcome::Up => 0,
        Outcome::Down => 1,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FillKey {
    order_id: String,
    market: MarketId,
    outcome: Outcome,
}

impl FillKey {
    fn new(order_id: &str, market: &MarketId, outcome: Outcome) -> Self {
        Self {
            order_id: order_id.to_owned(),
            market: market.clone(),
            outcome,
        }
    }
}

#[derive(Debug, Default)]
struct FillComparison {
    predicted: Option<FillTotals>,
    observed: Option<ObservedTotals>,
}

impl FillComparison {
    fn add_predicted(
        &mut self,
        price: Decimal,
        quantity: Decimal,
        timestamp_ms: i64,
    ) -> Result<(), FillDriftError> {
        self.predicted
            .get_or_insert_with(FillTotals::default)
            .add(price, quantity, timestamp_ms)
    }

    fn add_observed(
        &mut self,
        price: Decimal,
        quantity: Decimal,
        timestamp_ms: i64,
        source_time_ms: i64,
        receipt_time_ms: i64,
    ) -> Result<(), FillDriftError> {
        self.observed
            .get_or_insert_with(ObservedTotals::default)
            .add(
                price,
                quantity,
                ObservedClock {
                    fill_ms: timestamp_ms,
                    source_ms: source_time_ms,
                    receipt_ms: receipt_time_ms,
                },
            )
    }

    fn into_drift(self, key: FillKey) -> Result<FillDrift, FillDriftError> {
        let status = match (&self.predicted, &self.observed) {
            (Some(_), None) => FillDriftStatus::MissingObserved,
            (None, Some(_)) => FillDriftStatus::ExtraObserved,
            (Some(_), Some(_)) => FillDriftStatus::Compared,
            (None, None) => return Err(FillDriftError),
        };
        let predicted_quantity = self
            .predicted
            .as_ref()
            .map_or(Decimal::ZERO, |totals| totals.quantity);
        let observed_quantity = self
            .observed
            .as_ref()
            .map_or(Decimal::ZERO, |totals| totals.fills.quantity);
        let predicted_vwap = self
            .predicted
            .as_ref()
            .map(FillTotals::vwap)
            .transpose()?
            .flatten();
        let observed_vwap = self
            .observed
            .as_ref()
            .map(|totals| totals.fills.vwap())
            .transpose()?
            .flatten();
        let price_delta = match (predicted_vwap, observed_vwap) {
            (Some(predicted), Some(observed)) => {
                Some(observed.checked_sub(predicted).ok_or(FillDriftError)?)
            }
            _ => None,
        };
        let predicted_timestamp_ms = self
            .predicted
            .as_ref()
            .and_then(|totals| totals.latest_timestamp_ms);
        let latest_observed = self
            .observed
            .as_ref()
            .and_then(|totals| totals.latest_clock);
        let observed_timestamp_ms = latest_observed.map(|clock| clock.fill_ms);
        let observed_source_time_ms = latest_observed.map(|clock| clock.source_ms);
        let observed_receipt_time_ms = latest_observed.map(|clock| clock.receipt_ms);

        Ok(FillDrift {
            order_id: key.order_id,
            market: key.market,
            outcome: key.outcome,
            status,
            predicted_quantity,
            observed_quantity,
            quantity_delta: observed_quantity
                .checked_sub(predicted_quantity)
                .ok_or(FillDriftError)?,
            predicted_vwap,
            observed_vwap,
            price_delta,
            predicted_timestamp_ms,
            observed_timestamp_ms,
            observed_source_time_ms,
            observed_receipt_time_ms,
            fill_time_delta_ms: timestamp_delta(observed_timestamp_ms, predicted_timestamp_ms)?,
            source_time_delta_ms: timestamp_delta(observed_source_time_ms, predicted_timestamp_ms)?,
            receipt_time_delta_ms: timestamp_delta(
                observed_receipt_time_ms,
                predicted_timestamp_ms,
            )?,
            observed_receipt_latency_ms: timestamp_delta(
                observed_receipt_time_ms,
                observed_source_time_ms,
            )?,
        })
    }
}

#[derive(Debug, Default)]
struct FillTotals {
    quantity: Decimal,
    notional: Decimal,
    latest_timestamp_ms: Option<i64>,
}

impl FillTotals {
    fn add(
        &mut self,
        price: Decimal,
        quantity: Decimal,
        timestamp_ms: i64,
    ) -> Result<(), FillDriftError> {
        let notional = price.checked_mul(quantity).ok_or(FillDriftError)?;
        let total_quantity = self.quantity.checked_add(quantity).ok_or(FillDriftError)?;
        let total_notional = self.notional.checked_add(notional).ok_or(FillDriftError)?;
        self.quantity = total_quantity;
        self.notional = total_notional;
        self.latest_timestamp_ms = Some(
            self.latest_timestamp_ms
                .map_or(timestamp_ms, |latest| latest.max(timestamp_ms)),
        );
        Ok(())
    }

    fn vwap(&self) -> Result<Option<Decimal>, FillDriftError> {
        if self.quantity.is_zero() {
            Ok(None)
        } else {
            self.notional
                .checked_div(self.quantity)
                .map(Some)
                .ok_or(FillDriftError)
        }
    }
}

#[derive(Debug, Default)]
struct ObservedTotals {
    fills: FillTotals,
    latest_clock: Option<ObservedClock>,
}

impl ObservedTotals {
    fn add(
        &mut self,
        price: Decimal,
        quantity: Decimal,
        clock: ObservedClock,
    ) -> Result<(), FillDriftError> {
        self.fills.add(price, quantity, clock.fill_ms)?;
        let is_latest = self.latest_clock.is_none_or(|latest| {
            (clock.fill_ms, clock.source_ms, clock.receipt_ms)
                > (latest.fill_ms, latest.source_ms, latest.receipt_ms)
        });
        if is_latest {
            self.latest_clock = Some(clock);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
// All timestamp fields intentionally retain the explicit milliseconds unit.
#[allow(clippy::struct_field_names)]
struct ObservedClock {
    fill_ms: i64,
    source_ms: i64,
    receipt_ms: i64,
}

fn timestamp_delta(
    observed: Option<i64>,
    predicted: Option<i64>,
) -> Result<Option<i64>, FillDriftError> {
    observed
        .zip(predicted)
        .map(|(observed, predicted)| observed.checked_sub(predicted).ok_or(FillDriftError))
        .transpose()
}

#[cfg(test)]
mod tests {
    use crate::{FillDriftError, FillDriftStatus, MarketCategory, SimEngine, compare_fill_drift};
    use pmkit_book::{OrderBookL2, Side};
    use pmkit_core::{MarketId, PortfolioId};
    use pmkit_event::{
        FillIdentity, Liquidity, MarketEvent, PmAccountEnvelope, PmAccountEvent, StreamMetadata,
    };
    use pmkit_exec::{PlaceOrder, TimeInForce};
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;

    fn predicted_fill(
        order_id: &str,
        market: MarketId,
        outcome: Outcome,
        price: Decimal,
        quantity: Decimal,
        timestamp_ms: i64,
    ) -> MarketEvent {
        MarketEvent::Fill {
            strategy: None,
            order_id: order_id.into(),
            market,
            outcome,
            price,
            size: quantity,
            side: Side::Buy,
            fee: Decimal::ZERO,
            liquidity: Liquidity::Taker,
            timestamp_ms,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn observed_fill(
        order_id: &str,
        market: MarketId,
        outcome: Outcome,
        price: Decimal,
        quantity: Decimal,
        timestamp_ms: i64,
        source_time_ms: i64,
        receipt_time_ms: i64,
    ) -> Result<PmAccountEnvelope, Box<dyn std::error::Error>> {
        Ok(PmAccountEnvelope {
            portfolio: PortfolioId::new("portfolio")?,
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: "venue-user-stream".into(),
                source_time_ms,
                canonical_source_rank: 0,
                receipt_time_ms,
                connection_id: "connection-1".into(),
                connection_epoch: 0,
                frame_sequence: timestamp_ms,
                ingest_sequence: u64::try_from(timestamp_ms)?,
            },
            raw_frame: Vec::new(),
            fact: PmAccountEvent::Fill {
                identity: FillIdentity::Venue(format!("fill-{order_id}-{timestamp_ms}")),
                strategy: None,
                order_id: order_id.into(),
                market,
                outcome,
                price,
                size: quantity,
                side: Side::Buy,
                fee: Decimal::ZERO,
                liquidity: Liquidity::Taker,
                timestamp_ms,
            },
        })
    }

    #[test]
    fn equal_engine_fill_compares_exactly_and_exposes_source_and_receipt_clocks()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let mut engine = SimEngine::new("venue-order", 0, MarketCategory::Crypto);
        engine.update_book(
            &market,
            Outcome::Up,
            OrderBookL2 {
                bids: vec![(Decimal::new(39, 2), Decimal::from(10))],
                asks: vec![(Decimal::new(40, 2), Decimal::from(10))],
                timestamp_ms: 100,
                last_trade_price: None,
            },
        );
        engine.submit(
            &PlaceOrder {
                market: market.clone(),
                outcome: Outcome::Up,
                side: Side::Buy,
                price: Decimal::new(40, 2),
                qty: Decimal::from(10),
                post_only: false,
                tif: TimeInForce::Gtc,
            },
            100,
        );
        let predicted = engine.drain_fills();
        let MarketEvent::Fill {
            order_id,
            market,
            outcome,
            price,
            size,
            timestamp_ms,
            ..
        } = &predicted[0]
        else {
            return Err("SimEngine did not produce a fill".into());
        };
        let observed = [observed_fill(
            order_id,
            market.clone(),
            *outcome,
            *price,
            *size,
            *timestamp_ms,
            101,
            107,
        )?];

        let report = compare_fill_drift(&predicted, &observed)?;
        let drift = &report.fills[0];
        assert_eq!(drift.status, FillDriftStatus::Compared);
        assert_eq!(drift.quantity_delta, Decimal::ZERO);
        assert_eq!(drift.price_delta, Some(Decimal::ZERO));
        assert_eq!(drift.fill_time_delta_ms, Some(0));
        assert_eq!(drift.source_time_delta_ms, Some(1));
        assert_eq!(drift.receipt_time_delta_ms, Some(7));
        assert_eq!(drift.observed_receipt_latency_ms, Some(6));
        Ok(())
    }

    #[test]
    fn split_partial_fills_compare_cumulative_quantity_and_vwap()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let predicted = [predicted_fill(
            "order-1",
            market.clone(),
            Outcome::Up,
            Decimal::new(40, 2),
            Decimal::from(10),
            100,
        )];
        let observed = [
            observed_fill(
                "order-1",
                market.clone(),
                Outcome::Up,
                Decimal::new(40, 2),
                Decimal::from(4),
                101,
                101,
                104,
            )?,
            observed_fill(
                "order-1",
                market,
                Outcome::Up,
                Decimal::new(40, 2),
                Decimal::from(6),
                102,
                103,
                107,
            )?,
        ];

        let report = compare_fill_drift(&predicted, &observed)?;
        let drift = &report.fills[0];
        assert_eq!(drift.predicted_quantity, Decimal::from(10));
        assert_eq!(drift.observed_quantity, Decimal::from(10));
        assert_eq!(drift.quantity_delta, Decimal::ZERO);
        assert_eq!(drift.observed_vwap, Some(Decimal::new(40, 2)));
        assert_eq!(drift.price_delta, Some(Decimal::ZERO));
        assert_eq!(drift.fill_time_delta_ms, Some(2));
        assert_eq!(drift.source_time_delta_ms, Some(3));
        assert_eq!(drift.observed_receipt_latency_ms, Some(4));
        Ok(())
    }

    #[test]
    fn missing_and_extra_fills_keep_order_market_and_outcome_explicit()
    -> Result<(), Box<dyn std::error::Error>> {
        let btc = MarketId::new("btc-5m")?;
        let eth = MarketId::new("eth-5m")?;
        let predicted = [predicted_fill(
            "order-1",
            btc.clone(),
            Outcome::Up,
            Decimal::new(40, 2),
            Decimal::from(10),
            100,
        )];
        let observed = [
            observed_fill(
                "order-1",
                btc.clone(),
                Outcome::Down,
                Decimal::new(40, 2),
                Decimal::from(10),
                101,
                101,
                105,
            )?,
            observed_fill(
                "order-1",
                eth.clone(),
                Outcome::Up,
                Decimal::new(40, 2),
                Decimal::from(10),
                101,
                101,
                105,
            )?,
            observed_fill(
                "order-2",
                btc,
                Outcome::Up,
                Decimal::new(40, 2),
                Decimal::from(3),
                101,
                101,
                105,
            )?,
        ];

        let report = compare_fill_drift(&predicted, &observed)?;
        assert_eq!(report.fills.len(), 4);
        let missing = report
            .fills
            .iter()
            .find(|fill| fill.order_id == "order-1" && fill.outcome == Outcome::Up)
            .ok_or("missing simulated identity was not reported")?;
        assert_eq!(missing.status, FillDriftStatus::MissingObserved);
        assert_eq!(missing.quantity_delta, Decimal::from(-10));
        assert!(
            report
                .fills
                .iter()
                .filter(|fill| fill.order_id == "order-1" && fill.outcome == Outcome::Down)
                .all(|fill| fill.status == FillDriftStatus::ExtraObserved)
        );
        assert!(
            report
                .fills
                .iter()
                .filter(|fill| fill.market == eth)
                .all(|fill| fill.status == FillDriftStatus::ExtraObserved)
        );
        assert!(
            report
                .fills
                .iter()
                .filter(|fill| fill.order_id == "order-2")
                .all(|fill| fill.status == FillDriftStatus::ExtraObserved)
        );
        Ok(())
    }

    #[test]
    fn price_drift_uses_quantity_weighted_partial_fill_vwap()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let predicted = [predicted_fill(
            "order-1",
            market.clone(),
            Outcome::Up,
            Decimal::new(40, 2),
            Decimal::from(10),
            100,
        )];
        let observed = [
            observed_fill(
                "order-1",
                market.clone(),
                Outcome::Up,
                Decimal::new(40, 2),
                Decimal::from(4),
                101,
                101,
                104,
            )?,
            observed_fill(
                "order-1",
                market,
                Outcome::Up,
                Decimal::new(50, 2),
                Decimal::from(6),
                102,
                103,
                107,
            )?,
        ];

        let report = compare_fill_drift(&predicted, &observed)?;
        assert_eq!(report.fills[0].predicted_vwap, Some(Decimal::new(40, 2)));
        assert_eq!(report.fills[0].observed_vwap, Some(Decimal::new(46, 2)));
        assert_eq!(report.fills[0].price_delta, Some(Decimal::new(6, 2)));
        Ok(())
    }

    #[test]
    fn report_order_is_deterministic_for_reordered_inputs() -> Result<(), Box<dyn std::error::Error>>
    {
        let btc = MarketId::new("btc-5m")?;
        let predicted = [
            predicted_fill(
                "z-order",
                btc.clone(),
                Outcome::Down,
                Decimal::new(40, 2),
                Decimal::ONE,
                100,
            ),
            predicted_fill(
                "a-order",
                btc.clone(),
                Outcome::Up,
                Decimal::new(40, 2),
                Decimal::ONE,
                100,
            ),
        ];
        let observed = [
            observed_fill(
                "z-order",
                btc.clone(),
                Outcome::Down,
                Decimal::new(40, 2),
                Decimal::ONE,
                100,
                101,
                102,
            )?,
            observed_fill(
                "a-order",
                btc,
                Outcome::Up,
                Decimal::new(40, 2),
                Decimal::ONE,
                100,
                101,
                102,
            )?,
        ];
        let first = compare_fill_drift(&predicted, &observed)?;
        let reversed_predicted = predicted.into_iter().rev().collect::<Vec<_>>();
        let reversed_observed = observed.into_iter().rev().collect::<Vec<_>>();

        assert_eq!(
            first,
            compare_fill_drift(&reversed_predicted, &reversed_observed)?
        );
        assert_eq!(first.fills[0].order_id, "a-order");
        assert_eq!(first.fills[1].order_id, "z-order");
        Ok(())
    }

    #[test]
    fn decimal_notional_overflow_returns_fill_drift_error() -> Result<(), Box<dyn std::error::Error>>
    {
        let market = MarketId::new("btc-5m")?;
        let predicted = [predicted_fill(
            "decimal-overflow",
            market.clone(),
            Outcome::Up,
            Decimal::ONE,
            Decimal::ONE,
            100,
        )];
        let observed = [observed_fill(
            "decimal-overflow",
            market,
            Outcome::Up,
            Decimal::MAX,
            Decimal::from(2),
            100,
            100,
            100,
        )?];

        assert_eq!(
            compare_fill_drift(&predicted, &observed),
            Err(FillDriftError)
        );
        Ok(())
    }

    #[test]
    fn timestamp_difference_overflow_returns_fill_drift_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let predicted = [predicted_fill(
            "timestamp-overflow",
            market.clone(),
            Outcome::Up,
            Decimal::ONE,
            Decimal::ONE,
            i64::MIN,
        )];
        let observed = [observed_fill(
            "timestamp-overflow",
            market,
            Outcome::Up,
            Decimal::ONE,
            Decimal::ONE,
            i64::MAX,
            i64::MIN,
            i64::MIN,
        )?];

        assert_eq!(
            compare_fill_drift(&predicted, &observed),
            Err(FillDriftError)
        );
        Ok(())
    }
}
