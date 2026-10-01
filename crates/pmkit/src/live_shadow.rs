use std::collections::{HashMap, HashSet};

use pmkit_book::OrderBookL2;
use pmkit_core::{MarketId, StrategyId};
use pmkit_event::MarketEvent;
use pmkit_exec::{OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_sim::{MarketCategory, SimEngine, SimulationConfig};

pub struct LiveShadow {
    engine: SimEngine,
    venue_by_sim: HashMap<String, OrderId>,
}

impl LiveShadow {
    pub(super) fn new(config: SimulationConfig) -> Self {
        Self {
            engine: SimEngine::with_config("live-shadow", 0, MarketCategory::Crypto, config),
            venue_by_sim: HashMap::new(),
        }
    }

    pub(super) fn update_book(
        &mut self,
        market: &MarketId,
        outcome: Outcome,
        mut book: OrderBookL2,
        receipt_time_ms: i64,
    ) -> Vec<MarketEvent> {
        book.timestamp_ms = receipt_time_ms;
        self.engine.update_book(market, outcome, book);
        self.drain_predictions()
    }

    pub(super) fn submit_for_venue(
        &mut self,
        order: &PlaceOrder,
        strategy: StrategyId,
        receipt_time_ms: i64,
        venue_order_id: &OrderId,
    ) -> Vec<MarketEvent> {
        if let Some(sim_order_id) =
            self.engine
                .submit_for_strategy(order, strategy, receipt_time_ms)
        {
            self.venue_by_sim
                .insert(sim_order_id.0, venue_order_id.clone());
        }
        self.drain_predictions()
    }

    pub(super) fn cancel_venue_order(&mut self, venue_order_id: &str) {
        let sim_order_id = self
            .venue_by_sim
            .iter()
            .find_map(|(sim_id, venue_id)| (venue_id.0 == venue_order_id).then(|| sim_id.clone()));
        if let Some(sim_order_id) = sim_order_id {
            self.engine.cancel(&OrderId(sim_order_id.clone()));
            self.venue_by_sim.remove(&sim_order_id);
        }
    }

    fn drain_predictions(&mut self) -> Vec<MarketEvent> {
        let mut predictions = self.engine.drain_fills();
        for event in &mut predictions {
            if let MarketEvent::Fill { order_id, .. } = event
                && let Some(venue_id) = self.venue_by_sim.get(order_id)
            {
                *order_id = venue_id.0.clone();
            }
        }
        let open_order_ids: HashSet<_> = self
            .engine
            .open_orders()
            .into_iter()
            .map(|order| order.order_id.0)
            .collect();
        self.venue_by_sim
            .retain(|sim_id, _| open_order_ids.contains(sim_id));
        predictions
    }
}

#[cfg(test)]
mod tests {
    use pmkit_book::{OrderBookL2, Side};
    use pmkit_core::{MarketId, PortfolioId, StrategyId};
    use pmkit_event::{
        FillIdentity, Liquidity, MarketEvent, PmAccountEnvelope, PmAccountEvent, StreamMetadata,
    };
    use pmkit_exec::{OrderId, PlaceOrder, TimeInForce};
    use pmkit_market::Outcome;
    use pmkit_sim::{FillDriftStatus, SimulationConfig, compare_fill_drift};
    use rust_decimal::Decimal;

    use super::LiveShadow;

    fn book(ask: Decimal, timestamp_ms: i64) -> OrderBookL2 {
        OrderBookL2 {
            bids: vec![(Decimal::new(39, 2), Decimal::from(10))],
            asks: vec![(ask, Decimal::from(10))],
            timestamp_ms,
            last_trade_price: None,
        }
    }

    fn order(price: Decimal, post_only: bool) -> Result<PlaceOrder, Box<dyn std::error::Error>> {
        Ok(PlaceOrder {
            market: MarketId::new("btc-5m")?,
            outcome: Outcome::Up,
            side: Side::Buy,
            price,
            qty: Decimal::from(2),
            post_only,
            tif: TimeInForce::Gtc,
        })
    }

    fn observed_fill(
        order_id: &str,
        timestamp_ms: i64,
        source_time_ms: i64,
        receipt_time_ms: i64,
    ) -> Result<PmAccountEnvelope, Box<dyn std::error::Error>> {
        Ok(PmAccountEnvelope {
            portfolio: PortfolioId::new("alice")?,
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: "pm-account".into(),
                source_time_ms,
                canonical_source_rank: 0,
                receipt_time_ms,
                connection_id: "account-1".into(),
                connection_epoch: 0,
                frame_sequence: 1,
                ingest_sequence: 1,
            },
            raw_frame: Vec::new(),
            fact: PmAccountEvent::Fill {
                identity: FillIdentity::Venue("fill-1".into()),
                strategy: None,
                order_id: order_id.into(),
                market: MarketId::new("btc-5m")?,
                outcome: Outcome::Up,
                price: Decimal::new(40, 2),
                size: Decimal::from(2),
                side: Side::Buy,
                fee: Decimal::ZERO,
                liquidity: Liquidity::Taker,
                timestamp_ms,
            },
        })
    }

    #[test]
    fn remaps_immediate_taker_fill_before_draining_and_uses_receipt_clock()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let mut shadow = LiveShadow::new(SimulationConfig::default());
        shadow.update_book(&market, Outcome::Up, book(Decimal::new(40, 2), 10), 100);

        let predicted = shadow.submit_for_venue(
            &order(Decimal::new(40, 2), false)?,
            StrategyId::new("buyer")?,
            110,
            &OrderId("venue-taker".into()),
        );

        assert!(matches!(predicted.as_slice(), [MarketEvent::Fill {
            order_id,
            timestamp_ms: 110,
            ..
        }] if order_id == "venue-taker"));
        Ok(())
    }

    #[test]
    fn remaps_maker_fill_and_retains_partial_order_mapping()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let mut shadow = LiveShadow::new(SimulationConfig::default());
        shadow.update_book(&market, Outcome::Up, book(Decimal::new(50, 2), 10), 100);
        assert!(
            shadow
                .submit_for_venue(
                    &order(Decimal::new(40, 2), true)?,
                    StrategyId::new("maker")?,
                    101,
                    &OrderId("venue-maker".into()),
                )
                .is_empty()
        );

        let predicted =
            shadow.update_book(&market, Outcome::Up, book(Decimal::new(40, 2), 20), 120);

        assert!(matches!(predicted.as_slice(), [MarketEvent::Fill {
            order_id,
            liquidity: Liquidity::Maker,
            timestamp_ms: 120,
            ..
        }] if order_id == "venue-maker"));
        Ok(())
    }

    #[test]
    fn confirmed_cancel_stops_future_shadow_fills() -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let mut shadow = LiveShadow::new(SimulationConfig::default());
        shadow.update_book(&market, Outcome::Up, book(Decimal::new(50, 2), 10), 100);
        shadow.submit_for_venue(
            &order(Decimal::new(40, 2), true)?,
            StrategyId::new("maker")?,
            101,
            &OrderId("venue-maker".into()),
        );
        shadow.cancel_venue_order("venue-maker");

        assert!(
            shadow
                .update_book(&market, Outcome::Up, book(Decimal::new(40, 2), 20), 120)
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn comparator_keeps_missing_extra_and_distinct_account_clocks()
    -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let mut shadow = LiveShadow::new(SimulationConfig::default());
        shadow.update_book(&market, Outcome::Up, book(Decimal::new(50, 2), 10), 100);
        shadow.submit_for_venue(
            &order(Decimal::new(40, 2), true)?,
            StrategyId::new("maker")?,
            110,
            &OrderId("venue-predicted".into()),
        );
        let predicted =
            shadow.update_book(&market, Outcome::Up, book(Decimal::new(40, 2), 20), 111);
        let missing = compare_fill_drift(&predicted, &[])?;
        assert_eq!(missing.fills[0].status, FillDriftStatus::MissingObserved);

        let extra = compare_fill_drift(&[], &[observed_fill("venue-extra", 112, 120, 130)?])?;
        assert_eq!(extra.fills[0].status, FillDriftStatus::ExtraObserved);

        let predicted = vec![MarketEvent::Fill {
            strategy: None,
            order_id: "venue-predicted".into(),
            market,
            outcome: Outcome::Up,
            price: Decimal::new(40, 2),
            size: Decimal::from(2),
            side: Side::Buy,
            fee: Decimal::ZERO,
            liquidity: Liquidity::Maker,
            timestamp_ms: 111,
        }];
        let compared = compare_fill_drift(
            &predicted,
            &[observed_fill("venue-predicted", 112, 120, 130)?],
        )?;
        let drift = &compared.fills[0];
        assert_eq!(drift.predicted_timestamp_ms, Some(111));
        assert_eq!(drift.source_time_delta_ms, Some(9));
        assert_eq!(drift.receipt_time_delta_ms, Some(19));
        Ok(())
    }
}
