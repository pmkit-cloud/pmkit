use super::{MarketCategory, SimEngine, SimulationConfig};
use pmkit_book::{OrderBookL2, Side};
use pmkit_core::MarketId;
use pmkit_event::{Liquidity, MarketEvent};
use pmkit_exec::PlaceOrder;
use pmkit_market::Outcome;
use rust_decimal::Decimal;

fn ask_book() -> OrderBookL2 {
    OrderBookL2 {
        bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
        asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
        timestamp_ms: 0,
        last_trade_price: None,
    }
}

fn order(
    side: Side,
    price: Decimal,
    post_only: bool,
) -> Result<PlaceOrder, pmkit_core::EmptyIdError> {
    Ok(PlaceOrder {
        market: MarketId::new("btc-5m")?,
        outcome: Outcome::Up,
        side,
        price,
        qty: Decimal::from(10),
        post_only,
    })
}

#[test]
fn taker_buy_fills_immediately() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::new("paper", 0, MarketCategory::Crypto);
    engine.update_book(&MarketId::new("btc-5m")?, Outcome::Up, ask_book());
    let id = engine.submit(&order(Side::Buy, Decimal::new(50, 2), false)?, 100);
    assert!(id.is_some());

    let fills = engine.drain_fills();
    assert_eq!(fills.len(), 1);
    let MarketEvent::Fill {
        liquidity, size, ..
    } = &fills[0]
    else {
        return Err("expected a fill".into());
    };
    assert_eq!(*liquidity, Liquidity::Taker);
    assert_eq!(*size, Decimal::from(10));
    Ok(())
}

#[test]
fn maker_rests_then_fills_when_crossed() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::new("paper", 0, MarketCategory::Crypto);
    engine.update_book(&MarketId::new("btc-5m")?, Outcome::Up, ask_book());

    // Post-only buy below the ask rests without crossing.
    let id = engine.submit(&order(Side::Buy, Decimal::new(45, 2), true)?, 100);
    assert!(id.is_some());
    assert_eq!(engine.resting_count(), 1);
    assert!(engine.drain_fills().is_empty());

    // A new book whose ask drops to the resting price fills the maker.
    let crossed = OrderBookL2 {
        bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
        asks: vec![(Decimal::new(45, 2), Decimal::from(50))],
        timestamp_ms: 101,
        last_trade_price: None,
    };
    engine.update_book(&MarketId::new("btc-5m")?, Outcome::Up, crossed);

    let fills = engine.drain_fills();
    assert_eq!(fills.len(), 1);
    let MarketEvent::Fill { liquidity, fee, .. } = &fills[0] else {
        return Err("expected a fill".into());
    };
    assert_eq!(*liquidity, Liquidity::Maker);
    assert_eq!(*fee, Decimal::ZERO);
    assert_eq!(engine.resting_count(), 0);
    Ok(())
}

#[test]
fn activation_latency_delays_taker_fill_until_a_later_book()
-> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::with_config(
        "paper",
        0,
        MarketCategory::Crypto,
        SimulationConfig {
            activation_latency_ms: 10,
            ..SimulationConfig::default()
        },
    );
    let market = MarketId::new("btc-5m")?;
    engine.update_book(&market, Outcome::Up, ask_book());
    assert!(
        engine
            .submit(&order(Side::Buy, Decimal::new(50, 2), false)?, 0)
            .is_some()
    );
    assert!(engine.drain_fills().is_empty());
    let mut before = ask_book();
    before.timestamp_ms = 9;
    engine.update_book(&market, Outcome::Up, before);
    assert!(engine.drain_fills().is_empty());
    let mut active = ask_book();
    active.timestamp_ms = 10;
    engine.update_book(&market, Outcome::Up, active);
    assert!(matches!(
        engine.drain_fills().as_slice(),
        [MarketEvent::Fill {
            timestamp_ms: 10,
            ..
        }]
    ));
    Ok(())
}

#[test]
fn queue_model_partially_fills_crossed_maker() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::with_config(
        "paper",
        0,
        MarketCategory::Crypto,
        SimulationConfig {
            maker_queue_ahead_bps: 5_000,
            ..SimulationConfig::default()
        },
    );
    let market = MarketId::new("btc-5m")?;
    engine.update_book(&market, Outcome::Up, ask_book());
    engine.submit(&order(Side::Buy, Decimal::new(45, 2), true)?, 0);
    let crossed = OrderBookL2 {
        bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
        asks: vec![(Decimal::new(45, 2), Decimal::from(10))],
        timestamp_ms: 1,
        last_trade_price: None,
    };
    engine.update_book(&market, Outcome::Up, crossed);
    let fills = engine.drain_fills();
    assert!(matches!(
        fills.as_slice(),
        [MarketEvent::Fill { size, .. }] if *size == Decimal::from(5)
    ));
    assert_eq!(engine.resting_count(), 1);
    Ok(())
}

#[test]
fn slippage_and_impact_adjust_taker_price_without_crossing_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::with_config(
        "paper",
        0,
        MarketCategory::Crypto,
        SimulationConfig {
            slippage_bps: 50,
            market_impact_bps: 50,
            ..SimulationConfig::default()
        },
    );
    let market = MarketId::new("btc-5m")?;
    engine.update_book(&market, Outcome::Up, ask_book());
    engine.submit(&order(Side::Buy, Decimal::new(50, 2), false)?, 0);
    let fills = engine.drain_fills();
    assert!(matches!(
        fills.as_slice(),
        [MarketEvent::Fill { price, .. }] if *price == Decimal::new(4646, 4)
    ));
    Ok(())
}
