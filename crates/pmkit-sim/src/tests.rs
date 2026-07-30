use super::{FeeModel, MarketCategory, SimEngine, SimulationConfig};
use pmkit_book::{OrderBookL2, Side};
use pmkit_core::MarketId;
use pmkit_event::{Liquidity, MarketEvent};
use pmkit_exec::{PlaceOrder, TimeInForce};
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
        tif: TimeInForce::Gtc,
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

#[test]
fn custom_fee_applied() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a one-percent taker fee and one-percent maker rebate.
    let config = SimulationConfig {
        fee_model: Some(FeeModel::try_new(-100, 100)?),
        ..SimulationConfig::default()
    };
    let market = MarketId::new("btc-5m")?;
    let mut taker = SimEngine::with_config("taker", 0, MarketCategory::Crypto, config);
    taker.update_book(&market, Outcome::Up, ask_book());
    let mut maker = SimEngine::with_config("maker", 0, MarketCategory::Crypto, config);
    maker.update_book(&market, Outcome::Up, ask_book());
    maker.submit(&order(Side::Buy, Decimal::new(45, 2), true)?, 0);

    // When: the taker crosses immediately and the maker is crossed by a later book.
    taker.submit(&order(Side::Buy, Decimal::new(50, 2), false)?, 0);
    maker.update_book(
        &market,
        Outcome::Up,
        OrderBookL2 {
            bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
            asks: vec![(Decimal::new(45, 2), Decimal::from(50))],
            timestamp_ms: 1,
            last_trade_price: None,
        },
    );

    // Then: each fill carries the configured exact fee or rebate.
    assert!(matches!(
        taker.drain_fills().as_slice(),
        [MarketEvent::Fill { fee, .. }] if *fee == Decimal::new(2_484, 5)
    ));
    assert!(matches!(
        maker.drain_fills().as_slice(),
        [MarketEvent::Fill { fee, .. }] if *fee == Decimal::new(-2_475, 5)
    ));
    Ok(())
}

fn gtd_order(
    side: Side,
    price: Decimal,
    post_only: bool,
    expire_at_ms: i64,
) -> Result<PlaceOrder, pmkit_core::EmptyIdError> {
    Ok(PlaceOrder {
        tif: TimeInForce::Gtd { expire_at_ms },
        ..order(side, price, post_only)?
    })
}

#[test]
fn gtd_maker_expires_instead_of_filling() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::new("paper", 0, MarketCategory::Crypto);
    let market = MarketId::new("btc-5m")?;
    engine.update_book(&market, Outcome::Up, ask_book());
    let id = engine.submit(&gtd_order(Side::Buy, Decimal::new(45, 2), true, 50)?, 0);
    assert!(id.is_some());
    assert_eq!(engine.resting_count(), 1);

    // The crossing book arrives after expiry: the order is gone, not filled.
    let crossed = OrderBookL2 {
        bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
        asks: vec![(Decimal::new(45, 2), Decimal::from(50))],
        timestamp_ms: 101,
        last_trade_price: None,
    };
    engine.update_book(&market, Outcome::Up, crossed);
    assert!(engine.drain_fills().is_empty());
    assert_eq!(engine.resting_count(), 0);
    Ok(())
}

#[test]
fn gtd_maker_fills_before_expiry() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::new("paper", 0, MarketCategory::Crypto);
    let market = MarketId::new("btc-5m")?;
    engine.update_book(&market, Outcome::Up, ask_book());
    engine.submit(&gtd_order(Side::Buy, Decimal::new(45, 2), true, 200)?, 0);
    let crossed = OrderBookL2 {
        bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
        asks: vec![(Decimal::new(45, 2), Decimal::from(50))],
        timestamp_ms: 101,
        last_trade_price: None,
    };
    engine.update_book(&market, Outcome::Up, crossed);
    assert!(matches!(
        engine.drain_fills().as_slice(),
        [MarketEvent::Fill {
            liquidity: Liquidity::Maker,
            ..
        }]
    ));
    Ok(())
}

#[test]
fn invalid_fee_rejected() {
    // Given / When: fee inputs exceed the documented rebate floor and fee ceiling.
    let excessive_rebate = FeeModel::try_new(-10_001, 0);
    let overflow_inducing = FeeModel::try_new(0, i32::MAX);
    let negative_taker = FeeModel::try_new(0, -1);

    // Then: no invalid model can enter SimulationConfig.
    assert!(excessive_rebate.is_err());
    assert!(overflow_inducing.is_err());
    assert!(negative_taker.is_err());
}

#[test]
fn default_fee_unchanged() -> Result<(), Box<dyn std::error::Error>> {
    // Given: the simulation fee model is unset.
    let mut engine = SimEngine::with_config(
        "paper",
        0,
        MarketCategory::Crypto,
        SimulationConfig::default(),
    );
    engine.update_book(&MarketId::new("btc-5m")?, Outcome::Up, ask_book());

    // When: the same ten-share taker order crosses the 46-cent ask.
    engine.submit(&order(Side::Buy, Decimal::new(50, 2), false)?, 0);

    // Then: the fee is byte-for-byte equal to the legacy Crypto calculation.
    assert!(matches!(
        engine.drain_fills().as_slice(),
        [MarketEvent::Fill { fee, .. }] if *fee == Decimal::new(17_388, 5)
    ));
    Ok(())
}

#[test]
fn gtd_expiry_wins_over_fill_at_the_same_timestamp() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::new("paper", 0, MarketCategory::Crypto);
    let market = MarketId::new("btc-5m")?;
    engine.update_book(&market, Outcome::Up, ask_book());
    engine.submit(&gtd_order(Side::Buy, Decimal::new(45, 2), true, 101)?, 0);
    let crossed = OrderBookL2 {
        bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
        asks: vec![(Decimal::new(45, 2), Decimal::from(50))],
        timestamp_ms: 101,
        last_trade_price: None,
    };
    engine.update_book(&market, Outcome::Up, crossed);
    assert!(engine.drain_fills().is_empty());
    assert_eq!(engine.resting_count(), 0);
    Ok(())
}

#[test]
fn already_expired_gtd_order_is_rejected_on_submit() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::new("paper", 0, MarketCategory::Crypto);
    let market = MarketId::new("btc-5m")?;
    engine.update_book(&market, Outcome::Up, ask_book());
    let id = engine.submit(&gtd_order(Side::Buy, Decimal::new(50, 2), false, 50)?, 100);
    assert!(id.is_none());
    assert!(engine.drain_fills().is_empty());
    Ok(())
}

#[test]
fn delayed_taker_expiring_before_activation_never_fills() -> Result<(), Box<dyn std::error::Error>>
{
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
    engine.submit(&gtd_order(Side::Buy, Decimal::new(50, 2), false, 5)?, 0);
    assert!(engine.drain_fills().is_empty());
    let mut active = ask_book();
    active.timestamp_ms = 10;
    engine.update_book(&market, Outcome::Up, active);
    assert!(engine.drain_fills().is_empty());
    Ok(())
}

#[test]
fn sub_minimum_order_never_reaches_the_book() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = SimEngine::with_config(
        "paper",
        0,
        MarketCategory::Crypto,
        SimulationConfig {
            min_order_size: Some(Decimal::from(5)),
            ..SimulationConfig::default()
        },
    );
    let market = MarketId::new("btc-5m")?;
    engine.update_book(&market, Outcome::Up, ask_book());

    // A four-share taker at a crossing price is refused like the venue would.
    let mut small = order(Side::Buy, Decimal::new(50, 2), false)?;
    small.qty = Decimal::from(4);
    assert!(engine.submit(&small, 0).is_none());
    assert!(engine.drain_fills().is_empty());

    // A four-share maker never rests either.
    let mut resting = order(Side::Buy, Decimal::new(45, 2), true)?;
    resting.qty = Decimal::from(4);
    assert!(engine.submit(&resting, 0).is_none());
    assert_eq!(engine.resting_count(), 0);

    // At the minimum the order trades normally.
    let mut at_min = order(Side::Buy, Decimal::new(50, 2), false)?;
    at_min.qty = Decimal::from(5);
    assert!(engine.submit(&at_min, 0).is_some());
    Ok(())
}
