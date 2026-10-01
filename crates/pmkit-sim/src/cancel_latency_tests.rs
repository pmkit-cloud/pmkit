use super::{MarketCategory, SimEngine, SimulationConfig};
use pmkit_book::{OrderBookL2, Side};
use pmkit_core::MarketId;
use pmkit_exec::{PlaceOrder, TimeInForce};
use pmkit_market::Outcome;
use rust_decimal::Decimal;

#[test]
fn cancelling_delayed_order_prevents_fill_after_activation()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let mut engine = SimEngine::with_config(
        "paper",
        0,
        MarketCategory::Crypto,
        SimulationConfig {
            activation_latency_ms: 10,
            ..SimulationConfig::default()
        },
    );
    engine.update_book(
        &market,
        Outcome::Up,
        OrderBookL2 {
            bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
            asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
            timestamp_ms: 0,
            last_trade_price: None,
        },
    );
    let order = PlaceOrder {
        market: market.clone(),
        outcome: Outcome::Up,
        side: Side::Buy,
        price: Decimal::new(50, 2),
        qty: Decimal::from(10),
        post_only: false,
        tif: TimeInForce::Gtc,
    };

    let order_id = engine
        .submit(&order, 0)
        .ok_or("valid delayed order was rejected")?;
    assert!(engine.drain_fills().is_empty());
    assert_eq!(engine.open_orders().len(), 1);

    engine
        .cancel(&order_id)
        .ok_or("delayed order was not cancellable")?;
    assert!(engine.open_orders().is_empty());

    engine.update_book(
        &market,
        Outcome::Up,
        OrderBookL2 {
            bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
            asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
            timestamp_ms: 11,
            last_trade_price: None,
        },
    );
    assert!(engine.drain_fills().is_empty());
    assert!(engine.open_orders().is_empty());
    Ok(())
}
