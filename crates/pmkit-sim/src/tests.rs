use super::{MarketCategory, SimEngine};
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
        timestamp_ms: 1,
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
