//! Local user-tape sinks for `PMKit` market events.
//!
//! A [`UserTapeSink`] records [`MarketEvent`]s to durable local storage.
//! [`JsonLinesTape`] writes one JSON object per line over any [`Write`], which
//! `zstd`-compresses well and is trivial to replay.

use std::io::{self, Write};

use pmkit_book::Side;
use pmkit_event::{Liquidity, MarketEvent};
use serde_json::json;

/// A sink that records market events to a durable local tape.
pub trait UserTapeSink {
    /// Appends one event to the tape.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails.
    fn append(&mut self, event: &MarketEvent) -> io::Result<()>;

    /// Flushes buffered data to the underlying writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying writer fails to flush.
    fn flush(&mut self) -> io::Result<()>;
}

/// A JSON-lines tape over any writer.
#[derive(Debug)]
pub struct JsonLinesTape<W: Write> {
    writer: W,
}

impl<W: Write> JsonLinesTape<W> {
    /// Creates a tape that writes to `writer`.
    pub const fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Consumes the tape and returns the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> UserTapeSink for JsonLinesTape<W> {
    fn append(&mut self, event: &MarketEvent) -> io::Result<()> {
        let line = serde_json::to_string(&record(event)).unwrap_or_default();
        writeln!(self.writer, "{line}")
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

const fn liquidity_str(liquidity: Liquidity) -> &'static str {
    match liquidity {
        Liquidity::Maker => "maker",
        Liquidity::Taker => "taker",
    }
}

fn levels(levels: &[(rust_decimal::Decimal, rust_decimal::Decimal)]) -> Vec<serde_json::Value> {
    levels
        .iter()
        .map(|(price, size)| json!([price.to_string(), size.to_string()]))
        .collect()
}

#[allow(clippy::too_many_lines)]
fn record(event: &MarketEvent) -> serde_json::Value {
    match event {
        MarketEvent::BookUpdate {
            market,
            outcome,
            bids,
            asks,
            timestamp_ms,
        } => json!({
            "kind": "book_update",
            "ts": timestamp_ms,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "bids": levels(bids),
            "asks": levels(asks),
        }),
        MarketEvent::BestBidAsk {
            market,
            outcome,
            bid,
            ask,
            timestamp_ms,
        } => json!({
            "kind": "best_bid_ask",
            "ts": timestamp_ms,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "bid": bid.to_string(),
            "ask": ask.to_string(),
        }),
        MarketEvent::LastTrade {
            market,
            outcome,
            price,
            side,
            size,
            timestamp_ms,
        } => json!({
            "kind": "last_trade",
            "ts": timestamp_ms,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "price": price.to_string(),
            "side": side_str(*side),
            "size": size.to_string(),
        }),
        MarketEvent::ReferenceTrade {
            asset,
            exchange,
            price,
            qty,
            is_buyer_maker,
            timestamp_ms,
        } => json!({
            "kind": "reference_trade",
            "ts": timestamp_ms,
            "asset": asset.to_string(),
            "exchange": exchange.to_string(),
            "price": price.to_string(),
            "qty": qty.to_string(),
            "is_buyer_maker": is_buyer_maker,
        }),
        MarketEvent::ReferenceBbo {
            asset,
            exchange,
            bid_px,
            bid_qty,
            ask_px,
            ask_qty,
            timestamp_ms,
        } => json!({
            "kind": "reference_bbo",
            "ts": timestamp_ms,
            "asset": asset.to_string(),
            "exchange": exchange.to_string(),
            "bid_px": bid_px.to_string(),
            "bid_qty": bid_qty.to_string(),
            "ask_px": ask_px.to_string(),
            "ask_qty": ask_qty.to_string(),
        }),
        MarketEvent::Fill {
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
        } => json!({
            "kind": "fill",
            "ts": timestamp_ms,
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
            "market": market.to_string(),
            "outcome": outcome.to_string(),
            "price": price.to_string(),
            "size": size.to_string(),
            "side": side_str(*side),
            "fee": fee.to_string(),
            "liquidity": liquidity_str(*liquidity),
        }),
        MarketEvent::OrderAck {
            strategy,
            order_id,
            timestamp_ms,
        } => json!({
            "kind": "order_ack",
            "ts": timestamp_ms,
            "strategy": strategy.as_ref().map(ToString::to_string),
            "order_id": order_id,
        }),
        MarketEvent::Tick { timestamp_ms } => json!({
            "kind": "tick",
            "ts": timestamp_ms,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{JsonLinesTape, UserTapeSink};
    use pmkit_book::Side;
    use pmkit_core::MarketId;
    use pmkit_event::MarketEvent;
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;

    #[test]
    fn writes_one_json_line_per_event() -> Result<(), Box<dyn std::error::Error>> {
        let mut tape = JsonLinesTape::new(Vec::new());
        tape.append(&MarketEvent::LastTrade {
            market: MarketId::new("btc-5m")?,
            outcome: Outcome::Up,
            price: Decimal::new(46, 2),
            side: Side::Buy,
            size: Decimal::from(10),
            timestamp_ms: 1,
        })?;
        tape.append(&MarketEvent::Tick { timestamp_ms: 2 })?;
        tape.flush()?;

        let text = String::from_utf8(tape.into_inner())?;
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"kind\":\"last_trade\""));
        assert!(lines[0].contains("\"market\":\"btc-5m\""));
        assert!(lines[0].contains("\"side\":\"buy\""));
        assert!(lines[1].contains("\"kind\":\"tick\""));
        Ok(())
    }
}
