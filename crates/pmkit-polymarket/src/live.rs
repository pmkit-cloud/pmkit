use std::fmt;

use async_trait::async_trait;
use futures::StreamExt as _;
use pmkit_core::MarketId;
use pmkit_data::{DataSourceError, LiveDataSource};
use pmkit_event::MarketEvent;
use pmkit_market::Outcome;
use polymarket_client_sdk_v2::clob::ws::{BookUpdate, Client, LastTradePrice};
use polymarket_client_sdk_v2::error::Error as SdkError;
use tokio::sync::mpsc::Sender;

use crate::{MarketTokens, from_venue_side};

/// Polymarket order-book and trade WebSocket source.
#[derive(Clone)]
pub struct PolymarketLiveData {
    client: Client,
    tokens: MarketTokens,
}

impl PolymarketLiveData {
    /// Creates a live source from an SDK WebSocket client and market token map.
    #[must_use]
    pub const fn new(client: Client, tokens: MarketTokens) -> Self {
        Self { client, tokens }
    }
}

impl fmt::Debug for PolymarketLiveData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PolymarketLiveData")
            .field("tokens", &self.tokens)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl LiveDataSource for PolymarketLiveData {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<MarketEvent>,
    ) -> Result<(), DataSourceError> {
        if &market != self.tokens.market() {
            return Err(DataSourceError::NotAvailable);
        }
        let token = self.tokens.token(outcome);
        let books = self
            .client
            .subscribe_orderbook(vec![token])
            .map_err(|error| data_error(&error))?;
        let trades = match self.client.subscribe_last_trade_price(vec![token]) {
            Ok(trades) => trades,
            Err(error) => {
                self.client
                    .unsubscribe_orderbook(&[token])
                    .map_err(|error| data_error(&error))?;
                return Err(data_error(&error));
            }
        };
        let mut books = Box::pin(books);
        let mut trades = Box::pin(trades);

        let result = loop {
            tokio::select! {
                update = books.next() => match update {
                    Some(Ok(update)) => {
                        if sink.send(book_event(market.clone(), outcome, update)).await.is_err() {
                            break Err(DataSourceError::SinkClosed);
                        }
                    }
                    Some(Err(error)) => break Err(data_error(&error)),
                    None => break Ok(()),
                },
                update = trades.next() => match update {
                    Some(Ok(update)) => {
                        if let Some(event) = trade_event(market.clone(), outcome, &update)
                            && sink.send(event).await.is_err()
                        {
                            break Err(DataSourceError::SinkClosed);
                        }
                    }
                    Some(Err(error)) => break Err(data_error(&error)),
                    None => break Ok(()),
                },
            }
        };

        drop(books);
        drop(trades);
        let book_cleanup = self.client.unsubscribe_orderbook(&[token]);
        let trade_cleanup = self.client.unsubscribe_orderbook(&[token]);
        result?;
        book_cleanup.map_err(|error| data_error(&error))?;
        trade_cleanup.map_err(|error| data_error(&error))
    }
}

fn book_event(market: MarketId, outcome: Outcome, update: BookUpdate) -> MarketEvent {
    MarketEvent::BookUpdate {
        market,
        outcome,
        bids: update
            .bids
            .into_iter()
            .map(|level| (level.price, level.size))
            .collect(),
        asks: update
            .asks
            .into_iter()
            .map(|level| (level.price, level.size))
            .collect(),
        timestamp_ms: update.timestamp,
    }
}

fn trade_event(market: MarketId, outcome: Outcome, update: &LastTradePrice) -> Option<MarketEvent> {
    Some(MarketEvent::LastTrade {
        market,
        outcome,
        price: update.price,
        side: from_venue_side(update.side?)?,
        size: update.size?,
        timestamp_ms: update.timestamp,
    })
}

fn data_error(error: &SdkError) -> DataSourceError {
    DataSourceError::Unavailable {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use pmkit_book::Side;
    use pmkit_core::MarketId;
    use pmkit_event::MarketEvent;
    use pmkit_market::Outcome;
    use polymarket_client_sdk_v2::clob::ws::{BookUpdate, LastTradePrice};
    use rust_decimal::Decimal;

    use super::{book_event, trade_event};

    #[test]
    fn venue_updates_map_to_neutral_events() -> Result<(), Box<dyn std::error::Error>> {
        let market = MarketId::new("btc-5m")?;
        let book: BookUpdate = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","timestamp":"42","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}"#,
        )?;
        let trade: LastTradePrice = serde_json::from_str(
            r#"{"asset_id":"1","market":"0x0000000000000000000000000000000000000000000000000000000000000001","price":"0.5","side":"BUY","size":"4","timestamp":"43"}"#,
        )?;

        let MarketEvent::BookUpdate {
            bids,
            asks,
            timestamp_ms,
            ..
        } = book_event(market.clone(), Outcome::Up, book)
        else {
            return Err("expected book update".into());
        };
        assert_eq!(bids, vec![(Decimal::new(49, 2), Decimal::from(2))]);
        assert_eq!(asks, vec![(Decimal::new(51, 2), Decimal::from(3))]);
        assert_eq!(timestamp_ms, 42);

        let Some(MarketEvent::LastTrade {
            price,
            side,
            size,
            timestamp_ms,
            ..
        }) = trade_event(market, Outcome::Up, &trade)
        else {
            return Err("expected last trade".into());
        };
        assert_eq!(price, Decimal::new(5, 1));
        assert_eq!(side, Side::Buy);
        assert_eq!(size, Decimal::from(4));
        assert_eq!(timestamp_ms, 43);
        Ok(())
    }
}
