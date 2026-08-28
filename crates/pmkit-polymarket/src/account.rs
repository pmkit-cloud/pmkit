use std::fmt;

use futures::StreamExt as _;
use pmkit_core::PortfolioId;
use pmkit_data::{
    DataSourceError, LIVE_HEARTBEAT_INTERVAL_MS, LiveAccountDataSource, SourceSignal,
    live_watermark_now, now_ms,
};
use pmkit_event::{
    FillIdentity, Liquidity, PmAccountEnvelope, PmAccountEvent, SourceEnvelope, StreamMetadata,
};
use polymarket_client_sdk_v2::{
    auth::{Normal, state::Authenticated},
    clob::{
        types::{OrderStatusType, Side as VenueSide, TraderSide},
        ws::types::response::{OrderMessageType, TradeMessageStatus},
        ws::{Client, OrderMessage, TradeMessage, WsMessage},
    },
    types::{B256, U256},
};
use rust_decimal::Decimal;
use tokio::sync::mpsc::Sender;

use crate::{MarketTokens, from_venue_side};

const SOURCE_ID: &str = "polymarket:user-ws";

/// Authenticated Polymarket user-event source using the SDK's typed stream.
#[derive(Clone)]
pub struct PolymarketUserData {
    client: Client<Authenticated<Normal>>,
    portfolio: PortfolioId,
    tokens: MarketTokens,
    market_condition: B256,
}

impl fmt::Debug for PolymarketUserData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PolymarketUserData")
            .field("portfolio", &self.portfolio)
            .field("tokens", &self.tokens)
            .field("market_condition", &self.market_condition)
            .finish_non_exhaustive()
    }
}

impl PolymarketUserData {
    /// Creates a typed authenticated source for one portfolio and market.
    #[must_use]
    pub const fn new(
        client: Client<Authenticated<Normal>>,
        portfolio: PortfolioId,
        tokens: MarketTokens,
        market_condition: B256,
    ) -> Self {
        Self {
            client,
            portfolio,
            tokens,
            market_condition,
        }
    }
}

#[async_trait::async_trait]
impl LiveAccountDataSource for PolymarketUserData {
    async fn subscribe_account(
        &self,
        portfolio: PortfolioId,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if portfolio != self.portfolio {
            return Err(DataSourceError::NotAvailable);
        }
        let stream = self
            .client
            .subscribe_user_events(vec![self.market_condition])
            .map_err(|error| unavailable(error.to_string()))?;
        let mut stream = Box::pin(stream);
        let mut sequence = 0_u64;
        let mut heartbeat =
            tokio::time::interval(std::time::Duration::from_millis(LIVE_HEARTBEAT_INTERVAL_MS));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await;
        loop {
            let message = tokio::select! {
                _ = heartbeat.tick() => {
                    sink.send(SourceSignal::Watermark(live_watermark_now()))
                        .await
                        .map_err(|_| DataSourceError::SinkClosed)?;
                    continue;
                }
                message = stream.next() => message,
            };
            let Some(message) = message else {
                break;
            };
            let message = message.map_err(|error| unavailable(error.to_string()))?;
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| DataSourceError::ReplayGap {
                    message: "Polymarket user frame sequence overflow".into(),
                })?;
            let fact = account_event(&message, &self.tokens, self.market_condition)?;
            let timestamp_ms = account_timestamp(&fact);
            let frame_sequence =
                i64::try_from(sequence).map_err(|_| DataSourceError::ReplayGap {
                    message: "Polymarket user frame sequence exceeds signed range".into(),
                })?;
            sink.send(SourceSignal::Data(Box::new(SourceEnvelope::PmAccount(
                PmAccountEnvelope {
                    portfolio: self.portfolio.clone(),
                    metadata: StreamMetadata {
                        schema_version: 4,
                        source_id: SOURCE_ID.into(),
                        source_time_ms: timestamp_ms,
                        canonical_source_rank: 0,
                        receipt_time_ms: now_ms(),
                        connection_id: SOURCE_ID.into(),
                        connection_epoch: 0,
                        frame_sequence,
                        ingest_sequence: sequence,
                    },
                    raw_frame: Vec::new(),
                    fact,
                },
            ))))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        }
        Err(unavailable("Polymarket user stream ended"))
    }
}

fn account_event(
    message: &WsMessage,
    tokens: &MarketTokens,
    market_condition: B256,
) -> Result<PmAccountEvent, DataSourceError> {
    match message {
        WsMessage::Order(order) => order_event(order, tokens, market_condition),
        WsMessage::Trade(trade) => trade_event(trade, tokens, market_condition),
        _ => Err(DataSourceError::ReplayGap {
            message: "Polymarket user stream emitted a non-account event".into(),
        }),
    }
}

fn order_event(
    order: &OrderMessage,
    tokens: &MarketTokens,
    market_condition: B256,
) -> Result<PmAccountEvent, DataSourceError> {
    validate_market(order.market, order.asset_id, tokens, market_condition)?;
    let timestamp_ms = order.timestamp.ok_or_else(|| missing_timestamp("order"))?;
    let strategy = None;
    let order_id = order.id.clone();
    match order.msg_type.as_ref() {
        Some(OrderMessageType::Cancellation) => Ok(PmAccountEvent::OrderCancelled {
            strategy,
            order_id,
            timestamp_ms,
        }),
        Some(OrderMessageType::Placement | OrderMessageType::Update) => {
            Ok(PmAccountEvent::OrderAck {
                strategy,
                order_id,
                timestamp_ms,
            })
        }
        Some(OrderMessageType::Unknown(reason)) => Ok(PmAccountEvent::OrderRejected {
            strategy,
            order_id,
            reason: reason.clone(),
            timestamp_ms,
        }),
        None => match order.status.as_ref() {
            Some(OrderStatusType::Canceled) => Ok(PmAccountEvent::OrderCancelled {
                strategy,
                order_id,
                timestamp_ms,
            }),
            Some(OrderStatusType::Live | OrderStatusType::Matched | OrderStatusType::Delayed) => {
                Ok(PmAccountEvent::OrderAck {
                    strategy,
                    order_id,
                    timestamp_ms,
                })
            }
            Some(OrderStatusType::Unmatched) => Ok(PmAccountEvent::OrderRejected {
                strategy,
                order_id,
                reason: "unmatched".into(),
                timestamp_ms,
            }),
            Some(OrderStatusType::Unknown(reason)) => Ok(PmAccountEvent::OrderRejected {
                strategy,
                order_id,
                reason: reason.clone(),
                timestamp_ms,
            }),
            Some(_) => Ok(PmAccountEvent::OrderRejected {
                strategy,
                order_id,
                reason: "unsupported order status".into(),
                timestamp_ms,
            }),
            None => Err(DataSourceError::ReplayGap {
                message: "Polymarket order event has no type or status".into(),
            }),
        },
        _ => Err(DataSourceError::ReplayGap {
            message: "Polymarket order event has an unsupported type".into(),
        }),
    }
}

fn trade_event(
    trade: &TradeMessage,
    tokens: &MarketTokens,
    market_condition: B256,
) -> Result<PmAccountEvent, DataSourceError> {
    validate_market(trade.market, trade.asset_id, tokens, market_condition)?;
    let timestamp_ms = trade
        .timestamp
        .or(trade.matchtime)
        .or(trade.last_update)
        .ok_or_else(|| missing_timestamp("trade"))?;
    let order_id = trade_order_id(trade)?;
    match &trade.status {
        TradeMessageStatus::Matched | TradeMessageStatus::Mined | TradeMessageStatus::Confirmed => {
            let outcome = tokens
                .outcome(&trade.asset_id)
                .ok_or_else(|| unknown_asset(trade.asset_id))?;
            let side = from_venue_side(trade.side).ok_or_else(|| unknown_side(trade.side))?;
            let liquidity = match trade.trader_side.as_ref() {
                Some(TraderSide::Maker) => Liquidity::Maker,
                Some(TraderSide::Taker) => Liquidity::Taker,
                Some(TraderSide::Unknown(value)) => {
                    return Err(DataSourceError::ReplayGap {
                        message: format!("unknown Polymarket trader side: {value}"),
                    });
                }
                Some(_) => {
                    return Err(DataSourceError::ReplayGap {
                        message: "unsupported Polymarket trader side".into(),
                    });
                }
                None => {
                    return Err(DataSourceError::ReplayGap {
                        message: "Polymarket trade has no trader side".into(),
                    });
                }
            };
            Ok(PmAccountEvent::Fill {
                identity: FillIdentity::Venue(trade.id.clone()),
                strategy: None,
                order_id,
                market: tokens.market().clone(),
                outcome,
                price: trade.price,
                size: trade.size,
                side,
                fee: trade.fee_rate_bps.unwrap_or(Decimal::ZERO) * trade.price * trade.size
                    / Decimal::from(10_000_u32),
                liquidity,
                timestamp_ms,
            })
        }
        TradeMessageStatus::Failed => Ok(PmAccountEvent::OrderRejected {
            strategy: None,
            order_id,
            reason: "trade failed".into(),
            timestamp_ms,
        }),
        TradeMessageStatus::Retrying => Ok(PmAccountEvent::OrderStatus {
            strategy: None,
            order_id,
            status: "retrying".into(),
            timestamp_ms,
        }),
        TradeMessageStatus::Unknown(status) => Ok(PmAccountEvent::OrderStatus {
            strategy: None,
            order_id,
            status: status.clone(),
            timestamp_ms,
        }),
        _ => Err(DataSourceError::ReplayGap {
            message: "Polymarket trade event has an unsupported status".into(),
        }),
    }
}

fn trade_order_id(trade: &TradeMessage) -> Result<String, DataSourceError> {
    match trade.trader_side.as_ref() {
        Some(TraderSide::Taker) => trade.taker_order_id.clone(),
        Some(TraderSide::Maker | TraderSide::Unknown(_)) | None => trade
            .maker_orders
            .first()
            .map(|order| order.order_id.clone())
            .or_else(|| trade.taker_order_id.clone()),
        Some(_) => trade
            .maker_orders
            .first()
            .map(|order| order.order_id.clone())
            .or_else(|| trade.taker_order_id.clone()),
    }
    .ok_or_else(|| DataSourceError::ReplayGap {
        message: "Polymarket trade has no associated order id".into(),
    })
}

fn validate_market(
    market: B256,
    asset: U256,
    tokens: &MarketTokens,
    market_condition: B256,
) -> Result<(), DataSourceError> {
    if market != market_condition || tokens.outcome(&asset).is_none() {
        return Err(DataSourceError::ReplayGap {
            message: "Polymarket user event is outside the configured market".into(),
        });
    }
    Ok(())
}

const fn account_timestamp(event: &PmAccountEvent) -> i64 {
    match event {
        PmAccountEvent::Fill { timestamp_ms, .. }
        | PmAccountEvent::OrderAck { timestamp_ms, .. }
        | PmAccountEvent::OrderCancelled { timestamp_ms, .. }
        | PmAccountEvent::OrderRejected { timestamp_ms, .. }
        | PmAccountEvent::OrderStatus { timestamp_ms, .. }
        | PmAccountEvent::Settlement { timestamp_ms, .. } => *timestamp_ms,
    }
}

fn unavailable(message: impl Into<String>) -> DataSourceError {
    DataSourceError::Unavailable {
        message: message.into(),
    }
}

fn missing_timestamp(kind: &str) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: format!("Polymarket {kind} event has no timestamp"),
    }
}

fn unknown_asset(asset: U256) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: format!("unknown Polymarket asset id: {asset}"),
    }
}

fn unknown_side(side: VenueSide) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: format!("unknown Polymarket order side: {side:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::account_event;
    use crate::MarketTokens;
    use pmkit_core::MarketId;
    use pmkit_event::PmAccountEvent;
    use polymarket_client_sdk_v2::{
        clob::ws::WsMessage,
        types::{B256, U256},
    };

    const MARKET: B256 = B256::new([1; 32]);

    fn tokens() -> Result<MarketTokens, Box<dyn std::error::Error>> {
        Ok(MarketTokens::new(
            MarketId::new("btc-5m")?,
            U256::from(1),
            U256::from(2),
        ))
    }

    #[test]
    fn maps_typed_order_lifecycle_events() -> Result<(), Box<dyn std::error::Error>> {
        let order: WsMessage = serde_json::from_str(
            r#"{"event_type":"order","id":"order-1","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","price":"0.5","type":"PLACEMENT","timestamp":"42"}"#,
        )?;
        let tokens = tokens()?;
        let event = account_event(&order, &tokens, MARKET)?;
        assert!(
            matches!(event, PmAccountEvent::OrderAck { order_id, .. } if order_id == "order-1")
        );
        Ok(())
    }

    #[test]
    fn maps_failed_typed_trade_to_rejection() -> Result<(), Box<dyn std::error::Error>> {
        let trade: WsMessage = serde_json::from_str(
            r#"{"event_type":"trade","id":"trade-1","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","size":"2","price":"0.5","status":"FAILED","timestamp":"42","taker_order_id":"order-1"}"#,
        )?;
        let tokens = tokens()?;
        let event = account_event(&trade, &tokens, MARKET)?;
        assert!(
            matches!(event, PmAccountEvent::OrderRejected { order_id, .. } if order_id == "order-1")
        );
        Ok(())
    }

    #[test]
    fn maps_typed_trade_venue_fill_identity() -> Result<(), Box<dyn std::error::Error>> {
        let trade: WsMessage = serde_json::from_str(
            r#"{"event_type":"trade","id":"trade-1","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","size":"2","price":"0.5","status":"MATCHED","timestamp":"42","taker_order_id":"order-1","trader_side":"TAKER"}"#,
        )?;
        let tokens = tokens()?;

        let event = account_event(&trade, &tokens, MARKET)?;

        assert!(matches!(
            event,
            PmAccountEvent::Fill {
                identity: pmkit_event::FillIdentity::Venue(id),
                ..
            } if id == "trade-1"
        ));
        Ok(())
    }
}
