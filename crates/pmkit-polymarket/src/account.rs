use std::{
    fmt,
    sync::atomic::{AtomicI64, Ordering},
    time::Duration,
};

use futures::{SinkExt as _, StreamExt as _};
use pmkit_book::Side;
use pmkit_core::PortfolioId;
use pmkit_data::{
    DataSourceError, LiveAccountDataSource, SourceGap, SourceHeartbeat, SourceSignal,
};
use pmkit_event::{
    FillIdentity, Liquidity, PmAccountEnvelope, PmAccountEvent, SourceEnvelope, StreamMetadata,
};
use polymarket_client_sdk_v2::{
    auth::{ApiKey, Credentials},
    clob::{
        types::{OrderStatusType, Side as VenueSide, TraderSide},
        ws::types::response::{OrderMessageType, TradeMessageStatus},
        ws::{OrderMessage, SubscriptionRequest, TradeMessage, WsMessage},
    },
    types::{B256, U256},
    ws::WithCredentials,
};
use rust_decimal::Decimal;
use tokio::{
    sync::mpsc::Sender,
    time::{Instant, MissedTickBehavior, interval_at, sleep_until},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{MarketTokens, from_venue_side};

const SOURCE_ID: &str = "polymarket:user-ws";
const UNSUPPORTED_NATIVE_SETTLEMENT_FAILURE: &str = "unsupported native trade settlement failure";
const USER_WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const USER_PING_INTERVAL: Duration = Duration::from_secs(10);
const USER_PONG_TIMEOUT: Duration = Duration::from_secs(30);
static NEXT_CONNECTION_EPOCH: AtomicI64 = AtomicI64::new(0);

/// Authenticated Polymarket user-event source with replayable raw frames.
#[derive(Clone)]
pub struct PolymarketUserData {
    credentials: Credentials,
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
    /// Creates a raw authenticated source for one portfolio and market.
    /// The caller supplies the CLOB credentials.
    #[must_use]
    pub const fn new(
        credentials: Credentials,
        portfolio: PortfolioId,
        tokens: MarketTokens,
        market_condition: B256,
    ) -> Self {
        Self {
            credentials,
            portfolio,
            tokens,
            market_condition,
        }
    }

    async fn subscribe_at(
        &self,
        endpoint: &str,
        sink: Sender<SourceSignal>,
        ping_interval: Duration,
        pong_timeout: Duration,
    ) -> Result<(), DataSourceError> {
        let (mut socket, _) = connect_async(endpoint)
            .await
            .map_err(|_| unavailable("Polymarket user websocket connection failed"))?;
        let subscription = SubscriptionRequest::user(vec![self.market_condition])
            .as_authenticated(&self.credentials)
            .map_err(|_| unavailable("Polymarket user subscription encoding failed"))?;
        socket
            .send(Message::Text(subscription.into()))
            .await
            .map_err(|_| unavailable("Polymarket user subscription send failed"))?;

        let connection_epoch = NEXT_CONNECTION_EPOCH
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |epoch| {
                epoch.checked_add(1)
            })
            .map_err(|_| DataSourceError::ReplayGap {
                message: "Polymarket user connection epoch overflow".into(),
            })?;
        let connection_id = format!("{SOURCE_ID}#{connection_epoch}");
        let mut stream = UserStream {
            source: self,
            connection_id,
            connection_epoch,
            frame_counter: 0,
            ingest_sequence: 0,
        };
        let (mut write, mut read) = socket.split();
        let mut pings = interval_at(Instant::now(), ping_interval);
        pings.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let pong_deadline = sleep_until(Instant::now() + pong_timeout);
        tokio::pin!(pong_deadline);

        loop {
            tokio::select! {
                _ = pings.tick() => {
                    write
                        .send(Message::Text("PING".into()))
                        .await
                        .map_err(|_| unavailable("Polymarket user websocket ping failed"))?;
                }
                () = &mut pong_deadline => {
                    return Err(unavailable("Polymarket user websocket heartbeat timed out"));
                }
                incoming = read.next() => {
                    let Some(incoming) = incoming else {
                        return Err(unavailable("Polymarket user websocket ended"));
                    };
                    let message = incoming
                        .map_err(|_| unavailable("Polymarket user websocket receive failed"))?;
                    let receipt_time_ms = chrono::Utc::now().timestamp_millis();
                    handle_received_message(
                        message,
                        receipt_time_ms,
                        &mut stream,
                        &sink,
                        &mut write,
                        &mut pong_deadline,
                        pong_timeout,
                    )
                    .await?;
                }
            }
        }
    }
}

struct UserStream<'a> {
    source: &'a PolymarketUserData,
    connection_id: String,
    connection_epoch: i64,
    frame_counter: u64,
    ingest_sequence: u64,
}

async fn handle_received_message<W>(
    message: Message,
    receipt_time_ms: i64,
    stream: &mut UserStream<'_>,
    sink: &Sender<SourceSignal>,
    write: &mut W,
    pong_deadline: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    pong_timeout: Duration,
) -> Result<(), DataSourceError>
where
    W: futures::Sink<Message> + Unpin,
{
    match message {
        Message::Text(text) => {
            let frame_sequence = next_frame_sequence(&mut stream.frame_counter)?;
            handle_text_frame(
                text.as_ref(),
                receipt_time_ms,
                frame_sequence,
                stream,
                sink,
                pong_deadline,
                pong_timeout,
            )
            .await
        }
        Message::Ping(payload) => {
            let frame_sequence = next_frame_sequence(&mut stream.frame_counter)?;
            let ingest_sequence = next_ingest_sequence(&mut stream.ingest_sequence)?;
            write
                .send(Message::Pong(payload.clone()))
                .await
                .map_err(|_| unavailable("Polymarket user websocket pong failed"))?;
            send_heartbeat(
                receipt_time_ms,
                frame_sequence,
                ingest_sequence,
                payload.to_vec(),
                stream,
                sink,
            )
            .await?;
            Ok(())
        }
        Message::Pong(payload) => {
            let frame_sequence = next_frame_sequence(&mut stream.frame_counter)?;
            let ingest_sequence = next_ingest_sequence(&mut stream.ingest_sequence)?;
            pong_deadline.as_mut().reset(Instant::now() + pong_timeout);
            send_heartbeat(
                receipt_time_ms,
                frame_sequence,
                ingest_sequence,
                payload.to_vec(),
                stream,
                sink,
            )
            .await?;
            Ok(())
        }
        Message::Close(_) => Err(unavailable("Polymarket user websocket closed")),
        Message::Binary(_) => Err(DataSourceError::ReplayGap {
            message: "Polymarket user websocket emitted a binary frame".into(),
        }),
        Message::Frame(_) => Ok(()),
    }
}

async fn handle_text_frame(
    text: &str,
    receipt_time_ms: i64,
    frame_sequence: i64,
    stream: &mut UserStream<'_>,
    sink: &Sender<SourceSignal>,
    pong_deadline: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    pong_timeout: Duration,
) -> Result<(), DataSourceError> {
    let raw_frame = text.as_bytes().to_vec();
    if text == "PONG" {
        pong_deadline.as_mut().reset(Instant::now() + pong_timeout);
        let ingest_sequence = next_ingest_sequence(&mut stream.ingest_sequence)?;
        send_heartbeat(
            receipt_time_ms,
            frame_sequence,
            ingest_sequence,
            raw_frame,
            stream,
            sink,
        )
        .await?;
        return Ok(());
    }

    let message =
        serde_json::from_str::<WsMessage>(text).map_err(|_| DataSourceError::ReplayGap {
            message: "invalid Polymarket user event frame".into(),
        })?;
    let facts = match account_event(
        &message,
        &stream.source.tokens,
        stream.source.market_condition,
        stream.source.credentials.key(),
    ) {
        Ok(facts) => facts,
        Err(error) => {
            if let (WsMessage::Trade(trade), DataSourceError::ReplayGap { message: reason }) =
                (&message, &error)
                && matches!(&trade.status, TradeMessageStatus::Failed)
                && reason.starts_with(UNSUPPORTED_NATIVE_SETTLEMENT_FAILURE)
                && let Some(source_time_ms) =
                    trade.timestamp.or(trade.matchtime).or(trade.last_update)
            {
                let ingest_sequence = next_ingest_sequence(&mut stream.ingest_sequence)?;
                sink.send(SourceSignal::Gap(SourceGap {
                    source_id: SOURCE_ID.into(),
                    detected_at_ms: receipt_time_ms,
                    metadata: Some(stream_metadata(
                        source_time_ms,
                        receipt_time_ms,
                        &stream.connection_id,
                        stream.connection_epoch,
                        frame_sequence,
                        ingest_sequence,
                    )),
                    raw_frame: raw_frame.clone(),
                    range: None,
                    unresolved_count: None,
                    reason: reason.clone(),
                }))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            }
            return Err(error);
        }
    };
    for fact in facts {
        let ingest_sequence = next_ingest_sequence(&mut stream.ingest_sequence)?;
        sink.send(SourceSignal::Data(Box::new(SourceEnvelope::PmAccount(
            PmAccountEnvelope {
                portfolio: stream.source.portfolio.clone(),
                metadata: stream_metadata(
                    account_timestamp(&fact),
                    receipt_time_ms,
                    &stream.connection_id,
                    stream.connection_epoch,
                    frame_sequence,
                    ingest_sequence,
                ),
                raw_frame: raw_frame.clone(),
                fact,
            },
        ))))
        .await
        .map_err(|_| DataSourceError::SinkClosed)?;
    }
    Ok(())
}

async fn send_heartbeat(
    receipt_time_ms: i64,
    frame_sequence: i64,
    ingest_sequence: u64,
    raw_frame: Vec<u8>,
    stream: &UserStream<'_>,
    sink: &Sender<SourceSignal>,
) -> Result<(), DataSourceError> {
    sink.send(SourceSignal::Heartbeat(SourceHeartbeat {
        metadata: stream_metadata(
            receipt_time_ms,
            receipt_time_ms,
            &stream.connection_id,
            stream.connection_epoch,
            frame_sequence,
            ingest_sequence,
        ),
        raw_frame,
    }))
    .await
    .map_err(|_| DataSourceError::SinkClosed)
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
        self.subscribe_at(
            USER_WS_ENDPOINT,
            sink,
            USER_PING_INTERVAL,
            USER_PONG_TIMEOUT,
        )
        .await
    }
}

fn next_frame_sequence(sequence: &mut u64) -> Result<i64, DataSourceError> {
    let next = sequence
        .checked_add(1)
        .ok_or_else(|| DataSourceError::ReplayGap {
            message: "Polymarket user frame sequence overflow".into(),
        })?;
    let frame_sequence = i64::try_from(next).map_err(|_| DataSourceError::ReplayGap {
        message: "Polymarket user frame sequence exceeds signed range".into(),
    })?;
    *sequence = next;
    Ok(frame_sequence)
}

fn next_ingest_sequence(sequence: &mut u64) -> Result<u64, DataSourceError> {
    *sequence = sequence
        .checked_add(1)
        .ok_or_else(|| DataSourceError::ReplayGap {
            message: "Polymarket user ingest sequence overflow".into(),
        })?;
    Ok(*sequence)
}

fn stream_metadata(
    source_time_ms: i64,
    receipt_time_ms: i64,
    connection_id: &str,
    connection_epoch: i64,
    frame_sequence: i64,
    ingest_sequence: u64,
) -> StreamMetadata {
    StreamMetadata {
        schema_version: 4,
        source_id: SOURCE_ID.into(),
        source_time_ms,
        canonical_source_rank: 0,
        receipt_time_ms,
        connection_id: connection_id.into(),
        connection_epoch,
        frame_sequence,
        ingest_sequence,
    }
}

fn account_event(
    message: &WsMessage,
    tokens: &MarketTokens,
    market_condition: B256,
    api_key: ApiKey,
) -> Result<Vec<PmAccountEvent>, DataSourceError> {
    match message {
        WsMessage::Order(order) => Ok(vec![order_event(order, tokens, market_condition)?]),
        WsMessage::Trade(trade) => trade_event(trade, tokens, market_condition, api_key),
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

struct TradeOrderProjection<'a> {
    liquidity: Liquidity,
    own_maker_orders: Vec<&'a polymarket_client_sdk_v2::clob::ws::MakerOrder>,
    order_ids: Vec<String>,
}

fn trade_order_projection(
    trade: &TradeMessage,
    api_key: ApiKey,
) -> Result<TradeOrderProjection<'_>, DataSourceError> {
    let (liquidity, own_maker_orders, order_ids) = match trade.trader_side.as_ref() {
        Some(TraderSide::Maker) => {
            let own_maker_orders: Vec<_> = trade
                .maker_orders
                .iter()
                .filter(|maker| maker.owner == api_key)
                .collect();
            if own_maker_orders.is_empty() {
                return Err(DataSourceError::ReplayGap {
                    message: "Polymarket maker trade has no order owned by this API key".into(),
                });
            }
            let order_ids = own_maker_orders
                .iter()
                .map(|maker| maker.order_id.clone())
                .collect();
            (Liquidity::Maker, own_maker_orders, order_ids)
        }
        Some(TraderSide::Taker) => {
            if trade.trade_owner != Some(api_key)
                || trade.owner.is_some_and(|owner| owner != api_key)
            {
                return Err(DataSourceError::ReplayGap {
                    message: "Polymarket taker trade ownership does not match this API key".into(),
                });
            }
            let order_id =
                trade
                    .taker_order_id
                    .clone()
                    .ok_or_else(|| DataSourceError::ReplayGap {
                        message: "Polymarket taker trade has no taker order id".into(),
                    })?;
            (Liquidity::Taker, Vec::new(), vec![order_id])
        }
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
    if order_ids.iter().any(String::is_empty) {
        return Err(DataSourceError::ReplayGap {
            message: "Polymarket trade has an empty associated order id".into(),
        });
    }
    Ok(TradeOrderProjection {
        liquidity,
        own_maker_orders,
        order_ids,
    })
}

fn trade_event(
    trade: &TradeMessage,
    tokens: &MarketTokens,
    market_condition: B256,
    api_key: ApiKey,
) -> Result<Vec<PmAccountEvent>, DataSourceError> {
    validate_market(trade.market, trade.asset_id, tokens, market_condition)?;
    let timestamp_ms = trade
        .timestamp
        .or(trade.matchtime)
        .or(trade.last_update)
        .ok_or_else(|| missing_timestamp("trade"))?;
    if trade.id.is_empty() {
        return Err(DataSourceError::ReplayGap {
            message: "Polymarket trade has no trade id".into(),
        });
    }

    let TradeOrderProjection {
        liquidity,
        own_maker_orders,
        order_ids,
    } = trade_order_projection(trade, api_key)?;

    match &trade.status {
        TradeMessageStatus::Matched | TradeMessageStatus::Mined | TradeMessageStatus::Confirmed => {
            match liquidity {
                Liquidity::Maker => own_maker_orders
                    .into_iter()
                    .map(|maker| maker_fill(trade, maker, tokens, timestamp_ms))
                    .collect(),
                Liquidity::Taker => {
                    let outcome = tokens
                        .outcome(&trade.asset_id)
                        .ok_or_else(|| unknown_asset(trade.asset_id))?;
                    let side =
                        from_venue_side(trade.side).ok_or_else(|| unknown_side(trade.side))?;
                    if trade.size <= Decimal::ZERO
                        || trade.price <= Decimal::ZERO
                        || trade.price > Decimal::ONE
                    {
                        return Err(DataSourceError::ReplayGap {
                            message: "Polymarket taker trade has invalid fill values".into(),
                        });
                    }
                    let fee = taker_fee(trade)?;
                    let order_id =
                        order_ids
                            .into_iter()
                            .next()
                            .ok_or_else(|| DataSourceError::ReplayGap {
                                message: "Polymarket taker trade has no taker order id".into(),
                            })?;
                    Ok(vec![PmAccountEvent::Fill {
                        identity: venue_fill_identity(&trade.id, &order_id),
                        strategy: None,
                        order_id,
                        market: tokens.market().clone(),
                        outcome,
                        price: trade.price,
                        size: trade.size,
                        side,
                        fee,
                        liquidity,
                        timestamp_ms,
                    }])
                }
            }
        }
        TradeMessageStatus::Failed => Err(DataSourceError::ReplayGap {
            message: format!(
                "{UNSUPPORTED_NATIVE_SETTLEMENT_FAILURE}: trade_id={} owned_order_ids={order_ids:?}",
                trade.id
            ),
        }),
        TradeMessageStatus::Retrying => Ok(order_ids
            .into_iter()
            .map(|order_id| PmAccountEvent::OrderStatus {
                strategy: None,
                order_id,
                status: "retrying".into(),
                timestamp_ms,
            })
            .collect()),
        TradeMessageStatus::Unknown(status) => Ok(order_ids
            .into_iter()
            .map(|order_id| PmAccountEvent::OrderStatus {
                strategy: None,
                order_id,
                status: status.clone(),
                timestamp_ms,
            })
            .collect()),
        _ => Err(DataSourceError::ReplayGap {
            message: "Polymarket trade event has an unsupported status".into(),
        }),
    }
}

fn maker_fill(
    trade: &TradeMessage,
    maker: &polymarket_client_sdk_v2::clob::ws::MakerOrder,
    tokens: &MarketTokens,
    timestamp_ms: i64,
) -> Result<PmAccountEvent, DataSourceError> {
    validate_market(trade.market, maker.asset_id, tokens, trade.market)?;
    if tokens.token(pmkit_market::Outcome::Up) == tokens.token(pmkit_market::Outcome::Down) {
        return Err(DataSourceError::ReplayGap {
            message: "Polymarket binary outcome tokens are not distinct".into(),
        });
    }
    let taker_outcome = tokens
        .outcome(&trade.asset_id)
        .ok_or_else(|| unknown_asset(trade.asset_id))?;
    let outcome = tokens
        .outcome(&maker.asset_id)
        .ok_or_else(|| unknown_asset(maker.asset_id))?;
    let taker_side = from_venue_side(trade.side).ok_or_else(|| unknown_side(trade.side))?;
    let side = if outcome == taker_outcome {
        opposite_side(taker_side)
    } else {
        taker_side
    };
    if maker.matched_amount <= Decimal::ZERO
        || maker.price <= Decimal::ZERO
        || maker.price > Decimal::ONE
    {
        return Err(DataSourceError::ReplayGap {
            message: "Polymarket maker order has invalid fill values".into(),
        });
    }

    // Polymarket's prediction-market CLOB charges trading fees to takers, not makers.
    Ok(PmAccountEvent::Fill {
        identity: venue_fill_identity(&trade.id, &maker.order_id),
        strategy: None,
        order_id: maker.order_id.clone(),
        market: tokens.market().clone(),
        outcome,
        price: maker.price,
        size: maker.matched_amount,
        side,
        fee: Decimal::ZERO,
        liquidity: Liquidity::Maker,
        timestamp_ms,
    })
}

fn taker_fee(trade: &TradeMessage) -> Result<Decimal, DataSourceError> {
    if trade.fee_rate_bps == Some(Decimal::ZERO) {
        Ok(Decimal::ZERO)
    } else {
        Err(DataSourceError::ReplayGap {
            message: "Polymarket taker fee amount is unavailable in the trade event".into(),
        })
    }
}

const fn opposite_side(side: Side) -> Side {
    match side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

fn venue_fill_identity(trade_id: &str, order_id: &str) -> FillIdentity {
    FillIdentity::Venue(format!(
        "{}:{trade_id}:{}:{order_id}",
        trade_id.len(),
        order_id.len()
    ))
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
    use super::{PolymarketUserData, UserStream, account_event, handle_text_frame};
    use crate::MarketTokens;
    use futures::{SinkExt as _, StreamExt as _};
    use pmkit_core::{MarketId, PortfolioId};
    use pmkit_data::{DataSourceError, SourceSignal};
    use pmkit_event::{PmAccountEvent, SourceEnvelope};
    use polymarket_client_sdk_v2::{
        auth::{Credentials, Uuid},
        clob::ws::WsMessage,
        types::{B256, U256},
    };
    use tokio::{
        net::TcpListener,
        sync::mpsc,
        time::{Duration, timeout},
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    const MARKET: B256 = B256::new([1; 32]);
    const API_KEY: &str = "00000000-0000-0000-0000-000000000001";
    const OTHER_API_KEY: &str = "00000000-0000-0000-0000-000000000002";
    const SECRET: &str = "account-test-secret";
    const PASSPHRASE: &str = "account-test-passphrase";
    const ORDER_FRAME: &str = r#"{"event_type":"order","id":"order-1","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","price":"0.5","type":"PLACEMENT","timestamp":"42"}"#;
    const MAKER_TRADE_FRAME: &str = r#"{"event_type":"trade","id":"trade-1","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","size":"10","price":"0.60","status":"MATCHED","timestamp":"42","taker_order_id":"foreign-taker","trader_side":"MAKER","maker_orders":[{"order_id":"foreign-maker","owner":"00000000-0000-0000-0000-000000000002","asset_id":"1","matched_amount":"4","price":"0.60","outcome":"Yes"},{"order_id":"my-maker","owner":"00000000-0000-0000-0000-000000000001","asset_id":"2","matched_amount":"6","price":"0.40","outcome":"No"}]}"#;
    const MULTI_MAKER_TRADE_FRAME: &str = r#"{"event_type":"trade","id":"trade-2","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","size":"10","price":"0.60","status":"MATCHED","timestamp":"43","taker_order_id":"foreign-taker","trader_side":"MAKER","maker_orders":[{"order_id":"foreign-maker","owner":"00000000-0000-0000-0000-000000000002","asset_id":"1","matched_amount":"4","price":"0.60","outcome":"Yes"},{"order_id":"mine-yes","owner":"00000000-0000-0000-0000-000000000001","asset_id":"1","matched_amount":"2","price":"0.55","outcome":"Yes"},{"order_id":"mine-no","owner":"00000000-0000-0000-0000-000000000001","asset_id":"2","matched_amount":"6","price":"0.40","outcome":"No"}]}"#;

    fn test_error(message: &str) -> std::io::Error {
        std::io::Error::other(message.to_owned())
    }

    fn source() -> Result<PolymarketUserData, Box<dyn std::error::Error>> {
        Ok(PolymarketUserData::new(
            Credentials::new(Uuid::parse_str(API_KEY)?, SECRET.into(), PASSPHRASE.into()),
            PortfolioId::new("test-portfolio")?,
            tokens()?,
            MARKET,
        ))
    }

    fn tokens() -> Result<MarketTokens, Box<dyn std::error::Error>> {
        Ok(MarketTokens::new(
            MarketId::new("btc-5m")?,
            U256::from(1),
            U256::from(2),
        ))
    }

    fn decode(frame: &str) -> Result<Vec<PmAccountEvent>, Box<dyn std::error::Error>> {
        let message: WsMessage = serde_json::from_str(frame)?;
        Ok(account_event(
            &message,
            &tokens()?,
            MARKET,
            Uuid::parse_str(API_KEY)?,
        )?)
    }

    fn assert_debug_hides_credentials(source: &PolymarketUserData) {
        let debug = format!("{source:?}");
        assert!(!debug.contains(API_KEY));
        assert!(!debug.contains(SECRET));
        assert!(!debug.contains(PASSPHRASE));
    }

    async fn verify_subscription_and_ping(
        socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let subscription = socket
            .next()
            .await
            .ok_or_else(|| test_error("missing user subscription"))??;
        let Message::Text(subscription) = subscription else {
            return Err(test_error("expected user subscription text frame").into());
        };
        let subscription: serde_json::Value = serde_json::from_str(subscription.as_ref())?;
        assert_eq!(subscription["type"].as_str(), Some("user"));
        assert_eq!(subscription["operation"].as_str(), Some("subscribe"));
        let market = MARKET.to_string();
        assert_eq!(subscription["markets"][0].as_str(), Some(market.as_str()));
        assert_eq!(subscription["auth"]["apiKey"].as_str(), Some(API_KEY));
        assert_eq!(subscription["auth"]["secret"].as_str(), Some(SECRET));
        assert_eq!(
            subscription["auth"]["passphrase"].as_str(),
            Some(PASSPHRASE)
        );

        let ping = socket
            .next()
            .await
            .ok_or_else(|| test_error("missing client PING"))??;
        let Message::Text(ping) = ping else {
            return Err(test_error("expected client PING text frame").into());
        };
        assert_eq!(ping.as_str(), "PING");
        Ok(())
    }

    #[tokio::test]
    async fn authenticates_raw_stream_and_preserves_receipt_and_quiet_heartbeat()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            verify_subscription_and_ping(&mut socket).await?;
            socket.send(Message::Text("PONG".into())).await?;
            socket.send(Message::Text(ORDER_FRAME.into())).await?;
            socket
                .send(Message::Text(MULTI_MAKER_TRADE_FRAME.into()))
                .await?;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let source = source()?;
        assert_debug_hides_credentials(&source);

        let started_at_ms = chrono::Utc::now().timestamp_millis();
        let (sink, mut signals) = mpsc::channel(8);
        let user_source = source.clone();
        let run = tokio::spawn(async move {
            user_source
                .subscribe_at(
                    &endpoint,
                    sink,
                    Duration::from_millis(10),
                    Duration::from_millis(250),
                )
                .await
        });

        let heartbeat_signal = timeout(Duration::from_secs(1), signals.recv())
            .await?
            .ok_or_else(|| test_error("missing heartbeat signal"))?;
        let SourceSignal::Heartbeat(heartbeat) = heartbeat_signal else {
            return Err(test_error("first signal should be the actual server PONG").into());
        };
        assert_eq!(heartbeat.raw_frame, b"PONG");
        assert!(heartbeat.metadata.receipt_time_ms >= started_at_ms);
        assert!(
            heartbeat
                .metadata
                .connection_id
                .starts_with("polymarket:user-ws#")
        );
        assert_eq!(heartbeat.metadata.frame_sequence, 1);
        assert_eq!(heartbeat.metadata.ingest_sequence, 1);

        let account_signal = timeout(Duration::from_secs(1), signals.recv())
            .await?
            .ok_or_else(|| test_error("missing account data"))?;
        let SourceSignal::Data(data) = account_signal else {
            return Err(test_error("second signal should be the account event").into());
        };
        let SourceEnvelope::PmAccount(envelope) = *data else {
            return Err(test_error("expected account envelope").into());
        };
        assert_eq!(envelope.raw_frame, ORDER_FRAME.as_bytes());
        assert_eq!(envelope.metadata.source_time_ms, 42);
        assert!(envelope.metadata.receipt_time_ms >= started_at_ms);
        assert_eq!(envelope.metadata.frame_sequence, 2);
        assert_eq!(envelope.metadata.ingest_sequence, 2);
        assert!(matches!(envelope.fact, PmAccountEvent::OrderAck { .. }));

        let mut maker_metadata = None;
        for ingest_sequence in [3, 4] {
            let signal = timeout(Duration::from_secs(1), signals.recv())
                .await?
                .ok_or_else(|| test_error("missing projected maker fill"))?;
            let SourceSignal::Data(data) = signal else {
                return Err(test_error("expected projected maker fill data").into());
            };
            let SourceEnvelope::PmAccount(envelope) = *data else {
                return Err(test_error("expected projected account envelope").into());
            };
            assert_eq!(envelope.raw_frame, MULTI_MAKER_TRADE_FRAME.as_bytes());
            assert_eq!(envelope.metadata.frame_sequence, 3);
            assert_eq!(envelope.metadata.ingest_sequence, ingest_sequence);
            if let Some(receipt_time_ms) = maker_metadata {
                assert_eq!(envelope.metadata.receipt_time_ms, receipt_time_ms);
            } else {
                maker_metadata = Some(envelope.metadata.receipt_time_ms);
            }
            let expected_order_id = if ingest_sequence == 3 {
                "mine-yes"
            } else {
                "mine-no"
            };
            assert!(matches!(
                envelope.fact,
                PmAccountEvent::Fill { order_id, .. } if order_id == expected_order_id
            ));
        }

        let Err(error) = timeout(Duration::from_secs(1), run).await?? else {
            return Err(test_error("closed test socket should end the source").into());
        };
        assert!(!error.to_string().contains(SECRET));
        assert!(!error.to_string().contains(PASSPHRASE));
        server
            .await
            .map_err(|error| test_error(&error.to_string()))?
            .map_err(|error| test_error(&error.to_string()))?;
        Ok(())
    }

    #[tokio::test]
    async fn missing_pong_times_out_without_synthetic_heartbeat()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let endpoint = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut socket = accept_async(stream).await?;
            let _subscription = socket
                .next()
                .await
                .ok_or_else(|| test_error("missing subscription frame"))??;
            let ping = socket
                .next()
                .await
                .ok_or_else(|| test_error("missing client PING"))??;
            let Message::Text(ping) = ping else {
                return Err(test_error("expected client PING text frame").into());
            };
            assert_eq!(ping.as_str(), "PING");
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

        let source = source()?;
        let (sink, mut signals) = mpsc::channel(2);
        let Err(error) = timeout(
            Duration::from_secs(1),
            source.subscribe_at(
                &endpoint,
                sink,
                Duration::from_millis(5),
                Duration::from_millis(30),
            ),
        )
        .await?
        else {
            return Err(test_error("missing server PONG must fail closed").into());
        };
        assert!(error.to_string().contains("heartbeat timed out"));
        assert!(signals.try_recv().is_err());
        assert!(!error.to_string().contains(SECRET));
        assert!(!error.to_string().contains(PASSPHRASE));
        server
            .await
            .map_err(|error| test_error(&error.to_string()))?
            .map_err(|error| test_error(&error.to_string()))?;
        Ok(())
    }

    #[test]
    fn maps_typed_order_lifecycle_events() -> Result<(), Box<dyn std::error::Error>> {
        let events = decode(ORDER_FRAME)?;
        let [PmAccountEvent::OrderAck { order_id, .. }] = events.as_slice() else {
            return Err(test_error("expected one order acknowledgement").into());
        };
        assert_eq!(order_id, "order-1");
        Ok(())
    }

    #[test]
    fn fails_closed_on_owned_taker_settlement_failure() -> Result<(), Box<dyn std::error::Error>> {
        let message: WsMessage = serde_json::from_str(
            r#"{"event_type":"trade","id":"trade-1","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","size":"2","price":"0.5","status":"FAILED","timestamp":"42","taker_order_id":"order-1","owner":"00000000-0000-0000-0000-000000000001","trade_owner":"00000000-0000-0000-0000-000000000001","trader_side":"TAKER"}"#,
        )?;
        let Err(DataSourceError::ReplayGap { message }) =
            account_event(&message, &tokens()?, MARKET, Uuid::parse_str(API_KEY)?)
        else {
            return Err(test_error("failed taker settlement must return ReplayGap").into());
        };
        assert!(message.contains("unsupported native trade settlement failure"));
        assert!(message.contains("trade-1"));
        assert!(message.contains("order-1"));
        Ok(())
    }

    #[test]
    fn fails_closed_on_owned_maker_settlement_failure() -> Result<(), Box<dyn std::error::Error>> {
        let frame = MULTI_MAKER_TRADE_FRAME.replace("MATCHED", "FAILED");
        let message: WsMessage = serde_json::from_str(&frame)?;
        let Err(DataSourceError::ReplayGap { message }) =
            account_event(&message, &tokens()?, MARKET, Uuid::parse_str(API_KEY)?)
        else {
            return Err(test_error("failed maker settlement must return ReplayGap").into());
        };
        assert!(message.contains("unsupported native trade settlement failure"));
        assert!(message.contains("trade-2"));
        assert!(message.contains("mine-yes"));
        assert!(message.contains("mine-no"));
        assert!(!message.contains("foreign-maker"));
        Ok(())
    }

    #[tokio::test]
    async fn preserves_failed_trade_reporting_frame_in_explicit_gap()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = source()?;
        let mut stream = UserStream {
            source: &source,
            connection_id: "polymarket:user-ws#test".into(),
            connection_epoch: 7,
            frame_counter: 4,
            ingest_sequence: 8,
        };
        let frame = MULTI_MAKER_TRADE_FRAME.replace("MATCHED", "FAILED");
        let (sink, mut signals) = mpsc::channel(1);
        let pong_deadline = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(pong_deadline);
        let Err(DataSourceError::ReplayGap { message }) = handle_text_frame(
            &frame,
            99,
            4,
            &mut stream,
            &sink,
            &mut pong_deadline,
            Duration::from_secs(30),
        )
        .await
        else {
            return Err(test_error("failed settlement must return ReplayGap").into());
        };
        assert!(message.contains("unsupported native trade settlement failure"));
        let Some(SourceSignal::Gap(gap)) = signals.recv().await else {
            return Err(
                test_error("failed trade must preserve its reporting frame as a gap").into(),
            );
        };
        assert_eq!(gap.source_id, "polymarket:user-ws");
        assert_eq!(gap.detected_at_ms, 99);
        assert_eq!(gap.raw_frame, frame.as_bytes());
        assert!(gap.range.is_none());
        assert!(gap.unresolved_count.is_none());
        let Some(metadata) = gap.metadata else {
            return Err(test_error("reported failed trade gap must retain frame metadata").into());
        };
        assert_eq!(metadata.source_time_ms, 43);
        assert_eq!(metadata.receipt_time_ms, 99);
        assert_eq!(metadata.connection_id, "polymarket:user-ws#test");
        assert_eq!(metadata.connection_epoch, 7);
        assert_eq!(metadata.frame_sequence, 4);
        assert_eq!(metadata.ingest_sequence, 9);
        assert!(signals.try_recv().is_err());
        Ok(())
    }

    #[test]
    fn maps_only_our_complementary_maker_fill() -> Result<(), Box<dyn std::error::Error>> {
        let events = decode(MAKER_TRADE_FRAME)?;
        let [
            PmAccountEvent::Fill {
                identity: pmkit_event::FillIdentity::Venue(identity),
                order_id,
                outcome,
                price,
                size,
                side,
                fee,
                liquidity,
                ..
            },
        ] = events.as_slice()
        else {
            return Err(test_error("expected exactly one owned maker fill").into());
        };
        assert_eq!(identity, "7:trade-1:8:my-maker");
        assert_eq!(order_id, "my-maker");
        assert_eq!(*outcome, pmkit_market::Outcome::Down);
        assert_eq!(*price, rust_decimal::Decimal::new(40, 2));
        assert_eq!(*size, rust_decimal::Decimal::from(6));
        assert_eq!(*side, pmkit_book::Side::Buy);
        assert_eq!(*fee, rust_decimal::Decimal::ZERO);
        assert_eq!(*liquidity, pmkit_event::Liquidity::Maker);
        Ok(())
    }

    #[test]
    fn emits_distinct_owned_maker_fills_with_stable_redelivery_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut first_identities = None;
        for status in ["MATCHED", "MINED", "CONFIRMED"] {
            let frame = MULTI_MAKER_TRADE_FRAME.replace("MATCHED", status);
            let events = decode(&frame)?;
            if events.len() != 2 {
                return Err(test_error("expected both owned maker orders").into());
            }
            let mut identities = Vec::new();
            for event in &events {
                let PmAccountEvent::Fill {
                    identity,
                    order_id,
                    side,
                    ..
                } = event
                else {
                    return Err(test_error("expected maker fill for each owned order").into());
                };
                identities.push(identity.clone());
                match order_id.as_str() {
                    "mine-yes" => assert_eq!(*side, pmkit_book::Side::Sell),
                    "mine-no" => assert_eq!(*side, pmkit_book::Side::Buy),
                    _ => return Err(test_error("unexpected projected maker order").into()),
                }
            }
            let unique: std::collections::HashSet<_> = identities.iter().collect();
            assert_eq!(unique.len(), 2);
            if let Some(expected) = &first_identities {
                assert_eq!(expected, &identities);
            } else {
                first_identities = Some(identities);
            }
        }
        Ok(())
    }

    #[test]
    fn maps_owned_taker_order_without_using_maker_order_ids()
    -> Result<(), Box<dyn std::error::Error>> {
        let events = decode(
            r#"{"event_type":"trade","id":"trade-taker","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","size":"2","price":"0.5","status":"MATCHED","timestamp":"42","taker_order_id":"my-taker","owner":"00000000-0000-0000-0000-000000000001","trade_owner":"00000000-0000-0000-0000-000000000001","fee_rate_bps":"0","trader_side":"TAKER","maker_orders":[{"order_id":"foreign-maker","owner":"00000000-0000-0000-0000-000000000002","asset_id":"2","matched_amount":"3","price":"0.5","outcome":"No"}]}"#,
        )?;
        let [
            PmAccountEvent::Fill {
                identity: pmkit_event::FillIdentity::Venue(identity),
                order_id,
                size,
                liquidity,
                ..
            },
        ] = events.as_slice()
        else {
            return Err(test_error("expected one owned taker fill").into());
        };
        assert_eq!(identity, "11:trade-taker:8:my-taker");
        assert_eq!(order_id, "my-taker");
        assert_eq!(*size, rust_decimal::Decimal::from(2));
        assert_eq!(*liquidity, pmkit_event::Liquidity::Taker);
        Ok(())
    }

    #[test]
    fn rejects_missing_null_unknown_and_unowned_trade_roles()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut missing_role: serde_json::Value = serde_json::from_str(MAKER_TRADE_FRAME)?;
        let mut null_role = missing_role.clone();
        let Some(object) = missing_role.as_object_mut() else {
            return Err(test_error("maker trade fixture must be an object").into());
        };
        object.remove("trader_side");
        let missing_role = serde_json::to_string(&missing_role)?;
        let Some(object) = null_role.as_object_mut() else {
            return Err(test_error("maker trade fixture must be an object").into());
        };
        object.insert("trader_side".into(), serde_json::Value::Null);
        let null_role = serde_json::to_string(&null_role)?;
        let unknown_role = MAKER_TRADE_FRAME.replace("MAKER", "UNKNOWN");
        let unowned_makers = MAKER_TRADE_FRAME.replace(API_KEY, OTHER_API_KEY);
        for frame in [missing_role, null_role, unknown_role, unowned_makers] {
            let message: WsMessage = serde_json::from_str(&frame)?;
            let result = account_event(&message, &tokens()?, MARKET, Uuid::parse_str(API_KEY)?);
            assert!(matches!(
                result,
                Err(pmkit_data::DataSourceError::ReplayGap { .. })
            ));
        }
        Ok(())
    }

    #[test]
    fn rejects_nonzero_taker_fee_without_a_resolved_fee_amount()
    -> Result<(), Box<dyn std::error::Error>> {
        let frame = r#"{"event_type":"trade","id":"trade-taker","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","size":"2","price":"0.5","status":"MATCHED","timestamp":"42","taker_order_id":"my-taker","owner":"00000000-0000-0000-0000-000000000001","trade_owner":"00000000-0000-0000-0000-000000000001","fee_rate_bps":"100","trader_side":"TAKER"}"#;
        let message: WsMessage = serde_json::from_str(frame)?;
        let result = account_event(&message, &tokens()?, MARKET, Uuid::parse_str(API_KEY)?);
        assert!(matches!(
            result,
            Err(pmkit_data::DataSourceError::ReplayGap { .. })
        ));
        Ok(())
    }
}
