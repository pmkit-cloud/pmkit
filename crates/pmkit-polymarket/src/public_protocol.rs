use serde_json::Value;
use thiserror::Error;

use crate::subscription::PublicSubscription;

/// A public market event whose raw bytes remain independently auditable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicMarketEvent {
    /// Full orderbook snapshot or delta.
    OrderbookSnapshot,
    /// Price change batch.
    PriceChange,
    /// Last-trade-price update.
    LastTradePrice,
    /// Tick-size change.
    TickSizeChange,
    /// Best bid/ask update.
    BestBidAsk,
    /// Newly announced market.
    NewMarket,
    /// Resolved market.
    MarketResolved,
}

/// A complete unauthenticated inbound public WebSocket frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicInboundFrame {
    /// Server subscription acknowledgement/update.
    SubscriptionUpdate {
        /// Decoded subscription information.
        subscription: PublicSubscription,
        /// Original WebSocket bytes.
        raw: Vec<u8>,
    },
    /// Ping control frame.
    Ping {
        /// Original WebSocket bytes.
        raw: Vec<u8>,
    },
    /// Pong control frame.
    Pong {
        /// Original WebSocket bytes.
        raw: Vec<u8>,
    },
    /// A recognized market-bearing frame.
    Market {
        /// Recognized market event kind.
        event: PublicMarketEvent,
        /// Original WebSocket bytes.
        raw: Vec<u8>,
    },
    /// An unknown non-market control frame retained as evidence.
    UnknownControl {
        /// Provider event discriminator.
        event_type: String,
        /// Original WebSocket bytes.
        raw: Vec<u8>,
    },
}

impl PublicInboundFrame {
    /// Returns original WebSocket bytes without reserialization.
    #[must_use]
    pub fn raw(&self) -> &[u8] {
        match self {
            Self::SubscriptionUpdate { raw, .. }
            | Self::Ping { raw }
            | Self::Pong { raw }
            | Self::Market { raw, .. }
            | Self::UnknownControl { raw, .. } => raw,
        }
    }

    /// Returns a subscription update payload when this is one.
    ///
    /// # Errors
    ///
    /// Returns [`PublicProtocolError::Malformed`] for every other frame kind.
    pub fn subscription_update(&self) -> Result<&PublicSubscription, PublicProtocolError> {
        match self {
            Self::SubscriptionUpdate { subscription, .. } => Ok(subscription),
            _ => Err(PublicProtocolError::Malformed {
                detail: "frame is not a subscription update",
                raw: self.raw().to_vec(),
            }),
        }
    }
}

/// A public outbound control frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublicOutboundFrame {
    /// A market-channel subscription update.
    SubscriptionUpdate(PublicSubscription),
    /// Client keepalive ping.
    Ping,
    /// Client pong response.
    Pong,
}

/// A raw public-protocol decoding or encoding failure.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublicProtocolError {
    /// A frame was malformed at the external JSON boundary.
    #[error("malformed public protocol frame: {detail}")]
    Malformed {
        /// Stable error class detail.
        detail: &'static str,
        /// Original raw bytes.
        raw: Vec<u8>,
    },
    /// An unknown market-bearing event cannot become typed output.
    #[error("unsupported public market event {event_type}")]
    UnsupportedMarketEvent {
        /// Unknown provider event discriminator.
        event_type: String,
        /// Original raw bytes that must be persisted as evidence.
        raw: Vec<u8>,
    },
}

/// Decodes one inbound public frame exactly once while retaining original bytes.
///
/// # Errors
///
/// Returns [`PublicProtocolError`] without discarding raw evidence when the frame is malformed or
/// an unsupported market-bearing event.
pub fn decode_public_inbound(raw: &[u8]) -> Result<PublicInboundFrame, PublicProtocolError> {
    let value: Value = serde_json::from_slice(raw).map_err(|_| PublicProtocolError::Malformed {
        detail: "not JSON",
        raw: raw.to_vec(),
    })?;
    let event_type = value
        .get("event_type")
        .and_then(Value::as_str)
        .ok_or_else(|| PublicProtocolError::Malformed {
            detail: "missing event_type",
            raw: raw.to_vec(),
        })?;
    let raw = raw.to_vec();
    match event_type {
        "subscription_update" => Ok(PublicInboundFrame::SubscriptionUpdate {
            subscription: subscription(&value, raw.clone())?,
            raw,
        }),
        "ping" => Ok(PublicInboundFrame::Ping { raw }),
        "pong" => Ok(PublicInboundFrame::Pong { raw }),
        "book" => market(PublicMarketEvent::OrderbookSnapshot, &value, raw),
        "price_change" => market(PublicMarketEvent::PriceChange, &value, raw),
        "last_trade_price" => market(PublicMarketEvent::LastTradePrice, &value, raw),
        "tick_size_change" => market(PublicMarketEvent::TickSizeChange, &value, raw),
        "best_bid_ask" => market(PublicMarketEvent::BestBidAsk, &value, raw),
        "new_market" => market(PublicMarketEvent::NewMarket, &value, raw),
        "market_resolved" => market(PublicMarketEvent::MarketResolved, &value, raw),
        event_type if market_bearing(&value) => Err(PublicProtocolError::UnsupportedMarketEvent {
            event_type: event_type.to_owned(),
            raw,
        }),
        event_type => Ok(PublicInboundFrame::UnknownControl {
            event_type: event_type.to_owned(),
            raw,
        }),
    }
}

/// Encodes a typed outbound control frame without disabling custom features.
///
/// # Errors
///
/// Returns [`PublicProtocolError::Malformed`] only if serialization cannot encode the frame.
pub fn encode_public_outbound(frame: &PublicOutboundFrame) -> Result<Vec<u8>, PublicProtocolError> {
    let value = match frame {
        PublicOutboundFrame::SubscriptionUpdate(subscription) => serde_json::json!({
            "type": "market",
            "operation": "subscribe",
            "assets_ids": subscription.asset_ids(),
            "initial_dump": true,
            "custom_feature_enabled": subscription.custom_feature_enabled(),
        }),
        PublicOutboundFrame::Ping => serde_json::json!({"event_type": "ping"}),
        PublicOutboundFrame::Pong => serde_json::json!({"event_type": "pong"}),
    };
    serde_json::to_vec(&value).map_err(|_| PublicProtocolError::Malformed {
        detail: "cannot encode outbound frame",
        raw: Vec::new(),
    })
}

fn subscription(value: &Value, raw: Vec<u8>) -> Result<PublicSubscription, PublicProtocolError> {
    let assets = value
        .get("assets_ids")
        .and_then(Value::as_array)
        .ok_or_else(|| PublicProtocolError::Malformed {
            detail: "subscription update lacks assets_ids",
            raw: raw.clone(),
        })?
        .iter()
        .map(Value::as_str)
        .collect::<Option<Vec<_>>>()
        .ok_or(PublicProtocolError::Malformed {
            detail: "subscription assets_ids are invalid",
            raw,
        })?;
    Ok(PublicSubscription::new(
        assets.into_iter().map(str::to_owned).collect(),
    ))
}

fn market(
    event: PublicMarketEvent,
    value: &Value,
    raw: Vec<u8>,
) -> Result<PublicInboundFrame, PublicProtocolError> {
    if value.get("market").and_then(Value::as_str).is_none() {
        return Err(PublicProtocolError::Malformed {
            detail: "market event lacks market identity",
            raw,
        });
    }
    Ok(PublicInboundFrame::Market { event, raw })
}

fn market_bearing(value: &Value) -> bool {
    value.get("market").is_some()
        || value.get("asset_id").is_some()
        || value.get("assets_ids").is_some()
}
