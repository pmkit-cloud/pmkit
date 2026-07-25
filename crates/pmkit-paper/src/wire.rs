use pmkit_book::Side;
use pmkit_core::{MarketId, StrategyId};
use pmkit_market::Outcome;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ledger::{
    OrderState, PaperLedgerEntry, PaperLedgerError, PaperLedgerEvent, RecordedOrder,
};

// allow: SIZE_OK — one versioned codec keeps every ledger variant symmetric.

const PAPER_LEDGER_SCHEMA_VERSION: u16 = 1;
const PAPER_LEDGER_RECORD_TYPE: &str = "paper_ledger";

#[derive(Debug, Serialize, Deserialize)]
struct WireEntry {
    record_type: String,
    schema_version: u16,
    event_id: String,
    sequence: u64,
    timestamp_ms: i64,
    event: WireEvent,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WireEvent {
    CashMovement {
        amount: Decimal,
    },
    OrderPlaced {
        order: WireOrder,
    },
    OrderAck {
        placement_id: String,
        order_id: String,
        state: WireOrderState,
        active_at_ms: i64,
    },
    OrderRejected {
        placement_id: String,
        order_id: String,
    },
    OrderCancelled {
        order_id: String,
    },
    Fill {
        order_id: String,
        market: String,
        outcome: WireOutcome,
        price: Decimal,
        size: Decimal,
        side: WireSide,
        fee: Decimal,
    },
    Settlement {
        market: String,
        outcome: WireOutcome,
        settled_size: Decimal,
        proceeds: Decimal,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct WireOrder {
    market: String,
    #[serde(default)]
    strategy: Option<String>,
    outcome: WireOutcome,
    side: WireSide,
    price: Decimal,
    qty: Decimal,
    post_only: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireOrderState {
    Immediate,
    Resting,
    Delayed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireOutcome {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireSide {
    Buy,
    Sell,
}

impl PaperLedgerEntry {
    /// Returns the stable event identity used for durable deduplication.
    #[must_use]
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Returns the contiguous owner-scoped ledger sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the logical event timestamp.
    #[must_use]
    pub const fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Encodes this entry as the versioned durable payload.
    ///
    /// # Errors
    ///
    /// Returns [`PaperLedgerError::InvalidRecord`] if JSON encoding fails.
    pub fn to_value(&self) -> Result<Value, PaperLedgerError> {
        serde_json::to_value(WireEntry {
            record_type: PAPER_LEDGER_RECORD_TYPE.into(),
            schema_version: PAPER_LEDGER_SCHEMA_VERSION,
            event_id: self.event_id.clone(),
            sequence: self.sequence,
            timestamp_ms: self.timestamp_ms,
            event: WireEvent::from(&self.event),
        })
        .map_err(|error| invalid_json(&error))
    }

    /// Decodes a tagged paper record, ignoring other causal-decision payloads.
    ///
    /// # Errors
    ///
    /// Returns [`PaperLedgerError`] for malformed or unsupported paper records.
    pub fn from_value(value: &Value) -> Result<Option<Self>, PaperLedgerError> {
        if value.get("record_type").and_then(Value::as_str) != Some(PAPER_LEDGER_RECORD_TYPE) {
            return Ok(None);
        }
        let wire: WireEntry =
            serde_json::from_value(value.clone()).map_err(|error| invalid_json(&error))?;
        if wire.schema_version != PAPER_LEDGER_SCHEMA_VERSION {
            return Err(PaperLedgerError::UnsupportedVersion {
                version: wire.schema_version,
            });
        }
        let expected_event_id = format!("paper-ledger-{}", wire.sequence);
        if wire.event_id != expected_event_id {
            return Err(PaperLedgerError::InvalidRecord {
                message: "event identity does not match its sequence".into(),
            });
        }
        Ok(Some(Self {
            event_id: wire.event_id,
            sequence: wire.sequence,
            timestamp_ms: wire.timestamp_ms,
            event: PaperLedgerEvent::try_from(wire.event)?,
        }))
    }
}

impl From<&PaperLedgerEvent> for WireEvent {
    fn from(event: &PaperLedgerEvent) -> Self {
        match event {
            PaperLedgerEvent::CashMovement { amount } => Self::CashMovement { amount: *amount },
            PaperLedgerEvent::OrderPlaced { order } => Self::OrderPlaced {
                order: WireOrder::from(order),
            },
            PaperLedgerEvent::OrderAck {
                placement_id,
                order_id,
                state,
                active_at_ms,
            } => Self::OrderAck {
                placement_id: placement_id.clone(),
                order_id: order_id.clone(),
                state: WireOrderState::from(*state),
                active_at_ms: *active_at_ms,
            },
            PaperLedgerEvent::OrderRejected {
                placement_id,
                order_id,
            } => Self::OrderRejected {
                placement_id: placement_id.clone(),
                order_id: order_id.clone(),
            },
            PaperLedgerEvent::OrderCancelled { order_id } => Self::OrderCancelled {
                order_id: order_id.clone(),
            },
            PaperLedgerEvent::Fill {
                order_id,
                market,
                outcome,
                price,
                size,
                side,
                fee,
            } => Self::Fill {
                order_id: order_id.clone(),
                market: market.to_string(),
                outcome: WireOutcome::from(*outcome),
                price: *price,
                size: *size,
                side: WireSide::from(*side),
                fee: *fee,
            },
            PaperLedgerEvent::Settlement {
                market,
                outcome,
                settled_size,
                proceeds,
            } => Self::Settlement {
                market: market.to_string(),
                outcome: WireOutcome::from(*outcome),
                settled_size: *settled_size,
                proceeds: *proceeds,
            },
        }
    }
}

impl TryFrom<WireEvent> for PaperLedgerEvent {
    type Error = PaperLedgerError;

    fn try_from(event: WireEvent) -> Result<Self, Self::Error> {
        Ok(match event {
            WireEvent::CashMovement { amount } => Self::CashMovement { amount },
            WireEvent::OrderPlaced { order } => Self::OrderPlaced {
                order: RecordedOrder::try_from(order)?,
            },
            WireEvent::OrderAck {
                placement_id,
                order_id,
                state,
                active_at_ms,
            } => Self::OrderAck {
                placement_id,
                order_id,
                state: state.into(),
                active_at_ms,
            },
            WireEvent::OrderRejected {
                placement_id,
                order_id,
            } => Self::OrderRejected {
                placement_id,
                order_id,
            },
            WireEvent::OrderCancelled { order_id } => Self::OrderCancelled { order_id },
            WireEvent::Fill {
                order_id,
                market,
                outcome,
                price,
                size,
                side,
                fee,
            } => Self::Fill {
                order_id,
                market: parse_market(market)?,
                outcome: outcome.into(),
                price,
                size,
                side: side.into(),
                fee,
            },
            WireEvent::Settlement {
                market,
                outcome,
                settled_size,
                proceeds,
            } => Self::Settlement {
                market: parse_market(market)?,
                outcome: outcome.into(),
                settled_size,
                proceeds,
            },
        })
    }
}

impl From<&RecordedOrder> for WireOrder {
    fn from(order: &RecordedOrder) -> Self {
        Self {
            market: order.market.to_string(),
            strategy: order.strategy.as_ref().map(ToString::to_string),
            outcome: WireOutcome::from(order.outcome),
            side: WireSide::from(order.side),
            price: order.price,
            qty: order.qty,
            post_only: order.post_only,
        }
    }
}

impl TryFrom<WireOrder> for RecordedOrder {
    type Error = PaperLedgerError;

    fn try_from(order: WireOrder) -> Result<Self, Self::Error> {
        Ok(Self {
            market: parse_market(order.market)?,
            strategy: order.strategy.map(parse_strategy).transpose()?,
            outcome: order.outcome.into(),
            side: order.side.into(),
            price: order.price,
            qty: order.qty,
            post_only: order.post_only,
        })
    }
}

impl From<OrderState> for WireOrderState {
    fn from(state: OrderState) -> Self {
        match state {
            OrderState::Immediate => Self::Immediate,
            OrderState::Resting => Self::Resting,
            OrderState::Delayed => Self::Delayed,
        }
    }
}

impl From<WireOrderState> for OrderState {
    fn from(state: WireOrderState) -> Self {
        match state {
            WireOrderState::Immediate => Self::Immediate,
            WireOrderState::Resting => Self::Resting,
            WireOrderState::Delayed => Self::Delayed,
        }
    }
}

impl From<Outcome> for WireOutcome {
    fn from(outcome: Outcome) -> Self {
        match outcome {
            Outcome::Up => Self::Up,
            Outcome::Down => Self::Down,
        }
    }
}

impl From<WireOutcome> for Outcome {
    fn from(outcome: WireOutcome) -> Self {
        match outcome {
            WireOutcome::Up => Self::Up,
            WireOutcome::Down => Self::Down,
        }
    }
}

impl From<Side> for WireSide {
    fn from(side: Side) -> Self {
        match side {
            Side::Buy => Self::Buy,
            Side::Sell => Self::Sell,
        }
    }
}

impl From<WireSide> for Side {
    fn from(side: WireSide) -> Self {
        match side {
            WireSide::Buy => Self::Buy,
            WireSide::Sell => Self::Sell,
        }
    }
}

fn parse_market(value: String) -> Result<MarketId, PaperLedgerError> {
    MarketId::new(value).map_err(|error| PaperLedgerError::InvalidRecord {
        message: error.to_string(),
    })
}

fn parse_strategy(value: String) -> Result<StrategyId, PaperLedgerError> {
    StrategyId::new(value).map_err(|error| PaperLedgerError::InvalidRecord {
        message: error.to_string(),
    })
}

fn invalid_json(error: &serde_json::Error) -> PaperLedgerError {
    PaperLedgerError::InvalidRecord {
        message: error.to_string(),
    }
}
