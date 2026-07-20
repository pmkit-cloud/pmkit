//! The strategy contract for `PMKit`.
//!
//! A strategy is synchronous and non-blocking: it receives an immutable
//! [`StrategyContext`] on each event and returns [`Actions`] for the runtime to
//! validate. Strategies never receive credentials, sockets, or a mutable wallet,
//! and never call a venue directly.

use pmkit_book::{OrderBookL2, Position};
use pmkit_core::MarketId;
use pmkit_exec::{OrderId, PlaceOrder};
use thiserror::Error;

/// A logical timestamp in milliseconds (system clock for live, simulated time
/// for paper and backtest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalTimestamp(pub i64);

impl LogicalTimestamp {
    /// Creates a logical timestamp from a millisecond value.
    #[must_use]
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Returns the timestamp as milliseconds.
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0
    }
}

/// A single intent returned by a strategy for the runtime to validate.
#[derive(Debug, Clone)]
pub enum Action {
    /// Place one order.
    Place(PlaceOrder),
    /// Cancel one order by venue id.
    Cancel(OrderId),
    /// Atomically cancel resting quotes and place new ones.
    ReplaceQuotes {
        /// Orders to cancel.
        cancel: Vec<OrderId>,
        /// Orders to place.
        place: Vec<PlaceOrder>,
    },
    /// Cancel every order owned by this strategy.
    CancelAll,
}

/// An ordered set of [`Action`]s returned from [`Strategy::on_event`].
#[derive(Debug, Clone, Default)]
pub struct Actions(Vec<Action>);

impl Actions {
    /// Returns an empty action set (do nothing this event).
    #[must_use]
    pub const fn none() -> Self {
        Self(Vec::new())
    }

    /// Returns an action set that places a single order.
    #[must_use]
    pub fn place(order: PlaceOrder) -> Self {
        Self(vec![Action::Place(order)])
    }

    /// Returns an action set that cancels every order owned by the strategy.
    #[must_use]
    pub fn cancel_all() -> Self {
        Self(vec![Action::CancelAll])
    }

    /// Appends an action.
    pub fn push(&mut self, action: Action) {
        self.0.push(action);
    }

    /// Returns the actions as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[Action] {
        &self.0
    }

    /// Returns `true` when there are no actions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Immutable per-event context handed to a strategy.
#[derive(Debug)]
pub struct StrategyContext<'a> {
    /// The exact market the strategy trades.
    pub market: &'a MarketId,
    /// The current order book for the traded outcome token.
    pub book: &'a OrderBookL2,
    /// The strategy's current positions.
    pub positions: &'a [Position],
    /// The logical time of this event.
    pub now: LogicalTimestamp,
}

/// Raised when a strategy fails to handle an event.
#[derive(Debug, Error)]
#[error("strategy error: {message}")]
pub struct StrategyError {
    /// Human-readable failure detail.
    pub message: String,
}

/// Raised when a [`StrategyFactory`] fails to create a strategy instance.
#[derive(Debug, Error)]
#[error("strategy init error: {message}")]
pub struct StrategyInitError {
    /// Human-readable failure detail.
    pub message: String,
}

/// A single mutable strategy instance owned by one strategy key.
pub trait Strategy: Send {
    /// Handles one event and returns actions for the runtime to validate.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyError`] if the strategy cannot process the event; the
    /// runtime disables only the failing strategy key.
    fn on_event(&mut self, ctx: StrategyContext<'_>) -> Result<Actions, StrategyError>;
}

/// Creates fresh [`Strategy`] instances, one per strategy key.
pub trait StrategyFactory: Send + Sync {
    /// Creates a new strategy instance.
    ///
    /// # Errors
    ///
    /// Returns [`StrategyInitError`] if the instance cannot be constructed.
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError>;
}

#[cfg(test)]
mod tests {
    use super::{
        Actions, LogicalTimestamp, Strategy, StrategyContext, StrategyError, StrategyFactory,
    };
    use pmkit_book::OrderBookL2;
    use pmkit_core::MarketId;

    struct FlatStrategy;

    impl Strategy for FlatStrategy {
        fn on_event(&mut self, _ctx: StrategyContext<'_>) -> Result<Actions, StrategyError> {
            Ok(Actions::none())
        }
    }

    struct FlatFactory;

    impl StrategyFactory for FlatFactory {
        fn create(&self) -> Result<Box<dyn Strategy>, super::StrategyInitError> {
            Ok(Box::new(FlatStrategy))
        }
    }

    #[test]
    fn factory_creates_strategy_that_runs() -> Result<(), Box<dyn std::error::Error>> {
        let mut strategy = FlatFactory.create()?;
        let market = MarketId::new("btc-5m")?;
        let book = OrderBookL2::default();
        let actions = strategy.on_event(StrategyContext {
            market: &market,
            book: &book,
            positions: &[],
            now: LogicalTimestamp::from_millis(1_700_000_000_000),
        })?;
        assert!(actions.is_empty());
        Ok(())
    }
}
