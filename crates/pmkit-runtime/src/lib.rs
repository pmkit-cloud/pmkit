//! Runtime configuration, risk limits, and strategy registration for `PMKit`.
//!
//! These are neutral value types that describe *how* a run is configured. The
//! orchestration engine that consumes them lives above this crate.

use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pmkit_core::{MarketId, StrategyId};
use pmkit_money::Money;
use pmkit_strategy::StrategyFactory;

/// Explicit risk limits for a portfolio. There is no unlimited default; every
/// run must set these.
#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// Maximum notional per single order.
    pub max_order_notional: Money,
    /// Maximum notional held in one position.
    pub max_position_notional: Money,
    /// Maximum number of simultaneously open orders.
    pub max_open_orders: NonZeroU32,
    /// Maximum tolerated loss before the portfolio is killed.
    pub max_loss: Money,
}

/// What the runtime does with live orders during shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveOrderPolicy {
    /// Leave resting orders on the venue.
    Leave,
    /// Cancel only orders this runtime owns.
    CancelOwned,
    /// Cancel every order on the account (explicit opt-in).
    CancelAllExplicit,
}

/// Shutdown behavior configuration.
#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    /// Policy applied to live orders on shutdown.
    pub live_orders: LiveOrderPolicy,
    /// Maximum time to spend reconciling before giving up.
    pub reconciliation_timeout: Duration,
    /// Maximum time to spend flushing user tapes.
    pub tape_flush_timeout: Duration,
}

/// Top-level runtime configuration.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Maximum number of backtests running concurrently.
    pub backtest_concurrency: NonZeroUsize,
    /// Maximum time allowed for startup readiness.
    pub startup_timeout: Duration,
    /// Shutdown behavior.
    pub shutdown: ShutdownConfig,
    /// Directory where per-run manifests are written.
    pub manifest_dir: PathBuf,
}

/// Registration of one strategy factory for an exact market within a run.
#[derive(Clone)]
pub struct StrategyRegistration {
    id: StrategyId,
    market: MarketId,
    factory: Arc<dyn StrategyFactory>,
    name: Option<String>,
    version: Option<String>,
    config_revision: Option<String>,
}

impl StrategyRegistration {
    /// Creates a registration of `factory` for `id` on the exact `market`.
    #[must_use]
    pub fn new(id: StrategyId, market: MarketId, factory: Arc<dyn StrategyFactory>) -> Self {
        Self {
            id,
            market,
            factory,
            name: None,
            version: None,
            config_revision: None,
        }
    }

    /// Sets a human-readable strategy name.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the strategy version.
    #[must_use]
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Sets an opaque configuration revision for reproducibility.
    #[must_use]
    pub fn config_revision(mut self, revision: impl Into<String>) -> Self {
        self.config_revision = Some(revision.into());
        self
    }

    /// Returns the strategy id.
    #[must_use]
    pub const fn id(&self) -> &StrategyId {
        &self.id
    }

    /// Returns the exact market.
    #[must_use]
    pub const fn market(&self) -> &MarketId {
        &self.market
    }

    /// Returns the strategy factory.
    #[must_use]
    pub const fn factory(&self) -> &Arc<dyn StrategyFactory> {
        &self.factory
    }

    /// Returns the strategy name, if set.
    #[must_use]
    pub fn name_ref(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the strategy version, if set.
    #[must_use]
    pub fn version_ref(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Returns the configuration revision, if set.
    #[must_use]
    pub fn config_revision_ref(&self) -> Option<&str> {
        self.config_revision.as_deref()
    }
}

impl fmt::Debug for StrategyRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StrategyRegistration")
            .field("id", &self.id)
            .field("market", &self.market)
            .field("name", &self.name)
            .field("version", &self.version)
            .field("config_revision", &self.config_revision)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::{LiveOrderPolicy, RiskLimits, RuntimeConfig, ShutdownConfig, StrategyRegistration};
    use pmkit_core::{MarketId, StrategyId};
    use pmkit_money::Money;
    use pmkit_strategy::{
        Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
    };
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;

    struct FlatStrategy;

    impl Strategy for FlatStrategy {
        fn on_event(&mut self, _ctx: StrategyContext<'_>) -> Result<Actions, StrategyError> {
            Ok(Actions::none())
        }
    }

    struct FlatFactory;

    impl StrategyFactory for FlatFactory {
        fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
            Ok(Box::new(FlatStrategy))
        }
    }

    #[test]
    fn risk_limits_and_config_build() -> Result<(), Box<dyn std::error::Error>> {
        let limits = RiskLimits {
            max_order_notional: Money::usdc(100),
            max_position_notional: Money::usdc(1_000),
            max_open_orders: NonZeroU32::new(10).ok_or("nonzero orders")?,
            max_loss: Money::usdc(500),
        };
        assert!(limits.max_loss < limits.max_position_notional);

        let config = RuntimeConfig {
            backtest_concurrency: NonZeroUsize::new(4).ok_or("nonzero concurrency")?,
            startup_timeout: Duration::from_secs(30),
            shutdown: ShutdownConfig {
                live_orders: LiveOrderPolicy::CancelOwned,
                reconciliation_timeout: Duration::from_secs(30),
                tape_flush_timeout: Duration::from_secs(10),
            },
            manifest_dir: "./runs".into(),
        };
        assert_eq!(config.shutdown.live_orders, LiveOrderPolicy::CancelOwned);
        Ok(())
    }

    #[test]
    fn registration_builder_sets_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let factory: Arc<dyn StrategyFactory> = Arc::new(FlatFactory);
        let registration =
            StrategyRegistration::new(StrategyId::new("maker")?, MarketId::new("btc-5m")?, factory)
                .name("Two-sided maker")
                .version("1.3.0")
                .config_revision("sha256:abc");

        assert_eq!(registration.name_ref(), Some("Two-sided maker"));
        assert_eq!(registration.version_ref(), Some("1.3.0"));
        assert_eq!(registration.config_revision_ref(), Some("sha256:abc"));
        assert_eq!(registration.market(), &MarketId::new("btc-5m")?);
        assert_eq!(registration.id(), &StrategyId::new("maker")?);
        Ok(())
    }
}
