//! Reproducible run manifests for `PMKit`.
//!
//! [`build_manifest`] captures a run's topology — ids, mode, risk limits,
//! strategy registrations, and the replay window — as a redacted JSON value.
//! It reads only public run configuration and never touches credentials.

use pmkit_run::EvidenceRequirement;
use pmkit_runtime::{RiskLimits, RuntimeConfig, StrategyRegistration};
use pmkit_spec::RunSpec;
use serde_json::{Value, json};

/// Builds a redacted reproducibility manifest for `run` under `config`.
#[must_use]
pub fn build_manifest(run: &RunSpec, config: &RuntimeConfig) -> Value {
    let runtime = json!({
        "backtest_concurrency": config.backtest_concurrency.get(),
        "manifest_dir": config.manifest_dir.display().to_string(),
    });
    match run {
        RunSpec::Backtest(backtest) => json!({
            "mode": "backtest",
            "run": backtest.id().to_string(),
            "portfolio": backtest.portfolio().to_string(),
            "initial_cash": backtest.initial_cash().as_decimal().to_string(),
            "risk": risk_json(backtest.risk()),
            "strategies": strategies_json(backtest.strategies()),
            "replay": {
                "from": backtest.replay().from().to_rfc3339(),
                "to": backtest.replay().to().to_rfc3339(),
                "evidence": evidence_str(backtest.replay().evidence()),
            },
            "runtime": runtime,
        }),
        RunSpec::Paper(paper) => json!({
            "mode": "paper",
            "run": paper.id().to_string(),
            "portfolio": paper.portfolio().to_string(),
            "initial_cash": paper.initial_cash().as_decimal().to_string(),
            "risk": risk_json(paper.risk()),
            "strategies": strategies_json(paper.strategies()),
            "runtime": runtime,
        }),
        RunSpec::Live(live) => json!({
            "mode": "live",
            "run": live.id().to_string(),
            "portfolio": live.portfolio().to_string(),
            "risk": risk_json(live.risk()),
            "strategies": strategies_json(live.strategies()),
            "runtime": runtime,
        }),
    }
}

fn risk_json(risk: &RiskLimits) -> Value {
    json!({
        "max_order_notional": risk.max_order_notional.as_decimal().to_string(),
        "max_position_notional": risk.max_position_notional.as_decimal().to_string(),
        "max_open_orders": risk.max_open_orders.get(),
        "max_loss": risk.max_loss.as_decimal().to_string(),
    })
}

fn strategies_json(registrations: &[StrategyRegistration]) -> Value {
    let entries: Vec<Value> = registrations
        .iter()
        .map(|registration| {
            json!({
                "id": registration.id().to_string(),
                "market": registration.market().to_string(),
                "name": registration.name_ref(),
                "version": registration.version_ref(),
                "config_revision": registration.config_revision_ref(),
            })
        })
        .collect();
    Value::Array(entries)
}

const fn evidence_str(evidence: EvidenceRequirement) -> &'static str {
    match evidence {
        EvidenceRequirement::CorroboratedOnly => "corroborated_only",
        EvidenceRequirement::AllowSingleSource => "allow_single_source",
    }
}

#[cfg(test)]
mod tests {
    use super::build_manifest;
    use async_trait::async_trait;
    use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
    use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery};
    use pmkit_event::MarketEvent;
    use pmkit_money::Money;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use pmkit_runtime::{RiskLimits, RuntimeConfig, ShutdownConfig, StrategyRegistration};
    use pmkit_spec::{BacktestRun, ConservativeV1Config, ReplaySpec};
    use pmkit_strategy::{
        Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
    };
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;

    struct NoHistory;

    #[async_trait]
    impl HistoricalDataSource for NoHistory {
        async fn replay(
            &self,
            _query: ReplayQuery,
            _sink: Sender<MarketEvent>,
        ) -> Result<(), DataSourceError> {
            Ok(())
        }
    }

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
    fn backtest_manifest_captures_topology() -> Result<(), Box<dyn std::error::Error>> {
        let config = RuntimeConfig {
            backtest_concurrency: NonZeroUsize::new(4).ok_or("nonzero")?,
            startup_timeout: Duration::from_secs(30),
            shutdown: ShutdownConfig {
                live_orders: pmkit_runtime::LiveOrderPolicy::CancelOwned,
                reconciliation_timeout: Duration::from_secs(30),
                tape_flush_timeout: Duration::from_secs(10),
            },
            manifest_dir: "./runs".into(),
        };
        let replay = ReplaySpec::new(
            Arc::new(NoHistory),
            "2026-01-01T00:00:00Z".parse()?,
            "2026-02-01T00:00:00Z".parse()?,
            EvidenceRequirement::CorroboratedOnly,
            RetrievalWait::ReturnPending,
        );
        let run = BacktestRun::new(
            RunId::new("research")?,
            PortfolioId::new("research")?,
            replay,
            Money::usdc(100_000),
            RiskLimits {
                max_order_notional: Money::usdc(100),
                max_position_notional: Money::usdc(1_000),
                max_open_orders: NonZeroU32::new(10).ok_or("nonzero")?,
                max_loss: Money::usdc(500),
            },
            ConservativeV1Config {
                activation_latency: Duration::ZERO,
            },
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("maker")?,
            MarketId::new("btc-5m")?,
            Arc::new(FlatFactory),
        ));

        let manifest = build_manifest(&run.into(), &config);
        assert_eq!(manifest["mode"], "backtest");
        assert_eq!(manifest["run"], "research");
        assert_eq!(manifest["initial_cash"], "100000");
        assert_eq!(manifest["risk"]["max_open_orders"], 10);
        assert_eq!(manifest["strategies"][0]["id"], "maker");
        assert!(manifest["strategies"][0]["name"].is_null());
        assert_eq!(manifest["replay"]["evidence"], "corroborated_only");
        Ok(())
    }
}
