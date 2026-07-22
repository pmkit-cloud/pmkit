use crate::{
    AppHandle, Pmkit, RunReport, RuntimeError,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};
use pmkit_event::MarketEvent;
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_run::{EvidenceRequirement, RetrievalWait};
use pmkit_runtime::StrategyRegistration;
use pmkit_spec::{BacktestRun, ConservativeV1Config, ReplaySpec};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

struct ScriptedHistory {
    ticks: Vec<i64>,
}

#[async_trait]
impl HistoricalDataSource for ScriptedHistory {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let market = MarketId::new("btc-5m").map_err(|_| DataSourceError::NotAvailable)?;
        for &timestamp_ms in &self.ticks {
            let event = MarketEvent::BookUpdate {
                market: market.clone(),
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                timestamp_ms,
            };
            sink.send(SourceSignal::market_event(event))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
        Ok(())
    }
}

async fn backtest_app() -> Result<AppHandle, Box<dyn std::error::Error>> {
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: vec![1, 2] }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("bt")?,
        PortfolioId::new("research")?,
        replay,
        Money::usdc(1_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    let runtime_config = config()?;
    let app = Pmkit::builder(runtime_config).run(run).start().await?;
    Ok(app)
}

#[tokio::test]
async fn backtest_drives_replay_through_strategy_to_fill() -> Result<(), Box<dyn std::error::Error>>
{
    let app = backtest_app().await?;

    let report = app.wait_for(RunId::new("bt")?).await?;
    let RunReport::Backtest(backtest) = report else {
        return Err("expected a backtest report".into());
    };
    assert_eq!(backtest.events_processed, 2);
    assert!(
        backtest.fills >= 1,
        "the taker buy should fill against the ask"
    );
    let manifest = app.manifest(&RunId::new("bt")?).ok_or("missing manifest")?;
    assert_eq!(manifest["mode"], "backtest");
    assert_eq!(manifest["run"], "bt");
    Ok(())
}

#[tokio::test]
async fn wait_for_is_repeatable() -> Result<(), Box<dyn std::error::Error>> {
    let app = backtest_app().await?;

    let first = app.wait_for(RunId::new("bt")?).await?;
    let second = app.wait_for(RunId::new("bt")?).await?;

    let (RunReport::Backtest(first), RunReport::Backtest(second)) = (first, second) else {
        return Err("expected backtest reports".into());
    };
    assert_eq!(first.events_processed, second.events_processed);
    assert_eq!(first.fills, second.fills);
    Ok(())
}

#[tokio::test]
async fn wait_for_rejects_unknown_run() -> Result<(), Box<dyn std::error::Error>> {
    let app = backtest_app().await?;
    let unknown = RunId::new("missing")?;

    let result = app.wait_for(unknown.clone()).await;

    assert!(matches!(result, Err(RuntimeError::UnknownRun(run)) if run == unknown));
    Ok(())
}
