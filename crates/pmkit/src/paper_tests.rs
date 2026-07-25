use crate::{
    Pmkit, RunReport,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{DataSourceError, LiveDataSource, SourceSignal};
use pmkit_event::MarketEvent;
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_runtime::StrategyRegistration;
use pmkit_spec::{ConservativeV1Config, PaperRun};
use pmkit_store::{OwnerScope, TapeStore, TursoTapeStore};
use rust_decimal::Decimal;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;

struct ScriptedLive;

#[async_trait]
impl LiveDataSource for ScriptedLive {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market,
                outcome,
                bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                timestamp_ms: 1,
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        }
        sink.send(SourceSignal::Watermark(i64::MAX))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        Ok(())
    }
}

#[tokio::test]
async fn paper_run_drives_live_feed_to_fill() -> Result<(), Box<dyn std::error::Error>> {
    let run = PaperRun::new(
        RunId::new("paper")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    let app = Pmkit::builder(config()?).run(run).start().await?;
    let report = app.report(&RunId::new("paper")?).ok_or("missing report")?;
    let RunReport::Paper(paper) = report else {
        return Err("expected a paper report".into());
    };
    assert_eq!(paper.events_processed, 1);
    assert!(
        paper.fills >= 1,
        "the taker buy should fill against the ask"
    );
    assert!(paper.exposure.portfolio_notional > Decimal::ZERO);
    Ok(())
}

#[tokio::test]
async fn default_fee_unchanged() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a durable paper run with no explicit fee-model override.
    let directory = tempfile::tempdir()?;
    let store =
        Arc::new(TursoTapeStore::open_local(directory.path().join("default-fee.db")).await?);
    let run = PaperRun::new(
        RunId::new("paper-default-fee")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    // When: the default paper path fills ten shares at the 46-cent ask.
    Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;
    let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("paper-default-fee")?);
    let decisions = store.read_decisions(&scope).await?;
    let fill = decisions
        .iter()
        .find(|decision| decision.payload["event"]["kind"] == "fill")
        .ok_or("paper fill was not recorded")?;

    // Then: the durable fill fee exactly matches the legacy Crypto calculation.
    assert_eq!(fill.payload["event"]["fee"], "0.17388");
    drop(store);
    Ok(())
}
