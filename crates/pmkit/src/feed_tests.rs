use crate::feed::{FeedMode, MergedFeed, SourceDefinition, SourceTaskDefinition};
use crate::{Cancellation, FeedHealthSnapshot, RunMetrics};
use pmkit_core::{MarketId, RunId};
use pmkit_data::{DataSourceError, SourceSignal};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmMarketEnvelope, SourceEnvelope,
    StrategyFact, StreamMetadata,
};
use pmkit_market::{Asset, Exchange, Outcome};
use rust_decimal::Decimal;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc;

mod health;
mod mode_parity;

fn metadata(source_id: &str, timestamp_ms: i64, rank: i64, sequence: i64) -> StreamMetadata {
    StreamMetadata {
        schema_version: 1,
        source_id: source_id.to_owned(),
        source_time_ms: timestamp_ms,
        canonical_source_rank: rank,
        receipt_time_ms: timestamp_ms,
        connection_id: source_id.to_owned(),
        connection_epoch: 0,
        frame_sequence: sequence,
        ingest_sequence: 999,
    }
}

fn pm(timestamp_ms: i64, sequence: i64) -> Result<SourceSignal, Box<dyn std::error::Error>> {
    Ok(SourceSignal::Data(Box::new(SourceEnvelope::PmMarket(
        PmMarketEnvelope {
            metadata: metadata("pm", timestamp_ms, 1, sequence),
            raw_frame: Vec::new(),
            fact: MarketEvent::BookUpdate {
                market: MarketId::new("btc-5m")?,
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(49, 2), Decimal::ONE)],
                asks: vec![(Decimal::new(51, 2), Decimal::ONE)],
                timestamp_ms,
            },
        },
    ))))
}

fn cex(timestamp_ms: i64, aggregate_trade_id: u64) -> SourceSignal {
    SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
        CexReferenceEnvelope {
            metadata: metadata("binance", timestamp_ms, 2, 0),
            fact: CexReferenceEvent::Trade {
                asset: Asset::Btc,
                exchange: Exchange::Binance,
                aggregate_trade_id,
                price: Decimal::new(100_000, 2),
                qty: Decimal::ONE,
                is_buyer_maker: false,
                timestamp_ms,
            },
        },
    )))
}

fn pm_with_receipt(
    timestamp_ms: i64,
    sequence: i64,
    receipt_time_ms: i64,
) -> Result<SourceSignal, Box<dyn std::error::Error>> {
    let SourceSignal::Data(mut envelope) = pm(timestamp_ms, sequence)? else {
        unreachable!("pm helper always returns data")
    };
    let SourceEnvelope::PmMarket(pm) = envelope.as_mut() else {
        unreachable!("pm helper always returns PM market data")
    };
    pm.metadata.receipt_time_ms = receipt_time_ms;
    Ok(SourceSignal::Data(envelope))
}

struct DropProbe(Arc<AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn live_delivers_before_source_eof_and_cancels_cleanly()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = Cancellation::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let source_dropped = Arc::clone(&dropped);
    let (output, mut facts) = mpsc::channel(4);
    let feed = MergedFeed::from_tasks(
        FeedMode::Live,
        vec![SourceTaskDefinition::new("live", move |sink| async move {
            let _probe = DropProbe(source_dropped);
            sink.send(pm(10, 1).map_err(|error| DataSourceError::ReplayGap {
                message: error.to_string(),
            })?)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
            sink.send(SourceSignal::Watermark(10))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            std::future::pending::<Result<(), DataSourceError>>().await
        })],
        None,
    );
    let merge = tokio::spawn(feed.forward_with_cancellation(output, Some(cancellation.clone())));

    let fact = tokio::time::timeout(Duration::from_secs(1), facts.recv())
        .await?
        .ok_or("expected a fact before source EOF")?;
    assert!(matches!(fact.fact, StrategyFact::Market(_)));
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), merge)
            .await??
            .is_ok()
    );
    assert!(dropped.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn live_frontier_orders_facts_across_sources() -> Result<(), Box<dyn std::error::Error>> {
    let (output, mut facts) = mpsc::channel(4);
    let feed = MergedFeed::from_tasks(
        FeedMode::Paper,
        vec![
            SourceTaskDefinition::new("late", |sink| async move {
                sink.send(cex(10, 1))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                sink.send(SourceSignal::Watermark(10))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                tokio::time::sleep(Duration::from_millis(10)).await;
                sink.send(SourceSignal::Watermark(20))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                sink.send(SourceSignal::Eof)
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)
            }),
            SourceTaskDefinition::new("early", |sink| async move {
                sink.send(pm(20, 2).map_err(|error| DataSourceError::ReplayGap {
                    message: error.to_string(),
                })?)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
                sink.send(SourceSignal::Watermark(20))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                sink.send(SourceSignal::Eof)
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)
            }),
        ],
        None,
    );
    let merge = tokio::spawn(feed.forward(output));
    let first = facts.recv().await.ok_or("missing first fact")?;
    let second = facts.recv().await.ok_or("missing second fact")?;
    merge.await??;
    assert!(matches!(first.fact, StrategyFact::Reference(_)));
    assert!(matches!(second.fact, StrategyFact::Market(_)));
    Ok(())
}

#[tokio::test]
async fn unexpected_source_termination_fails_closed() {
    let result = MergedFeed::from_tasks(
        FeedMode::Live,
        vec![SourceTaskDefinition::new("closed", |_sink| async {
            Ok(())
        })],
        None,
    )
    .collect()
    .await;
    assert!(matches!(
        result,
        Err(DataSourceError::ReplayGap { message }) if message == "premature EOF"
    ));
}

#[tokio::test]
async fn deterministic_source_merge_matches_all_modes() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = vec![
        SourceDefinition::finite(
            "pm",
            vec![pm(10, 1)?, SourceSignal::Watermark(20), SourceSignal::Eof],
        ),
        SourceDefinition::finite(
            "binance",
            vec![cex(10, 7), SourceSignal::Watermark(20), SourceSignal::Eof],
        ),
    ];
    let mut expected = None;
    for mode in [FeedMode::Backtest, FeedMode::Paper, FeedMode::Live] {
        let facts = MergedFeed::from_fixture(mode, fixture.clone(), Some(20))
            .collect()
            .await?;
        if let Some(expected) = &expected {
            assert_eq!(&facts, expected);
        } else {
            expected = Some(facts);
        }
    }
    assert!(matches!(
        expected.as_deref(),
        Some([StrategyFact::Market(_), StrategyFact::Reference(_)])
    ));
    Ok(())
}

#[tokio::test]
async fn feed_reports_logical_lag() -> Result<(), Box<dyn std::error::Error>> {
    // Given: two sources whose safe merge frontier is 20 but whose latest events differ.
    let metrics = RunMetrics::new(&RunId::new("feed-logical-lag")?);
    let facts = MergedFeed::from_fixture(
        FeedMode::Backtest,
        vec![
            SourceDefinition::finite(
                "pm",
                vec![pm(10, 1)?, SourceSignal::Watermark(20), SourceSignal::Eof],
            ),
            SourceDefinition::finite(
                "binance",
                vec![cex(20, 7), SourceSignal::Watermark(20), SourceSignal::Eof],
            ),
        ],
        Some(20),
    )
    .with_metrics(metrics.clone())
    .collect()
    .await?;

    // When: the merge finishes after releasing both safe facts.
    let health = metrics.snapshot().feed_health;

    // Then: source lag is measured only from the logical watermark frontier.
    assert_eq!(facts.len(), 2);
    assert_eq!(
        health,
        vec![
            FeedHealthSnapshot {
                source: "binance".to_owned(),
                last_event_timestamp_ms: Some(20),
                watermark_ms: Some(20),
                logical_lag_ms: Some(0),
                gap_count: 0,
            },
            FeedHealthSnapshot {
                source: "pm".to_owned(),
                last_event_timestamp_ms: Some(10),
                watermark_ms: Some(20),
                logical_lag_ms: Some(10),
                gap_count: 0,
            },
        ]
    );
    Ok(())
}

#[tokio::test]
async fn source_gap_aborts_before_strategy_evaluation() -> Result<(), Box<dyn std::error::Error>> {
    for signals in [
        vec![pm(10, 1)?, SourceSignal::Watermark(10), SourceSignal::Eof],
        vec![SourceSignal::Watermark(20), pm(10, 1)?, SourceSignal::Eof],
    ] {
        let result = MergedFeed::from_fixture(
            FeedMode::Backtest,
            vec![SourceDefinition::finite("pm", signals)],
            Some(20),
        )
        .collect()
        .await;
        assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));
    }
    Ok(())
}

#[tokio::test]
async fn live_frame_is_checked_before_its_receipt_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    // The first frame establishes its one-millisecond receipt bound only after acceptance.
    let facts = MergedFeed::from_fixture(
        FeedMode::Live,
        vec![SourceDefinition::finite(
            "pm",
            vec![
                pm_with_receipt(1_000, 1, 10_000)?,
                SourceSignal::Watermark(10_000),
                SourceSignal::Eof,
            ],
        )],
        None,
    )
    .collect()
    .await?;
    assert_eq!(facts.len(), 1);
    Ok(())
}

#[tokio::test]
async fn paper_and_live_accept_late_event_time_after_inferred_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    // The event times step from 1788128198575 to 1788128198574, while the
    // first receipt only infers a frontier of 1788128198575. No declared
    // watermark precedes the late receipt; the final watermark closes both.
    for mode in [FeedMode::Paper, FeedMode::Live] {
        let facts = MergedFeed::from_fixture(
            mode,
            vec![SourceDefinition::finite(
                "pm",
                vec![
                    pm_with_receipt(1_788_128_198_575, 1, 1_788_128_198_576)?,
                    pm_with_receipt(1_788_128_198_574, 2, 1_788_128_198_574)?,
                    SourceSignal::Watermark(1_788_128_198_576),
                    SourceSignal::Eof,
                ],
            )],
            None,
        )
        .collect()
        .await?;
        let timestamps = facts
            .iter()
            .map(|fact| match fact {
                StrategyFact::Market(MarketEvent::BookUpdate { timestamp_ms, .. }) => {
                    Some(*timestamp_ms)
                }
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        assert_eq!(timestamps, Some(vec![1_788_128_198_574, 1_788_128_198_575]));
    }
    Ok(())
}

#[tokio::test]
async fn paper_and_live_reject_data_older_than_emitted_watermark()
-> Result<(), Box<dyn std::error::Error>> {
    // The same 1788128198575 -> 1788128198574 pair is a gap once 8575 is declared.
    for mode in [FeedMode::Paper, FeedMode::Live] {
        let result = MergedFeed::from_fixture(
            mode,
            vec![SourceDefinition::finite(
                "pm",
                vec![
                    pm_with_receipt(1_788_128_198_575, 1, 1_788_128_198_575)?,
                    SourceSignal::Watermark(1_788_128_198_575),
                    pm_with_receipt(1_788_128_198_574, 2, 1_788_128_198_574)?,
                    SourceSignal::Eof,
                ],
            )],
            None,
        )
        .collect()
        .await;
        assert!(
            matches!(result, Err(DataSourceError::ReplayGap { message }) if message.starts_with("late record from pm:"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn paper_and_live_reject_fact_older_than_already_released_order()
-> Result<(), Box<dyn std::error::Error>> {
    for mode in [FeedMode::Paper, FeedMode::Live] {
        let result = MergedFeed::from_fixture(
            mode,
            vec![SourceDefinition::finite(
                "pm",
                vec![
                    pm_with_receipt(100, 1, 100)?,
                    pm_with_receipt(102, 2, 102)?,
                    pm_with_receipt(99, 3, 99)?,
                    SourceSignal::Watermark(102),
                    SourceSignal::Eof,
                ],
            )],
            None,
        )
        .collect()
        .await;
        assert!(
            matches!(result, Err(DataSourceError::ReplayGap { message }) if message.starts_with("late record from pm:"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn backtest_health_does_not_report_live_bounded_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    let metrics = RunMetrics::new(&RunId::new("backtest-health-frontier")?);
    let result = MergedFeed::from_fixture(
        FeedMode::Backtest,
        vec![SourceDefinition::finite(
            "pm",
            vec![pm(10, 1)?, SourceSignal::Eof],
        )],
        None,
    )
    .with_metrics(metrics.clone())
    .collect()
    .await;
    assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));
    let health = metrics.snapshot().feed_health;
    assert_eq!(health.len(), 1);
    assert_eq!(health[0].watermark_ms, None);
    Ok(())
}

#[tokio::test]
async fn cancellation_wakes_idle_merge_and_drops_source_guard()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = Cancellation::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let source_dropped = Arc::clone(&dropped);
    let (output, _facts) = mpsc::channel(1);
    let feed = MergedFeed::from_tasks(
        FeedMode::Live,
        vec![SourceTaskDefinition::new("idle", move |_sink| async move {
            let _probe = DropProbe(source_dropped);
            std::future::pending::<Result<(), DataSourceError>>().await
        })],
        None,
    );
    let merge = feed.spawn(output, Some(cancellation.clone()));
    tokio::time::sleep(Duration::from_millis(10)).await;
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), merge.join()).await??;
    assert!(result.is_ok());
    assert!(dropped.load(Ordering::SeqCst));
    Ok(())
}
