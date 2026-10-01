use super::{cex, pm};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_core::RunId;
use pmkit_data::{DataSourceError, SourceHeartbeat, SourceSignal};
use pmkit_event::StrategyFact;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

fn heartbeat(source_id: &str, sequence: i64) -> SourceSignal {
    SourceSignal::Heartbeat(SourceHeartbeat {
        metadata: super::metadata(source_id, 999_999, 9, sequence),
        raw_frame: b"heartbeat".to_vec(),
    })
}

#[tokio::test]
async fn live_and_paper_tasks_release_data_without_watermarks()
-> Result<(), Box<dyn std::error::Error>> {
    let event = pm(10, 1)?;
    for mode in [FeedMode::Live, FeedMode::Paper] {
        let event = event.clone();
        let source = SourceTaskDefinition::new("pm", move |sink| async move {
            sink.send(event)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            sink.send(SourceSignal::Eof)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            Ok(())
        });

        let facts = MergedFeed::from_tasks(mode, vec![source], None)
            .collect()
            .await?;
        assert!(matches!(facts.as_slice(), [StrategyFact::Market(_)]));
    }
    Ok(())
}

#[tokio::test]
async fn live_and_paper_heartbeat_keeps_a_quiet_source_alive_without_advancing_time()
-> Result<(), Box<dyn std::error::Error>> {
    for (index, mode) in [FeedMode::Live, FeedMode::Paper].into_iter().enumerate() {
        let event = pm(10, 1)?;
        let source = SourceTaskDefinition::new("pm", move |sink| async move {
            sink.send(event)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            for sequence in 0..6 {
                tokio::time::sleep(Duration::from_millis(25)).await;
                sink.send(heartbeat("pm", sequence))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
            }
            sink.send(SourceSignal::Eof)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            Ok(())
        });
        let metrics = crate::RunMetrics::new(&RunId::new(format!("quiet-heartbeat-{index}"))?);
        let facts = MergedFeed::from_tasks(mode, vec![source], None)
            .with_source_silence_timeout(Duration::from_millis(100))
            .with_metrics(metrics.clone())
            .collect()
            .await?;
        assert!(matches!(facts.as_slice(), [StrategyFact::Market(_)]));
        let health = metrics.snapshot().feed_health;
        assert_eq!(health[0].last_event_timestamp_ms, Some(10));
        assert_eq!(health[0].watermark_ms, None);
    }
    Ok(())
}

#[tokio::test]
async fn noisy_source_cannot_starve_silence_detection_for_another_source()
-> Result<(), Box<dyn std::error::Error>> {
    let noisy = SourceTaskDefinition::new("noisy", |sink| async move {
        let mut sequence = 0;
        loop {
            sink.send(heartbeat("noisy", sequence))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            sequence += 1;
            tokio::task::yield_now().await;
        }
    });
    let silent = SourceTaskDefinition::new("silent", |_sink| async {
        std::future::pending::<Result<(), DataSourceError>>().await
    });
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        MergedFeed::from_tasks(FeedMode::Live, vec![noisy, silent], None)
            .with_source_silence_timeout(Duration::from_millis(20))
            .collect(),
    )
    .await?;
    let Err(DataSourceError::ReplayGap { message }) = result else {
        return Err("expected the silent source to fail closed".into());
    };
    assert!(message.contains("silent"));
    assert!(!message.contains("noisy"));
    Ok(())
}

#[tokio::test]
async fn slow_reference_source_does_not_block_live_market_data()
-> Result<(), Box<dyn std::error::Error>> {
    let market_event = pm(10, 1)?;
    let market = SourceTaskDefinition::new("pm", move |sink| async move {
        sink.send(market_event)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        Ok(())
    });

    let (release_reference, wait_for_reference) = oneshot::channel();
    let (reference_started, reference_started_rx) = oneshot::channel();
    let reference = SourceTaskDefinition::new("reference", move |sink| async move {
        reference_started
            .send(())
            .map_err(|()| DataSourceError::SinkClosed)?;
        wait_for_reference
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(cex(5, 7))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        Ok(())
    });

    let (output, mut facts) = mpsc::channel(4);
    let merge = tokio::spawn(
        MergedFeed::from_tasks(FeedMode::Live, vec![market, reference], None)
            .with_source_silence_timeout(Duration::from_secs(2))
            .forward(output),
    );
    reference_started_rx.await?;
    let first = tokio::time::timeout(Duration::from_millis(250), facts.recv()).await?;
    let Some(first) = first else {
        return Err("market fact was not released while reference was pending".into());
    };
    assert!(matches!(first.fact, StrategyFact::Market(_)));

    release_reference
        .send(())
        .map_err(|()| "reference source ended before release")?;
    let second = tokio::time::timeout(Duration::from_secs(1), facts.recv()).await?;
    let Some(second) = second else {
        return Err("reference fact was not forwarded".into());
    };
    assert!(matches!(second.fact, StrategyFact::Reference(_)));
    merge.await??;
    Ok(())
}

#[tokio::test]
async fn backtest_task_merge_keeps_event_time_order_and_watermark_gaps()
-> Result<(), Box<dyn std::error::Error>> {
    let market_event = pm(20, 1)?;
    let market = SourceTaskDefinition::new("pm", move |sink| async move {
        for signal in [market_event, SourceSignal::Watermark(20), SourceSignal::Eof] {
            sink.send(signal)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
        Ok(())
    });
    let reference = SourceTaskDefinition::new("reference", |sink| async move {
        for signal in [cex(10, 7), SourceSignal::Watermark(20), SourceSignal::Eof] {
            sink.send(signal)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
        Ok(())
    });
    let facts = MergedFeed::from_tasks(FeedMode::Backtest, vec![market, reference], Some(20))
        .collect()
        .await?;
    assert!(matches!(
        facts.as_slice(),
        [StrategyFact::Reference(_), StrategyFact::Market(_)]
    ));

    let late_start = pm(10, 1)?;
    let incomplete = SourceTaskDefinition::new("incomplete", move |sink| async move {
        sink.send(late_start)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        Ok(())
    });
    assert!(matches!(
        MergedFeed::from_tasks(FeedMode::Backtest, vec![incomplete], Some(10))
            .collect()
            .await,
        Err(DataSourceError::ReplayGap { .. })
    ));
    Ok(())
}
