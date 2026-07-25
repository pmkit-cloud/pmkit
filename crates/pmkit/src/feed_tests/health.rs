use super::{cex, pm};
use crate::feed::{FeedMode, MergedFeed, SourceDefinition, SourceTaskDefinition};
use crate::{FeedHealthSnapshot, RunMetrics};
use pmkit_core::RunId;
use pmkit_data::{DataSourceError, SourceSignal};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn metrics(name: &str) -> Result<RunMetrics, Box<dyn std::error::Error>> {
    Ok(RunMetrics::new(&RunId::new(name)?))
}

fn health(
    metrics: &RunMetrics,
    source: &str,
) -> Result<FeedHealthSnapshot, Box<dyn std::error::Error>> {
    metrics
        .snapshot()
        .feed_health
        .into_iter()
        .find(|health| health.source == source)
        .ok_or_else(|| format!("missing health for source {source}").into())
}

#[tokio::test]
async fn feed_health_retains_max_accepted_event_timestamp() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: one source delivers an older event after its latest logical event.
    let metrics = metrics("health-max-timestamp")?;
    let result = MergedFeed::from_fixture(
        FeedMode::Paper,
        vec![
            SourceDefinition::finite(
                "pm",
                vec![
                    pm(20, 1)?,
                    pm(10, 2)?,
                    SourceSignal::Watermark(20),
                    SourceSignal::Eof,
                ],
            ),
            SourceDefinition::finite(
                "binance",
                vec![cex(20, 7), SourceSignal::Watermark(20), SourceSignal::Eof],
            ),
        ],
        None,
    )
    .with_metrics(metrics.clone())
    .collect()
    .await;

    // When: the merge reaches the shared logical frontier.
    result?;

    // Then: the reported timestamp is the maximum accepted canonical timestamp.
    let pm = health(&metrics, "pm")?;
    assert_eq!(pm.last_event_timestamp_ms, Some(20));
    assert_eq!(pm.logical_lag_ms, Some(0));
    Ok(())
}

#[tokio::test]
async fn feed_health_clamps_lag_and_counts_stranded_queue() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: unequal watermarks leave one accepted event beyond the terminal frontier.
    let metrics = metrics("health-stranded")?;
    let result = MergedFeed::from_fixture(
        FeedMode::Paper,
        vec![
            SourceDefinition::finite(
                "fast",
                vec![pm(30, 1)?, SourceSignal::Watermark(30), SourceSignal::Eof],
            ),
            SourceDefinition::finite(
                "slow",
                vec![cex(10, 7), SourceSignal::Watermark(20), SourceSignal::Eof],
            ),
        ],
        None,
    )
    .with_metrics(metrics.clone())
    .collect()
    .await;

    // When: no remaining source can advance the safe frontier.
    assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));

    // Then: the queued fact's source owns one gap and lag never goes negative.
    let fast = health(&metrics, "fast")?;
    assert_eq!(fast.last_event_timestamp_ms, Some(30));
    assert_eq!(fast.logical_lag_ms, Some(0));
    assert_eq!(fast.gap_count, 1);
    Ok(())
}

#[tokio::test]
async fn duplicate_source_names_fail_before_tasks_start() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: two task definitions claim one public source identity.
    let started = Arc::new(AtomicUsize::new(0));
    let first_started = Arc::clone(&started);
    let second_started = Arc::clone(&started);
    let metrics = metrics("health-duplicate")?;
    let result = MergedFeed::from_tasks(
        FeedMode::Paper,
        vec![
            SourceTaskDefinition::new("duplicate", move |_sink| {
                first_started.fetch_add(1, Ordering::Relaxed);
                async { Ok(()) }
            }),
            SourceTaskDefinition::new("duplicate", move |_sink| {
                second_started.fetch_add(1, Ordering::Relaxed);
                async { Ok(()) }
            }),
        ],
        None,
    )
    .with_metrics(metrics.clone())
    .collect()
    .await;

    // When: the merge validates its source topology.
    assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));

    // Then: neither ambiguous task starts and no health entry silently collapses.
    assert_eq!(started.load(Ordering::Relaxed), 0);
    assert!(metrics.snapshot().feed_health.is_empty());
    Ok(())
}

#[tokio::test]
async fn source_task_panic_counts_its_named_gap() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one source task panics before emitting a lifecycle signal.
    let metrics = metrics("health-panic")?;
    let result = MergedFeed::from_tasks(
        FeedMode::Paper,
        vec![SourceTaskDefinition::new("panic", |_sink| async move {
            std::panic::resume_unwind(Box::new("intentional source task panic"))
        })],
        None,
    )
    .with_metrics(metrics.clone())
    .collect()
    .await;

    // When: the merge receives the failed task join.
    assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));

    // Then: the join failure is attributed to the source task that panicked.
    assert_eq!(health(&metrics, "panic")?.gap_count, 1);
    Ok(())
}

#[tokio::test]
async fn non_gap_source_error_is_preserved() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a source returns a typed availability error rather than a replay gap.
    let metrics = metrics("health-unavailable")?;
    let result = MergedFeed::from_tasks(
        FeedMode::Paper,
        vec![SourceTaskDefinition::new("unavailable", |_sink| async {
            Err(DataSourceError::NotAvailable)
        })],
        None,
    )
    .with_metrics(metrics.clone())
    .collect()
    .await;

    // When: the merge reaches the source failure boundary.
    assert!(matches!(result, Err(DataSourceError::NotAvailable)));

    // Then: the non-gap error and zero gap count both remain exact.
    assert_eq!(health(&metrics, "unavailable")?.gap_count, 0);
    Ok(())
}

#[tokio::test]
async fn replay_gap_source_error_is_preserved_and_counted() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: a source returns its own typed replay gap.
    let metrics = metrics("health-replay-gap")?;
    let result = MergedFeed::from_tasks(
        FeedMode::Paper,
        vec![SourceTaskDefinition::new("gap", |_sink| async {
            Err(DataSourceError::ReplayGap {
                message: "upstream gap".to_owned(),
            })
        })],
        None,
    )
    .with_metrics(metrics.clone())
    .collect()
    .await;

    // When: the merge propagates the source's terminal error.
    let Err(DataSourceError::ReplayGap { message }) = result else {
        return Err("expected the original replay gap".into());
    };

    // Then: the original error stays intact and its source owns the gap.
    assert_eq!(message, "upstream gap");
    assert_eq!(health(&metrics, "gap")?.gap_count, 1);
    Ok(())
}
