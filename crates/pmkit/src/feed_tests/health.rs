use super::{cex, metadata, pm};
use crate::feed::{FeedMode, MergedFeed, SourceDefinition, SourceTaskDefinition};
use crate::{FeedHealthSnapshot, RunMetrics};
use pmkit_core::RunId;
use pmkit_data::{
    DataSourceError, GapRange, GapSequenceDomain, SourceGap, SourceHeartbeat, SourceSignal,
};
use pmkit_tape::{RawCapturePayload, RawCaptureReader};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

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

fn audit_capture(
    path: &Path,
) -> Result<Vec<pmkit_tape::RawCaptureRecord>, Box<dyn std::error::Error>> {
    let mut reader = RawCaptureReader::new_audit(BufReader::new(File::open(path)?));
    let mut records = Vec::new();
    while let Some(record) = reader.next_record()? {
        records.push(record);
    }
    Ok(records)
}

fn pm_with_raw_frame(
    timestamp_ms: i64,
    frame_sequence: i64,
    raw_frame: &[u8],
) -> Result<SourceSignal, Box<dyn std::error::Error>> {
    let SourceSignal::Data(mut data) = pm(timestamp_ms, frame_sequence)? else {
        unreachable!("pm helper returns data")
    };
    let pmkit_event::SourceEnvelope::PmMarket(envelope) = data.as_mut() else {
        unreachable!("pm helper returns a market envelope")
    };
    envelope.raw_frame = raw_frame.to_vec();
    Ok(SourceSignal::Data(data))
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
    // Given: a source reports a heartbeat before returning its own replay gap.
    let metrics = metrics("health-replay-gap")?;
    let directory = tempfile::tempdir()?;
    let directory_path = directory.path().to_owned();
    let capture_run = RunId::new("health-replay-gap-capture")?;
    let capture_config = crate::capture::CaptureConfig::new(directory.path());
    let capture_path = capture_config.path_for(&capture_run);
    let recorder = capture_config.open(&capture_run)?;
    let result = MergedFeed::from_tasks(
        FeedMode::Paper,
        vec![SourceTaskDefinition::new("gap", |sink| async move {
            sink.send(SourceSignal::Heartbeat(SourceHeartbeat {
                metadata: metadata("publisher", 123, 1, 4),
                raw_frame: b"heartbeat".to_vec(),
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
            Err(DataSourceError::ReplayGap {
                message: "upstream gap".to_owned(),
            })
        })],
        None,
    )
    .with_metrics(metrics.clone())
    .with_capture(Some(recorder))
    .collect()
    .await;

    // When: the merge captures its local gap and propagates the source's terminal error.
    let Err(DataSourceError::ReplayGap { message }) = result else {
        return Err("expected the original replay gap".into());
    };

    // Then: the source reason survives capture and the local gap has no fake transport.
    assert_eq!(message, "upstream gap");
    assert_eq!(health(&metrics, "gap")?.gap_count, 1);
    let records = audit_capture(&capture_path)?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].capture.source_id, "gap");
    assert!(records[1].capture.transport.is_none());
    let RawCapturePayload::Gap {
        range: None,
        unresolved_count: None,
        reason,
        raw_frame,
    } = &records[1].capture.payload
    else {
        return Err("source error gap contained fabricated transport data".into());
    };
    assert!(reason.contains("last observed frame 4 at source time 123 ms"));
    assert!(raw_frame.is_empty());
    drop(directory);
    assert!(!directory_path.exists());
    Ok(())
}

#[tokio::test]
async fn late_frame_gap_reason_uses_last_accepted_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let directory_path = directory.path().to_owned();
    let capture_run = RunId::new("health-late-frame-capture")?;
    let capture_config = crate::capture::CaptureConfig::new(directory.path());
    let capture_path = capture_config.path_for(&capture_run);
    let recorder = capture_config.open(&capture_run)?;
    let accepted_raw = b"accepted-frame";
    let rejected_raw = b"rejected-late-frame";
    let result = MergedFeed::from_fixture(
        FeedMode::Backtest,
        vec![SourceDefinition::finite(
            "pm",
            vec![
                pm_with_raw_frame(20, 41, accepted_raw)?,
                SourceSignal::Watermark(20),
                pm_with_raw_frame(10, 42, rejected_raw)?,
            ],
        )],
        None,
    )
    .with_capture(Some(recorder))
    .collect()
    .await;
    assert!(matches!(
        result,
        Err(DataSourceError::ReplayGap { message }) if message == "late record"
    ));

    let records = audit_capture(&capture_path)?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].capture.source_id, "pm");
    assert!(
        records[0]
            .capture
            .transport
            .as_ref()
            .is_some_and(|transport| { transport.frame_sequence == 41 })
    );
    assert!(matches!(
        &records[0].capture.payload,
        RawCapturePayload::Frame { raw_bytes } if raw_bytes == accepted_raw
    ));
    assert_eq!(records[1].capture.source_id, "pm");
    assert!(records[1].capture.receipt_time_ms > 0);
    assert!(records[1].capture.transport.is_none());
    assert!(records[1].capture.normalized_projection.is_none());
    let RawCapturePayload::Gap {
        range: None,
        unresolved_count: None,
        reason,
        raw_frame,
    } = &records[1].capture.payload
    else {
        return Err("late-frame local gap contained fabricated provenance".into());
    };
    assert!(reason.contains("last observed frame 41 at source time 20 ms"));
    assert!(!reason.contains("frame 42"));
    assert!(!reason.contains("source time 10 ms"));
    assert!(raw_frame.is_empty());
    drop(directory);
    assert!(!directory_path.exists());
    Ok(())
}

#[tokio::test]
async fn explicit_source_gap_fails_closed_and_counts_a_gap()
-> Result<(), Box<dyn std::error::Error>> {
    let metrics = metrics("health-explicit-gap")?;
    let directory = tempfile::tempdir()?;
    let directory_path = directory.path().to_owned();
    let capture_run = RunId::new("health-explicit-gap-capture")?;
    let capture_config = crate::capture::CaptureConfig::new(directory.path());
    let capture_path = capture_config.path_for(&capture_run);
    let recorder = capture_config.open(&capture_run)?;
    let gap = SourceSignal::Gap(SourceGap {
        source_id: "pm".to_owned(),
        detected_at_ms: 30,
        metadata: Some(metadata("pm", 30, 1, 4)),
        raw_frame: b"gap-frame".to_vec(),
        range: Some(GapRange {
            domain: GapSequenceDomain::Channel,
            first_missing: 2,
            last_missing: 3,
        }),
        unresolved_count: Some(7),
        reason: "sequence discontinuity".to_owned(),
    });
    let result = MergedFeed::from_fixture(
        FeedMode::Backtest,
        vec![SourceDefinition::finite("pm", vec![gap])],
        None,
    )
    .with_metrics(metrics.clone())
    .with_capture(Some(recorder))
    .collect()
    .await;
    let Err(DataSourceError::ReplayGap { message }) = result else {
        return Err("expected an explicit source gap to fail closed".into());
    };

    assert!(message.contains("Channel sequences 2..=3"));
    assert!(message.contains("unresolved count Some(7)"));
    assert!(message.contains("sequence discontinuity"));
    assert_eq!(health(&metrics, "pm")?.gap_count, 1);

    let records = audit_capture(&capture_path)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].capture.source_id, "pm");
    assert_eq!(records[0].capture.receipt_time_ms, 30);
    assert!(records[0].capture.transport.is_some());
    let RawCapturePayload::Gap {
        range: Some(range),
        unresolved_count: Some(7),
        reason,
        raw_frame,
    } = &records[0].capture.payload
    else {
        return Err("reported gap provenance was not captured".into());
    };
    assert_eq!(range.domain, pmkit_tape::GapSequenceDomain::Channel);
    assert_eq!((range.first_missing, range.last_missing), (2, 3));
    assert_eq!(reason, "sequence discontinuity");
    assert_eq!(raw_frame, b"gap-frame");
    drop(directory);
    assert!(!directory_path.exists());
    Ok(())
}

#[tokio::test]
async fn silent_live_source_fails_closed_and_counts_a_gap() -> Result<(), Box<dyn std::error::Error>>
{
    let metrics = metrics("health-source-silence")?;
    let source = SourceTaskDefinition::new("quiet", |_sink| async {
        std::future::pending::<Result<(), DataSourceError>>().await
    });

    let result = MergedFeed::from_tasks(FeedMode::Live, vec![source], None)
        .with_source_silence_timeout(Duration::from_millis(10))
        .with_metrics(metrics.clone())
        .collect()
        .await;
    let Err(DataSourceError::ReplayGap { message }) = result else {
        return Err("expected a typed source-silence replay gap".into());
    };

    assert!(message.contains("silent") && message.contains("quiet"));
    assert_eq!(health(&metrics, "quiet")?.gap_count, 1);
    Ok(())
}

#[tokio::test]
async fn silent_source_capture_records_unknown_gap_without_fabricated_transport()
-> Result<(), Box<dyn std::error::Error>> {
    let metrics = metrics("health-source-silence-capture")?;
    let directory = tempfile::tempdir()?;
    let directory_path = directory.path().to_owned();
    let capture_run = RunId::new("health-source-silence-capture-tape")?;
    let capture_config = crate::capture::CaptureConfig::new(directory.path());
    let capture_path = capture_config.path_for(&capture_run);
    let recorder = capture_config.open(&capture_run)?;
    let source = SourceTaskDefinition::new("quiet", |_sink| async {
        std::future::pending::<Result<(), DataSourceError>>().await
    });

    let result = MergedFeed::from_tasks(FeedMode::Live, vec![source], None)
        .with_source_silence_timeout(Duration::from_millis(10))
        .with_metrics(metrics.clone())
        .with_capture(Some(recorder))
        .collect()
        .await;
    let Err(DataSourceError::ReplayGap { message }) = result else {
        return Err("expected a captured source-silence replay gap".into());
    };
    assert!(message.contains("silent") && message.contains("quiet"));
    assert_eq!(health(&metrics, "quiet")?.gap_count, 1);

    let records = audit_capture(&capture_path)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].capture.source_id, "quiet");
    assert!(records[0].capture.receipt_time_ms > 0);
    assert!(records[0].capture.transport.is_none());
    let RawCapturePayload::Gap {
        range: None,
        unresolved_count: None,
        reason,
        raw_frame,
    } = &records[0].capture.payload
    else {
        return Err("unobserved local gap contained fabricated provenance".into());
    };
    assert_eq!(reason, "locally detected source silence");
    assert!(raw_frame.is_empty());
    drop(directory);
    assert!(!directory_path.exists());
    Ok(())
}
