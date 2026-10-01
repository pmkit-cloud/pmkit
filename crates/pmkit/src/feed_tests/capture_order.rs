use crate::capture::{CaptureConfig, capture_merged_source_signal};
use crate::feed::{FeedMode, MergedFeed, SourceTaskDefinition};
use pmkit_core::RunId;
use pmkit_data::{DataSourceError, SourceGap, SourceHeartbeat, SourceSignal};
use pmkit_event::SourceEnvelope;
use pmkit_tape::{RawCapturePayload, RawCaptureReader, RawCaptureRecord};
use std::fs::File;
use std::io::{self, BufReader};
use std::path::Path;
use tokio::sync::{mpsc, oneshot};

type TestError = Box<dyn std::error::Error + Send + Sync>;
type TestResult<T> = Result<T, TestError>;

const SOURCE_ID: &str = "oracle:test";

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    ingest_sequence: u64,
    frame_sequence: u64,
    source_ingest_sequence: u64,
    payload: RawCapturePayload,
}

#[derive(Debug, Default)]
struct ConsumerEvidence {
    delivered: Vec<u64>,
    captured_in_consumer: Vec<u64>,
    delivered_source_times_ms: Vec<i64>,
    durable_accepted: Vec<u64>,
    strategy_evaluated: Vec<u64>,
    capture_error: Option<String>,
}

#[derive(Debug)]
struct RunEvidence {
    tape: Vec<Observation>,
    consumer: ConsumerEvidence,
    merge_error: Option<String>,
}

fn data(frame_sequence: i64) -> TestResult<(SourceSignal, Vec<u8>)> {
    data_at(frame_sequence, 100 + frame_sequence)
}

fn data_at(frame_sequence: i64, timestamp_ms: i64) -> TestResult<(SourceSignal, Vec<u8>)> {
    let source_ingest_sequence = u64::try_from(frame_sequence)?;
    let fixture_id = source_ingest_sequence
        .checked_add(1)
        .ok_or_else(|| io::Error::other("fixture id overflow"))?;
    let raw_frame = format!("synthetic transport frame {frame_sequence}").into_bytes();
    let SourceSignal::Data(mut envelope) = super::cex(timestamp_ms, fixture_id) else {
        unreachable!("cex helper returns data")
    };
    let SourceEnvelope::CexReference(reference) = envelope.as_mut() else {
        unreachable!("cex helper returns a reference envelope")
    };
    reference.metadata = super::metadata(SOURCE_ID, timestamp_ms, 2, frame_sequence);
    reference.metadata.ingest_sequence = source_ingest_sequence;
    reference.raw_frame = raw_frame.clone();
    Ok((SourceSignal::Data(envelope), raw_frame))
}

fn heartbeat() -> (SourceSignal, Vec<u8>) {
    let raw_frame = b"synthetic heartbeat transport frame 1".to_vec();
    let mut metadata = super::metadata(SOURCE_ID, 101, 2, 1);
    metadata.ingest_sequence = 1;
    (
        SourceSignal::Heartbeat(SourceHeartbeat {
            metadata,
            raw_frame: raw_frame.clone(),
        }),
        raw_frame,
    )
}

fn reporting_gap() -> (SourceSignal, Vec<u8>) {
    let raw_frame = b"synthetic reporting gap transport frame 1".to_vec();
    (
        SourceSignal::Gap(SourceGap {
            source_id: SOURCE_ID.to_owned(),
            detected_at_ms: 101,
            metadata: Some({
                let mut metadata = super::metadata(SOURCE_ID, 101, 2, 1);
                metadata.ingest_sequence = 1;
                metadata
            }),
            raw_frame: raw_frame.clone(),
            // The fixture has no exact application-sequence bounds to report.
            range: None,
            unresolved_count: None,
            reason: "reported discontinuity with unknown bounds".to_owned(),
        }),
        raw_frame,
    )
}

fn observations(path: &Path) -> TestResult<Vec<Observation>> {
    let file = File::open(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("open capture {}: {error}", path.display()),
        )
    })?;
    let mut reader = RawCaptureReader::new_audit(BufReader::new(file));
    let mut observations = Vec::new();
    while let Some(RawCaptureRecord {
        ingest_sequence,
        capture,
        ..
    }) = reader
        .next_record()
        .map_err(|error| io::Error::other(format!("read capture record: {error}")))?
    {
        let transport = capture
            .transport
            .as_ref()
            .ok_or_else(|| io::Error::other("capture record has no transport metadata"))?;
        observations.push(Observation {
            ingest_sequence,
            frame_sequence: transport.frame_sequence,
            source_ingest_sequence: transport.ingest_sequence,
            payload: capture.payload,
        });
    }
    Ok(observations)
}

fn contains_data_frame(path: &Path, frame_sequence: u64) -> TestResult<bool> {
    Ok(observations(path)?.iter().any(|observation| {
        observation.frame_sequence == frame_sequence
            && matches!(observation.payload, RawCapturePayload::Frame { .. })
    }))
}

fn consume(
    merged: crate::feed::MergedFact,
    recorder: &crate::capture::CaptureRecorder,
    path: &Path,
    evidence: &mut ConsumerEvidence,
) -> TestResult<bool> {
    let frame_sequence = u64::try_from(merged.metadata.frame_sequence)?;
    evidence.delivered.push(frame_sequence);
    evidence
        .delivered_source_times_ms
        .push(merged.metadata.source_time_ms);

    // The consumer does not append frames already captured by the merge.
    if !contains_data_frame(path, frame_sequence)? {
        let signal = SourceSignal::Data(Box::new(merged.source));
        if let Err(error) = capture_merged_source_signal(Some(recorder), &signal) {
            evidence.capture_error = Some(error.to_string());
            return Ok(false);
        }
        evidence.captured_in_consumer.push(frame_sequence);
    }

    // Match the driver ordering: durable acceptance and strategy evaluation
    // happen only after raw capture has succeeded.
    evidence.durable_accepted.push(frame_sequence);
    evidence.strategy_evaluated.push(frame_sequence);
    Ok(true)
}

async fn capture_with_gated_consumer(
    run_name: &str,
    mode: FeedMode,
    signals: Vec<SourceSignal>,
) -> TestResult<RunEvidence> {
    let directory = tempfile::tempdir()?;
    let run = RunId::new(run_name)?;
    let config = CaptureConfig::new(directory.path());
    let path = config.path_for(&run);
    let recorder = config.open(&run)?;
    let source = SourceTaskDefinition::new(SOURCE_ID, move |sink| async move {
        for signal in signals {
            sink.send(signal)
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
        Ok(())
    });
    let (output, mut events) = mpsc::channel(1024);
    let merge = tokio::spawn(
        MergedFeed::from_tasks(mode, vec![source], None)
            .with_capture(Some(recorder.clone()))
            .forward(output),
    );
    let (consumer_ready, ready) = oneshot::channel();
    let (resume_consumer, resume) = oneshot::channel();
    let consumer_path = path.clone();
    let consumer: tokio::task::JoinHandle<TestResult<ConsumerEvidence>> =
        tokio::spawn(async move {
            let mut evidence = ConsumerEvidence::default();
            let Some(first) = events.recv().await else {
                let _ = consumer_ready.send(());
                return Ok(evidence);
            };
            let _ = consumer_ready.send(());
            if resume.await.is_err() || !consume(first, &recorder, &consumer_path, &mut evidence)? {
                return Ok(evidence);
            }
            while let Some(merged) = events.recv().await {
                if !consume(merged, &recorder, &consumer_path, &mut evidence)? {
                    break;
                }
            }
            Ok(evidence)
        });

    ready.await?;
    let merge_error = merge.await?.err().map(|error| error.to_string());
    resume_consumer
        .send(())
        .map_err(|()| "gated consumer exited before release")?;
    let consumer = consumer.await??;
    Ok(RunEvidence {
        tape: observations(&path)?,
        consumer,
        merge_error,
    })
}

#[tokio::test]
async fn live_capture_preserves_data_heartbeat_data_transport_order() -> TestResult<()> {
    let (data0, raw0) = data(0)?;
    let (heartbeat, heartbeat_raw) = heartbeat();
    let (data2, raw2) = data(2)?;
    let evidence = capture_with_gated_consumer(
        "capture-order-heartbeat",
        FeedMode::Live,
        vec![data0, heartbeat, data2, SourceSignal::Eof],
    )
    .await?;

    assert_eq!(evidence.merge_error, None);
    assert_eq!(
        evidence.tape,
        vec![
            Observation {
                ingest_sequence: 0,
                frame_sequence: 0,
                source_ingest_sequence: 0,
                payload: RawCapturePayload::Frame { raw_bytes: raw0 },
            },
            Observation {
                ingest_sequence: 1,
                frame_sequence: 1,
                source_ingest_sequence: 1,
                payload: RawCapturePayload::Control {
                    name: "heartbeat".to_owned(),
                    raw_bytes: heartbeat_raw,
                },
            },
            Observation {
                ingest_sequence: 2,
                frame_sequence: 2,
                source_ingest_sequence: 2,
                payload: RawCapturePayload::Frame { raw_bytes: raw2 },
            },
        ],
        "consumer evidence: {:#?}",
        evidence.consumer
    );
    assert_eq!(evidence.consumer.delivered, [0, 2]);
    assert_eq!(evidence.consumer.captured_in_consumer, Vec::<u64>::new());
    assert_eq!(evidence.consumer.durable_accepted, [0, 2]);
    assert_eq!(evidence.consumer.strategy_evaluated, [0, 2]);
    assert_eq!(evidence.consumer.capture_error, None);
    Ok(())
}

#[tokio::test]
async fn backtest_capture_keeps_transport_order_while_heap_sorts_by_source_time() -> TestResult<()>
{
    let (data0, raw0) = data_at(0, 20)?;
    let (heartbeat, heartbeat_raw) = heartbeat();
    let (data2, raw2) = data_at(2, 10)?;
    // One FIFO source delivers Data0 before Heartbeat1 without sleep-based timing.
    let evidence = capture_with_gated_consumer(
        "capture-order-backtest",
        FeedMode::Backtest,
        vec![
            data0,
            heartbeat,
            data2,
            SourceSignal::Watermark(i64::MAX),
            SourceSignal::Eof,
        ],
    )
    .await?;

    assert_eq!(evidence.merge_error, None);
    assert_eq!(
        evidence.tape,
        vec![
            Observation {
                ingest_sequence: 0,
                frame_sequence: 0,
                source_ingest_sequence: 0,
                payload: RawCapturePayload::Frame { raw_bytes: raw0 },
            },
            Observation {
                ingest_sequence: 1,
                frame_sequence: 1,
                source_ingest_sequence: 1,
                payload: RawCapturePayload::Control {
                    name: "heartbeat".to_owned(),
                    raw_bytes: heartbeat_raw,
                },
            },
            Observation {
                ingest_sequence: 2,
                frame_sequence: 2,
                source_ingest_sequence: 2,
                payload: RawCapturePayload::Frame { raw_bytes: raw2 },
            },
        ],
        "consumer evidence: {:#?}",
        evidence.consumer
    );
    assert_eq!(evidence.consumer.delivered, [2, 0]);
    assert_eq!(evidence.consumer.delivered_source_times_ms, [10, 20]);
    assert_eq!(evidence.consumer.captured_in_consumer, Vec::<u64>::new());
    assert_eq!(evidence.consumer.durable_accepted, [2, 0]);
    assert_eq!(evidence.consumer.strategy_evaluated, [2, 0]);
    assert_eq!(evidence.consumer.capture_error, None);
    Ok(())
}

#[tokio::test]
async fn live_capture_keeps_a_reporting_gap_after_its_data_frame() -> TestResult<()> {
    let (data0, raw0) = data(0)?;
    let (gap, gap_raw) = reporting_gap();
    let evidence = capture_with_gated_consumer(
        "capture-order-gap",
        FeedMode::Live,
        vec![data0, gap, SourceSignal::Eof],
    )
    .await?;

    assert!(
        evidence
            .merge_error
            .as_deref()
            .is_some_and(|message| message.starts_with("replay gap:"))
    );
    assert_eq!(
        evidence.tape,
        vec![
            Observation {
                ingest_sequence: 0,
                frame_sequence: 0,
                source_ingest_sequence: 0,
                payload: RawCapturePayload::Frame { raw_bytes: raw0 },
            },
            Observation {
                ingest_sequence: 1,
                frame_sequence: 1,
                source_ingest_sequence: 1,
                payload: RawCapturePayload::Gap {
                    range: None,
                    unresolved_count: None,
                    reason: "reported discontinuity with unknown bounds".to_owned(),
                    raw_frame: gap_raw,
                },
            },
        ],
        "consumer evidence: {:#?}",
        evidence.consumer
    );
    assert_eq!(evidence.consumer.delivered, [0]);
    assert_eq!(evidence.consumer.captured_in_consumer, Vec::<u64>::new());
    assert_eq!(evidence.consumer.durable_accepted, [0]);
    assert_eq!(evidence.consumer.strategy_evaluated, [0]);
    assert_eq!(evidence.consumer.capture_error, None);
    Ok(())
}
