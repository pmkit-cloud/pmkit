use std::collections::VecDeque;
use std::error::Error;
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use pmkit_tape::{RawJsonLinesTape, RawTapeRecord, decode_raw_record};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

use super::{
    CollectedFrame, CollectorConfig, CollectorError, Connection, Subscription, Transport,
    TransportError, plan_shards, run, shutdown_channel,
};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

fn config(shard_size: usize, capacity: usize) -> CollectorConfig {
    CollectorConfig {
        shard_size: nz(shard_size),
        channel_capacity: nz(capacity),
        heartbeat_interval: Duration::from_millis(20),
        reconnect_backoff: Duration::from_millis(1),
        max_consecutive_reconnect_failures: nz(3),
    }
}

fn frame(receipt_time_ms: i64, raw: &str) -> CollectedFrame {
    CollectedFrame {
        receipt_time_ms,
        raw: raw.to_owned(),
    }
}

/// One scripted recv step for a connection.
enum Step {
    Frame(CollectedFrame),
    CleanClose,
    Error(String),
}

/// A scripted per-connection plan.
enum ConnectStep {
    Ok(Vec<Step>),
    Fail(String),
}

struct ScriptedTransport {
    scripts: Mutex<VecDeque<ConnectStep>>,
    connects: AtomicU64,
    heartbeats: Arc<AtomicU64>,
    on_exhausted: Option<super::ShutdownHandle>,
}

impl ScriptedTransport {
    fn new(scripts: Vec<ConnectStep>, on_exhausted: Option<super::ShutdownHandle>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into_iter().collect()),
            connects: AtomicU64::new(0),
            heartbeats: Arc::new(AtomicU64::new(0)),
            on_exhausted,
        }
    }
}

#[async_trait]
impl Transport for ScriptedTransport {
    async fn connect(
        &self,
        _shard: &[Subscription],
    ) -> Result<Box<dyn Connection>, TransportError> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        let next = self
            .scripts
            .lock()
            .ok()
            .and_then(|mut scripts| scripts.pop_front());
        match next {
            Some(ConnectStep::Ok(steps)) => Ok(Box::new(ScriptedConnection {
                steps: steps.into(),
                heartbeats: Arc::clone(&self.heartbeats),
            })),
            Some(ConnectStep::Fail(message)) => Err(TransportError::Connect { message }),
            None => {
                if let Some(handle) = &self.on_exhausted {
                    handle.shutdown();
                }
                std::future::pending().await
            }
        }
    }
}

struct ScriptedConnection {
    steps: VecDeque<Step>,
    heartbeats: Arc<AtomicU64>,
}

#[async_trait]
impl Connection for ScriptedConnection {
    async fn recv(&mut self) -> Result<Option<CollectedFrame>, TransportError> {
        match self.steps.pop_front() {
            Some(Step::Frame(frame)) => Ok(Some(frame)),
            Some(Step::CleanClose) => Ok(None),
            Some(Step::Error(message)) => Err(TransportError::Stream { message }),
            // Exhausted: idle so heartbeats can still fire until cancelled.
            None => std::future::pending().await,
        }
    }

    async fn heartbeat(&mut self) -> Result<(), TransportError> {
        self.heartbeats.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A `Write` tape shared with the test for live inspection.
#[derive(Clone, Default)]
struct SharedTape {
    bytes: Arc<Mutex<Vec<u8>>>,
    flushes: Arc<AtomicUsize>,
}

impl SharedTape {
    fn records(&self) -> Vec<RawTapeRecord> {
        let bytes = self
            .bytes
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        bytes
            .split_inclusive(|byte| *byte == b'\n')
            .filter(|line| line.ends_with(b"\n"))
            .filter_map(|line| decode_raw_record(line).ok())
            .collect()
    }
}

impl Write for SharedTape {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Ok(mut bytes) = self.bytes.lock() {
            bytes.extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn wait_for_records(tape: &SharedTape, count: usize) -> Result<(), Box<dyn Error>> {
    for _ in 0..200 {
        if tape.records().len() >= count {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err(format!("timed out waiting for {count} records").into())
}

#[test]
fn plan_shards_splits_by_size() {
    let subscriptions: Vec<Subscription> = (0..5)
        .map(|index| Subscription::new(format!("t{index}")))
        .collect();
    let shards = plan_shards(&subscriptions, nz(2));
    assert_eq!(shards.len(), 3);
    assert_eq!(shards[0].len(), 2);
    assert_eq!(shards[1].len(), 2);
    assert_eq!(shards[2].len(), 1);
}

#[tokio::test]
async fn reconnect_uses_a_new_connection_id() -> Result<(), Box<dyn Error>> {
    let (handle, shutdown) = shutdown_channel();
    let transport = Arc::new(ScriptedTransport::new(
        vec![
            ConnectStep::Ok(vec![Step::Frame(frame(1, r#"{"n":1}"#)), Step::CleanClose]),
            ConnectStep::Ok(vec![
                Step::Frame(frame(2, r#"{"n":2}"#)),
                Step::Error("drop".to_owned()),
            ]),
        ],
        Some(handle),
    ));

    let (sink, report) = run(
        transport,
        vec![Subscription::new("book")],
        config(8, 8),
        RawJsonLinesTape::new(Vec::new()),
        shutdown,
    )
    .await?;

    let bytes = sink.into_inner();
    let records: Vec<RawTapeRecord> = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .filter_map(|line| decode_raw_record(line).ok())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].connection_id, "shard-0-epoch-0");
    assert_eq!(records[1].connection_id, "shard-0-epoch-1");
    assert_eq!(records[0].raw, r#"{"n":1}"#);
    assert_eq!(records[1].raw, r#"{"n":2}"#);
    assert_eq!(report.frames_written, 2);
    assert!(report.reconnects >= 1);
    Ok(())
}

#[tokio::test]
async fn bounded_channel_preserves_every_frame_in_order() -> Result<(), Box<dyn Error>> {
    let (handle, shutdown) = shutdown_channel();
    let steps: Vec<Step> = (1..=5)
        .map(|index| Step::Frame(frame(index, &format!(r#"{{"n":{index}}}"#))))
        .chain(std::iter::once(Step::Error("end".to_owned())))
        .collect();
    let transport = Arc::new(ScriptedTransport::new(
        vec![ConnectStep::Ok(steps)],
        Some(handle),
    ));

    // Capacity one forces backpressure between producer and writer.
    let (sink, report) = run(
        transport,
        vec![Subscription::new("book")],
        config(8, 1),
        RawJsonLinesTape::new(Vec::new()),
        shutdown,
    )
    .await?;

    let bytes = sink.into_inner();
    let raws: Vec<String> = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .filter_map(|line| decode_raw_record(line).ok())
        .map(|record| record.raw)
        .collect();
    assert_eq!(
        raws,
        vec![
            r#"{"n":1}"#,
            r#"{"n":2}"#,
            r#"{"n":3}"#,
            r#"{"n":4}"#,
            r#"{"n":5}"#
        ]
    );
    assert_eq!(report.frames_written, 5);
    Ok(())
}

#[tokio::test]
async fn heartbeats_fire_on_the_interval() -> Result<(), Box<dyn Error>> {
    let transport = Arc::new(ScriptedTransport::new(
        vec![ConnectStep::Ok(vec![Step::Frame(frame(1, "{}"))])],
        None,
    ));
    let heartbeats = Arc::clone(&transport.heartbeats);
    let (handle, shutdown) = shutdown_channel();

    let stopper = handle.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(90)).await;
        stopper.shutdown();
    });

    let (_sink, report) = run(
        transport,
        vec![Subscription::new("book")],
        config(8, 8),
        RawJsonLinesTape::new(Vec::new()),
        shutdown,
    )
    .await?;

    assert_eq!(report.frames_written, 1);
    assert!(heartbeats.load(Ordering::SeqCst) >= 1);
    Ok(())
}

#[tokio::test]
async fn graceful_shutdown_drains_and_flushes() -> Result<(), Box<dyn Error>> {
    let transport = Arc::new(ScriptedTransport::new(
        vec![ConnectStep::Ok(vec![
            Step::Frame(frame(1, r#"{"n":1}"#)),
            Step::Frame(frame(2, r#"{"n":2}"#)),
        ])],
        None,
    ));
    let (handle, shutdown) = shutdown_channel();
    let tape = SharedTape::default();

    let run_tape = tape.clone();
    let runner = tokio::spawn(run(
        transport,
        vec![Subscription::new("book")],
        config(8, 8),
        RawJsonLinesTape::new(run_tape),
        shutdown,
    ));

    wait_for_records(&tape, 2).await?;
    handle.shutdown();

    let (_sink, report) = runner.await??;
    assert_eq!(report.frames_written, 2);
    assert_eq!(tape.records().len(), 2);
    assert!(tape.flushes.load(Ordering::SeqCst) >= 1);
    Ok(())
}

#[tokio::test]
async fn reconnect_budget_exhaustion_fails_closed() -> Result<(), Box<dyn Error>> {
    let (_handle, shutdown) = shutdown_channel();
    let transport = Arc::new(ScriptedTransport::new(
        vec![
            ConnectStep::Fail("down".to_owned()),
            ConnectStep::Fail("down".to_owned()),
            ConnectStep::Fail("down".to_owned()),
        ],
        None,
    ));

    let outcome = run(
        transport,
        vec![Subscription::new("book")],
        config(8, 8),
        RawJsonLinesTape::new(Vec::new()),
        shutdown,
    )
    .await;

    match outcome {
        Err(CollectorError::ReconnectExhausted { shard, .. }) => assert_eq!(shard, 0),
        other => return Err(format!("expected reconnect exhaustion, got {other:?}").into()),
    }
    Ok(())
}

#[tokio::test]
async fn websocket_transport_collects_a_real_frame() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await?;
        let mut socket = accept_async(stream).await?;
        // Absorb the subscribe frame, emit one data frame, then absorb until close.
        let _ = socket.next().await;
        socket
            .send(Message::Text(r#"{"event":"book"}"#.into()))
            .await?;
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });

    let transport = Arc::new(super::WebSocketTransport::new(format!("ws://{address}")));
    let (handle, shutdown) = shutdown_channel();
    let tape = SharedTape::default();

    let run_tape = tape.clone();
    let runner = tokio::spawn(run(
        transport,
        vec![Subscription::new("subscribe")],
        config(8, 8),
        RawJsonLinesTape::new(run_tape),
        shutdown,
    ));

    wait_for_records(&tape, 1).await?;
    handle.shutdown();

    let (_sink, report) = runner.await??;
    assert!(report.frames_written >= 1);
    let records = tape.records();
    assert_eq!(records[0].connection_id, "shard-0-epoch-0");
    assert_eq!(records[0].raw, r#"{"event":"book"}"#);
    server.await?.map_err(|error| error.to_string())?;
    Ok(())
}
