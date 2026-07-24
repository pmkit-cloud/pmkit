//! Reliable OSS raw-frame collector.
//!
//! The collector drives an injected [`Transport`], preserving every received
//! text frame into a [`pmkit_tape::RawTapeSink`] without dropping frames. It
//! owns the reliability machinery an OSS deployment needs:
//!
//! - **Reconnect**: a failed or closed connection reconnects with a fresh
//!   `connection_id` (see the v1 raw tape format), bounded by a reconnect
//!   budget so a permanently broken source fails closed instead of spinning.
//! - **Subscription sharding**: subscriptions are split into fixed-size shards,
//!   each driven by its own connection.
//! - **Bounded channels and backpressure**: frames flow through one bounded
//!   channel. A slow tape writer blocks the producers rather than discarding
//!   evidence.
//! - **Heartbeat**: each connection is pinged on a fixed interval.
//! - **Graceful shutdown**: on the shutdown signal the shards stop, the writer
//!   drains buffered frames, and the sink is flushed.
//!
//! The collector is transport-agnostic: [`WebSocketTransport`] is the concrete
//! `tokio-tungstenite` implementation, and tests inject scripted transports.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use pmkit_tape::RawTapeSink;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

mod websocket;

pub use websocket::WebSocketTransport;

/// One venue-neutral subscription request.
///
/// `topic` is the exact text the transport uses to subscribe; the collector
/// never interprets it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// Exact subscribe-frame text for the transport.
    pub topic: String,
}

impl Subscription {
    /// Creates a subscription from a topic string.
    #[must_use]
    pub fn new(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
        }
    }
}

/// A raw text frame received on a connection, before venue adaptation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectedFrame {
    /// Local Unix receipt time in milliseconds.
    pub receipt_time_ms: i64,
    /// Exact UTF-8 text frame received from the source.
    pub raw: String,
}

/// A transport-level failure that triggers reconnect or fails closed.
#[derive(Debug, Error)]
pub enum TransportError {
    /// A new connection could not be established.
    #[error("connection failed: {message}")]
    Connect {
        /// Human-readable detail.
        message: String,
    },
    /// An established connection failed mid-stream.
    #[error("stream failed: {message}")]
    Stream {
        /// Human-readable detail.
        message: String,
    },
}

/// One live connection lifetime for a shard.
#[async_trait]
pub trait Connection: Send {
    /// Receives the next frame, `Ok(None)` on a clean close.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Stream`] when the connection fails mid-stream.
    async fn recv(&mut self) -> Result<Option<CollectedFrame>, TransportError>;

    /// Sends a keepalive heartbeat.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Stream`] when the heartbeat cannot be sent.
    async fn heartbeat(&mut self) -> Result<(), TransportError>;
}

/// Establishes connections for a shard of subscriptions.
#[async_trait]
pub trait Transport: Send + Sync + 'static {
    /// Opens a new connection subscribed to `shard`.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError::Connect`] when the connection cannot be
    /// established.
    async fn connect(&self, shard: &[Subscription]) -> Result<Box<dyn Connection>, TransportError>;
}

/// Tuning for one collector run.
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// Maximum subscriptions per connection.
    pub shard_size: NonZeroUsize,
    /// Bounded frame-channel capacity. This is the backpressure window.
    pub channel_capacity: NonZeroUsize,
    /// Interval between connection heartbeats.
    pub heartbeat_interval: Duration,
    /// Delay before a reconnect attempt.
    pub reconnect_backoff: Duration,
    /// Consecutive connect failures tolerated before failing closed.
    pub max_consecutive_reconnect_failures: NonZeroUsize,
}

/// A summary of a completed collector run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CollectorReport {
    /// Number of subscription shards driven.
    pub shards: usize,
    /// Total frames written to the tape.
    pub frames_written: u64,
    /// Total reconnects performed across all shards.
    pub reconnects: u64,
}

/// A collector failure.
#[derive(Debug, Error)]
pub enum CollectorError {
    /// A shard exhausted its reconnect budget.
    #[error("shard {shard} exhausted its reconnect budget: {message}")]
    ReconnectExhausted {
        /// Index of the failing shard.
        shard: usize,
        /// Last transport error observed.
        message: String,
    },
    /// The tape writer rejected a frame or failed to flush.
    #[error("raw tape write failed: {0}")]
    Tape(#[from] std::io::Error),
    /// A collector task panicked.
    #[error("collector task panicked")]
    TaskPanicked,
}

/// A handle used to request graceful shutdown.
#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    tx: Arc<watch::Sender<bool>>,
}

impl ShutdownHandle {
    /// Signals every shard and the writer to stop after draining.
    pub fn shutdown(&self) {
        let _ = self.tx.send(true);
    }
}

/// Creates a shutdown handle paired with the receiver passed to [`run`].
#[must_use]
pub fn shutdown_channel() -> (ShutdownHandle, watch::Receiver<bool>) {
    let (tx, rx) = watch::channel(false);
    (ShutdownHandle { tx: Arc::new(tx) }, rx)
}

/// Splits `subscriptions` into fixed-size shards.
#[must_use]
pub fn plan_shards(
    subscriptions: &[Subscription],
    shard_size: NonZeroUsize,
) -> Vec<Vec<Subscription>> {
    subscriptions
        .chunks(shard_size.get())
        .map(<[Subscription]>::to_vec)
        .collect()
}

#[derive(Debug)]
struct RawRecord {
    receipt_time_ms: i64,
    connection_id: String,
    raw: String,
}

enum PumpOutcome {
    Shutdown,
    Reconnect,
    SinkClosed,
}

/// Joined result of the tape-writer task: the writer's own outcome wrapped in
/// the task-join outcome.
type WriterResult<S> = Result<Result<(S, u64), std::io::Error>, tokio::task::JoinError>;

/// Runs the collector until shutdown, a shard fails closed, or the writer fails.
///
/// Returns the sink and a run report on success so the caller can inspect or
/// close durable state.
///
/// # Errors
///
/// Returns [`CollectorError`] when a shard exhausts its reconnect budget, the
/// tape write fails, or a task panics. On any error every other task is
/// cancelled before returning.
pub async fn run<T, S>(
    transport: Arc<T>,
    subscriptions: Vec<Subscription>,
    config: CollectorConfig,
    sink: S,
    external_shutdown: watch::Receiver<bool>,
) -> Result<(S, CollectorReport), CollectorError>
where
    T: Transport,
    S: RawTapeSink + Send + 'static,
{
    let shards = plan_shards(&subscriptions, config.shard_size);
    let shard_count = shards.len();

    let (frame_tx, mut frame_rx) = mpsc::channel::<RawRecord>(config.channel_capacity.get());

    // Internal cancellation: set on external shutdown, shard error, or writer
    // completion so every task stops cooperatively.
    let (cancel_tx, cancel_rx) = watch::channel(false);
    let cancel_tx = Arc::new(cancel_tx);
    spawn_shutdown_bridge(external_shutdown, Arc::clone(&cancel_tx));

    let writer = tokio::spawn(async move {
        let mut sink = sink;
        let mut written: u64 = 0;
        while let Some(record) = frame_rx.recv().await {
            sink.append_raw(record.receipt_time_ms, &record.connection_id, &record.raw)?;
            written += 1;
        }
        sink.flush()?;
        Ok::<(S, u64), std::io::Error>((sink, written))
    });

    let mut tasks = JoinSet::new();
    for (shard_id, shard) in shards.into_iter().enumerate() {
        tasks.spawn(run_shard(
            shard_id,
            shard,
            Arc::clone(&transport),
            config.clone(),
            frame_tx.clone(),
            cancel_rx.clone(),
        ));
    }
    drop(frame_tx);

    let mut reconnects: u64 = 0;
    let mut shard_error: Option<CollectorError> = None;
    let mut writer_result: Option<WriterResult<S>> = None;
    let mut writer = writer;

    loop {
        tokio::select! {
            joined = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Some(Ok(Ok(shard_reconnects))) => reconnects += shard_reconnects,
                    Some(Ok(Err(error))) => {
                        if shard_error.is_none() {
                            shard_error = Some(error);
                        }
                        let _ = cancel_tx.send(true);
                    }
                    Some(Err(_)) => {
                        if shard_error.is_none() {
                            shard_error = Some(CollectorError::TaskPanicked);
                        }
                        let _ = cancel_tx.send(true);
                    }
                    None => {}
                }
            }
            result = &mut writer, if writer_result.is_none() => {
                writer_result = Some(result);
                // Whether the writer finished cleanly or with an error, no more
                // frames will be consumed; stop the shards.
                let _ = cancel_tx.send(true);
            }
            else => break,
        }

        if tasks.is_empty() && writer_result.is_some() {
            break;
        }
    }

    let writer_result = match writer_result {
        Some(result) => result,
        None => (&mut writer).await,
    };

    if let Some(error) = shard_error {
        return Err(error);
    }

    match writer_result {
        Ok(Ok((sink, written))) => Ok((
            sink,
            CollectorReport {
                shards: shard_count,
                frames_written: written,
                reconnects,
            },
        )),
        Ok(Err(error)) => Err(CollectorError::Tape(error)),
        Err(_) => Err(CollectorError::TaskPanicked),
    }
}

fn spawn_shutdown_bridge(
    mut external_shutdown: watch::Receiver<bool>,
    cancel_tx: Arc<watch::Sender<bool>>,
) {
    tokio::spawn(async move {
        if *external_shutdown.borrow_and_update() {
            let _ = cancel_tx.send(true);
            return;
        }
        while external_shutdown.changed().await.is_ok() {
            if *external_shutdown.borrow() {
                let _ = cancel_tx.send(true);
                return;
            }
        }
    });
}

async fn run_shard<T: Transport>(
    shard_id: usize,
    shard: Vec<Subscription>,
    transport: Arc<T>,
    config: CollectorConfig,
    frame_tx: mpsc::Sender<RawRecord>,
    mut cancel: watch::Receiver<bool>,
) -> Result<u64, CollectorError> {
    let mut epoch: u64 = 0;
    let mut reconnects: u64 = 0;
    let mut consecutive_failures: usize = 0;

    loop {
        if *cancel.borrow_and_update() {
            return Ok(reconnects);
        }

        let connect = tokio::select! {
            biased;
            _ = cancel.changed() => return Ok(reconnects),
            result = transport.connect(&shard) => result,
        };

        let mut connection = match connect {
            Ok(connection) => {
                consecutive_failures = 0;
                connection
            }
            Err(error) => {
                consecutive_failures += 1;
                if consecutive_failures >= config.max_consecutive_reconnect_failures.get() {
                    return Err(CollectorError::ReconnectExhausted {
                        shard: shard_id,
                        message: error.to_string(),
                    });
                }
                if backoff_or_cancel(&config.reconnect_backoff, &mut cancel).await {
                    return Ok(reconnects);
                }
                continue;
            }
        };

        let connection_id = format!("shard-{shard_id}-epoch-{epoch}");
        epoch += 1;

        match pump_connection(
            connection.as_mut(),
            &connection_id,
            &config,
            &frame_tx,
            &mut cancel,
        )
        .await
        {
            PumpOutcome::Shutdown | PumpOutcome::SinkClosed => return Ok(reconnects),
            PumpOutcome::Reconnect => {
                reconnects += 1;
                if backoff_or_cancel(&config.reconnect_backoff, &mut cancel).await {
                    return Ok(reconnects);
                }
            }
        }
    }
}

/// Sleeps for `backoff` unless cancelled first. Returns `true` when cancelled.
async fn backoff_or_cancel(backoff: &Duration, cancel: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        biased;
        _ = cancel.changed() => true,
        () = tokio::time::sleep(*backoff) => false,
    }
}

async fn pump_connection(
    connection: &mut dyn Connection,
    connection_id: &str,
    config: &CollectorConfig,
    frame_tx: &mpsc::Sender<RawRecord>,
    cancel: &mut watch::Receiver<bool>,
) -> PumpOutcome {
    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    // Swallow the immediate first tick so the first heartbeat is one interval in.
    heartbeat.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = cancel.changed() => return PumpOutcome::Shutdown,
            _ = heartbeat.tick() => {
                if connection.heartbeat().await.is_err() {
                    return PumpOutcome::Reconnect;
                }
            }
            received = connection.recv() => {
                match received {
                    Ok(Some(frame)) => {
                        let record = RawRecord {
                            receipt_time_ms: frame.receipt_time_ms,
                            connection_id: connection_id.to_owned(),
                            raw: frame.raw,
                        };
                        if frame_tx.send(record).await.is_err() {
                            return PumpOutcome::SinkClosed;
                        }
                    }
                    Ok(None) | Err(_) => return PumpOutcome::Reconnect,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
