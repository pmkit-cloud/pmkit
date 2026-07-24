//! Runs the collector against an in-memory scripted transport and prints the
//! resulting raw tape. This exercises the public collector surface without a
//! live venue: `cargo run -p pmkit-collector --example collect`.

use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use pmkit_collector::{
    CollectedFrame, CollectorConfig, Connection, Subscription, Transport, TransportError, run,
    shutdown_channel,
};
use pmkit_tape::RawJsonLinesTape;

/// Emits two frames on the first connection, drops, then reconnects with a
/// second connection emitting one more frame before shutting the run down.
struct ScriptedTransport {
    connects: Mutex<u32>,
    shutdown: pmkit_collector::ShutdownHandle,
}

struct ScriptedConnection {
    frames: Vec<CollectedFrame>,
    dropped: bool,
}

#[async_trait]
impl Transport for ScriptedTransport {
    async fn connect(
        &self,
        _shard: &[Subscription],
    ) -> Result<Box<dyn Connection>, TransportError> {
        let attempt = {
            let mut connects = self.connects.lock().map_err(|_| TransportError::Connect {
                message: "poisoned".to_owned(),
            })?;
            *connects += 1;
            *connects
        };
        match attempt {
            1 => Ok(Box::new(ScriptedConnection {
                frames: vec![
                    CollectedFrame {
                        receipt_time_ms: 1,
                        raw: r#"{"event":"book","seq":1}"#.to_owned(),
                    },
                    CollectedFrame {
                        receipt_time_ms: 2,
                        raw: r#"{"event":"trade","seq":2}"#.to_owned(),
                    },
                ],
                dropped: false,
            })),
            2 => Ok(Box::new(ScriptedConnection {
                frames: vec![CollectedFrame {
                    receipt_time_ms: 3,
                    raw: r#"{"event":"book","seq":3}"#.to_owned(),
                }],
                dropped: false,
            })),
            _ => {
                self.shutdown.shutdown();
                std::future::pending().await
            }
        }
    }
}

#[async_trait]
impl Connection for ScriptedConnection {
    async fn recv(&mut self) -> Result<Option<CollectedFrame>, TransportError> {
        if !self.frames.is_empty() {
            return Ok(Some(self.frames.remove(0)));
        }
        if !self.dropped {
            self.dropped = true;
            return Err(TransportError::Stream {
                message: "connection dropped".to_owned(),
            });
        }
        std::future::pending().await
    }

    async fn heartbeat(&mut self) -> Result<(), TransportError> {
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let (handle, shutdown) = shutdown_channel();
    let transport = Arc::new(ScriptedTransport {
        connects: Mutex::new(0),
        shutdown: handle,
    });
    let config = CollectorConfig {
        shard_size: NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
        channel_capacity: NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        heartbeat_interval: Duration::from_millis(50),
        reconnect_backoff: Duration::from_millis(1),
        max_consecutive_reconnect_failures: NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN),
    };

    let (sink, report) = run(
        transport,
        vec![Subscription::new("book"), Subscription::new("trade")],
        config,
        RawJsonLinesTape::new(Vec::new()),
        shutdown,
    )
    .await?;

    let bytes = sink.into_inner();
    println!(
        "shards={} frames_written={} reconnects={}",
        report.shards, report.frames_written, report.reconnects
    );
    print!("{}", String::from_utf8_lossy(&bytes));
    Ok(())
}
