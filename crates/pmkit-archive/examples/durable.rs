//! Writes raw evidence through the durable object-store sink into a temp
//! directory, then reopens and recovers it. Exercises the public archive
//! surface: `cargo run -p pmkit-archive --example durable`.

use std::error::Error;
use std::num::NonZeroUsize;
use std::time::Duration;

use pmkit_archive::{DurableRawSink, DurableSinkConfig, FsObjectStore, RetryPolicy, recover};

fn line(index: i64) -> String {
    format!(
        r#"{{"schema_version":1,"receipt_time_ms":{index},"connection_id":"connection-1","raw":"{{}}"}}"#
    ) + "\n"
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let config = DurableSinkConfig {
        records_per_segment: NonZeroUsize::new(2).unwrap_or(NonZeroUsize::MIN),
        part_size_bytes: NonZeroUsize::new(32).unwrap_or(NonZeroUsize::MIN),
        retry: RetryPolicy {
            max_attempts: NonZeroUsize::new(3).unwrap_or(NonZeroUsize::MIN),
            backoff: Duration::from_millis(1),
        },
    };

    let store = FsObjectStore::new(dir.path());
    let mut sink = DurableRawSink::open(store, config).await?;
    for index in 1..=5 {
        sink.append_record(line(index).as_bytes()).await?;
    }
    let committed = sink.close().await?;
    println!("committed segments: {}", committed.segments.len());

    // Reopen from durable state alone and recover.
    let store = FsObjectStore::new(dir.path());
    let recovered = recover(&store).await?;
    println!(
        "recovered segments: {} records: {}",
        recovered.segments.len(),
        recovered
            .segments
            .iter()
            .map(|segment| segment.records)
            .sum::<u64>()
    );
    for segment in &recovered.segments {
        println!(
            "  {} sha256={} records={}",
            segment.key, segment.sha256_hex, segment.records
        );
    }
    Ok(())
}
