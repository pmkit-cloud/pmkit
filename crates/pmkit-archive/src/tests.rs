use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use pmkit_tape::{RawJsonLinesTape, RawTapeSink, decode_raw_record};

use super::{
    DurableRawSink, DurableSinkConfig, DurableSinkError, FsObjectStore, MANIFEST_KEY, Manifest,
    ObjectStore, ObjectStoreError, PartId, PendingUpload, RetryPolicy, recover, sha256_hex,
};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap_or(NonZeroUsize::MIN)
}

fn config(
    records_per_segment: usize,
    part_size_bytes: usize,
    max_attempts: usize,
) -> DurableSinkConfig {
    DurableSinkConfig {
        records_per_segment: nz(records_per_segment),
        part_size_bytes: nz(part_size_bytes),
        retry: RetryPolicy {
            max_attempts: nz(max_attempts),
            backoff: Duration::from_millis(1),
        },
    }
}

/// Builds one v1 raw tape line (including its trailing newline).
fn raw_line(receipt_time_ms: i64) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut tape = RawJsonLinesTape::new(Vec::new());
    tape.append_raw(receipt_time_ms, "connection-1", r#"{"event":"book"}"#)?;
    tape.flush()?;
    Ok(tape.into_inner())
}

async fn concat_segments(
    store: &FsObjectStore,
    manifest: &Manifest,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut assembled = Vec::new();
    for segment in &manifest.segments {
        let bytes = store
            .get(&segment.key)
            .await?
            .ok_or("committed segment missing")?;
        assembled.extend_from_slice(&bytes);
    }
    Ok(assembled)
}

#[tokio::test]
async fn round_trips_and_recovers_committed_evidence() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let store = FsObjectStore::new(dir.path());

    let mut expected = Vec::new();
    let mut sink = DurableRawSink::open(store, config(2, 16, 3)).await?;
    for index in 1..=5 {
        let line = raw_line(index)?;
        expected.extend_from_slice(&line);
        sink.append_record(&line).await?;
    }
    let manifest = sink.close().await?;

    // 5 records, 2 per segment -> 3 segments (2 + 2 + 1).
    assert_eq!(manifest.segments.len(), 3);
    assert_eq!(manifest.segments.iter().map(|s| s.records).sum::<u64>(), 5);

    // Reopen and recover from durable state alone.
    let store = FsObjectStore::new(dir.path());
    let recovered = recover(&store).await?;
    assert_eq!(recovered, manifest);

    let assembled = concat_segments(&store, &recovered).await?;
    assert_eq!(assembled, expected);

    // Evidence survives byte-identical and decodes back to v1 records.
    let receipts: Vec<i64> = assembled
        .split_inclusive(|byte| *byte == b'\n')
        .filter_map(|line| decode_raw_record(line).ok())
        .map(|record| record.receipt_time_ms)
        .collect();
    assert_eq!(receipts, vec![1, 2, 3, 4, 5]);
    Ok(())
}

#[tokio::test]
async fn tampered_segment_fails_closed_on_recovery() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let store = FsObjectStore::new(dir.path());
    let mut sink = DurableRawSink::open(store, config(4, 64, 3)).await?;
    for index in 1..=3 {
        sink.append_record(&raw_line(index)?).await?;
    }
    let manifest = sink.close().await?;
    let segment_key = manifest
        .segments
        .first()
        .ok_or("expected one segment")?
        .key
        .clone();

    // Corrupt the committed segment on disk.
    tokio::fs::write(dir.path().join(&segment_key), b"tampered\n").await?;

    let store = FsObjectStore::new(dir.path());
    match recover(&store).await {
        Err(DurableSinkError::Corrupt { message }) => assert!(message.contains("checksum")),
        other => return Err(format!("expected checksum corruption, got {other:?}").into()),
    }
    Ok(())
}

#[tokio::test]
async fn process_loss_aborts_dangling_uploads() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let store = FsObjectStore::new(dir.path());

    // Simulate a crash mid-multipart: initiate and upload a part, never complete.
    let upload_id = store
        .create_multipart("segments/segment-00000.jsonl")
        .await?;
    store
        .upload_part("segments/segment-00000.jsonl", &upload_id, 1, b"partial")
        .await?;
    assert_eq!(store.list_pending_uploads().await?.len(), 1);

    let recovered = recover(&store).await?;
    assert_eq!(recovered, Manifest::default());
    assert!(store.list_pending_uploads().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn completed_segment_without_manifest_commit_is_not_durable() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let store = FsObjectStore::new(dir.path());

    // Complete a segment upload but never write the manifest.
    let key = "segments/segment-00000.jsonl";
    let upload_id = store.create_multipart(key).await?;
    let body = raw_line(1)?;
    let part = store.upload_part(key, &upload_id, 1, &body).await?;
    store.complete_multipart(key, &upload_id, &[part]).await?;
    assert!(store.get(key).await?.is_some());

    // The manifest is the sole truth: the orphan segment is not durable.
    let recovered = recover(&store).await?;
    assert!(recovered.segments.is_empty());
    Ok(())
}

#[tokio::test]
async fn manifest_version_mismatch_fails_closed() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let store = FsObjectStore::new(dir.path());
    store
        .put(MANIFEST_KEY, br#"{"schema_version":2,"segments":[]}"#)
        .await?;

    match recover(&store).await {
        Err(DurableSinkError::Corrupt { message }) => {
            assert!(message.contains("unsupported manifest schema version"));
        }
        other => return Err(format!("expected version rejection, got {other:?}").into()),
    }
    Ok(())
}

/// Wraps a store and fails the first `fail_puts` `put` calls transiently.
struct FlakyStore {
    inner: FsObjectStore,
    remaining_put_failures: AtomicUsize,
}

#[async_trait]
impl ObjectStore for FlakyStore {
    async fn put(&self, key: &str, body: &[u8]) -> Result<(), ObjectStoreError> {
        if self.remaining_put_failures.load(Ordering::SeqCst) > 0 {
            self.remaining_put_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(ObjectStoreError::Transient {
                message: "temporarily throttled".to_owned(),
            });
        }
        self.inner.put(key, body).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        self.inner.get(key).await
    }

    async fn create_multipart(&self, key: &str) -> Result<String, ObjectStoreError> {
        self.inner.create_multipart(key).await
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
    ) -> Result<PartId, ObjectStoreError> {
        self.inner
            .upload_part(key, upload_id, part_number, body)
            .await
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[PartId],
    ) -> Result<(), ObjectStoreError> {
        self.inner.complete_multipart(key, upload_id, parts).await
    }

    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<(), ObjectStoreError> {
        self.inner.abort_multipart(key, upload_id).await
    }

    async fn list_pending_uploads(&self) -> Result<Vec<PendingUpload>, ObjectStoreError> {
        self.inner.list_pending_uploads().await
    }
}

#[tokio::test]
async fn transient_put_failures_are_retried() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let store = FlakyStore {
        inner: FsObjectStore::new(dir.path()),
        remaining_put_failures: AtomicUsize::new(2),
    };

    let mut sink = DurableRawSink::open(store, config(1, 64, 5)).await?;
    sink.append_record(&raw_line(1)?).await?;
    let manifest = sink.close().await?;

    assert_eq!(manifest.segments.len(), 1);
    assert_eq!(manifest.segments[0].records, 1);

    // The committed segment is intact and checksummed.
    let store = FsObjectStore::new(dir.path());
    let recovered = recover(&store).await?;
    assert_eq!(recovered.segments.len(), 1);
    let bytes = store
        .get(&recovered.segments[0].key)
        .await?
        .ok_or("segment missing")?;
    assert_eq!(sha256_hex(&bytes), recovered.segments[0].sha256_hex);
    Ok(())
}

#[tokio::test]
async fn reopen_appends_without_rewriting_committed_segments() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;

    // First session: 3 records, 2 per segment -> segments s0(2), s1(1).
    let mut sink = DurableRawSink::open(FsObjectStore::new(dir.path()), config(2, 64, 3)).await?;
    for index in 1..=3 {
        sink.append_record(&raw_line(index)?).await?;
    }
    let first = sink.close().await?;
    assert_eq!(first.segments.len(), 2);

    // Snapshot committed segment bytes before the second session.
    let store = FsObjectStore::new(dir.path());
    let mut before = Vec::new();
    for segment in &first.segments {
        let bytes = store.get(&segment.key).await?.ok_or("segment missing")?;
        before.push((segment.clone(), bytes));
    }

    // Second session: append 2 more records -> new segment s2(2).
    let mut sink = DurableRawSink::open(FsObjectStore::new(dir.path()), config(2, 64, 3)).await?;
    for index in 4..=5 {
        sink.append_record(&raw_line(index)?).await?;
    }
    let second = sink.close().await?;
    assert_eq!(second.segments.len(), 3);

    // Every previously committed segment is byte- and checksum-identical.
    for (segment, bytes) in &before {
        assert!(second.segments.contains(segment));
        let now = store.get(&segment.key).await?.ok_or("segment missing")?;
        assert_eq!(&now, bytes);
    }
    Ok(())
}
