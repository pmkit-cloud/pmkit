//! Durable object-store retention plane for raw tape evidence.
//!
//! This crate defines the durability *contract*, not a cloud client. Raw
//! evidence is uploaded as immutable segments through an S3-shaped multipart
//! [`ObjectStore`], and a segment becomes durable only once the atomic
//! [`Manifest`] references it. Every part and segment carries a SHA-256
//! checksum, transient failures are retried, and a process that dies
//! mid-upload recovers by reading the manifest (the sole source of truth) and
//! aborting any dangling multipart uploads.
//!
//! [`FsObjectStore`] is the filesystem reference implementation used for tests
//! and local deployments. A concrete S3 adapter is a separate
//! product-infrastructure project and simply implements [`ObjectStore`].

use std::num::NonZeroUsize;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

mod fs_store;

pub use fs_store::FsObjectStore;

/// Manifest schema version. Readers reject any other version fail-closed.
pub const MANIFEST_SCHEMA_VERSION: u16 = 1;

/// Object key of the atomic durability manifest.
pub const MANIFEST_KEY: &str = "manifest.json";

/// A failure from the object-store transport, classified for retry.
#[derive(Debug, Error)]
pub enum ObjectStoreError {
    /// A retryable failure (throttling, timeout, temporary unavailability).
    #[error("transient object-store error: {message}")]
    Transient {
        /// Human-readable detail.
        message: String,
    },
    /// A non-retryable failure (auth, invalid request, missing bucket).
    #[error("permanent object-store error: {message}")]
    Permanent {
        /// Human-readable detail.
        message: String,
    },
}

/// A completed multipart part identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartId {
    /// One-based part number.
    pub part_number: u32,
    /// SHA-256 of the part body, lowercase hex.
    pub sha256_hex: String,
}

/// A multipart upload observed as pending during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUpload {
    /// Target object key of the upload.
    pub key: String,
    /// Transport-assigned upload identity.
    pub upload_id: String,
}

/// An S3-shaped multipart object transport.
///
/// Small objects (manifests) use [`ObjectStore::put`]/[`ObjectStore::get`].
/// Large segments use the multipart lifecycle: create, upload parts, then
/// complete or abort.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Writes `body` to `key` atomically, overwriting any existing object.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError`] when the write fails.
    async fn put(&self, key: &str, body: &[u8]) -> Result<(), ObjectStoreError>;

    /// Reads `key`, or `None` when it does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError`] when the read fails.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectStoreError>;

    /// Initiates a multipart upload for `key`, returning an upload identity.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError`] when the upload cannot be initiated.
    async fn create_multipart(&self, key: &str) -> Result<String, ObjectStoreError>;

    /// Uploads one part of an in-progress multipart upload.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError`] when the part cannot be stored.
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
    ) -> Result<PartId, ObjectStoreError>;

    /// Completes a multipart upload, assembling `parts` in order into `key`.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError`] when completion fails.
    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[PartId],
    ) -> Result<(), ObjectStoreError>;

    /// Aborts an in-progress multipart upload, discarding its parts.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError`] when the abort fails.
    async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<(), ObjectStoreError>;

    /// Lists multipart uploads that were initiated but never completed.
    ///
    /// # Errors
    ///
    /// Returns [`ObjectStoreError`] when the listing fails.
    async fn list_pending_uploads(&self) -> Result<Vec<PendingUpload>, ObjectStoreError>;
}

/// One durable segment referenced by the manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentRef {
    /// Object key of the committed segment.
    pub key: String,
    /// SHA-256 of the full segment body, lowercase hex.
    pub sha256_hex: String,
    /// Number of raw records in the segment.
    pub records: u64,
}

/// The atomic durability manifest: the sole source of truth for durability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest schema version.
    pub schema_version: u16,
    /// Committed segments in append order.
    pub segments: Vec<SegmentRef>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            schema_version: MANIFEST_SCHEMA_VERSION,
            segments: Vec::new(),
        }
    }
}

/// A bounded retry policy for transient object-store failures.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum attempts, including the first.
    pub max_attempts: NonZeroUsize,
    /// Delay between attempts.
    pub backoff: Duration,
}

/// Configuration for a [`DurableRawSink`].
#[derive(Debug, Clone)]
pub struct DurableSinkConfig {
    /// Records buffered before a segment is uploaded and committed.
    pub records_per_segment: NonZeroUsize,
    /// Maximum bytes per multipart part.
    pub part_size_bytes: NonZeroUsize,
    /// Retry policy for transient transport failures.
    pub retry: RetryPolicy,
}

/// A failure while operating the durable sink.
#[derive(Debug, Error)]
pub enum DurableSinkError {
    /// The underlying object store failed.
    #[error(transparent)]
    Store(#[from] ObjectStoreError),
    /// A stored manifest or segment is corrupt or unsupported.
    #[error("durable evidence corrupt: {message}")]
    Corrupt {
        /// Corruption detail.
        message: String,
    },
}

/// Lowercase-hex SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // Writing to a String never fails.
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// A durable raw-evidence sink over an [`ObjectStore`].
///
/// Records are buffered into fixed-size segments, uploaded via multipart with
/// retry, and committed by atomically writing the manifest. A segment that
/// uploads but whose manifest commit never lands is ignored on recovery, so
/// durability never overstates what survived.
#[derive(Debug)]
pub struct DurableRawSink<O: ObjectStore> {
    store: O,
    config: DurableSinkConfig,
    manifest: Manifest,
    buffer: Vec<u8>,
    buffered_records: u64,
}

impl<O: ObjectStore> DurableRawSink<O> {
    /// Opens a sink over `store`, recovering any existing durable manifest.
    ///
    /// Recovery verifies every committed segment's checksum and aborts any
    /// dangling multipart uploads left by a previous process. A version
    /// mismatch or checksum mismatch fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`DurableSinkError`] when the store fails or durable evidence is
    /// corrupt.
    pub async fn open(store: O, config: DurableSinkConfig) -> Result<Self, DurableSinkError> {
        let manifest = recover(&store).await?;
        Ok(Self {
            store,
            config,
            manifest,
            buffer: Vec::new(),
            buffered_records: 0,
        })
    }

    /// The durable manifest as last committed.
    #[must_use]
    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Appends one raw record. `record` is the exact byte line (including its
    /// trailing newline) to preserve as immutable evidence.
    ///
    /// A full segment is uploaded and committed before returning.
    ///
    /// # Errors
    ///
    /// Returns [`DurableSinkError`] when a segment flush fails.
    pub async fn append_record(&mut self, record: &[u8]) -> Result<(), DurableSinkError> {
        self.buffer.extend_from_slice(record);
        self.buffered_records += 1;
        let threshold = u64::try_from(self.config.records_per_segment.get()).unwrap_or(u64::MAX);
        if self.buffered_records >= threshold {
            self.flush_segment().await?;
        }
        Ok(())
    }

    /// Flushes any buffered records as a final segment and returns the manifest.
    ///
    /// # Errors
    ///
    /// Returns [`DurableSinkError`] when the final flush fails.
    pub async fn close(mut self) -> Result<Manifest, DurableSinkError> {
        self.flush_segment().await?;
        Ok(self.manifest)
    }

    async fn flush_segment(&mut self) -> Result<(), DurableSinkError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let index = self.manifest.segments.len();
        let key = format!("segments/segment-{index:08}.jsonl");

        let upload_id = retry(&self.config.retry, || self.store.create_multipart(&key)).await?;

        let mut parts = Vec::new();
        for (offset, chunk) in self
            .buffer
            .chunks(self.config.part_size_bytes.get())
            .enumerate()
        {
            let part_number = u32::try_from(offset + 1).map_err(|_| DurableSinkError::Corrupt {
                message: "segment exceeds the multipart part limit".to_owned(),
            })?;
            let part = retry(&self.config.retry, || {
                self.store.upload_part(&key, &upload_id, part_number, chunk)
            })
            .await?;
            parts.push(part);
        }

        retry(&self.config.retry, || {
            self.store.complete_multipart(&key, &upload_id, &parts)
        })
        .await?;

        let segment = SegmentRef {
            key,
            sha256_hex: sha256_hex(&self.buffer),
            records: self.buffered_records,
        };
        let mut manifest = self.manifest.clone();
        manifest.segments.push(segment);
        let body = serde_json::to_vec(&manifest).map_err(|error| DurableSinkError::Corrupt {
            message: format!("manifest serialization failed: {error}"),
        })?;

        // Atomic commit: the segment counts only once the manifest lands.
        retry(&self.config.retry, || self.store.put(MANIFEST_KEY, &body)).await?;

        self.manifest = manifest;
        self.buffer.clear();
        self.buffered_records = 0;
        Ok(())
    }
}

/// Reads and validates the durable manifest, aborting dangling uploads.
///
/// # Errors
///
/// Returns [`DurableSinkError`] when the store fails, the manifest version is
/// unsupported, or a committed segment fails its checksum.
pub async fn recover<O: ObjectStore>(store: &O) -> Result<Manifest, DurableSinkError> {
    let manifest = match store.get(MANIFEST_KEY).await? {
        Some(bytes) => {
            let manifest: Manifest =
                serde_json::from_slice(&bytes).map_err(|error| DurableSinkError::Corrupt {
                    message: format!("manifest decode failed: {error}"),
                })?;
            if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
                return Err(DurableSinkError::Corrupt {
                    message: format!(
                        "unsupported manifest schema version {}",
                        manifest.schema_version
                    ),
                });
            }
            for segment in &manifest.segments {
                let stored =
                    store
                        .get(&segment.key)
                        .await?
                        .ok_or_else(|| DurableSinkError::Corrupt {
                            message: format!("committed segment {} is missing", segment.key),
                        })?;
                if sha256_hex(&stored) != segment.sha256_hex {
                    return Err(DurableSinkError::Corrupt {
                        message: format!("committed segment {} failed its checksum", segment.key),
                    });
                }
            }
            manifest
        }
        None => Manifest::default(),
    };

    // Dangling multipart uploads are process-loss debris, never durable.
    for pending in store.list_pending_uploads().await? {
        store
            .abort_multipart(&pending.key, &pending.upload_id)
            .await?;
    }

    Ok(manifest)
}

async fn retry<T, F, Fut>(policy: &RetryPolicy, mut operation: F) -> Result<T, ObjectStoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, ObjectStoreError>>,
{
    let mut attempt = 1usize;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(ObjectStoreError::Transient { message }) => {
                if attempt >= policy.max_attempts.get() {
                    return Err(ObjectStoreError::Transient { message });
                }
                attempt += 1;
                tokio::time::sleep(policy.backoff).await;
            }
            Err(permanent) => return Err(permanent),
        }
    }
}

#[cfg(test)]
mod tests;
