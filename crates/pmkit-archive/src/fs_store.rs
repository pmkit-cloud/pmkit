//! Filesystem reference implementation of [`ObjectStore`].
//!
//! This is the OSS reference store used for tests and local deployments. It
//! models S3 multipart semantics on a local directory: parts live under a
//! per-upload staging directory and are assembled into the final object only
//! on completion. `put` is atomic via a temp-file rename. A concrete S3
//! adapter replaces this file with real bucket calls.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::{ObjectStore, ObjectStoreError, PartId, PendingUpload};

const UPLOADS_DIR: &str = ".uploads";
const TARGET_FILE: &str = "target";

/// A local-directory [`ObjectStore`] with S3-shaped multipart semantics.
#[derive(Debug)]
pub struct FsObjectStore {
    root: PathBuf,
    counter: AtomicU64,
}

impl FsObjectStore {
    /// Creates a store rooted at `root`.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            counter: AtomicU64::new(0),
        }
    }

    fn object_path(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn upload_dir(&self, upload_id: &str) -> PathBuf {
        self.root.join(UPLOADS_DIR).join(upload_id)
    }

    fn next_unique(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }
}

fn permanent(context: &str, error: &std::io::Error) -> ObjectStoreError {
    ObjectStoreError::Permanent {
        message: format!("{context}: {error}"),
    }
}

async fn write_atomic(path: &Path, unique: u64, body: &[u8]) -> Result<(), ObjectStoreError> {
    let parent = path.parent().ok_or_else(|| ObjectStoreError::Permanent {
        message: format!("object key has no parent directory: {}", path.display()),
    })?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| permanent("create object directory", &error))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ObjectStoreError::Permanent {
            message: format!("object key has no file name: {}", path.display()),
        })?;
    let temp = parent.join(format!("{file_name}.tmp-{unique}"));
    tokio::fs::write(&temp, body)
        .await
        .map_err(|error| permanent("write temporary object", &error))?;
    tokio::fs::rename(&temp, path)
        .await
        .map_err(|error| permanent("commit object rename", &error))
}

#[async_trait]
impl ObjectStore for FsObjectStore {
    async fn put(&self, key: &str, body: &[u8]) -> Result<(), ObjectStoreError> {
        let path = self.object_path(key);
        write_atomic(&path, self.next_unique(), body).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        match tokio::fs::read(self.object_path(key)).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(permanent("read object", &error)),
        }
    }

    async fn create_multipart(&self, key: &str) -> Result<String, ObjectStoreError> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let upload_id = format!("upload-{nanos}-{}", self.next_unique());
        let dir = self.upload_dir(&upload_id);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|error| permanent("create upload directory", &error))?;
        tokio::fs::write(dir.join(TARGET_FILE), key.as_bytes())
            .await
            .map_err(|error| permanent("record upload target", &error))?;
        Ok(upload_id)
    }

    async fn upload_part(
        &self,
        _key: &str,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
    ) -> Result<PartId, ObjectStoreError> {
        let dir = self.upload_dir(upload_id);
        let part_path = dir.join(format!("part-{part_number:08}"));
        tokio::fs::write(&part_path, body)
            .await
            .map_err(|error| permanent("write part", &error))?;
        Ok(PartId {
            part_number,
            sha256_hex: crate::sha256_hex(body),
        })
    }

    async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[PartId],
    ) -> Result<(), ObjectStoreError> {
        let dir = self.upload_dir(upload_id);
        let mut assembled = Vec::new();
        for part in parts {
            let part_path = dir.join(format!("part-{:08}", part.part_number));
            let bytes = tokio::fs::read(&part_path)
                .await
                .map_err(|error| permanent("read part for completion", &error))?;
            if crate::sha256_hex(&bytes) != part.sha256_hex {
                return Err(ObjectStoreError::Permanent {
                    message: format!("part {} failed its checksum", part.part_number),
                });
            }
            assembled.extend_from_slice(&bytes);
        }
        write_atomic(&self.object_path(key), self.next_unique(), &assembled).await?;
        tokio::fs::remove_dir_all(&dir)
            .await
            .map_err(|error| permanent("clear completed upload", &error))
    }

    async fn abort_multipart(&self, _key: &str, upload_id: &str) -> Result<(), ObjectStoreError> {
        match tokio::fs::remove_dir_all(self.upload_dir(upload_id)).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(permanent("abort upload", &error)),
        }
    }

    async fn list_pending_uploads(&self) -> Result<Vec<PendingUpload>, ObjectStoreError> {
        let uploads_root = self.root.join(UPLOADS_DIR);
        let mut entries = match tokio::fs::read_dir(&uploads_root).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(permanent("list uploads", &error)),
        };

        let mut pending = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|error| permanent("read upload entry", &error))?
        {
            let upload_id = entry.file_name().to_string_lossy().into_owned();
            let target = entry.path().join(TARGET_FILE);
            let key = match tokio::fs::read(&target).await {
                Ok(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(permanent("read upload target", &error)),
            };
            pending.push(PendingUpload { key, upload_id });
        }
        Ok(pending)
    }
}
