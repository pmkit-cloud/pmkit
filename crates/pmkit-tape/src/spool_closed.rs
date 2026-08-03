use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::spool_record::{checksum_hex, decode_record};
use crate::{RAW_SPOOL_SCHEMA_VERSION, SpoolCheckpoint, SpoolChunk, SpoolError, SpoolFrame};

/// A closed chunk that passed checksum, metadata, and complete-record verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedSpoolChunk {
    chunk: SpoolChunk,
    path: PathBuf,
    byte_length: u64,
    sha256_hex: String,
}

impl VerifiedSpoolChunk {
    /// Decodes each complete raw frame after its file has been verified.
    ///
    /// # Errors
    ///
    /// Returns [`SpoolError`] when the file changed after verification or a record is invalid.
    pub fn records(&self) -> Result<Vec<SpoolFrame>, SpoolError> {
        let bytes = fs::read(&self.path)?;
        let byte_length = u64::try_from(bytes.len()).map_err(|_| SpoolError::ChecksumMismatch {
            path: self.path.clone(),
        })?;
        if byte_length != self.byte_length || checksum_hex(&bytes) != self.sha256_hex {
            return Err(SpoolError::ChecksumMismatch {
                path: self.path.clone(),
            });
        }
        let mut records = Vec::new();
        for line in bytes.split_inclusive(|byte| *byte == b'\n') {
            if !line.ends_with(b"\n") {
                return Err(SpoolError::IncompleteClosedChunk {
                    path: self.path.clone(),
                });
            }
            let (chunk, frame) = decode_record(line)?;
            if chunk != self.chunk {
                return Err(SpoolError::ChecksumMismatch {
                    path: self.path.clone(),
                });
            }
            records.push(frame);
        }
        Ok(records)
    }

    /// Returns the only checkpoint identity construction path for spool consumers.
    #[must_use]
    pub fn checkpoint(&self) -> SpoolCheckpoint {
        SpoolCheckpoint {
            chunk: self.chunk.clone(),
            path: self.path.clone(),
            byte_length: self.byte_length,
            sha256_hex: self.sha256_hex.clone(),
        }
    }
}

/// Enumerates only complete, checksummed, verified `.jsonl` chunks beneath `root`.
///
/// # Errors
///
/// Returns [`SpoolError`] when a closed chunk or its sidecar cannot be verified.
pub fn enumerate_closed_chunks(root: &Path) -> Result<Vec<VerifiedSpoolChunk>, SpoolError> {
    let mut chunks = Vec::new();
    let root_entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(chunks),
        Err(error) => return Err(SpoolError::Io(error)),
    };
    for replica in root_entries {
        let replica = replica?;
        if !replica.file_type()?.is_dir() {
            continue;
        }
        let replica_id = replica.file_name().to_string_lossy().into_owned();
        for shard in fs::read_dir(replica.path())? {
            let shard = shard?;
            if !shard.file_type()?.is_dir() {
                continue;
            }
            let shard_id = shard.file_name().to_string_lossy().into_owned();
            for entry in fs::read_dir(shard.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.strip_suffix(".jsonl").is_none() {
                    continue;
                }
                let chunk = SpoolChunk::from_filename(&replica_id, &shard_id, &name)?;
                chunks.push(verify_closed_chunk(root, chunk)?);
            }
        }
    }
    chunks.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(chunks)
}

#[derive(Serialize, Deserialize)]
struct ChecksumSidecar {
    schema_version: u16,
    replica_id: String,
    shard_id: String,
    minute_start_ms: i64,
    byte_length: u64,
    sha256_hex: String,
}

fn verify_closed_chunk(root: &Path, chunk: SpoolChunk) -> Result<VerifiedSpoolChunk, SpoolError> {
    let path = chunk.closed_path(root);
    let checksum_path = chunk.checksum_path(root);
    let sidecar_bytes = match fs::read(&checksum_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(SpoolError::MissingChecksum {
                path: checksum_path,
            });
        }
        Err(error) => return Err(SpoolError::Io(error)),
    };
    let sidecar: ChecksumSidecar =
        serde_json::from_slice(&sidecar_bytes).map_err(|_| SpoolError::ChecksumMismatch {
            path: checksum_path.clone(),
        })?;
    let bytes = fs::read(&path)?;
    let byte_length = u64::try_from(bytes.len())
        .map_err(|_| SpoolError::ChecksumMismatch { path: path.clone() })?;
    if sidecar.schema_version != RAW_SPOOL_SCHEMA_VERSION
        || sidecar.replica_id != chunk.replica_id()
        || sidecar.shard_id != chunk.shard_id()
        || sidecar.minute_start_ms != chunk.minute_start_ms()
        || sidecar.byte_length != byte_length
        || sidecar.sha256_hex != checksum_hex(&bytes)
    {
        return Err(SpoolError::ChecksumMismatch { path });
    }
    let verified = VerifiedSpoolChunk {
        chunk,
        path,
        byte_length: sidecar.byte_length,
        sha256_hex: sidecar.sha256_hex,
    };
    let _ = verified.records()?;
    Ok(verified)
}

pub fn write_checksum_sidecar(
    path: &Path,
    chunk: &SpoolChunk,
    bytes: &[u8],
) -> Result<(), SpoolError> {
    let sidecar = ChecksumSidecar {
        schema_version: RAW_SPOOL_SCHEMA_VERSION,
        replica_id: chunk.replica_id().to_owned(),
        shard_id: chunk.shard_id().to_owned(),
        minute_start_ms: chunk.minute_start_ms(),
        byte_length: u64::try_from(bytes.len()).map_err(|_| SpoolError::MalformedRecord {
            message: "spool exceeds u64".to_owned(),
        })?,
        sha256_hex: checksum_hex(bytes),
    };
    let temporary = temporary_checksum_path(path);
    remove_if_present(&temporary)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    serde_json::to_writer(&mut file, &sidecar).map_err(|error| SpoolError::MalformedRecord {
        message: error.to_string(),
    })?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.sync_all()?;
    drop(file);
    publish_without_replacing(&temporary, path)
}

pub fn publish_without_replacing(source: &Path, destination: &Path) -> Result<(), SpoolError> {
    match fs::hard_link(source, destination) {
        Ok(()) => fs::remove_file(source).map_err(SpoolError::Io),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(SpoolError::ClosedChunkExists {
                path: destination.to_path_buf(),
            })
        }
        Err(error) => Err(SpoolError::Io(error)),
    }
}

pub fn sync_parent_directory(path: &Path) -> Result<(), SpoolError> {
    let parent = path.parent().ok_or_else(|| SpoolError::MalformedRecord {
        message: "spool path has no parent directory".to_owned(),
    })?;
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

pub fn temporary_checksum_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.open", path.display()))
}

pub fn remove_if_present(path: &Path) -> Result<(), SpoolError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(SpoolError::Io(error)),
    }
}
