use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::spool_closed::{
    publish_without_replacing, remove_if_present, sync_parent_directory, temporary_checksum_path,
    write_checksum_sidecar,
};
use crate::spool_record::encode_record;
use crate::{SpoolChunk, SpoolError, SpoolFrame, recover_open_chunk};

/// Append-only writer for one portable raw-spool chunk.
#[derive(Debug)]
pub struct RawSpoolWriter {
    chunk: SpoolChunk,
    open_path: PathBuf,
    closed_path: PathBuf,
    checksum_path: PathBuf,
    file: fs::File,
}

impl RawSpoolWriter {
    /// Opens a mutable chunk, recovering only a possible interrupted final line.
    ///
    /// # Errors
    ///
    /// Returns [`SpoolError`] when a closed chunk exists or recovery/opening fails.
    pub fn open(root: &Path, chunk: SpoolChunk) -> Result<Self, SpoolError> {
        let open_path = chunk.open_path(root);
        let closed_path = chunk.closed_path(root);
        let checksum_path = chunk.checksum_path(root);
        if closed_path.exists() {
            return Err(SpoolError::ClosedChunkExists { path: closed_path });
        }
        let parent = open_path
            .parent()
            .ok_or_else(|| SpoolError::MalformedRecord {
                message: "spool chunk has no parent directory".to_owned(),
            })?;
        fs::create_dir_all(parent)?;
        let file = if open_path.exists() {
            recover_open_chunk(&open_path)?;
            remove_if_present(&checksum_path)?;
            remove_if_present(&temporary_checksum_path(&checksum_path))?;
            OpenOptions::new().append(true).open(&open_path)?
        } else {
            OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(&open_path)?
        };
        Ok(Self {
            chunk,
            open_path,
            closed_path,
            checksum_path,
            file,
        })
    }

    /// Appends one raw frame without adapting its original bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SpoolError`] when frame metadata is invalid or the file rejects the write.
    pub fn append(&mut self, frame: &SpoolFrame) -> Result<(), SpoolError> {
        let line = encode_record(&self.chunk, frame)?;
        self.file.write_all(&line)?;
        self.file.write_all(b"\n")?;
        Ok(())
    }

    /// Fsyncs the chunk, publishes its checksum sidecar, then atomically closes it.
    ///
    /// # Errors
    ///
    /// Returns [`SpoolError`] when syncing, sidecar publication, or atomic rename fails.
    pub fn close(mut self) -> Result<(), SpoolError> {
        if self.closed_path.exists() {
            return Err(SpoolError::ClosedChunkExists {
                path: self.closed_path,
            });
        }
        self.file.flush()?;
        self.file.sync_all()?;
        drop(self.file);
        let bytes = fs::read(&self.open_path)?;
        write_checksum_sidecar(&self.checksum_path, &self.chunk, &bytes)?;
        publish_without_replacing(&self.open_path, &self.closed_path)?;
        sync_parent_directory(&self.closed_path)
    }
}
