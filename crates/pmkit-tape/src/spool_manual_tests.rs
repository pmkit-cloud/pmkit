use std::fs::OpenOptions;
use std::io::Write;

use crate::{RawSpoolWriter, SpoolChunk, SpoolFrame, enumerate_closed_chunks, recover_open_chunk};

const MINUTE_MS: i64 = 1_700_000_040_000;
const DISCOVERY_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[test]
fn manual_spool_contract_probe() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let closed_chunk = SpoolChunk::new("lane-a", "shard-0", MINUTE_MS)?;
    let mut writer = RawSpoolWriter::open(root.path(), closed_chunk.clone())?;
    writer.append(&SpoolFrame::new(
        0,
        0,
        MINUTE_MS + 42,
        DISCOVERY_DIGEST,
        b"probe".to_vec(),
    ))?;
    writer.close()?;
    let verified = enumerate_closed_chunks(root.path())?.remove(0);
    let checkpoint = verified.checkpoint();

    let open_chunk = SpoolChunk::new("lane-b", "shard-0", MINUTE_MS)?;
    let mut writer = RawSpoolWriter::open(root.path(), open_chunk.clone())?;
    writer.append(&SpoolFrame::new(
        0,
        0,
        MINUTE_MS + 42,
        DISCOVERY_DIGEST,
        b"complete".to_vec(),
    ))?;
    drop(writer);
    let mut open = OpenOptions::new()
        .append(true)
        .open(open_chunk.open_path(root.path()))?;
    open.write_all(b"partial")?;
    drop(open);
    let recovery = recover_open_chunk(&open_chunk.open_path(root.path()))?;

    println!(
        "closed={} sidecar={} checkpoint={:?} digest={} recovery={:?}",
        closed_chunk
            .closed_path(root.path())
            .file_name()
            .ok_or("closed chunk file name missing")?
            .to_string_lossy(),
        closed_chunk
            .checksum_path(root.path())
            .file_name()
            .ok_or("checksum file name missing")?
            .to_string_lossy(),
        checkpoint.chunk(),
        checkpoint.sha256_hex(),
        recovery.uncertainty,
    );
    Ok(())
}
