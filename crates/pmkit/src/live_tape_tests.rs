use super::live_run;
use crate::{live, test_support::config};
use pmkit_run::TapePolicy;
use std::fs;

#[tokio::test]
async fn live_run_writes_account_events_to_required_tape() -> Result<(), Box<dyn std::error::Error>>
{
    let mut runtime = config()?;
    let tape_dir = std::env::temp_dir().join(format!("pmkit-live-tape-{}", std::process::id()));
    if tape_dir.exists() {
        fs::remove_dir_all(&tape_dir)?;
    }
    runtime.manifest_dir = tape_dir.clone();

    let report = live::drive(&live_run()?.tape(TapePolicy::Required), &runtime).await?;
    let tape_file = fs::read_dir(&tape_dir)?
        .next()
        .transpose()?
        .ok_or("tape file")?
        .path();
    let tape = fs::read_to_string(tape_file)?;
    let record: serde_json::Value = serde_json::from_str(tape.trim())?;

    assert_eq!(report.events_processed, 2);
    assert_eq!(tape.lines().count(), 1);
    assert_eq!(record["schema_version"], 4);
    assert_eq!(record["payload"]["kind"], "fill");
    assert_eq!(record["payload"]["identity"]["source"], "transport");
    assert_eq!(record["payload"]["identity"]["source_id"], "pmkit-live");
    assert_eq!(record["payload"]["identity"]["frame_sequence"], 0);
    fs::remove_dir_all(tape_dir)?;
    Ok(())
}
