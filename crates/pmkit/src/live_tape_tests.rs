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

    assert_eq!(report.events_processed, 2);
    assert_eq!(tape.lines().count(), 1);
    assert!(tape.contains("\"kind\":\"fill\""));
    assert!(tape.contains("\"schema_version\":1"));
    fs::remove_dir_all(tape_dir)?;
    Ok(())
}
