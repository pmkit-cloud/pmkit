//! Publishes one sealed public-tape day through `PMKit`'s durable Cloud bridge.

use std::{
    env,
    error::Error,
    fs, io,
    path::{Path, PathBuf},
};

use pmkit_core::{PortfolioId, RunId};
use pmkit_store::{
    CloudPublisher, OwnerScope, StoreError, TursoTapeStore,
    cloud_materialization_from_sealed_manifest, decode_sealed_closed_day_manifest,
    export_market_segments_with_artifacts, reconcile_materialization,
};
use serde_json::Value;

const MAX_MANIFEST_BYTES: u64 = 1_048_576;

struct Command {
    database: PathBuf,
    endpoint: String,
    manifest: PathBuf,
    portfolio: String,
    run: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    if env::args().any(|argument| argument == "--help" || argument == "-h") {
        println!(
            "Usage: PMKIT_STORAGE_TOKEN=... pmkit-cloud-publish --database PATH --manifest PATH --portfolio ID --run ID --endpoint URL"
        );
        return Ok(());
    }
    let command = parse_command()?;
    let storage_token =
        env::var("PMKIT_STORAGE_TOKEN").map_err(|_| invalid("PMKIT_STORAGE_TOKEN is required"))?;
    if storage_token.is_empty() {
        return Err(invalid("PMKIT_STORAGE_TOKEN is required"));
    }
    let manifest = load_sealed_manifest(&command.manifest)?;
    let scope = OwnerScope::new(
        PortfolioId::new(command.portfolio)?,
        RunId::new(command.run)?,
    );
    let store = TursoTapeStore::open_local(command.database).await?;
    let materialized = export_market_segments_with_artifacts(&store, &scope, &manifest).await?;
    if materialized.segments.is_empty() {
        return Err(invalid("sealed day has no materialized segments"));
    }
    let artifact_sha256 = materialized
        .manifest
        .get("artifact_sha256")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("materialized manifest is missing artifact_sha256"))?;
    let materialization = cloud_materialization_from_sealed_manifest(
        &manifest,
        &format!("closed_day:{}", manifest.closed_day()),
        artifact_sha256,
    )?;
    let publisher = CloudPublisher::new(command.endpoint, storage_token)?;
    let state = reconcile_materialization(&store, &materialization, move |bundle_id| async move {
        publisher
            .publish(&bundle_id, &materialized)
            .await
            .map_err(StoreError::from)
    })
    .await?;
    drop(store);
    println!(
        "bundle_id={} state={:?} release_id={}",
        state.bundle_id,
        state.state,
        state.release_id.as_deref().unwrap_or("pending"),
    );
    Ok(())
}

fn parse_command() -> Result<Command, Box<dyn Error>> {
    let mut database = None;
    let mut endpoint = None;
    let mut manifest = None;
    let mut portfolio = None;
    let mut run = None;
    let mut arguments = env::args().skip(1);
    while let Some(flag) = arguments.next() {
        let value = arguments
            .next()
            .ok_or_else(|| invalid("command option is missing its value"))?;
        match flag.as_str() {
            "--database" => database = Some(PathBuf::from(value)),
            "--endpoint" => endpoint = Some(value),
            "--manifest" => manifest = Some(PathBuf::from(value)),
            "--portfolio" => portfolio = Some(value),
            "--run" => run = Some(value),
            _ => return Err(invalid("unknown command option")),
        }
    }
    Ok(Command {
        database: database.ok_or_else(|| invalid("--database is required"))?,
        endpoint: endpoint.ok_or_else(|| invalid("--endpoint is required"))?,
        manifest: manifest.ok_or_else(|| invalid("--manifest is required"))?,
        portfolio: portfolio.ok_or_else(|| invalid("--portfolio is required"))?,
        run: run.ok_or_else(|| invalid("--run is required"))?,
    })
}

fn load_sealed_manifest(
    path: &Path,
) -> Result<pmkit_store::SealedClosedDayManifest, Box<dyn Error>> {
    if fs::metadata(path)?.len() > MAX_MANIFEST_BYTES {
        return Err(invalid("manifest exceeds the 1 MiB limit"));
    }
    let document = serde_json::from_slice(&fs::read(path)?)?;
    Ok(decode_sealed_closed_day_manifest(document)?)
}

fn invalid(message: &'static str) -> Box<dyn Error> {
    io::Error::new(io::ErrorKind::InvalidInput, message).into()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::load_sealed_manifest;

    #[test]
    fn load_sealed_manifest_rejects_the_legacy_closed_day_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: the pre-v2 sealed-day document accepted by older publishers.
        let mut manifest = tempfile::NamedTempFile::new()?;
        writeln!(
            manifest,
            r#"{{"schema_version":1,"closed_day":"1970-01-02","day_seal":"sealed"}}"#
        )?;

        // When: the production CLI reads it before any storage or HTTP work.
        let result = load_sealed_manifest(manifest.path());

        // Then: legacy input cannot progress to publication.
        assert!(result.is_err());
        Ok(())
    }
}
