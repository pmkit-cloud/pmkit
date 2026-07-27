//! Materializes a treated market segment and optionally publishes it to Cloud.
//!
//! Run locally with:
//! `cargo run -p pmkit-store --example publish_market_segments`.
//!
//! Set `PMKIT_CLOUD_URL` and `PMKIT_STORAGE_TOKEN` to perform publication;
//! without them the example prints the manifest and upload plan only.

use std::error::Error;

use pmkit_core::{PortfolioId, RunId};
use pmkit_store::{
    CloudPublisher, OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, TapeStore, TursoTapeStore,
    export_market_segments_with_artifacts,
};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
#[allow(clippy::significant_drop_tightening)]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pmkit-market-segments.db");
    let scope = OwnerScope::new(PortfolioId::new("research")?, RunId::new("bt")?);
    let store = TursoTapeStore::open_local(&path).await?;
    store
        .store_envelope(&PmEnvelope {
            schema_version: PM_ENVELOPE_VERSION,
            scope: scope.clone(),
            venue_id: "polymarket".into(),
            config_hash: "config-sha256".into(),
            source_id: "market-channel".into(),
            connection_id: "connection-1".into(),
            source_timestamp_ms: 1_000,
            canonical_source_rank: 0,
            connection_epoch: 0,
            frame_sequence: 0,
            receipt_timestamp_ms: 1_001,
            ingest_sequence: 1,
            raw_frame: br#"{"event_type":"price_change","price":"0.42"}"#.to_vec(),
            normalized: json!({
                "canonical_market_id": "token-1",
                "kind": "market_price",
                "price": "0.42"
            }),
        })
        .await?;

    let output = export_market_segments_with_artifacts(
        &store,
        &scope,
        &json!({"mode": "backtest", "run": "bt", "portfolio": "research"}),
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&output.manifest)?);
    println!("segments to upload: {}", output.segments.len());

    match (
        std::env::var("PMKIT_CLOUD_URL"),
        std::env::var("PMKIT_STORAGE_TOKEN"),
    ) {
        (Ok(endpoint), Ok(token)) => {
            let publisher = CloudPublisher::new(endpoint, token)?;
            let mut release_id = None;
            publisher
                .publish_with_progress("pmkit-example-bt", &output, |progress| {
                    release_id = Some(progress.release_id);
                    println!(
                        "uploaded segments: {}/{}",
                        progress.uploaded_segments, progress.total_segments
                    );
                })
                .await?;
            println!(
                "published release: {}",
                release_id.ok_or("publication returned no release")?
            );
        }
        _ => println!("dry run: set PMKIT_CLOUD_URL and PMKIT_STORAGE_TOKEN to publish"),
    }

    store.delete_database()?;
    Ok(())
}
