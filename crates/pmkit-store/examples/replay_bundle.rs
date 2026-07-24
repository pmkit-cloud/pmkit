//! Records one PM envelope and one causal decision in a file-backed store,
//! exports a replay bundle, and prints it. Exercises the public bundle surface:
//! `cargo run -p pmkit-store --example replay_bundle`.

use std::error::Error;

use pmkit_core::{PortfolioId, RunId};
use pmkit_store::{
    CacheChecksum, CausalDecision, CausalIdentity, OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope,
    TapeStore, TursoTapeStore, export_replay_bundle,
};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
#[allow(clippy::significant_drop_tightening)]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pmkit-replay-bundle.db");
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
            normalized: json!({"kind": "market_price", "price": "0.42"}),
        })
        .await?;
    store
        .store_decision(&CausalDecision {
            identity: CausalIdentity {
                scope: scope.clone(),
                correlation_id: "intent-1".into(),
                source_timestamp_ms: 1_000,
                ingest_sequence: 1,
            },
            payload: json!({"kind": "quote", "actions": 1}),
        })
        .await?;

    let manifest = json!({"mode": "backtest", "run": "bt", "portfolio": "research"});
    let checksums = [CacheChecksum {
        key: "BTCUSDT-aggTrades-2026-01-01.zip".into(),
        sha256_hex: "0000000000000000000000000000000000000000000000000000000000000000".into(),
    }];

    let bundle = export_replay_bundle(&store, &scope, &manifest, &checksums).await?;
    println!("{}", serde_json::to_string_pretty(&bundle)?);
    store.delete_database()?;
    Ok(())
}
