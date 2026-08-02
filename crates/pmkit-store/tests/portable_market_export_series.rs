//! Recurring-series portable market export tests.

use pmkit_core::{PortfolioId, RunId};
use pmkit_store::{
    OwnerScope, PM_ENVELOPE_VERSION, PmEnvelope, TapeStore, TursoTapeStore,
    decode_sealed_closed_day_manifest, export_market_segments,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct Instance {
    market_id: String,
    condition_id: String,
    up_token_id: String,
    down_token_id: String,
    open_time_ms: i64,
    close_time_ms: i64,
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn portable_market_export_keeps_twelve_instances_in_one_series()
-> Result<(), Box<dyn std::error::Error>> {
    let instances: Vec<Instance> =
        serde_json::from_slice(include_bytes!("fixtures/portable-market-series-v1.json"))?;
    let directory = tempfile::tempdir()?;
    let scope = OwnerScope::new(PortfolioId::new("research")?, RunId::new("series")?);
    let store = TursoTapeStore::open_local(directory.path().join("export.db")).await?;
    for (ordinal, instance) in instances.iter().enumerate() {
        store.store_envelope(&PmEnvelope { schema_version: PM_ENVELOPE_VERSION, scope: scope.clone(), venue_id: "venue".into(), config_hash: "config".into(), source_id: "observer".into(), connection_id: "connection".into(), source_timestamp_ms: instance.open_time_ms + 1_000, canonical_source_rank: i64::try_from(ordinal)?, connection_epoch: 0, frame_sequence: 0, receipt_timestamp_ms: instance.open_time_ms + 1_001, ingest_sequence: i64::try_from(ordinal + 1)?, raw_frame: Vec::new(), normalized: json!({"canonical_market_id": instance.market_id, "portable_market": {"series_id":"btc-usd-5m","asset":"BTC","duration_seconds":300,"market_id":instance.market_id,"condition_id":instance.condition_id,"outcome_tokens":[{"outcome":"up","token_id":instance.up_token_id},{"outcome":"down","token_id":instance.down_token_id}],"open_time_ms":instance.open_time_ms,"close_time_ms":instance.close_time_ms},"payload":{"price":"0.42"}}) }).await?;
    }
    let manifest = decode_sealed_closed_day_manifest(
        json!({"version":2,"day":"1970-01-01","day_seal":"sealed"}),
    )?;
    let first = export_market_segments(&store, &scope, &manifest).await?;
    let second = export_market_segments(&store, &scope, &manifest).await?;
    let segments = first["segments"].as_array().ok_or("segments")?;
    assert_eq!(first, second);
    assert_eq!(segments.len(), 12);
    for (instance, segment) in instances.iter().zip(segments) {
        assert_eq!(segment["series_id"], "btc-usd-5m");
        assert_eq!(segment["market_id"], instance.market_id);
        assert_eq!(segment["condition_id"], instance.condition_id);
        assert_eq!(segment["market_open_time_ms"], instance.open_time_ms);
        assert_eq!(segment["market_close_time_ms"], instance.close_time_ms);
        assert_eq!(
            segment["outcome_tokens"][0]["token_id"],
            instance.up_token_id
        );
        assert_eq!(
            segment["outcome_tokens"][1]["token_id"],
            instance.down_token_id
        );
    }
    store.delete_database()?;
    Ok(())
}
