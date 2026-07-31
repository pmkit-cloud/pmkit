//! Imports only certified v2 public-market input and demonstrates partitioned gap rejection.

use std::{collections::BTreeMap, io::Cursor, sync::Arc};

use pmkit_core::{MarketId, PortfolioId, RunId};
use pmkit_polymarket::{MarketTokens, PublicTapeImporter, RawPolymarketFrameAdapter};
use pmkit_store::{
    OwnerScope, TursoTapeStore, decode_sealed_closed_day_manifest, export_market_segments,
};
use polymarket_client_sdk_v2::types::U256;
use serde_json::json;
use sha2::{Digest, Sha256};

const MARKET: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";

#[tokio::main]
#[allow(clippy::significant_drop_tightening)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let market_root = root.path().join("pm-market");
    std::fs::create_dir_all(market_root.join("mappings"))?;
    let mapping = format!("{{\"version\":2,\"mappings\":{{\"1\":\"{MARKET}\"}}}}");
    let hash = format!("{:x}", Sha256::digest(mapping.as_bytes()));
    std::fs::write(
        market_root.join("mappings").join(format!("{hash}.json")),
        mapping,
    )?;
    let raw = format!(
        r#"{{"event_type":"book","market":"{MARKET}","asset_id":"1","timestamp":"10","bids":[{{"price":"0.49","size":"2"}}],"asks":[{{"price":"0.51","size":"3"}}]}}"#
    );
    let record = format!(
        "{}\n",
        json!({
            "version": 2, "record_type": "frame", "received_at_ms": 20, "source_time_ms": 10,
            "source_id": "polymarket-market", "connection_id": 7, "epoch": 2, "frame_sequence": 4,
            "ingest_sequence": 1, "mapping_snapshot_sha256": hash, "raw": raw,
            "subframes": [{"index": 0, "projection": "book", "duplicate_of": null}],
        })
    );
    let tape = market_root.join("fixture.v2.ndjson.zst");
    std::fs::write(
        &tape,
        zstd::stream::encode_all(Cursor::new(record.as_bytes()), 1)?,
    )?;
    let scope = OwnerScope::new(PortfolioId::new("demo")?, RunId::new("v2")?);
    let store = Arc::new(TursoTapeStore::open_local(root.path().join("import.db")).await?);
    let adapter = RawPolymarketFrameAdapter::new(store.clone(), scope.clone(), "fixture");
    let importer = PublicTapeImporter::new(
        adapter,
        scope.clone(),
        BTreeMap::from([(
            MARKET.into(),
            MarketTokens::new(
                MarketId::new("btc-5m")?,
                U256::from(1_u64),
                U256::from(2_u64),
            ),
        )]),
    );
    let report = importer.import_file(root.path(), &tape).await?;
    let manifest = decode_sealed_closed_day_manifest(json!({
        "version": 2,
        "day": "1970-01-01",
        "day_seal": "sealed",
    }))?;
    let segment = export_market_segments(&*store, &scope, &manifest).await?;
    println!(
        "happy: projected={} segments={}",
        report.projected_frames,
        segment["segments"].as_array().map_or(0, Vec::len)
    );
    std::fs::create_dir_all(market_root.join("gaps"))?;
    std::fs::write(market_root.join("gaps/unrelated.jsonl"), b"{\"version\":2,\"record_type\":\"gap\",\"reason\":\"disconnect\",\"scope\":\"other-market:0\",\"start_time_ms\":10,\"end_time_ms\":10}\n")?;
    importer.import_file(root.path(), &tape).await?;
    let unrelated_partition_exported = export_market_segments(&*store, &scope, &manifest)
        .await
        .is_ok();
    println!("unrelated_partition_exported={unrelated_partition_exported}");
    std::fs::write(market_root.join("gaps/gap.jsonl"), b"{\"version\":2,\"record_type\":\"gap\",\"reason\":\"disconnect\",\"scope\":\"btc-5m:0\",\"start_time_ms\":10,\"end_time_ms\":10}\n")?;
    importer.import_file(root.path(), &tape).await?;
    let rejected = export_market_segments(&*store, &scope, &manifest)
        .await
        .is_err();
    println!("gap_rejected={rejected}");
    let legacy = market_root.join("legacy.tsv");
    std::fs::write(&legacy, &record)?;
    println!(
        "legacy_rejected={}",
        importer.import_file(root.path(), &legacy).await.is_err()
    );
    let private = root.path().join("pm-user/private.v2.ndjson");
    std::fs::create_dir_all(private.parent().ok_or("private tape parent")?)?;
    std::fs::write(&private, &record)?;
    println!(
        "private_rejected={}",
        importer.import_file(root.path(), &private).await.is_err()
    );
    drop(importer);
    let store = Arc::try_unwrap(store).map_err(|_| "store still referenced")?;
    store.delete_database()?;
    Ok(())
}
