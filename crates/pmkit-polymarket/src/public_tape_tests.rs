#![allow(clippy::significant_drop_tightening)]

use std::{collections::BTreeMap, io::Cursor, path::PathBuf, sync::Arc};

use pmkit_core::{MarketId, PortfolioId, RunId};
use pmkit_store::{
    OwnerScope, ReplayItem, TapeStore, TursoTapeStore, decode_sealed_closed_day_manifest,
    export_market_segments,
};
use polymarket_client_sdk_v2::types::U256;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{MarketTokens, PublicTapeImportError, PublicTapeImporter, RawPolymarketFrameAdapter};

const PRODUCER_MARKET: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";

fn scope() -> Result<OwnerScope, Box<dyn std::error::Error>> {
    Ok(OwnerScope::new(
        PortfolioId::new("tape")?,
        RunId::new("v2")?,
    ))
}

fn tokens() -> Result<MarketTokens, Box<dyn std::error::Error>> {
    Ok(MarketTokens::new(
        MarketId::new("btc-5m")?,
        U256::from(1_u64),
        U256::from(2_u64),
    ))
}

fn setup() -> Result<(tempfile::TempDir, PathBuf, PathBuf, String), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mapping = format!("{{\"version\":2,\"mappings\":{{\"1\":\"{PRODUCER_MARKET}\"}}}}");
    let hash = format!("{:x}", Sha256::digest(mapping.as_bytes()));
    let market = root.path().join("pm-market");
    std::fs::create_dir_all(market.join("mappings"))?;
    std::fs::write(
        market.join("mappings").join(format!("{hash}.json")),
        mapping,
    )?;
    let tape = market.join("fixture.v2.ndjson.zst");
    let database = root.path().join("import.db");
    Ok((root, tape, database, hash))
}

fn frame(hash: &str, subframes: &serde_json::Value) -> Vec<u8> {
    let raw = format!(
        r#"[{{"event_type":"book","market":"{PRODUCER_MARKET}","asset_id":"1","timestamp":"10","bids":[{{"price":"0.49","size":"2"}}],"asks":[{{"price":"0.51","size":"3"}}]}},{{"event_type":"last_trade_price","market":"{PRODUCER_MARKET}","asset_id":"1","timestamp":"11","price":"0.5","side":"BUY","size":"4"}},{{"event_type":"book","market":"{PRODUCER_MARKET}","asset_id":"1","timestamp":"10","bids":[{{"price":"0.49","size":"2"}}],"asks":[{{"price":"0.51","size":"3"}}]}},{{"event_type":"market_resolved","timestamp":"12"}}]"#
    );
    format!(
        "{}\n",
        json!({
            "version": 2,
            "record_type": "frame",
            "received_at_ms": 20,
            "source_time_ms": 10,
            "source_id": "polymarket-market",
            "connection_id": 7,
            "epoch": 2,
            "frame_sequence": 4,
            "ingest_sequence": 1,
            "mapping_snapshot_sha256": hash,
            "raw": raw,
            "subframes": subframes,
        })
    )
    .into_bytes()
}

fn subframes() -> serde_json::Value {
    json!([
        {"index": 0, "projection": "book", "duplicate_of": null},
        {"index": 1, "projection": "last_trade_price", "duplicate_of": null},
        {"index": 2, "projection": "book", "duplicate_of": 0},
        {"index": 3, "projection": "intentionally_unprojected", "duplicate_of": null}
    ])
}

fn importer(store: Arc<TursoTapeStore>) -> Result<PublicTapeImporter, Box<dyn std::error::Error>> {
    let scope = scope()?;
    let adapter = RawPolymarketFrameAdapter::new(store, scope.clone(), "fixture");
    Ok(PublicTapeImporter::new(
        adapter,
        scope,
        BTreeMap::from([(PRODUCER_MARKET.into(), tokens()?)]),
    ))
}

#[tokio::test]
async fn imports_compressed_v2_frame_with_deterministic_subframe_ranks()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, tape, database, hash) = setup()?;
    std::fs::write(
        &tape,
        zstd::stream::encode_all(Cursor::new(frame(&hash, &subframes())), 1)?,
    )?;
    let store = Arc::new(TursoTapeStore::open_local(&database).await?);
    let report = importer(store.clone())?
        .import_file(root.path(), &tape)
        .await?;
    let page = store
        .read_envelopes(
            &scope()?,
            None,
            std::num::NonZeroUsize::new(8).ok_or("limit")?,
        )
        .await?;
    let ranks = page
        .items
        .into_iter()
        .map(|item| match item {
            ReplayItem::Envelope(frame) => Ok(frame.frame_sequence),
            ReplayItem::Gap(_) => Err("unexpected gap"),
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(report.projected_frames, 3);
    assert_eq!(report.audit_frames, 1);
    assert_eq!(ranks, vec![17_179_869_184, 17_179_869_185, 17_179_869_186]);
    assert_eq!(
        store.read_public_tape_audit_frames(&scope()?).await?.len(),
        1
    );
    assert!(
        export_market_segments(
            &*store,
            &scope()?,
            &decode_sealed_closed_day_manifest(json!({
                "version": 2,
                "day": "1970-01-01",
                "day_seal": "sealed",
            }))?,
        )
        .await
        .is_ok()
    );
    Arc::try_unwrap(store)
        .map_err(|_| "store still referenced")?
        .delete_database()?;
    Ok(())
}

#[tokio::test]
async fn rejects_gap_intersecting_import_before_segment_export()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, tape, database, hash) = setup()?;
    std::fs::create_dir_all(root.path().join("pm-market/gaps"))?;
    std::fs::write(root.path().join("pm-market/gaps/fixture.jsonl"), b"{\"version\":2,\"record_type\":\"gap\",\"reason\":\"disconnect\",\"scope\":\"all_subscribed\",\"start_time_ms\":10,\"end_time_ms\":11}\n")?;
    std::fs::write(
        &tape,
        zstd::stream::encode_all(Cursor::new(frame(&hash, &subframes())), 1)?,
    )?;
    let store = Arc::new(TursoTapeStore::open_local(&database).await?);
    let report = importer(store.clone())?
        .import_file(root.path(), &tape)
        .await?;
    assert_eq!(report.replay_gaps, 1);
    assert_eq!(
        store.read_public_tape_audit_frames(&scope()?).await?.len(),
        1
    );
    assert!(
        export_market_segments(
            &*store,
            &scope()?,
            &decode_sealed_closed_day_manifest(json!({
                "version": 2,
                "day": "1970-01-01",
                "day_seal": "sealed",
            }))?,
        )
        .await
        .is_err()
    );
    Arc::try_unwrap(store)
        .map_err(|_| "store still referenced")?
        .delete_database()?;
    Ok(())
}

#[tokio::test]
async fn resumes_an_interrupted_import_with_identical_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, tape, database, hash) = setup()?;
    std::fs::write(
        &tape,
        zstd::stream::encode_all(Cursor::new(frame(&hash, &subframes())), 1)?,
    )?;
    let store = Arc::new(TursoTapeStore::open_local(&database).await?);
    importer(store.clone())?
        .import_file(root.path(), &tape)
        .await?;
    let resumed = importer(store.clone())?
        .import_file(root.path(), &tape)
        .await?;
    assert_eq!(resumed.projected_frames, 3);
    assert_eq!(
        store
            .read_envelopes(
                &scope()?,
                None,
                std::num::NonZeroUsize::new(8).ok_or("limit")?
            )
            .await?
            .items
            .len(),
        3
    );
    Arc::try_unwrap(store)
        .map_err(|_| "store still referenced")?
        .delete_database()?;
    Ok(())
}

#[tokio::test]
async fn rolls_back_gap_audit_and_envelopes_when_a_later_envelope_write_fails()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a real storage failure on the third projected envelope.
    let (root, tape, database, hash) = setup()?;
    std::fs::create_dir_all(root.path().join("pm-market/gaps"))?;
    std::fs::write(
        root.path().join("pm-market/gaps/fixture.jsonl"),
        b"{\"version\":2,\"record_type\":\"gap\",\"reason\":\"disconnect\",\"scope\":\"all_subscribed\",\"start_time_ms\":10,\"end_time_ms\":11}\n",
    )?;
    std::fs::write(
        &tape,
        zstd::stream::encode_all(Cursor::new(frame(&hash, &subframes())), 1)?,
    )?;
    let store = Arc::new(TursoTapeStore::open_local(&database).await?);
    let database_handle = turso::Builder::new_local(&database.to_string_lossy())
        .build()
        .await?;
    let connection = database_handle.connect()?;
    connection
        .execute(
            "CREATE TRIGGER fail_third_public_tape_envelope BEFORE INSERT ON pm_envelopes WHEN NEW.frame_sequence = 17179869186 BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END",
            (),
        )
        .await?;

    // When: the otherwise valid import reaches the injected later write failure.
    let result = importer(store.clone())?
        .import_file(root.path(), &tape)
        .await;

    // Then: no partial public-tape state is durable.
    assert!(matches!(result, Err(PublicTapeImportError::Adapter(_))));
    assert!(
        store
            .read_public_tape_audit_frames(&scope()?)
            .await?
            .is_empty()
    );
    assert!(store.read_replay_gaps(&scope()?).await?.is_empty());
    assert!(
        store
            .read_envelopes(
                &scope()?,
                None,
                std::num::NonZeroUsize::new(8).ok_or("limit")?
            )
            .await?
            .items
            .is_empty()
    );
    drop(connection);
    drop(database_handle);
    Arc::try_unwrap(store)
        .map_err(|_| "store still referenced")?
        .delete_database()?;
    Ok(())
}

#[tokio::test]
async fn fails_closed_on_malformed_stale_or_out_of_order_v2_evidence()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, tape, database, hash) = setup()?;
    let store = Arc::new(TursoTapeStore::open_local(&database).await?);
    let importer = importer(store.clone())?;
    std::fs::write(
        &tape,
        zstd::stream::encode_all(Cursor::new(b"not-json\n"), 1)?,
    )?;
    assert!(matches!(
        importer.import_file(root.path(), &tape).await,
        Err(PublicTapeImportError::Invalid { .. })
    ));
    std::fs::write(
        root.path()
            .join("pm-market/mappings")
            .join(format!("{hash}.json")),
        b"stale",
    )?;
    std::fs::write(
        &tape,
        zstd::stream::encode_all(Cursor::new(frame(&hash, &subframes())), 1)?,
    )?;
    assert!(matches!(
        importer.import_file(root.path(), &tape).await,
        Err(PublicTapeImportError::Invalid { .. })
    ));
    let mapping = format!("{{\"version\":2,\"mappings\":{{\"1\":\"{PRODUCER_MARKET}\"}}}}");
    std::fs::write(
        root.path()
            .join("pm-market/mappings")
            .join(format!("{hash}.json")),
        mapping,
    )?;
    let invalid_subframes = json!([
        {"index": 1, "projection": "book", "duplicate_of": null},
        {"index": 0, "projection": "last_trade_price", "duplicate_of": null},
        {"index": 2, "projection": "book", "duplicate_of": 0},
        {"index": 3, "projection": "intentionally_unprojected", "duplicate_of": null}
    ]);
    std::fs::create_dir_all(root.path().join("pm-market/gaps"))?;
    std::fs::write(
        root.path().join("pm-market/gaps/fixture.jsonl"),
        b"{\"version\":2,\"record_type\":\"gap\",\"reason\":\"disconnect\",\"scope\":\"all_subscribed\",\"start_time_ms\":10,\"end_time_ms\":11}\n",
    )?;
    std::fs::write(
        &tape,
        zstd::stream::encode_all(
            Cursor::new([frame(&hash, &subframes()), frame(&hash, &invalid_subframes)].concat()),
            1,
        )?,
    )?;
    assert!(matches!(
        importer.import_file(root.path(), &tape).await,
        Err(PublicTapeImportError::Invalid { .. })
    ));
    assert!(
        store
            .read_public_tape_audit_frames(&scope()?)
            .await?
            .is_empty()
    );
    assert!(store.read_replay_gaps(&scope()?).await?.is_empty());
    assert!(
        store
            .read_envelopes(
                &scope()?,
                None,
                std::num::NonZeroUsize::new(8).ok_or("limit")?
            )
            .await?
            .items
            .is_empty()
    );
    drop(importer);
    Arc::try_unwrap(store)
        .map_err(|_| "store still referenced")?
        .delete_database()?;
    Ok(())
}

#[tokio::test]
async fn rejects_legacy_tsv_before_audit_or_replay_import() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: an archival TSV under the otherwise public tape root.
    let (root, _tape, database, hash) = setup()?;
    let legacy = root.path().join("pm-market/legacy.tsv");
    std::fs::write(&legacy, frame(&hash, &subframes()))?;
    let store = Arc::new(TursoTapeStore::open_local(&database).await?);

    // When: the v2 importer receives the legacy artifact.
    let result = importer(store.clone())?
        .import_file(root.path(), &legacy)
        .await;

    // Then: no evidence crosses the certification boundary.
    assert!(matches!(result, Err(PublicTapeImportError::Invalid { .. })));
    assert!(
        store
            .read_public_tape_audit_frames(&scope()?)
            .await?
            .is_empty()
    );
    Arc::try_unwrap(store)
        .map_err(|_| "store still referenced")?
        .delete_database()?;
    Ok(())
}

#[tokio::test]
async fn rejects_private_tape_path_before_audit_or_replay_import()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a syntactically valid v2 market frame in a private/user path.
    let (root, _tape, database, hash) = setup()?;
    let private_tape = root.path().join("pm-user/fixture.v2.ndjson.zst");
    std::fs::create_dir_all(private_tape.parent().ok_or("private tape parent")?)?;
    std::fs::write(
        &private_tape,
        zstd::stream::encode_all(Cursor::new(frame(&hash, &subframes())), 1)?,
    )?;
    let store = Arc::new(TursoTapeStore::open_local(&database).await?);

    // When: the public importer receives the private path.
    let result = importer(store.clone())?
        .import_file(root.path(), &private_tape)
        .await;

    // Then: no evidence crosses the certification boundary.
    assert!(matches!(result, Err(PublicTapeImportError::Invalid { .. })));
    assert!(
        store
            .read_public_tape_audit_frames(&scope()?)
            .await?
            .is_empty()
    );
    Arc::try_unwrap(store)
        .map_err(|_| "store still referenced")?
        .delete_database()?;
    Ok(())
}

#[tokio::test]
async fn rejects_private_source_record_before_audit_or_replay_import()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a public path containing a private/user source record.
    let (root, tape, database, hash) = setup()?;
    let private = String::from_utf8(frame(&hash, &subframes()))?
        .replace("polymarket-market", "polymarket:user-ws");
    std::fs::write(&tape, zstd::stream::encode_all(Cursor::new(private), 1)?)?;
    std::fs::create_dir_all(root.path().join("pm-market/gaps"))?;
    std::fs::write(
        root.path().join("pm-market/gaps/private.jsonl"),
        b"{\"version\":2,\"record_type\":\"gap\",\"reason\":\"disconnect\",\"scope\":\"all_subscribed\",\"start_time_ms\":10,\"end_time_ms\":11}\n",
    )?;
    let store = Arc::new(TursoTapeStore::open_local(&database).await?);

    // When: the public importer receives the private record.
    let result = importer(store.clone())?
        .import_file(root.path(), &tape)
        .await;

    // Then: it rejects before retaining audit evidence or creating a replay projection.
    assert!(matches!(result, Err(PublicTapeImportError::Invalid { .. })));
    assert!(
        store
            .read_public_tape_audit_frames(&scope()?)
            .await?
            .is_empty()
    );
    assert!(store.read_replay_gaps(&scope()?).await?.is_empty());
    assert!(
        store
            .read_envelopes(
                &scope()?,
                None,
                std::num::NonZeroUsize::new(8).ok_or("limit")?
            )
            .await?
            .items
            .is_empty()
    );
    Arc::try_unwrap(store)
        .map_err(|_| "store still referenced")?
        .delete_database()?;
    Ok(())
}
