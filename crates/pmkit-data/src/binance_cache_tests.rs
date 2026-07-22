use pmkit_market::Asset;

use crate::{
    DataSourceError,
    binance_cache_test_support::{
        ROW, TempRoot, TestServer, archive, archive_path, cache, checksum, date, responses,
        write_cached, zip_count,
    },
    parse_binance_vision_agg_trade_row,
};

#[tokio::test]
async fn verified_binance_cache_replays() -> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new()?;
    let date = date(1)?;
    let archive = archive(ROW)?;
    let checksum = checksum(&archive);
    let mut server = TestServer::new(responses(date, &checksum, archive.clone()), 2)?;
    let cache = cache(&root, 1_000_000, &server.url);

    let records = cache.replay(Asset::Btc, date).await?;
    let cached_records = cache.replay(Asset::Btc, date).await?;

    assert_eq!(
        records,
        vec![parse_binance_vision_agg_trade_row(ROW.trim(), Asset::Btc)?]
    );
    assert_eq!(cached_records, records);
    server.join()?;
    assert_eq!(server.calls(), 2);
    Ok(())
}

#[tokio::test]
async fn corrupt_or_oversized_archive_is_replay_gap() -> Result<(), Box<dyn std::error::Error>> {
    let corrupt_root = TempRoot::new()?;
    let corrupt_date = date(2)?;
    let expected = archive(ROW)?;
    let expected_checksum = checksum(&expected);
    let mut server = TestServer::new(
        responses(corrupt_date, &expected_checksum, b"corrupt".to_vec()),
        2,
    )?;
    let corrupt_cache = cache(&corrupt_root, 1_000_000, &server.url);

    let corrupt = corrupt_cache.replay(Asset::Btc, corrupt_date).await;

    assert!(matches!(corrupt, Err(DataSourceError::ReplayGap { .. })));
    assert!(!archive_path(corrupt_root.path(), corrupt_date).exists());
    server.join()?;

    let oversized_root = TempRoot::new()?;
    let retained_date = date(3)?;
    let retained = archive(ROW)?;
    write_cached(oversized_root.path(), retained_date, &retained)?;
    let oversized_date = date(4)?;
    let oversized = archive(&format!("{ROW}{ROW}{ROW}{ROW}{ROW}{ROW}"))?;
    assert!(oversized.len() > retained.len());
    let oversized_checksum = checksum(&oversized);
    let mut server = TestServer::new(responses(oversized_date, &oversized_checksum, oversized), 2)?;
    let oversized_cache = cache(&oversized_root, retained.len() as u64, &server.url);

    let retained_records = oversized_cache.replay(Asset::Btc, retained_date).await?;
    let oversized_result = oversized_cache.replay(Asset::Btc, oversized_date).await;

    assert_eq!(retained_records.len(), 1);
    assert!(matches!(
        oversized_result,
        Err(DataSourceError::ReplayGap { .. })
    ));
    assert!(archive_path(oversized_root.path(), retained_date).exists());
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn concurrent_same_key_download_creates_one_artifact()
-> Result<(), Box<dyn std::error::Error>> {
    let root = TempRoot::new()?;
    let date = date(5)?;
    let archive = archive(ROW)?;
    let checksum = checksum(&archive);
    let mut server = TestServer::new(responses(date, &checksum, archive), 2)?;
    let cache = cache(&root, 1_000_000, &server.url);

    let (first, second) = tokio::join!(
        cache.replay(Asset::Btc, date),
        cache.replay(Asset::Btc, date)
    );

    assert_eq!(first?, second?);
    assert_eq!(zip_count(root.path())?, 1);
    server.join()?;
    assert_eq!(server.calls(), 2);
    Ok(())
}
