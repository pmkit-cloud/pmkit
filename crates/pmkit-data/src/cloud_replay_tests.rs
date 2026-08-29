use pmkit_event::{MarketEvent, SourceEnvelope};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;

use super::cloud_test_support::{Response, TestServer};
use super::{CloudApiKey, CloudReplayQuery, PmKitCloudDataSource};
use crate::{HistoricalDataSource, ReplayQuery, SourceSignal};
use pmkit_core::MarketId;
use pmkit_run::{EvidenceRequirement, RetrievalWait};

#[tokio::test]
async fn hot_segment_is_verified_decoded_and_cached() -> Result<(), Box<dyn std::error::Error>> {
    let logical = br#"{"event_time_ms":1000,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[{"price":"0.49","size":"2"}],"asks":[{"price":"0.51","size":"3"}]}}
"#;
    let encoded = zstd::stream::encode_all(logical.as_slice(), 0)?;
    let logical_sha = digest(logical);
    let encoded_sha = digest(&encoded);
    let page = segment_page(
        "hot",
        logical.len(),
        &logical_sha,
        encoded.len(),
        &encoded_sha,
        None,
    );
    let mut server = TestServer::new(vec![
        Response::json(200, available_coverage()),
        Response::json(200, &page),
        Response::bytes(200, encoded, &encoded_sha, &logical_sha),
        Response::json(200, available_coverage()),
        Response::json(200, &page),
    ])?;
    let source = PmKitCloudDataSource::with_base_url(
        CloudApiKey::new("secret-value")?,
        &format!("{}/v1", server.url),
    )?;
    for _ in 0..2 {
        let (tx, mut rx) = mpsc::channel(8);
        source.replay_cloud(query(), tx).await?;
        let mut facts = Vec::new();
        let mut terminal = Vec::new();
        while let Some(signal) = rx.recv().await {
            match signal {
                SourceSignal::Data(envelope) => match *envelope {
                    SourceEnvelope::PmMarket(envelope) => facts.push(envelope.fact),
                    SourceEnvelope::PmAccount(_) | SourceEnvelope::CexReference(_) => {
                        return Err("Cloud replay emitted a non-market envelope".into());
                    }
                },
                SourceSignal::Watermark(timestamp_ms) => {
                    terminal.push(SourceSignal::Watermark(timestamp_ms));
                }
                SourceSignal::Eof => terminal.push(SourceSignal::Eof),
            }
        }
        assert!(matches!(
            facts.as_slice(),
            [MarketEvent::BookUpdate {
                timestamp_ms: 1000,
                ..
            }]
        ));
        assert_eq!(
            terminal,
            vec![SourceSignal::Watermark(60_000), SourceSignal::Eof]
        );
    }
    assert_eq!(server.calls(), 5);
    assert!(
        server
            .requests()
            .iter()
            .all(
                |request| request.authorization.as_deref() == Some("Bearer secret-value")
                    && !request.path.contains("secret-value")
            )
    );
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn duplicate_segments_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let logical = br#"{"event_time_ms":1000,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[],"asks":[]}}
"#;
    let encoded = zstd::stream::encode_all(logical.as_slice(), 0)?;
    let logical_sha = digest(logical);
    let encoded_sha = digest(&encoded);
    let first_page = segment_page(
        "hot",
        logical.len(),
        &logical_sha,
        encoded.len(),
        &encoded_sha,
        Some("page-2"),
    );
    let second_page = segment_page(
        "hot",
        logical.len(),
        &logical_sha,
        encoded.len(),
        &encoded_sha,
        None,
    );
    let mut server = TestServer::new(vec![
        Response::json(200, available_coverage()),
        Response::json(200, &first_page),
        Response::bytes(200, encoded, &encoded_sha, &logical_sha),
        Response::json(200, &second_page),
    ])?;
    let source = PmKitCloudDataSource::with_base_url(
        super::CloudApiKey::new("secret-value")?,
        &format!("{}/v1", server.url),
    )?;
    let (tx, _rx) = mpsc::channel(1);
    assert_eq!(
        source.replay_cloud(query(), tx).await,
        Err(super::CloudReplayError::MalformedResponse)
    );
    assert_eq!(server.calls(), 4);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn coverage_exposes_concrete_instances_without_listing_segments()
-> Result<(), Box<dyn std::error::Error>> {
    let mut server = TestServer::new(vec![Response::json(
        200,
        r#"{"intervals":[{"status":"available","from_ts_ms":0,"to_ts_ms":59999}],"instances":[{"market_id":"market-1","condition_id":"condition-1","outcome_tokens":[{"outcome":"up","token_id":"token-up"},{"outcome":"down","token_id":"token-down"}]}],"sealed_through_ms":59999}"#,
    )])?;
    let source = PmKitCloudDataSource::with_base_url(
        CloudApiKey::new("secret-value")?,
        &format!("{}/v1", server.url),
    )?;

    let coverage = source.coverage(query()).await?;
    assert_eq!(coverage.sealed_through_ms, Some(59_999));
    assert_eq!(coverage.instances.len(), 1);
    assert_eq!(coverage.instances[0].market_id, "market-1");
    assert_eq!(coverage.instances[0].outcome_tokens.len(), 2);
    assert_eq!(server.calls(), 1);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn historical_replay_emits_one_terminal_pair_for_multiple_markets()
-> Result<(), Box<dyn std::error::Error>> {
    let logical = br#"{"event_time_ms":1000,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[],"asks":[]}}
"#;
    let encoded = zstd::stream::encode_all(logical.as_slice(), 0)?;
    let logical_sha = digest(logical);
    let encoded_sha = digest(&encoded);
    let mut server = TestServer::new(vec![
        Response::json(200, available_coverage()),
        Response::json(
            200,
            &segment_page_for_market(
                "hot",
                "market-1",
                logical.len(),
                &logical_sha,
                encoded.len(),
                &encoded_sha,
                None,
            ),
        ),
        Response::bytes(200, encoded.clone(), &encoded_sha, &logical_sha),
        Response::json(200, available_coverage()),
        Response::json(
            200,
            &segment_page_for_market(
                "hot",
                "market-2",
                logical.len(),
                &logical_sha,
                encoded.len(),
                &encoded_sha,
                None,
            ),
        ),
        Response::bytes(200, encoded, &encoded_sha, &logical_sha),
    ])?;
    let source = PmKitCloudDataSource::with_base_url(
        CloudApiKey::new("secret-value")?,
        &format!("{}/v1", server.url),
    )?;
    let (tx, mut rx) = mpsc::channel(8);
    source
        .replay(
            ReplayQuery {
                markets: vec![MarketId::new("market-1")?, MarketId::new("market-2")?],
                from: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
                to: "1970-01-01T00:01:00Z".parse()?,
                evidence: EvidenceRequirement::AllowSingleSource,
                retrieval_wait: RetrievalWait::ReturnPending,
            },
            tx,
        )
        .await?;

    let mut events = 0;
    let mut terminal = Vec::new();
    while let Some(signal) = rx.recv().await {
        match signal {
            SourceSignal::Data(_) => events += 1,
            SourceSignal::Watermark(timestamp_ms) => {
                terminal.push(SourceSignal::Watermark(timestamp_ms));
            }
            SourceSignal::Eof => terminal.push(SourceSignal::Eof),
        }
    }
    assert_eq!(events, 2);
    assert_eq!(
        terminal,
        vec![SourceSignal::Watermark(60_000), SourceSignal::Eof]
    );
    assert_eq!(server.calls(), 6);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn pagination_uses_the_cursor_from_the_previous_page()
-> Result<(), Box<dyn std::error::Error>> {
    let logical = br#"{"event_time_ms":1000,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[],"asks":[]}}
"#;
    let encoded = zstd::stream::encode_all(logical.as_slice(), 0)?;
    let logical_sha = digest(logical);
    let encoded_sha = digest(&encoded);
    let mut server = TestServer::new(vec![
        Response::json(200, available_coverage()),
        Response::json(200, r#"{"next_cursor":"page-2","segments":[]}"#),
        Response::json(
            200,
            &segment_page(
                "hot",
                logical.len(),
                &logical_sha,
                encoded.len(),
                &encoded_sha,
                None,
            ),
        ),
        Response::bytes(200, encoded, &encoded_sha, &logical_sha),
    ])?;
    let source = PmKitCloudDataSource::with_base_url(
        CloudApiKey::new("secret-value")?,
        &format!("{}/v1", server.url),
    )?;
    let (tx, mut rx) = mpsc::channel(8);
    source.replay_cloud(query(), tx).await?;
    while rx.recv().await.is_some() {}
    assert!(
        server
            .requests()
            .iter()
            .any(|request| request.path.contains("cursor=page-2"))
    );
    server.join()?;
    Ok(())
}

fn available_coverage() -> &'static str {
    r#"{"coverage":"observed","intervals":[{"status":"available","from_ts_ms":0,"to_ts_ms":59999}],"instances":[],"sealed_through_ms":59999,"selector":{"kind":"series","seriesId":"btc-usd-5m"}}"#
}
fn query() -> CloudReplayQuery {
    CloudReplayQuery {
        selector: super::CloudReplaySelector::Series("btc-usd-5m".into()),
        from: "1970-01-01T00:00:00Z"
            .parse()
            .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH),
        to: "1970-01-01T00:01:00Z"
            .parse()
            .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH),
    }
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn segment_page(
    state: &str,
    logical_bytes: usize,
    logical_sha: &str,
    encoded_bytes: usize,
    encoded_sha: &str,
    next_cursor: Option<&str>,
) -> String {
    segment_page_for_market(
        state,
        "market-1",
        logical_bytes,
        logical_sha,
        encoded_bytes,
        encoded_sha,
        next_cursor,
    )
}

fn segment_page_for_market(
    state: &str,
    market_id: &str,
    logical_bytes: usize,
    logical_sha: &str,
    encoded_bytes: usize,
    encoded_sha: &str,
    next_cursor: Option<&str>,
) -> String {
    let release_id = format!("release-{market_id}");
    let segment_id = format!("segment-{market_id}");
    format!(
        r#"{{"next_cursor":{},"segments":[{{"bytes":{},"encoded_bytes":{},"encoded_sha256":"{}","from_ts_ms":1000,"release_id":"{}","rows":1,"segment_id":"{}","sha256":"{}","source_manifest_sha256":"{}","to_ts_ms":1000,"market_id":"{}","condition_id":"condition-1","series_id":"btc-usd-5m","asset":"BTC","duration_seconds":300,"outcome_tokens":[{{"outcome":"up","token_id":"token-up"}},{{"outcome":"down","token_id":"token-down"}}],"availability":{{"state":"{}"}}}}],"sealed_through_ms":59999,"selector":{{"kind":"series","seriesId":"btc-usd-5m"}}}}"#,
        next_cursor.map_or_else(|| "null".into(), |cursor| format!("\"{cursor}\"")),
        logical_bytes,
        encoded_bytes,
        encoded_sha,
        release_id,
        segment_id,
        logical_sha,
        "a".repeat(64),
        market_id,
        state,
    )
}
