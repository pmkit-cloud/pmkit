use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;

use super::cloud_test_support::{Response, TestServer};
use super::{CloudApiKey, CloudReplayError, CloudReplayQuery, PmKitCloudDataSource};

#[test]
fn api_key_and_source_debug_are_redacted() -> Result<(), Box<dyn std::error::Error>> {
    let key = CloudApiKey::new("secret-value")?;
    let source = PmKitCloudDataSource::new(key.clone())?;
    assert!(!format!("{key:?} {key} {source:?}").contains("secret-value"));
    Ok(())
}

#[tokio::test]
async fn known_gap_fails_before_segment_listing() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = TestServer::new(vec![Response::json(
        200,
        r#"{"coverage":"observed","intervals":[{"status":"known_gap","from_ts_ms":0,"to_ts_ms":59999}],"instances":[],"sealed_through_ms":null,"selector":{"kind":"series","seriesId":"btc-usd-5m"}}"#,
    )])?;
    let source = source(&server)?;
    let (tx, _rx) = mpsc::channel(8);
    assert_eq!(
        source.replay_cloud(query(), tx).await,
        Err(CloudReplayError::KnownGap)
    );
    assert_eq!(server.calls(), 1);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn empty_coverage_fails_before_segment_listing() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = TestServer::new(vec![Response::json(
        200,
        r#"{"coverage":"observed","intervals":[],"instances":[],"sealed_through_ms":59999,"selector":{"kind":"series","seriesId":"btc-usd-5m"}}"#,
    )])?;
    let (tx, _rx) = mpsc::channel(8);
    assert_eq!(
        source(&server)?.replay_cloud(query(), tx).await,
        Err(CloudReplayError::KnownGap)
    );
    assert_eq!(server.calls(), 1);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn empty_segment_listing_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = TestServer::new(vec![
        Response::json(200, available_coverage()),
        Response::json(200, r#"{"next_cursor":null,"segments":[]}"#),
    ])?;
    let (tx, _rx) = mpsc::channel(1);
    assert_eq!(
        source(&server)?.replay_cloud(query(), tx).await,
        Err(CloudReplayError::MalformedResponse)
    );
    assert_eq!(server.calls(), 2);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn public_http_failures_are_typed_without_leaking_the_key()
-> Result<(), Box<dyn std::error::Error>> {
    let cases = [
        (401, CloudReplayError::Unauthorized),
        (403, CloudReplayError::Forbidden),
        (
            409,
            CloudReplayError::RetrievalRequired {
                state: super::RetrievalState::RestoreRequired,
            },
        ),
        (429, CloudReplayError::QuotaExceeded),
        (503, CloudReplayError::ServiceUnavailable),
    ];
    let mut server = TestServer::new(
        cases
            .iter()
            .map(|(status, _)| Response::json(*status, r#"{"error":"fixture"}"#))
            .collect(),
    )?;
    let source = source(&server)?;
    for (_, expected) in cases {
        let (tx, _rx) = mpsc::channel(1);
        let error = source.replay_cloud(query(), tx).await;
        assert_eq!(error, Err(expected));
        assert!(!format!("{error:?}").contains("secret-value"));
    }
    assert_eq!(server.calls(), 5);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn cold_state_is_returned_without_implicit_restore() -> Result<(), Box<dyn std::error::Error>>
{
    let page = segment_page("restore_required", 1, &"a".repeat(64), 1, &"b".repeat(64));
    let mut server = TestServer::new(vec![
        Response::json(200, available_coverage()),
        Response::json(200, &page),
    ])?;
    let source = source(&server)?;
    let (tx, _rx) = mpsc::channel(1);
    assert_eq!(
        source.replay_cloud(query(), tx).await,
        Err(CloudReplayError::RetrievalRequired {
            state: super::RetrievalState::RestoreRequired
        })
    );
    assert_eq!(server.calls(), 2);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn encoded_and_logical_corruption_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let logical = br#"{"event_time_ms":1000,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[],"asks":[]}}
"#;
    let encoded = zstd::stream::encode_all(logical.as_slice(), 0)?;
    let logical_sha = digest(logical);
    let encoded_sha = digest(&encoded);
    for (declared_encoded, declared_logical) in [
        (encoded_sha.clone(), "f".repeat(64)),
        ("e".repeat(64), logical_sha.clone()),
    ] {
        let page = segment_page(
            "hot",
            logical.len(),
            &declared_logical,
            encoded.len(),
            &declared_encoded,
        );
        let mut server = TestServer::new(vec![
            Response::json(200, available_coverage()),
            Response::json(200, &page),
            Response::bytes(200, encoded.clone(), &encoded_sha, &logical_sha),
        ])?;
        let (tx, _rx) = mpsc::channel(1);
        assert_eq!(
            source(&server)?.replay_cloud(query(), tx).await,
            Err(CloudReplayError::IntegrityMismatch)
        );
        server.join()?;
    }
    Ok(())
}

#[tokio::test]
async fn corrupt_download_bodies_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let logical = br#"{"event_time_ms":1000,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[],"asks":[]}}
"#;
    let encoded = zstd::stream::encode_all(logical.as_slice(), 0)?;
    let logical_sha = digest(logical);
    let encoded_sha = digest(&encoded);
    let mut encoded_corrupt = encoded.clone();
    encoded_corrupt[0] ^= 1;
    let logical_corrupt = zstd::stream::encode_all(
        br#"{"event_time_ms":1001,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[],"asks":[]}}
"#
        .as_slice(),
        0,
    )?;
    for (body, body_sha) in [
        (encoded_corrupt, encoded_sha.clone()),
        (logical_corrupt, String::new()),
    ] {
        let body_sha = if body_sha.is_empty() {
            digest(&body)
        } else {
            body_sha
        };
        let page = segment_page("hot", logical.len(), &logical_sha, body.len(), &body_sha);
        let mut server = TestServer::new(vec![
            Response::json(200, available_coverage()),
            Response::json(200, &page),
            Response::bytes(200, body, &body_sha, &logical_sha),
        ])?;
        let (tx, _rx) = mpsc::channel(1);
        assert_eq!(
            source(&server)?.replay_cloud(query(), tx).await,
            Err(CloudReplayError::IntegrityMismatch)
        );
        server.join()?;
    }
    Ok(())
}

#[tokio::test]
async fn invalid_query_fails_before_coverage() -> Result<(), Box<dyn std::error::Error>> {
    let mut server = TestServer::new(vec![])?;
    let source = source(&server)?;
    let mut invalid = query();
    invalid.to = invalid.from;
    let (tx, _rx) = mpsc::channel(1);
    assert_eq!(
        source.replay_cloud(invalid, tx).await,
        Err(CloudReplayError::InvalidQuery)
    );
    assert_eq!(server.calls(), 0);
    server.join()?;
    assert!(matches!(
        CloudApiKey::new("  "),
        Err(CloudReplayError::InvalidConfiguration)
    ));
    Ok(())
}

#[tokio::test]
async fn uncovered_interval_fails_before_segment_listing() -> Result<(), Box<dyn std::error::Error>>
{
    let mut server = TestServer::new(vec![Response::json(
        200,
        r#"{"coverage":"observed","intervals":[{"status":"available","from_ts_ms":0,"to_ts_ms":1000},{"status":"available","from_ts_ms":2000,"to_ts_ms":59999}],"instances":[],"sealed_through_ms":59999,"selector":{"kind":"series","seriesId":"btc-usd-5m"}}"#,
    )])?;
    let (tx, _rx) = mpsc::channel(1);
    assert_eq!(
        source(&server)?.replay_cloud(query(), tx).await,
        Err(CloudReplayError::KnownGap)
    );
    assert_eq!(server.calls(), 1);
    server.join()?;
    Ok(())
}

#[tokio::test]
async fn out_of_segment_rows_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    let logical = br#"{"event_time_ms":2000,"event_ordinal":7,"payload":{"event_type":"book","asset_id":"token-up","bids":[],"asks":[]}}
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
    );
    let mut server = TestServer::new(vec![
        Response::json(200, available_coverage()),
        Response::json(200, &page),
        Response::bytes(200, encoded, &encoded_sha, &logical_sha),
    ])?;
    let (tx, _rx) = mpsc::channel(1);
    assert_eq!(
        source(&server)?.replay_cloud(query(), tx).await,
        Err(CloudReplayError::MalformedResponse)
    );
    assert_eq!(server.calls(), 3);
    server.join()?;
    Ok(())
}

fn source(server: &TestServer) -> Result<PmKitCloudDataSource, CloudReplayError> {
    PmKitCloudDataSource::with_base_url(
        CloudApiKey::new("secret-value")?,
        &format!("{}/v1", server.url),
    )
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
) -> String {
    format!(
        r#"{{"next_cursor":null,"segments":[{{"bytes":{},"encoded_bytes":{},"encoded_sha256":"{}","from_ts_ms":1000,"release_id":"release-1","rows":1,"segment_id":"segment-1","sha256":"{}","source_manifest_sha256":"{}","to_ts_ms":1000,"market_id":"market-1","condition_id":"condition-1","series_id":"btc-usd-5m","asset":"BTC","duration_seconds":300,"outcome_tokens":[{{"outcome":"up","token_id":"token-up"}},{{"outcome":"down","token_id":"token-down"}}],"availability":{{"state":"{}"}}}}],"sealed_through_ms":59999,"selector":{{"kind":"series","seriesId":"btc-usd-5m"}}}}"#,
        logical_bytes,
        encoded_bytes,
        encoded_sha,
        logical_sha,
        "a".repeat(64),
        state
    )
}
