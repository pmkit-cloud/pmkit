use std::{
    error::Error,
    io::{Read as _, Write as _},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use chrono::{DateTime, Utc};
use pmkit_core::MarketId;
use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};
use pmkit_event::{MarketEvent, SourceEnvelope};
use pmkit_market::Outcome;
use pmkit_run::{EvidenceRequirement, RetrievalWait};
use rust_decimal::Decimal;

use super::{
    OrbscanClient, OrbscanHistoricalDataSource, OrbscanMarket, OrbscanMarketStatus, OrbscanOutcome,
};

type CapturedRequests = Arc<Mutex<Vec<String>>>;

#[tokio::test]
async fn discovery_pages_markets_and_keeps_coverage_advisory() -> Result<(), Box<dyn Error>> {
    let (base_url, requests) = fixture_server(vec![
        success_page(&market_json("123", "11", "12"), Some("page-2")),
        success_page(&market_json("124", "21", "22"), None),
        r#"{"message":"OK","status_code":"1","data":{"polymarket":{"orderbook":{"availableFrom":10,"availableTo":20}}}}"#.to_owned(),
    ])?;
    let client = OrbscanClient::with_endpoint("fixture-key", base_url)?;

    let markets = client
        .markets()
        .await
        .map_err(|error| format!("markets: {error}"))?;
    let coverage = client
        .coverage()
        .await
        .map_err(|error| format!("coverage: {error}"))?;
    assert_eq!(markets.len(), 2);
    assert_eq!(markets[0].status, OrbscanMarketStatus::Active);
    assert_eq!(coverage.available_from_ms, 10);
    assert_eq!(coverage.available_to_ms, 20);
    let mapping = markets[0].map_to(MarketId::new("btc-5m")?, "Yes", "No")?;
    assert_eq!(mapping.up.token_id, "11");
    assert_eq!(mapping.down.token_id, "12");

    let captured = requests.lock().map_err(|_| "request capture poisoned")?;
    assert!(captured[0].contains("/v1/orderbook/markets?limit=1000"));
    assert!(captured[1].contains("cursor=page-2"));
    assert!(captured[2].contains("/v1/orderbook/coverage"));
    for request in captured.iter() {
        let request_line = request.lines().next().ok_or("missing request line")?;
        assert!(request_line.starts_with("GET "));
        assert!(!request_line.contains("fixture-key"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-key")
        );
    }
    drop(captured);
    Ok(())
}

#[tokio::test]
async fn market_page_requires_its_continuation_field() -> Result<(), Box<dyn Error>> {
    let (base_url, _) = fixture_server(vec![
        r#"{"message":"OK","status_code":"1","data":{"items":[]}}"#.to_owned(),
    ])?;
    let client = OrbscanClient::with_endpoint("fixture-key", base_url)?;
    assert!(matches!(
        client.markets().await,
        Err(super::OrbscanError::MalformedResponse)
    ));
    Ok(())
}

#[tokio::test]
async fn market_pagination_rejects_a_repeated_cursor() -> Result<(), Box<dyn Error>> {
    let (base_url, _) = fixture_server(vec![
        success_page(&market_json("123", "11", "12"), Some("repeat")),
        success_page(&market_json("124", "21", "22"), Some("repeat")),
    ])?;
    let client = OrbscanClient::with_endpoint("fixture-key", base_url)?;
    assert!(matches!(
        client.markets().await,
        Err(super::OrbscanError::RepeatedCursor)
    ));
    Ok(())
}

#[tokio::test]
async fn replay_reconstructs_books_and_streams_ordered_watermarks() -> Result<(), Box<dyn Error>> {
    let up_snapshot = book_event(
        "up-snapshot",
        "11",
        "Yes",
        5,
        r#"[["0.3","1"],["0.5","2"]]"#,
        r#"[["0.8","4"],["0.7","5"],["0.6","3"]]"#,
    );
    let down_snapshot = book_event(
        "down-snapshot",
        "12",
        "No",
        7,
        r#"[["0.45","5"]]"#,
        r#"[["0.55","4"]]"#,
    );
    let pre_window = delta_event("up-change-1", "11", "Yes", 8, "BUY", "0.4", "3");
    let in_window = delta_event("up-change-2", "11", "Yes", 12, "SELL", "0.7", "0");
    let (base_url, requests) = fixture_server(vec![
        success_page(&up_snapshot, None),
        success_page(&down_snapshot, None),
        success_page(&pre_window, Some("up-page-2")),
        success_page(&in_window, None),
        success_page("", None),
    ])?;
    let client = OrbscanClient::with_endpoint("fixture-key", base_url)?;
    let source = OrbscanHistoricalDataSource::new(client, [mapping()?])?;
    let replay_query = query(EvidenceRequirement::AllowSingleSource)?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let replay = tokio::spawn(async move { source.replay(replay_query, tx).await });

    let (books, watermarks, saw_eof) = receive_replay(rx).await?;
    replay.await??;
    assert_reconstructed_books(&books, &watermarks, saw_eof);
    let captured = requests.lock().map_err(|_| "request capture poisoned")?;
    assert_replay_requests(captured.as_slice());
    drop(captured);
    Ok(())
}

struct RecordedBook {
    outcome: Outcome,
    timestamp_ms: i64,
    bids: Vec<(Decimal, Decimal)>,
    asks: Vec<(Decimal, Decimal)>,
    source_id: String,
    raw_frame: Vec<u8>,
}

async fn receive_replay(
    mut rx: tokio::sync::mpsc::Receiver<SourceSignal>,
) -> Result<(Vec<RecordedBook>, Vec<i64>, bool), Box<dyn Error>> {
    let mut books = Vec::new();
    let mut watermarks = Vec::new();
    let mut saw_eof = false;
    while let Some(signal) = rx.recv().await {
        match signal {
            SourceSignal::Data(envelope) => {
                let SourceEnvelope::PmMarket(envelope) = *envelope else {
                    return Err("expected Orbscan PM market envelope".into());
                };
                let MarketEvent::BookUpdate {
                    outcome,
                    bids,
                    asks,
                    timestamp_ms,
                    ..
                } = envelope.fact
                else {
                    return Err("expected reconstructed book update".into());
                };
                books.push(RecordedBook {
                    outcome,
                    timestamp_ms,
                    bids,
                    asks,
                    source_id: envelope.metadata.source_id,
                    raw_frame: envelope.raw_frame,
                });
            }
            SourceSignal::Watermark(timestamp_ms) => watermarks.push(timestamp_ms),
            SourceSignal::Eof => {
                saw_eof = true;
                break;
            }
        }
    }
    Ok((books, watermarks, saw_eof))
}

fn assert_reconstructed_books(books: &[RecordedBook], watermarks: &[i64], saw_eof: bool) {
    assert_eq!(books.len(), 3);
    assert_eq!(
        (books[0].outcome, books[0].timestamp_ms),
        (Outcome::Down, 10)
    );
    assert_eq!((books[1].outcome, books[1].timestamp_ms), (Outcome::Up, 10));
    assert_eq!((books[2].outcome, books[2].timestamp_ms), (Outcome::Up, 12));
    assert_eq!(books[1].bids[0], (Decimal::new(5, 1), Decimal::from(2)));
    assert_eq!(books[1].bids[1], (Decimal::new(4, 1), Decimal::from(3)));
    assert_eq!(books[1].asks[0], (Decimal::new(6, 1), Decimal::from(3)));
    assert_eq!(
        books[2].asks,
        vec![
            (Decimal::new(6, 1), Decimal::from(3)),
            (Decimal::new(8, 1), Decimal::from(4))
        ]
    );
    assert!(
        books
            .iter()
            .all(|book| book.source_id == "orbscan-orderbook")
    );
    assert!(books[0].raw_frame.is_empty() && books[1].raw_frame.is_empty());
    assert!(!books[2].raw_frame.is_empty());
    assert_eq!(watermarks, [9, 11, 15]);
    assert!(saw_eof);
}

fn assert_replay_requests(requests: &[String]) {
    let event_requests = requests
        .iter()
        .filter(|request| request.contains("/v1/orderbook/events"))
        .collect::<Vec<_>>();
    assert_eq!(event_requests.len(), 5);
    assert!(
        event_requests[..2]
            .iter()
            .all(|request| request.contains("to=10"))
    );
    assert!(
        event_requests[..2]
            .iter()
            .all(|request| request.contains("limit=1"))
    );
    let delta_requests = event_requests[2..]
        .iter()
        .filter(|request| request.contains("eventType=price_change"))
        .collect::<Vec<_>>();
    assert_eq!(delta_requests.len(), 3);
    assert!(
        delta_requests
            .iter()
            .all(|request| request.contains("to=14"))
    );
    assert!(
        delta_requests
            .iter()
            .all(|request| request.contains("limit=1000"))
    );
    assert!(delta_requests[0].contains("cursor=up-snapshot"));
    assert!(delta_requests[1].contains("cursor=up-page-2"));
}

#[test]
fn exact_decimal_parsing_rejects_underflow_without_deleting_a_level() -> Result<(), Box<dyn Error>>
{
    let price = Decimal::new(5, 1);
    let mut book = super::BookLevels::default();
    book.bids.insert(price, Decimal::ONE);
    let event: super::OrbscanEvent = serde_json::from_str(&delta_event(
        "tiny-size",
        "11",
        "Yes",
        12,
        "BUY",
        "0.5",
        "0.00000000000000000000000000001",
    ))?;

    assert!(matches!(
        book.apply(&event),
        Err(super::OrbscanError::MalformedEvent("invalid decimal"))
    ));
    assert_eq!(book.bids.get(&price), Some(&Decimal::ONE));
    Ok(())
}

#[tokio::test]
async fn empty_window_emits_watermark_and_exactly_one_eof() -> Result<(), Box<dyn Error>> {
    let client = OrbscanClient::with_endpoint("fixture-key", "http://127.0.0.1:1")?;
    let source = OrbscanHistoricalDataSource::new(client, [mapping()?])?;
    let mut empty_query = query(EvidenceRequirement::AllowSingleSource)?;
    let boundary = empty_query.to;
    empty_query.from = boundary;
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);

    assert!(source.replay(empty_query, tx).await.is_ok());
    assert!(matches!(rx.recv().await, Some(SourceSignal::Watermark(15))));
    assert!(matches!(rx.recv().await, Some(SourceSignal::Eof)));
    assert!(rx.recv().await.is_none());
    Ok(())
}

#[tokio::test]
async fn replay_cancels_before_initialization_when_the_sink_is_closed() -> Result<(), Box<dyn Error>>
{
    let client = OrbscanClient::with_endpoint("fixture-key", "http://127.0.0.1:1")?;
    let source = OrbscanHistoricalDataSource::new(client, [mapping()?])?;
    let query = query(EvidenceRequirement::AllowSingleSource)?;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    drop(rx);

    let result = source.replay(query, tx).await;
    assert!(matches!(result, Err(DataSourceError::SinkClosed)));
    Ok(())
}

#[tokio::test]
async fn replay_fails_closed_for_unapproved_evidence_and_invalid_snapshots()
-> Result<(), Box<dyn Error>> {
    let client = OrbscanClient::with_endpoint("fixture-key", "http://127.0.0.1:1")?;
    let source = OrbscanHistoricalDataSource::new(client, [mapping()?])?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let result = source
        .replay(query(EvidenceRequirement::CorroboratedOnly)?, tx)
        .await;
    assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));
    assert!(rx.recv().await.is_none());

    let (base_url, _) = fixture_server(vec![success_page("", None)])?;
    let client = OrbscanClient::with_endpoint("fixture-key", base_url)?;
    let source = OrbscanHistoricalDataSource::new(client, [mapping()?])?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let result = source
        .replay(query(EvidenceRequirement::AllowSingleSource)?, tx)
        .await;
    assert!(matches!(result, Err(DataSourceError::NotAvailable)));
    assert!(rx.recv().await.is_none());

    let malformed = book_event(
        "bad-snapshot",
        "11",
        "Yes",
        5,
        r#"[["0.3","1"]]"#,
        r#"[["0.8","1"]]"#,
    )
    .replace("\"side\":null", "\"side\":\"BUY\"");
    let (base_url, _) = fixture_server(vec![success_page(&malformed, None)])?;
    let client = OrbscanClient::with_endpoint("fixture-key", base_url)?;
    let source = OrbscanHistoricalDataSource::new(client, [mapping()?])?;
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let result = source
        .replay(query(EvidenceRequirement::AllowSingleSource)?, tx)
        .await;
    assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));
    assert!(rx.recv().await.is_none());
    Ok(())
}

fn mapping() -> Result<super::OrbscanMarketMapping, Box<dyn Error>> {
    let market = OrbscanMarket {
        market_id: "123".to_owned(),
        condition_id: format!("0x{}", "a".repeat(64)),
        question: "Will BTC go up?".to_owned(),
        slug: "btc-up".to_owned(),
        status: OrbscanMarketStatus::Active,
        open_at: 0,
        close_at: 100,
        outcomes: vec![
            OrbscanOutcome {
                name: "Yes".to_owned(),
                token_id: "11".to_owned(),
            },
            OrbscanOutcome {
                name: "No".to_owned(),
                token_id: "12".to_owned(),
            },
        ],
    };
    Ok(market.map_to(MarketId::new("btc-5m")?, "Yes", "No")?)
}

fn query(evidence: EvidenceRequirement) -> Result<ReplayQuery, Box<dyn Error>> {
    Ok(ReplayQuery {
        markets: vec![MarketId::new("btc-5m")?],
        from: DateTime::<Utc>::from_timestamp_millis(10).ok_or("invalid from timestamp")?,
        to: DateTime::<Utc>::from_timestamp_millis(15).ok_or("invalid to timestamp")?,
        evidence,
        retrieval_wait: RetrievalWait::ReturnPending,
    })
}

fn market_json(market_id: &str, up_token: &str, down_token: &str) -> String {
    format!(
        r#"{{"marketId":"{market_id}","conditionId":"0x{}","question":"BTC?","slug":"btc","status":"active","openAt":0,"closeAt":100,"outcomes":[{{"name":"Yes","tokenId":"{up_token}"}},{{"name":"No","tokenId":"{down_token}"}}]}}"#,
        "a".repeat(64),
    )
}

fn book_event(
    cursor: &str,
    token_id: &str,
    outcome: &str,
    timestamp: i64,
    bids: &str,
    asks: &str,
) -> String {
    format!(
        r#"{{"cursor":"{cursor}","marketId":"123","conditionId":"0x{}","tokenId":"{token_id}","outcome":"{outcome}","eventType":"book","timestamp":{timestamp},"indexedTimestamp":{timestamp},"side":null,"price":null,"sizeAfter":null,"bids":{bids},"asks":{asks},"sourceHash":"fixture-hash"}}"#,
        "a".repeat(64),
    )
}

fn delta_event(
    cursor: &str,
    token_id: &str,
    outcome: &str,
    timestamp: i64,
    side: &str,
    price: &str,
    size_after: &str,
) -> String {
    format!(
        r#"{{"cursor":"{cursor}","marketId":"123","conditionId":"0x{}","tokenId":"{token_id}","outcome":"{outcome}","eventType":"price_change","timestamp":{timestamp},"indexedTimestamp":{timestamp},"side":"{side}","price":"{price}","sizeAfter":"{size_after}","bids":null,"asks":null,"sourceHash":"fixture-hash"}}"#,
        "a".repeat(64),
    )
}

fn success_page(item: &str, next_cursor: Option<&str>) -> String {
    let next = next_cursor.map_or_else(|| "null".to_owned(), |cursor| format!("\"{cursor}\""));
    format!(
        r#"{{"message":"OK","status_code":"1","data":{{"items":[{item}],"nextCursor":{next}}}}}"#
    )
}

fn fixture_server(responses: Vec<String>) -> Result<(String, CapturedRequests), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    thread::spawn(move || {
        for body in responses {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = [0_u8; 8192];
            let Ok(read) = stream.read(&mut request) else {
                return;
            };
            if let Ok(mut requests) = captured.lock() {
                requests.push(String::from_utf8_lossy(&request[..read]).into_owned());
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            if stream.write_all(response.as_bytes()).is_err() || stream.flush().is_err() {
                return;
            }
        }
    });
    Ok((format!("http://{address}"), requests))
}
