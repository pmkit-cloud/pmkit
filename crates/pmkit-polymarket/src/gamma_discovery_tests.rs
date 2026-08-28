use std::{
    collections::BTreeMap,
    io::{Read as _, Write as _},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use crate::{DiscoveryError, GammaDiscovery, RecurringFamily};
use polymarket_client_sdk_v2::gamma::Client as SdkGammaClient;

type CapturedRequests = Arc<Mutex<Vec<String>>>;

#[tokio::test]
async fn gamma_discovery_uses_fixed_limit_offset_pages() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a Gamma server with two full pages and a short final page.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    thread::spawn(move || {
        for body in [gamma_page(0, 2), gamma_page(2, 2), gamma_page(4, 1)] {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = [0_u8; 4096];
            let Ok(read) = stream.read(&mut request) else {
                return;
            };
            if let Ok(mut requests) = captured.lock() {
                requests.push(String::from_utf8_lossy(&request[..read]).into_owned());
            }
            let reply = response(&body);
            if stream.write_all(reply.as_bytes()).is_err() || stream.flush().is_err() {
                return;
            }
        }
    });
    let discovery = GammaDiscovery::with_client(SdkGammaClient::new(&format!("http://{address}"))?);

    // When: the adapter requests a complete snapshot.
    let families = BTreeMap::default();
    let snapshot = discovery.snapshot(2, 2, &families).await?;

    // Then: Gamma receives the actual fixed limit/offset progression through the short page.
    assert_eq!(snapshot.markets().len(), 5);
    let guard = requests.lock().map_err(|_| "request capture poisoned")?;
    assert!(guard[0].contains("limit=2") && guard[0].contains("offset=0"));
    assert!(guard[1].contains("limit=2") && guard[1].contains("offset=2"));
    assert!(guard[2].contains("limit=2") && guard[2].contains("offset=4"));
    drop(guard);
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_rejects_nonzero_transport_failure()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: Gamma serves one complete page then closes the next pagination request.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut request = [0_u8; 4096];
        if stream.read(&mut request).is_err() {
            return;
        }
        let reply = response(&gamma_page(0, 2));
        if stream.write_all(reply.as_bytes()).is_err() || stream.flush().is_err() {
            return;
        }
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let _ = stream.read(&mut request);
    });
    let discovery = GammaDiscovery::with_client(SdkGammaClient::new(&format!("http://{address}"))?);

    // When: a later physical request fails after the first full page.
    let result = discovery.snapshot(2, 2, &BTreeMap::new()).await;

    // Then: the adapter exposes no partial snapshot and preserves the failed page offset.
    assert!(matches!(
        result,
        Err(DiscoveryError::IncompletePagination { offset: 2 })
    ));
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_rejects_initial_unavailability() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: a Gamma endpoint that cannot accept the first discovery request.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let discovery = GammaDiscovery::with_client(SdkGammaClient::new(&format!("http://{address}"))?);

    // When: discovery attempts its initial fixed-offset page.
    let result = discovery.snapshot(2, 2, &BTreeMap::new()).await;

    // Then: discovery reports unavailability and produces no snapshot.
    assert!(matches!(result, Err(DiscoveryError::Unavailable)));
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_rejects_nonzero_decode_failure() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: Gamma serves a full first page then malformed JSON at the next offset.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    thread::spawn(move || {
        let mut request = [0_u8; 4096];
        let Ok((mut first, _)) = listener.accept() else {
            return;
        };
        if first.read(&mut request).is_err() {
            return;
        }
        let first_reply = response(&gamma_page(0, 2));
        if first.write_all(first_reply.as_bytes()).is_err() || first.flush().is_err() {
            return;
        }
        let Ok((mut second, _)) = listener.accept() else {
            return;
        };
        if second.read(&mut request).is_err() {
            return;
        }
        let second_reply = response("{");
        let _ = second.write_all(second_reply.as_bytes());
        let _ = second.flush();
    });
    let discovery = GammaDiscovery::with_client(SdkGammaClient::new(&format!("http://{address}"))?);

    // When: the next fixed-offset page cannot decode.
    let result = discovery.snapshot(2, 2, &BTreeMap::new()).await;

    // Then: the complete snapshot is withheld at the failing offset.
    assert!(matches!(
        result,
        Err(DiscoveryError::IncompletePagination { offset: 2 })
    ));
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_derives_recurring_open_time_from_duration()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: Gamma's listing start differs from the configured recurring window.
    let discovery = one_page_discovery(gamma_page_with_window(
        "10192",
        "2025-01-01T00:00:00Z",
        "2026-01-01T00:15:00Z",
    ))?;
    let families = BTreeMap::from([(
        "10192".to_owned(),
        RecurringFamily::new("10192", Some("BTC"), Some("15m")),
    )]);

    // When: the mapped recurring family enters discovery.
    let snapshot = discovery.snapshot(2, 2, &families).await?;

    // Then: the opening instant is derived from the close, not Gamma's raw start.
    let market = &snapshot.markets()[0];
    assert_eq!(market.open_time_ms, market.close_time_ms - 900_000);
    assert_ne!(market.open_time_ms, 1_735_689_600_000_i64);
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_retains_raw_start_without_duration()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: Gamma reports an unknown family without configured duration.
    let discovery = one_page_discovery(gamma_page_with_window(
        "unknown-series",
        "2025-01-01T00:00:00Z",
        "2026-01-01T00:15:00Z",
    ))?;

    // When: discovery normalizes the market without structured duration.
    let snapshot = discovery.snapshot(2, 2, &BTreeMap::new()).await?;

    // Then: the raw Gamma start remains the fallback.
    assert_eq!(snapshot.markets()[0].open_time_ms, 1_735_689_600_000_i64);
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_rejects_invalid_configured_duration()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a mapped family with an invalid duration string.
    let discovery = one_page_discovery(gamma_page_with_window(
        "10192",
        "2025-01-01T00:00:00Z",
        "2026-01-01T00:15:00Z",
    ))?;
    let families = BTreeMap::from([(
        "10192".to_owned(),
        RecurringFamily::new("10192", Some("BTC"), Some("1h")),
    )]);

    // When: discovery parses the configured duration.
    let result = discovery.snapshot(2, 2, &families).await;

    // Then: malformed configuration fails closed.
    assert!(matches!(
        result,
        Err(DiscoveryError::MalformedPage {
            reason: "market family has invalid duration"
        })
    ));
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_rejects_missing_activity_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a Gamma market payload whose activity status is absent.
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut request = [0_u8; 4096];
        if stream.read(&mut request).is_err() {
            return;
        }
        let body = gamma_page(0, 1).replacen("\"active\":true,", "", 1);
        let reply = response(&body);
        let _ = stream.write_all(reply.as_bytes());
        let _ = stream.flush();
    });
    let discovery = GammaDiscovery::with_client(SdkGammaClient::new(&format!("http://{address}"))?);

    // When: the activity metadata crosses the discovery boundary.
    let result = discovery.snapshot(2, 2, &BTreeMap::new()).await;

    // Then: the malformed page cannot be published as an empty snapshot.
    assert!(matches!(result, Err(DiscoveryError::MalformedPage { .. })));
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_market_by_event_slug_uses_event_route_and_normalizes_market()
-> Result<(), Box<dyn std::error::Error>> {
    let market = gamma_page_with_window("10192", "2025-01-01T00:00:00Z", "2026-01-01T00:15:00Z");
    let (discovery, requests) = event_discovery(format!(
        r#"{{"id":"event-0","slug":"btc-up","markets":{market}}}"#
    ))?;
    let families = BTreeMap::from([(
        "10192".to_owned(),
        RecurringFamily::new("10192", Some("BTC"), Some("15m")),
    )]);

    let normalized = discovery.market_by_event_slug("btc-up", &families).await?;

    assert_eq!(normalized.market_id, "market-0");
    assert_eq!(normalized.condition_id, format!("0x{:064x}", 1));
    assert_eq!(normalized.open_time_ms, normalized.close_time_ms - 900_000);
    assert!(normalized.active);
    assert_eq!(normalized.outcomes.len(), 2);
    let requests = requests.lock().map_err(|_| "request capture poisoned")?;
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("GET /events/slug/btc-up HTTP/1.1"));
    drop(requests);
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_market_by_event_slug_rejects_blank_slug()
-> Result<(), Box<dyn std::error::Error>> {
    let discovery = GammaDiscovery::with_client(SdkGammaClient::new("http://127.0.0.1:1")?);

    let result = discovery.market_by_event_slug("  ", &BTreeMap::new()).await;

    assert!(matches!(
        result,
        Err(DiscoveryError::MalformedPage {
            reason: "event slug is blank"
        })
    ));
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_market_by_event_slug_requires_exactly_one_market()
-> Result<(), Box<dyn std::error::Error>> {
    let (empty, _) = event_discovery(r#"{"id":"event-0","markets":[]}"#.to_owned())?;
    let empty_result = empty.market_by_event_slug("target", &BTreeMap::new()).await;
    assert!(matches!(
        empty_result,
        Err(DiscoveryError::MalformedPage {
            reason: "event must contain exactly one market"
        })
    ));

    let market = gamma_page_with_window("10192", "2025-01-01T00:00:00Z", "2026-01-01T00:15:00Z");
    let market = &market[1..market.len() - 1];
    let (multiple, _) = event_discovery(format!(
        r#"{{"id":"event-0","markets":[{market},{market}]}}"#
    ))?;
    let multiple_result = multiple
        .market_by_event_slug("target", &BTreeMap::new())
        .await;
    assert!(matches!(
        multiple_result,
        Err(DiscoveryError::MalformedPage {
            reason: "event must contain exactly one market"
        })
    ));
    Ok(())
}

#[tokio::test]
async fn gamma_discovery_market_by_event_slug_rejects_malformed_market()
-> Result<(), Box<dyn std::error::Error>> {
    let market = gamma_page_with_window("10192", "2025-01-01T00:00:00Z", "2026-01-01T00:15:00Z")
        .replacen("\"active\":true,", "", 1);
    let (discovery, _) = event_discovery(format!(r#"{{"id":"event-0","markets":{market}}}"#))?;

    let result = discovery
        .market_by_event_slug("target", &BTreeMap::new())
        .await;

    assert!(matches!(result, Err(DiscoveryError::MalformedPage { .. })));
    Ok(())
}

fn event_discovery(
    body: String,
) -> Result<(GammaDiscovery, CapturedRequests), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut request = [0_u8; 4096];
        let Ok(read) = stream.read(&mut request) else {
            return;
        };
        if let Ok(mut requests) = captured.lock() {
            requests.push(String::from_utf8_lossy(&request[..read]).into_owned());
        }
        let reply = response(&body);
        let _ = stream.write_all(reply.as_bytes());
        let _ = stream.flush();
    });
    Ok((
        GammaDiscovery::with_client(SdkGammaClient::new(&format!("http://{address}"))?),
        requests,
    ))
}

fn one_page_discovery(body: String) -> Result<GammaDiscovery, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    thread::spawn(move || {
        let Ok((mut stream, _)) = listener.accept() else {
            return;
        };
        let mut request = [0_u8; 4096];
        if stream.read(&mut request).is_err() {
            return;
        }
        let reply = response(&body);
        let _ = stream.write_all(reply.as_bytes());
        let _ = stream.flush();
    });
    Ok(GammaDiscovery::with_client(SdkGammaClient::new(&format!(
        "http://{address}"
    ))?))
}

fn gamma_page_with_window(series_id: &str, start_date: &str, end_date: &str) -> String {
    format!(
        r#"[{{"id":"market-0","conditionId":"0x{:064x}","startDate":"{start_date}","endDate":"{end_date}","active":true,"closed":false,"outcomes":"[\"Up\",\"Down\"]","clobTokenIds":"[\"1\",\"2\"]","events":[{{"id":"event-0","series":[{{"id":"{series_id}"}}]}}]}}]"#,
        1,
    )
}

fn response(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn gamma_page(start: usize, count: usize) -> String {
    let markets = (start..start + count)
        .map(|index| format!(
            r#"{{"id":"market-{index}","conditionId":"0x{:064x}","startDate":"2026-01-01T00:00:00Z","endDate":"2026-01-01T00:05:00Z","active":true,"closed":false,"outcomes":"[\"Up\",\"Down\"]","clobTokenIds":"[\"{}\",\"{}\"]","events":[{{"id":"event-{index}","series":[{{"id":"btc-5m"}}]}}]}}"#,
            index + 1,
            index * 2 + 1,
            index * 2 + 2,
        ))
        .collect::<Vec<_>>();
    format!("[{}]", markets.join(","))
}
