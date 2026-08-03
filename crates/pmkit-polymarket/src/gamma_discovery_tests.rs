use std::{
    collections::BTreeMap,
    io::{Read as _, Write as _},
    net::TcpListener,
    sync::{Arc, Mutex},
    thread,
};

use crate::{DiscoveryError, GammaDiscovery};
use polymarket_client_sdk_v2::gamma::Client as SdkGammaClient;

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
