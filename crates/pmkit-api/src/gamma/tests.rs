use super::{DiscoveryError, GammaClient, GammaMarket};
use pmkit_core::MarketId;
use polymarket_client_sdk_v2::gamma::Client as SdkGammaClient;
use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::thread;

fn fixture_client(body: &'static str) -> Result<GammaClient, Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let response = body.to_owned();

    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut request = [0_u8; 2048];
            let Ok(read) = stream.read(&mut request) else {
                return;
            };
            let request = String::from_utf8_lossy(&request[..read]);
            if !request.starts_with("GET /markets?closed=false HTTP/1.1") {
                return;
            }

            let content_length = response.len();
            let reply = format!(
                "HTTP/1.1 200 OK
content-type: application/json
content-length: {content_length}
connection: close

{response}"
            );
            let _ = stream.write_all(reply.as_bytes());
            let _ = stream.flush();
        }
    });

    Ok(GammaClient {
        client: SdkGammaClient::new(&format!("http://{address}"))?,
    })
}

#[test]
fn binary_market_helpers_use_token_order() -> Result<(), Box<dyn std::error::Error>> {
    let market = GammaMarket {
        event_id: "event".into(),
        end_date_iso: None,
        negative_risk: false,
        outcomes: vec!["Yes".into(), "No".into()],
        clob_token_ids: vec!["a".into(), "b".into()],
        closed: true,
        closed_time: Some(1),
        outcome_prices: vec![1.0, 0.0],
        title: "Question".into(),
        slug: "market".into(),
        event_slug: "event".into(),
    };

    if market.opposite_token("a") != Some("b") {
        return Err("expected opposite token in binary market".into());
    }
    if market.outcome_for_token("b") != Some("No") {
        return Err("expected binary market outcome lookup to follow token order".into());
    }
    if market.resolution_price("a") != Some(1.0) {
        return Err("expected binary market resolution price".into());
    }

    Ok(())
}

#[tokio::test]
async fn discovery_lists_markets() -> Result<(), Box<dyn std::error::Error>> {
    // Given an SDK-backed Gamma client pointed at a deterministic local fixture.
    let client = fixture_client(
        r#"[
            {"id":"market-1"},
            {"id":"market-2"}
        ]"#,
    )?;

    // When active market discovery is exercised through the public seam.
    let result: Result<Vec<MarketId>, DiscoveryError> = client.list_active_markets().await;

    // Then the returned market identifiers are exact and typed.
    let expected = vec![MarketId::new("market-1")?, MarketId::new("market-2")?];
    match result {
        Ok(market_ids) => assert_eq!(market_ids, expected),
        Err(err) => return Err(format!("expected active discovery to succeed, got {err:?}").into()),
    }

    Ok(())
}

#[tokio::test]
async fn discovery_malformed_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    // Given an SDK-backed Gamma client pointed at a deterministic local fixture.
    let client = fixture_client(
        r#"[
            {"id":"market-1"},
            {"question":"missing id"}
        ]"#,
    )?;

    // When discovery encounters a malformed market entry.
    let result: Result<Vec<MarketId>, DiscoveryError> = client.list_active_markets().await;

    // Then it fails closed instead of exposing a partial list.
    match result {
        Err(DiscoveryError::Listing { .. }) => {}
        Ok(_) => return Err("expected malformed fixture to fail closed".into()),
        Err(err) => return Err(format!("expected listing error, got {err:?}").into()),
    }

    Ok(())
}
