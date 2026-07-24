use super::{DiscoveryError, GammaClient, GammaError, GammaMarket};
use pmkit_core::MarketId;
use polymarket_client_sdk_v2::gamma::Client as SdkGammaClient;
use rust_decimal::Decimal;
use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::str::FromStr as _;
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
            if !request.starts_with("GET /markets?") {
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
        outcome_prices: vec![Decimal::ONE, Decimal::ZERO],
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
    if market.resolution_price("a") != Some(Decimal::ONE) {
        return Err("expected binary market resolution price".into());
    }

    Ok(())
}

#[tokio::test]
async fn gamma_resolution_decimal() -> Result<(), Box<dyn std::error::Error>> {
    // Given a resolved Gamma market whose prices require exact decimal parsing.
    let client = fixture_client(
        r#"[{
            "id":"market-1",
            "question":"Will the market resolve up?",
            "slug":"market-1",
            "negRisk":false,
            "outcomes":"[\"Up\",\"Down\"]",
            "outcomePrices":"[\"0.123456789012345678\",\"0.876543210987654322\"]",
            "closed":true,
            "closedTime":"2026-07-24T12:34:56Z",
            "clobTokenIds":"[\"1\",\"2\"]",
            "events":[{"id":"event-1","slug":"event-1"}]
        }]"#,
    )?;

    // When the market is loaded through the public Gamma client seam.
    let result = client.market_by_token("1").await;

    // Then every price remains exact and token resolution follows the same ordering.
    let expected_up = Decimal::from_str("0.123456789012345678")?;
    let expected_down = Decimal::from_str("0.876543210987654322")?;
    match result {
        Ok(Some(market)) => {
            assert_eq!(market.outcome_prices, vec![expected_up, expected_down]);
            assert_eq!(market.resolution_price("1"), Some(expected_up));
        }
        Ok(None) => return Err("expected fixture market".into()),
        Err(error) => return Err(format!("expected exact decimal prices, got {error:?}").into()),
    }

    Ok(())
}

#[tokio::test]
async fn gamma_malformed_price_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    // Given a Gamma payload containing one malformed price among valid market fields.
    let client = fixture_client(
        r#"[{
            "id":"market-1",
            "question":"Will the market resolve up?",
            "slug":"market-1",
            "negRisk":false,
            "outcomes":"[\"Up\",\"Down\"]",
            "outcomePrices":"[\"1\",\"not-a-decimal\"]",
            "closed":true,
            "closedTime":"2026-07-24T12:34:56Z",
            "clobTokenIds":"[\"1\",\"2\"]",
            "events":[{"id":"event-1","slug":"event-1"}]
        }]"#,
    )?;

    // When the market is loaded through the public Gamma client seam.
    let result = client.market_by_token("1").await;

    // Then decoding fails as a typed Gamma error instead of dropping the bad price.
    match result {
        Err(GammaError::Request { .. }) => {}
        Ok(_) => return Err("expected malformed outcome price to fail closed".into()),
        Err(error) => return Err(format!("expected request decoding error, got {error:?}").into()),
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
