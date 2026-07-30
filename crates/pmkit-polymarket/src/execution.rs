use chrono::{DateTime, Utc};
use pmkit_exec::{
    ExecError, ExecutionSnapshot, Executor, OrderId, OrderStatus, OrderStatusDetails, PlaceOrder,
    TimeInForce,
};
use pmkit_market::Outcome;
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Normal, Signer};
use polymarket_client_sdk_v2::clob::Client;
use polymarket_client_sdk_v2::clob::types::request::{OrderBookSummaryRequest, OrdersRequest};
use polymarket_client_sdk_v2::clob::types::response::OpenOrderResponse;
use polymarket_client_sdk_v2::clob::types::{OrderStatusType, OrderType};
use polymarket_client_sdk_v2::error::{Error as SdkError, Kind, Status, StatusCode};
use rust_decimal::Decimal;

use crate::{MarketTokens, venue_order_inputs};

/// Polymarket enforces a one-minute security threshold on GTD expirations: an
/// order meant to stop resting at `t` must be sent with expiration `t + 60s`.
const GTD_SECURITY_BUFFER_MS: i64 = 60_000;

/// Builds the SDK expiration datetime for a GTD order expiring at
/// `expire_at_ms` (Unix epoch milliseconds).
///
/// The CLOB v2 backend expects the order `expiration` field in
/// **milliseconds**, but SDK `0.6.0-canary.1` serializes
/// `expiration.timestamp()` — whole seconds — verbatim, and the backend
/// rejects a seconds value with HTTP 400 "invalid expiration value". Until
/// the SDK is fixed, this forges a datetime whose seconds timestamp carries
/// the millisecond value. Bumping the pinned SDK requires re-verifying this
/// workaround first: an SDK that converts to milliseconds itself would
/// multiply this value by 1000 again, silently producing an order that
/// effectively never expires.
fn gtd_expiration(expire_at_ms: i64) -> Option<DateTime<Utc>> {
    let padded_ms = expire_at_ms.checked_add(GTD_SECURITY_BUFFER_MS)?;
    DateTime::from_timestamp(padded_ms, 0)
}

/// Authenticated Polymarket CLOB order executor.
#[derive(Debug)]
pub struct PolymarketExecutor<S> {
    client: Client<Authenticated<Normal>>,
    signer: S,
    tokens: MarketTokens,
    min_order_size: Decimal,
    tick_size: Decimal,
}

impl<S> PolymarketExecutor<S> {
    /// Creates an executor whose venue market limits are already known.
    ///
    /// Prefer [`Self::connect`], which reads both values from the venue
    /// itself. Polymarket publishes `orderMinSize` on market metadata (Gamma
    /// and CLOB), and the unit is **shares**, not a dollar notional: reading
    /// 5 shares as $5 and dividing by a $0.45 price turns the minimum into
    /// 12 shares, silently starving any strategy whose size cap sits between
    /// the two. `tick_size` is the market's price increment: valid prices
    /// are multiples of it inside `[tick, 1 - tick]`.
    #[must_use]
    pub const fn with_market_limits(
        client: Client<Authenticated<Normal>>,
        signer: S,
        tokens: MarketTokens,
        min_order_size: Decimal,
        tick_size: Decimal,
    ) -> Self {
        Self {
            client,
            signer,
            tokens,
            min_order_size,
            tick_size,
        }
    }

    /// Creates an executor, reading the venue market limits — minimum order
    /// size (shares) and price tick — from the market's own book metadata.
    ///
    /// The limits are not optional: no executor exists without them, so the
    /// guards can neither drift from the venue nor be forgotten and leave
    /// submissions falling back to the venue's opaque 400 rejection. Both
    /// values come from one CLOB book summary (`min_order_size` in shares,
    /// `tick_size`) of the Up outcome token; Polymarket publishes one set of
    /// limits per market, so both outcome tokens carry the same values.
    ///
    /// # Errors
    ///
    /// Returns the mapped [`ExecError`] when the book metadata cannot be
    /// fetched; construction fails loudly rather than running unguarded.
    pub async fn connect(
        client: Client<Authenticated<Normal>>,
        signer: S,
        tokens: MarketTokens,
    ) -> Result<Self, ExecError> {
        let request = OrderBookSummaryRequest::builder()
            .token_id(tokens.token(Outcome::Up))
            .build();
        let book = client
            .order_book(&request)
            .await
            .map_err(|error| exec_error(&error))?;
        Ok(Self {
            client,
            signer,
            tokens,
            min_order_size: book.min_order_size,
            tick_size: book.tick_size.as_decimal(),
        })
    }

    async fn snapshot(&self) -> Result<ExecutionSnapshot, ExecError>
    where
        S: Sync,
    {
        let request = OrdersRequest::default();
        let mut cursor = None;
        let mut open_orders = Vec::new();
        loop {
            let page = self
                .client
                .orders(&request, cursor)
                .await
                .map_err(|error| exec_error(&error))?;
            open_orders.extend(page.data.into_iter().map(|order| OrderId(order.id)));
            if page.next_cursor == "LTE=" {
                break;
            }
            cursor = Some(page.next_cursor);
        }
        open_orders.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        open_orders.dedup();
        Ok(ExecutionSnapshot { open_orders })
    }
}

#[async_trait::async_trait]
impl<S> Executor for PolymarketExecutor<S>
where
    S: Signer + Send + Sync,
{
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        self.snapshot().await
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        self.snapshot().await
    }

    async fn query_status(&self, order_id: &OrderId) -> Result<OrderStatus, ExecError> {
        let order = self
            .client
            .order(&order_id.0)
            .await
            .map_err(|error| query_error(&error, order_id))?;
        order_status(order_id, &order)
    }

    async fn submit(&self, order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        ensure_min_order_size(order, self.min_order_size)?;
        ensure_price_on_tick(order, self.tick_size)?;
        let inputs =
            venue_order_inputs(order, &self.tokens).ok_or_else(|| ExecError::Rejected {
                reason: format!(
                    "order market {} does not match token map {}",
                    order.market,
                    self.tokens.market()
                ),
            })?;
        let builder = self
            .client
            .limit_order()
            .token_id(inputs.token_id)
            .side(inputs.side)
            .price(inputs.price)
            .size(inputs.size)
            .post_only(inputs.post_only);
        let builder = match order.tif {
            TimeInForce::Gtc => builder.order_type(OrderType::GTC),
            TimeInForce::Gtd { expire_at_ms } => {
                let expiration =
                    gtd_expiration(expire_at_ms).ok_or_else(|| ExecError::Rejected {
                        reason: format!("GTD expiration out of range: {expire_at_ms}"),
                    })?;
                builder.order_type(OrderType::GTD).expiration(expiration)
            }
        };
        let response = builder
            .build_sign_and_post(&self.signer)
            .await
            .map_err(|error| exec_error(&error))?;

        if response.success && !response.order_id.is_empty() {
            Ok(OrderId(response.order_id))
        } else {
            Err(ExecError::Rejected {
                reason: response
                    .error_msg
                    .unwrap_or_else(|| "Polymarket rejected the order".to_owned()),
            })
        }
    }

    async fn cancel(&self, order_id: &OrderId) -> Result<(), ExecError> {
        let response = self
            .client
            .cancel_order(&order_id.0)
            .await
            .map_err(|error| exec_error(&error))?;
        if response.canceled.iter().any(|id| id == &order_id.0) {
            Ok(())
        } else {
            Err(ExecError::Rejected {
                reason: response
                    .not_canceled
                    .get(&order_id.0)
                    .cloned()
                    .unwrap_or_else(|| format!("Polymarket did not cancel order {}", order_id.0)),
            })
        }
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        let response = self
            .client
            .cancel_all_orders()
            .await
            .map_err(|error| exec_error(&error))?;
        if response.not_canceled.is_empty() {
            Ok(())
        } else {
            Err(ExecError::Rejected {
                reason: format!("Polymarket did not cancel: {:?}", response.not_canceled),
            })
        }
    }
}

/// Rejects a sub-minimum order locally with a typed error carrying the
/// shares semantics, instead of letting the venue answer with an opaque 400.
fn ensure_min_order_size(order: &PlaceOrder, min_order_size: Decimal) -> Result<(), ExecError> {
    if order.qty < min_order_size {
        return Err(ExecError::Rejected {
            reason: format!(
                "order size {} is below the venue minimum of {min_order_size} shares",
                order.qty
            ),
        });
    }
    Ok(())
}

/// Rejects a price off the venue tick grid locally with a typed error,
/// instead of letting the venue answer with an opaque 400. Valid Polymarket
/// prices are multiples of the market tick inside `[tick, 1 - tick]`.
fn ensure_price_on_tick(order: &PlaceOrder, tick_size: Decimal) -> Result<(), ExecError> {
    let max_price = Decimal::ONE - tick_size;
    if order.price < tick_size || order.price > max_price || !(order.price % tick_size).is_zero() {
        return Err(ExecError::Rejected {
            reason: format!(
                "price {} is off the venue tick grid: prices are multiples of {tick_size} within \
                 [{tick_size}, {max_price}]",
                order.price
            ),
        });
    }
    Ok(())
}

fn order_status(order_id: &OrderId, order: &OpenOrderResponse) -> Result<OrderStatus, ExecError> {
    let details = OrderStatusDetails {
        filled_qty: Some(order.size_matched),
        price: Some(order.price),
        fee: None,
        settlement_reference: None,
    };
    let status = match &order.status {
        OrderStatusType::Live | OrderStatusType::Delayed => {
            return Ok(OrderStatus::Open(details));
        }
        OrderStatusType::Matched => return Ok(OrderStatus::Accepted(details)),
        OrderStatusType::Canceled => return Ok(OrderStatus::Cancelled(details)),
        OrderStatusType::Unmatched => return Ok(OrderStatus::Rejected(details)),
        OrderStatusType::Unknown(status) => status.clone(),
        status => status.to_string(),
    };
    Err(ExecError::Transport {
        message: format!("ambiguous status {status} for order {}", order_id.0),
    })
}

fn query_error(error: &SdkError, order_id: &OrderId) -> ExecError {
    if error
        .downcast_ref::<Status>()
        .is_some_and(|status| status.status_code == StatusCode::NOT_FOUND)
    {
        ExecError::NotFound {
            order_id: order_id.0.clone(),
        }
    } else {
        exec_error(error)
    }
}

fn exec_error(error: &SdkError) -> ExecError {
    let message = error.to_string();
    match error.kind() {
        Kind::Status => {
            let definitive_rejection = error.downcast_ref::<Status>().is_some_and(|status| {
                status.status_code.is_client_error()
                    && status.status_code != StatusCode::REQUEST_TIMEOUT
                    && status.status_code != StatusCode::TOO_MANY_REQUESTS
            });
            if definitive_rejection {
                ExecError::Rejected { reason: message }
            } else {
                ExecError::Transport { message }
            }
        }
        Kind::Validation | Kind::Geoblock => ExecError::Rejected { reason: message },
        Kind::Synchronization | Kind::Internal | Kind::WebSocket => {
            ExecError::Transport { message }
        }
        _ => ExecError::Transport { message },
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::str::FromStr as _;
    use std::thread;

    use pmkit_book::Side;
    use pmkit_core::MarketId;
    use pmkit_exec::{ExecError, Executor, OrderId, OrderStatus, PlaceOrder, TimeInForce};
    use pmkit_market::Outcome;
    use polymarket_client_sdk_v2::POLYGON;
    use polymarket_client_sdk_v2::auth::state::Authenticated;
    use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Normal, Signer, Uuid};
    use polymarket_client_sdk_v2::clob::{Client, Config};
    use polymarket_client_sdk_v2::error::{Error as SdkError, Kind, Method, StatusCode};
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal::Decimal;

    use super::{
        GTD_SECURITY_BUFFER_MS, PolymarketExecutor, ensure_min_order_size, ensure_price_on_tick,
        exec_error, gtd_expiration,
    };
    use crate::MarketTokens;

    const PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    /// A CLOB book summary publishing a five-share venue minimum.
    const BOOK_SUMMARY: &str = r#"{
        "market":"0x0000000000000000000000000000000000000000000000000000000000000001",
        "asset_id":"1",
        "timestamp":"0",
        "bids":[],
        "asks":[],
        "min_order_size":"5",
        "neg_risk":false,
        "tick_size":"0.01"
    }"#;

    fn test_signer() -> Result<impl Signer + Send + Sync, Box<dyn std::error::Error>> {
        Ok(LocalSigner::from_str(PRIVATE_KEY)?.with_chain_id(Some(POLYGON)))
    }

    fn fixture_tokens() -> Result<MarketTokens, Box<dyn std::error::Error>> {
        Ok(MarketTokens::new(
            MarketId::new("fixture")?,
            U256::from(1),
            U256::from(2),
        ))
    }

    async fn authenticated_client<S: Signer + Sync>(
        address: std::net::SocketAddr,
        signer: &S,
    ) -> Result<Client<Authenticated<Normal>>, Box<dyn std::error::Error>> {
        let credentials = Credentials::new(
            Uuid::nil(),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned(),
            "fixture-passphrase".to_owned(),
        );
        Ok(
            Client::new(&format!("http://{address}"), Config::default())?
                .authentication_builder(signer)
                .credentials(credentials)
                .authenticate()
                .await?,
        )
    }

    async fn executor_at(
        address: std::net::SocketAddr,
    ) -> Result<impl Executor, Box<dyn std::error::Error>> {
        let signer = test_signer()?;
        let client = authenticated_client(address, &signer).await?;
        Ok(PolymarketExecutor::with_market_limits(
            client,
            signer,
            fixture_tokens()?,
            Decimal::ONE,
            Decimal::new(1, 2),
        ))
    }

    async fn fixture_executor(body: String) -> Result<impl Executor, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut request = [0_u8; 4096];
            let Ok(read) = stream.read(&mut request) else {
                return;
            };
            if !String::from_utf8_lossy(&request[..read]).starts_with("GET /data/order/order-1 ") {
                return;
            }
            let reply = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(reply.as_bytes());
            let _ = stream.flush();
        });

        executor_at(address).await
    }

    fn http_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// Reads one HTTP request (request line + headers + content-length body).
    fn read_http_request(stream: &mut std::net::TcpStream) -> Option<(String, String)> {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut chunk).ok()?;
            if read == 0 {
                return None;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
        let request_line = headers.lines().next()?.to_owned();
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())?
            })
            .unwrap_or(0);
        while buffer.len() < header_end + content_length {
            let read = stream.read(&mut chunk).ok()?;
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
        }
        let body = String::from_utf8_lossy(&buffer[header_end..]).to_string();
        Some((request_line, body))
    }

    /// Serves every endpoint the adapter touches (`version`, `tick-size`,
    /// `neg-risk` for `build_sign_and_post`, `book` for the minimum-size
    /// lookup) and captures the body posted to `/order`.
    fn spawn_order_capture_server()
    -> Result<(std::net::SocketAddr, std::sync::mpsc::Receiver<String>), Box<dyn std::error::Error>>
    {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let (body_tx, body_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else {
                    break;
                };
                let Some((request_line, body)) = read_http_request(&mut stream) else {
                    continue;
                };
                let reply = if request_line.starts_with("GET /version") {
                    http_response(r#"{"version":2}"#)
                } else if request_line.starts_with("GET /tick-size") {
                    http_response(r#"{"minimum_tick_size":"0.01"}"#)
                } else if request_line.starts_with("GET /book") {
                    http_response(BOOK_SUMMARY)
                } else if request_line.starts_with("GET /neg-risk") {
                    http_response(r#"{"neg_risk":false}"#)
                } else if request_line.starts_with("POST /order") {
                    let _ = body_tx.send(body);
                    http_response(
                        r#"{"errorMsg":null,"makingAmount":"","takingAmount":"","orderID":"order-1","status":"live","success":true}"#,
                    )
                } else {
                    "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                        .to_owned()
                };
                let _ = stream.write_all(reply.as_bytes());
                let _ = stream.flush();
            }
        });
        Ok((address, body_rx))
    }

    fn order_response(status: &str) -> String {
        format!(
            r#"{{
                "id":"order-1",
                "status":"{status}",
                "owner":"00000000-0000-0000-0000-000000000000",
                "maker_address":"0x0000000000000000000000000000000000000001",
                "market":"0x0000000000000000000000000000000000000000000000000000000000000001",
                "asset_id":"1",
                "side":"BUY",
                "original_size":"10",
                "size_matched":"4",
                "price":"0.52",
                "associate_trades":["trade-1"],
                "outcome":"Yes",
                "created_at":1700000000,
                "expiration":"0",
                "order_type":"GTC"
            }}"#
        )
    }

    #[tokio::test]
    async fn status_query_enriched() -> Result<(), Box<dyn std::error::Error>> {
        // Given an authenticated venue client returning a matched order fixture.
        let executor = fixture_executor(order_response("MATCHED")).await?;

        // When status is queried through the Executor seam.
        let status = executor
            .query_status(&OrderId("order-1".to_owned()))
            .await?;

        // Then only fields provided by the order response are populated.
        let OrderStatus::Accepted(details) = status else {
            return Err("expected accepted status".into());
        };
        assert_eq!(details.filled_qty, Some(Decimal::from(4)));
        assert_eq!(details.price, Some(Decimal::new(52, 2)));
        assert_eq!(details.fee, None);
        assert_eq!(details.settlement_reference, None);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_status_retains_quantity_without_fee()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given: Polymarket returns a cancelled order with a partial matched quantity.
        let executor = fixture_executor(order_response("CANCELED")).await?;

        // When: status is queried through the real adapter seam.
        let status = executor
            .query_status(&OrderId("order-1".to_owned()))
            .await?;

        // Then: exact quantity is retained and unavailable fee economics remain absent.
        let OrderStatus::Cancelled(details) = status else {
            return Err("expected cancelled status".into());
        };
        assert_eq!(details.filled_qty, Some(Decimal::from(4)));
        assert_eq!(details.price, Some(Decimal::new(52, 2)));
        assert_eq!(details.fee, None);
        Ok(())
    }

    #[tokio::test]
    async fn status_ambiguous_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        // Given a venue response whose status is not part of the known lifecycle.
        let executor = fixture_executor(order_response("PENDING_REVIEW")).await?;

        // When status is queried through the Executor seam.
        let result = executor.query_status(&OrderId("order-1".to_owned())).await;

        // Then the unknown value is preserved in a typed fail-closed error.
        match result {
            Err(ExecError::Transport { message }) => {
                assert_eq!(message, "ambiguous status PENDING_REVIEW for order order-1");
            }
            other => return Err(format!("expected ambiguous status error, got {other:?}").into()),
        }
        Ok(())
    }

    #[test]
    fn gtd_expiration_carries_the_millisecond_value_in_the_seconds_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        // 2026-07-24T00:00:00Z as Unix milliseconds.
        let expire_at_ms = 1_784_851_200_000_i64;
        let Some(expiration) = gtd_expiration(expire_at_ms) else {
            return Err("expected an in-range expiration".into());
        };

        // The SDK serializes `timestamp()` (whole seconds) verbatim while the
        // CLOB v2 backend reads milliseconds, so the forged datetime's seconds
        // slot must hold the buffered millisecond value. If an SDK bump makes
        // this assertion fail, the SDK now converts to milliseconds itself and
        // the workaround must be removed, not kept: keeping it would produce
        // orders that effectively never expire.
        assert_eq!(
            expiration.timestamp(),
            expire_at_ms + GTD_SECURITY_BUFFER_MS
        );
        Ok(())
    }

    #[test]
    fn gtd_expiration_rejects_out_of_range_values() {
        assert!(gtd_expiration(i64::MAX).is_none());
    }

    #[tokio::test]
    async fn gtd_submit_posts_millisecond_expiration_with_venue_buffer()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given a venue serving the SDK's order-building endpoints and
        // capturing the serialized order submission.
        let (address, posted) = spawn_order_capture_server()?;
        let executor = executor_at(address).await?;

        // When a GTD order is submitted through the real adapter seam.
        let expire_at_ms = 1_784_851_200_000_i64;
        let order = PlaceOrder {
            market: MarketId::new("fixture")?,
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(50, 2),
            qty: Decimal::from(10),
            post_only: false,
            tif: TimeInForce::Gtd { expire_at_ms },
        };
        let order_id = executor.submit(&order, 0).await?;
        assert_eq!(order_id.0, "order-1");

        // Then the wire request is a GTD order whose expiration field carries
        // the intended millisecond deadline plus the venue one-minute buffer.
        // The CLOB v2 backend reads this field as milliseconds while the SDK
        // serializes `timestamp()` seconds verbatim; if an SDK bump converts
        // to milliseconds itself, this assertion fails and the workaround in
        // `gtd_expiration` must be removed rather than kept.
        let body = posted.recv_timeout(std::time::Duration::from_secs(10))?;
        let request: serde_json::Value = serde_json::from_str(&body)?;
        assert_eq!(request["orderType"], "GTD");
        assert_eq!(
            request["order"]["expiration"],
            (expire_at_ms + GTD_SECURITY_BUFFER_MS).to_string()
        );
        Ok(())
    }

    #[test]
    fn sdk_errors_preserve_rejection_or_transport_semantics() {
        let rejected_error = SdkError::validation("bad order");
        let transport_error =
            SdkError::with_source(Kind::Internal, std::io::Error::other("offline"));
        let rejected = exec_error(&rejected_error);
        let transport = exec_error(&transport_error);

        assert!(matches!(rejected, ExecError::Rejected { .. }));
        assert!(matches!(transport, ExecError::Transport { .. }));
    }

    #[test]
    fn status_errors_only_reject_definitive_client_failures() {
        let rejected = exec_error(&SdkError::status(
            StatusCode::BAD_REQUEST,
            Method::POST,
            "/order".to_owned(),
            "bad order",
        ));
        let server = exec_error(&SdkError::status(
            StatusCode::INTERNAL_SERVER_ERROR,
            Method::POST,
            "/order".to_owned(),
            "unknown outcome",
        ));
        let timeout = exec_error(&SdkError::status(
            StatusCode::REQUEST_TIMEOUT,
            Method::POST,
            "/order".to_owned(),
            "unknown outcome",
        ));

        assert!(matches!(rejected, ExecError::Rejected { .. }));
        assert!(matches!(server, ExecError::Transport { .. }));
        assert!(matches!(timeout, ExecError::Transport { .. }));
    }

    fn sized_order(qty: Decimal) -> Result<PlaceOrder, Box<dyn std::error::Error>> {
        Ok(PlaceOrder {
            market: MarketId::new("fixture")?,
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(50, 2),
            qty,
            post_only: false,
            tif: TimeInForce::Gtc,
        })
    }

    #[test]
    fn sub_minimum_order_rejected_locally_with_shares_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let order = sized_order(Decimal::from(3))?;
        let Err(ExecError::Rejected { reason }) = ensure_min_order_size(&order, Decimal::from(5))
        else {
            return Err("expected a typed rejection".into());
        };
        assert!(reason.contains("below the venue minimum of 5 shares"));
        Ok(())
    }

    #[test]
    fn at_minimum_passes() -> Result<(), Box<dyn std::error::Error>> {
        let at_minimum = sized_order(Decimal::from(5))?;
        assert!(ensure_min_order_size(&at_minimum, Decimal::from(5)).is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn resolved_minimum_from_book_metadata_guards_submissions()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given a venue publishing a five-share minimum in its book summary.
        let (address, _posted) = spawn_order_capture_server()?;
        let signer = test_signer()?;
        let client = authenticated_client(address, &signer).await?;

        // When the executor reads its minimum at construction.
        let executor = PolymarketExecutor::connect(client, signer, fixture_tokens()?).await?;

        // Then a sub-minimum order is rejected locally, before any submission.
        let order = sized_order(Decimal::from(3))?;
        let Err(ExecError::Rejected { reason }) = executor.submit(&order, 0).await else {
            return Err("expected a typed rejection".into());
        };
        assert!(reason.contains("below the venue minimum of 5 shares"));
        Ok(())
    }

    #[test]
    fn off_tick_price_rejected_with_grid_semantics() -> Result<(), Box<dyn std::error::Error>> {
        let tick_size = Decimal::new(1, 2);
        let mut order = sized_order(Decimal::from(10))?;

        // 0.455 sits between the 0.01 grid points.
        order.price = Decimal::new(455, 3);
        let Err(ExecError::Rejected { reason }) = ensure_price_on_tick(&order, tick_size) else {
            return Err("expected a typed rejection".into());
        };
        assert!(reason.contains("off the venue tick grid"));

        // The bounds are exclusive of 0 and 1 even though both sit on the grid.
        order.price = Decimal::ZERO;
        assert!(ensure_price_on_tick(&order, tick_size).is_err());
        order.price = Decimal::ONE;
        assert!(ensure_price_on_tick(&order, tick_size).is_err());
        Ok(())
    }

    #[test]
    fn on_grid_prices_pass_including_boundaries() -> Result<(), Box<dyn std::error::Error>> {
        let tick_size = Decimal::new(1, 2);
        let mut order = sized_order(Decimal::from(10))?;
        for cents in [1_i64, 50, 99] {
            order.price = Decimal::new(cents, 2);
            assert!(ensure_price_on_tick(&order, tick_size).is_ok());
        }
        Ok(())
    }

    #[tokio::test]
    async fn resolved_tick_from_book_metadata_guards_submissions()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given a venue publishing a one-cent tick in its book summary.
        let (address, _posted) = spawn_order_capture_server()?;
        let signer = test_signer()?;
        let client = authenticated_client(address, &signer).await?;
        let executor = PolymarketExecutor::connect(client, signer, fixture_tokens()?).await?;

        // When an off-grid price is submitted (size clears the minimum).
        let mut order = sized_order(Decimal::from(10))?;
        order.price = Decimal::new(455, 3);

        // Then it is rejected locally, before any submission.
        let Err(ExecError::Rejected { reason }) = executor.submit(&order, 0).await else {
            return Err("expected a typed rejection".into());
        };
        assert!(reason.contains("off the venue tick grid"));
        Ok(())
    }
}
