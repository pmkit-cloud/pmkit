use chrono::{DateTime, Utc};
use pmkit_exec::{
    ExecError, ExecutionSnapshot, Executor, OrderId, OrderStatus, OrderStatusDetails, PlaceOrder,
    TimeInForce,
};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Normal, Signer};
use polymarket_client_sdk_v2::clob::Client;
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::clob::types::response::OpenOrderResponse;
use polymarket_client_sdk_v2::clob::types::{OrderStatusType, OrderType};
use polymarket_client_sdk_v2::error::{Error as SdkError, Kind, Status, StatusCode};

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
}

impl<S> PolymarketExecutor<S> {
    /// Creates an executor from an authenticated SDK client and its signer.
    #[must_use]
    pub const fn new(
        client: Client<Authenticated<Normal>>,
        signer: S,
        tokens: MarketTokens,
    ) -> Self {
        Self {
            client,
            signer,
            tokens,
        }
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

    use pmkit_core::MarketId;
    use pmkit_exec::{ExecError, Executor, OrderId, OrderStatus};
    use polymarket_client_sdk_v2::POLYGON;
    use polymarket_client_sdk_v2::auth::{Credentials, LocalSigner, Signer as _, Uuid};
    use polymarket_client_sdk_v2::clob::{Client, Config};
    use polymarket_client_sdk_v2::error::{Error as SdkError, Kind, Method, StatusCode};
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal::Decimal;

    use super::{GTD_SECURITY_BUFFER_MS, PolymarketExecutor, exec_error, gtd_expiration};
    use crate::MarketTokens;

    const PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

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

        let signer = LocalSigner::from_str(PRIVATE_KEY)?.with_chain_id(Some(POLYGON));
        let credentials = Credentials::new(
            Uuid::nil(),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned(),
            "fixture-passphrase".to_owned(),
        );
        let client = Client::new(&format!("http://{address}"), Config::default())?
            .authentication_builder(&signer)
            .credentials(credentials)
            .authenticate()
            .await?;
        let tokens = MarketTokens::new(MarketId::new("fixture")?, U256::from(1), U256::from(2));
        Ok(PolymarketExecutor::new(client, signer, tokens))
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
    fn gtd_expiration_carries_the_millisecond_value_in_the_seconds_slot() {
        // 2026-07-24T00:00:00Z as Unix milliseconds.
        let expire_at_ms = 1_784_851_200_000_i64;
        let expiration = gtd_expiration(expire_at_ms).expect("in range");

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
    }

    #[test]
    fn gtd_expiration_rejects_out_of_range_values() {
        assert!(gtd_expiration(i64::MAX).is_none());
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
}
