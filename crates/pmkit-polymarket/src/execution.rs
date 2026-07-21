use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Normal, Signer};
use polymarket_client_sdk_v2::clob::Client;
use polymarket_client_sdk_v2::clob::types::OrderType;
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::error::{Error as SdkError, Kind, Status, StatusCode};

use crate::{MarketTokens, venue_order_inputs};

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

    async fn submit(&self, order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        let inputs =
            venue_order_inputs(order, &self.tokens).ok_or_else(|| ExecError::Rejected {
                reason: format!(
                    "order market {} does not match token map {}",
                    order.market,
                    self.tokens.market()
                ),
            })?;
        let response = self
            .client
            .limit_order()
            .token_id(inputs.token_id)
            .side(inputs.side)
            .price(inputs.price)
            .size(inputs.size)
            .post_only(inputs.post_only)
            .order_type(OrderType::GTC)
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
    use pmkit_exec::ExecError;
    use polymarket_client_sdk_v2::error::{Error as SdkError, Kind, Method, StatusCode};

    use super::exec_error;

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
