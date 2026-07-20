use pmkit_exec::{ExecError, Executor, OrderId, PlaceOrder};
use polymarket_client_sdk_v2::auth::state::Authenticated;
use polymarket_client_sdk_v2::auth::{Normal, Signer};
use polymarket_client_sdk_v2::clob::Client;
use polymarket_client_sdk_v2::clob::types::OrderType;
use polymarket_client_sdk_v2::error::{Error as SdkError, Kind};

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
}

#[async_trait::async_trait]
impl<S> Executor for PolymarketExecutor<S>
where
    S: Signer + Send + Sync,
{
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
        Kind::Status | Kind::Validation | Kind::Geoblock => ExecError::Rejected { reason: message },
        Kind::Synchronization | Kind::Internal | Kind::WebSocket => {
            ExecError::Transport { message }
        }
        _ => ExecError::Transport { message },
    }
}

#[cfg(test)]
mod tests {
    use pmkit_exec::ExecError;
    use polymarket_client_sdk_v2::error::{Error as SdkError, Kind};

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
}
