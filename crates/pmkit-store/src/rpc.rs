use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::{
    Address, BlockHead, ChainId, ChainSourceError, FinalizedBlockCoverage, FinalizedBlockRange,
    FinalizedRawLogBatch, FinalizedRawLogProvider, ProviderIdentity, RawLogIdentity, RawRpcLog,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounded operational behavior for one JSON-RPC provider.
#[derive(Debug, Clone, Copy)]
pub struct RpcProviderConfig {
    /// Maximum inclusive block count in one `eth_getLogs` request.
    pub max_range_blocks: u64,
    /// Number of retries after a transport or HTTP failure.
    pub max_retries: u8,
    /// Delay between retry attempts.
    pub retry_delay: Duration,
    /// Maximum concurrent HTTP requests to this provider.
    pub max_concurrent_requests: usize,
}

impl Default for RpcProviderConfig {
    fn default() -> Self {
        Self {
            max_range_blocks: 2_000,
            max_retries: 2,
            retry_delay: Duration::from_millis(200),
            max_concurrent_requests: 4,
        }
    }
}

/// A narrow JSON-RPC provider for finalized Polygon logs.
#[derive(Debug, Clone)]
pub struct JsonRpcFinalizedProvider {
    client: reqwest::Client,
    endpoint: String,
    provider: ProviderIdentity,
    chain_id: ChainId,
    config: RpcProviderConfig,
    requests: Arc<Semaphore>,
}

impl JsonRpcFinalizedProvider {
    /// Creates a provider using an HTTPS JSON-RPC endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`ChainSourceError::ProviderFailure`] when the HTTP client cannot
    /// be configured with the bounded request timeout.
    pub fn new(
        endpoint: impl Into<String>,
        provider: ProviderIdentity,
        chain_id: ChainId,
    ) -> Result<Self, ChainSourceError> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|error| ChainSourceError::ProviderFailure {
                provider: provider.clone(),
                message: error.to_string(),
            })?;
        let config = RpcProviderConfig::default();
        Ok(Self {
            client,
            endpoint: endpoint.into(),
            provider,
            chain_id,
            requests: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            config,
        })
    }

    /// Applies bounded request, retry, and range settings.
    #[must_use]
    pub fn with_config(mut self, config: RpcProviderConfig) -> Self {
        self.requests = Arc::new(Semaphore::new(config.max_concurrent_requests.max(1)));
        self.config = config;
        self
    }

    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, ChainSourceError> {
        let body = serde_json::to_vec(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }))
        .map_err(|error| self.failure(error.to_string()))?;
        let mut attempt = 0_u8;
        let response = loop {
            let _permit = self
                .requests
                .acquire()
                .await
                .map_err(|error| self.failure(error.to_string()))?;
            let response = self
                .client
                .post(&self.endpoint)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => break response,
                Ok(response) => {
                    if attempt >= self.config.max_retries {
                        return Err(self.failure(format!("HTTP status {}", response.status())));
                    }
                }
                Err(error) => {
                    if attempt >= self.config.max_retries {
                        return Err(self.failure(error.to_string()));
                    }
                }
            }
            attempt = attempt.saturating_add(1);
            tokio::time::sleep(self.config.retry_delay).await;
        };
        let envelope: RpcResponse = serde_json::from_slice(
            &response
                .bytes()
                .await
                .map_err(|error| self.failure(error.to_string()))?,
        )
        .map_err(|error| self.failure(error.to_string()))?;
        if let Some(error) = envelope.error {
            return Err(self.failure(format!("JSON-RPC {}: {}", error.code, error.message)));
        }
        envelope
            .result
            .ok_or_else(|| self.failure("missing JSON-RPC result"))
    }

    async fn block(&self, tag: &str) -> Result<BlockHead, ChainSourceError> {
        let result = self
            .call("eth_getBlockByNumber", serde_json::json!([tag, false]))
            .await?;
        let block: RpcBlock =
            serde_json::from_value(result).map_err(|error| self.failure(error.to_string()))?;
        Ok(BlockHead::new(
            self.chain_id,
            parse_quantity(&block.number).map_err(|error| self.failure(error))?,
            block.hash,
            block.parent_hash,
        ))
    }

    fn failure(&self, message: impl Into<String>) -> ChainSourceError {
        ChainSourceError::ProviderFailure {
            provider: self.provider.clone(),
            message: message.into(),
        }
    }
}

#[async_trait]
impl FinalizedRawLogProvider for JsonRpcFinalizedProvider {
    async fn finalized_head(&self) -> Result<(BlockHead, BlockHead), ChainSourceError> {
        let head = self.block("latest").await?;
        let finalized = self.block("finalized").await?;
        Ok((head, finalized))
    }

    async fn fetch_finalized_logs(
        &self,
        range: &FinalizedBlockRange,
    ) -> Result<FinalizedRawLogBatch, ChainSourceError> {
        if range.chain_id != self.chain_id {
            return Err(self.failure("requested chain does not match provider"));
        }
        let requested_blocks = range
            .to_block
            .checked_sub(range.from_block)
            .and_then(|width| width.checked_add(1))
            .ok_or_else(|| self.failure("requested range size overflowed"))?;
        if requested_blocks > self.config.max_range_blocks {
            return Err(ChainSourceError::RangeTooLarge {
                requested_blocks,
                maximum_blocks: self.config.max_range_blocks,
            });
        }
        let (head, finalized) = self.finalized_head().await?;
        if range.to_block > finalized.block_number {
            return Err(ChainSourceError::FinalityViolation {
                requested_to_block: range.to_block,
                finalized_block: finalized.block_number,
            });
        }
        let result = self
            .call(
                "eth_getLogs",
                serde_json::json!([{
                    "fromBlock": quantity(range.from_block),
                    "toBlock": quantity(range.to_block),
                }]),
            )
            .await?;
        let logs: Vec<RpcLog> =
            serde_json::from_value(result).map_err(|error| self.failure(error.to_string()))?;
        let logs = logs
            .into_iter()
            .map(|log| self.raw_log(log))
            .collect::<Result<Vec<_>, _>>()?;
        let mut blocks = Vec::new();
        for block_number in range.from_block..=range.to_block {
            blocks.push(self.block(&quantity(block_number)).await?);
        }
        let coverage = FinalizedBlockCoverage::new(range.clone(), blocks)?;
        FinalizedRawLogBatch::new(
            self.provider.clone(),
            range.clone(),
            head,
            finalized,
            coverage,
            logs,
        )
    }
}

impl JsonRpcFinalizedProvider {
    fn raw_log(&self, log: RpcLog) -> Result<RawRpcLog, ChainSourceError> {
        Ok(RawRpcLog {
            identity: RawLogIdentity {
                provider: self.provider.clone(),
                chain_id: self.chain_id,
                block_number: parse_quantity(&log.block_number)
                    .map_err(|error| self.failure(error))?,
                block_hash: log.block_hash,
                transaction_hash: log.transaction_hash,
                transaction_index: parse_quantity(&log.transaction_index)
                    .map_err(|error| self.failure(error))?,
                log_index: parse_quantity(&log.log_index).map_err(|error| self.failure(error))?,
            },
            contract_address: Address::new(log.address)
                .map_err(|_| self.failure("invalid log address"))?,
            topics: log.topics,
            data: log.data,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    result: Option<serde_json::Value>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RpcBlock {
    number: String,
    hash: String,
    #[serde(rename = "parentHash")]
    parent_hash: String,
}

#[derive(Debug, Deserialize)]
struct RpcLog {
    address: String,
    topics: Vec<String>,
    data: String,
    #[serde(rename = "blockHash")]
    block_hash: String,
    #[serde(rename = "blockNumber")]
    block_number: String,
    #[serde(rename = "transactionHash")]
    transaction_hash: String,
    #[serde(rename = "transactionIndex")]
    transaction_index: String,
    #[serde(rename = "logIndex")]
    log_index: String,
}

fn quantity(value: u64) -> String {
    format!("0x{value:x}")
}

fn parse_quantity(value: &str) -> Result<u64, String> {
    value
        .strip_prefix("0x")
        .ok_or_else(|| "JSON-RPC quantity is not 0x-prefixed".to_owned())
        .and_then(|value| u64::from_str_radix(value, 16).map_err(|error| error.to_string()))
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::expect_used,
        reason = "local HTTP fixture setup must fail loudly when malformed"
    )]

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use crate::{ChainId, FinalizedBlockRange, FinalizedRawLogProvider, ProviderIdentity};

    use super::{parse_quantity, quantity};

    #[test]
    fn parses_json_rpc_quantities_without_decimal_fallbacks() {
        // Given: canonical JSON-RPC quantity encodings.
        // When: quantities are parsed and emitted.
        // Then: the conversion is exact and rejects non-hex input.
        assert_eq!(parse_quantity("0x2a"), Ok(42));
        assert_eq!(quantity(42), "0x2a");
        assert!(parse_quantity("42").is_err());
    }

    #[tokio::test]
    async fn fetches_finalized_heads_and_logs_over_json_rpc() {
        // Given: a local JSON-RPC server returning a finalized Polygon block.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
        let endpoint = format!("http://{}/", listener.local_addr().expect("server address"));
        let server = thread::spawn(move || {
            for _ in 0..4 {
                let (mut stream, _) = listener.accept().expect("accept fixture request");
                let mut request = [0_u8; 4096];
                let size = stream.read(&mut request).expect("read fixture request");
                let request = String::from_utf8_lossy(&request[..size]);
                let body = if request.contains("eth_getLogs") {
                    r#"{"jsonrpc":"2.0","id":1,"result":[]}"#
                } else if request.contains("\"latest\"") {
                    r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x2","hash":"0xhead","parentHash":"0xf1"}}"#
                } else {
                    r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","hash":"0xf1","parentHash":"0xgenesis"}}"#
                };
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .expect("write fixture response");
            }
        });

        // When: the provider fetches one finalized range.
        let provider = super::super::JsonRpcFinalizedProvider::new(
            endpoint,
            ProviderIdentity::new("fixture-rpc"),
            ChainId::POLYGON,
        )
        .expect("provider configuration");
        let batch = provider
            .fetch_finalized_logs(&FinalizedBlockRange::new(ChainId::POLYGON, 1, 1).expect("range"))
            .await
            .expect("JSON-RPC fixture response");
        server.join().expect("fixture server joins");

        // Then: the adapter retains the finalized heads and returns an empty, valid range.
        assert_eq!(batch.head.block_number, 2);
        assert_eq!(batch.finalized.block_number, 1);
        assert_eq!(batch.coverage.blocks, vec![batch.finalized.clone()]);
        assert!(batch.logs.is_empty());
    }

    #[tokio::test]
    async fn rejects_ranges_larger_than_the_configured_bound_before_rpc() {
        let provider = super::super::JsonRpcFinalizedProvider::new(
            "http://127.0.0.1:1/",
            ProviderIdentity::new("fixture-rpc"),
            ChainId::POLYGON,
        )
        .expect("provider configuration")
        .with_config(super::super::RpcProviderConfig {
            max_range_blocks: 1,
            max_retries: 0,
            retry_delay: std::time::Duration::ZERO,
            max_concurrent_requests: 0,
        });

        let error = provider
            .fetch_finalized_logs(&FinalizedBlockRange::new(ChainId::POLYGON, 1, 2).expect("range"))
            .await
            .expect_err("oversized range must fail closed");

        assert!(matches!(
            error,
            crate::ChainSourceError::RangeTooLarge {
                requested_blocks: 2,
                maximum_blocks: 1
            }
        ));
    }

    #[tokio::test]
    async fn retries_transient_http_failures() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
        let endpoint = format!("http://{}/", listener.local_addr().expect("server address"));
        let server = thread::spawn(move || {
            for request_number in 0..5 {
                let (mut stream, _) = listener.accept().expect("accept fixture request");
                let mut request = [0_u8; 4096];
                let size = stream.read(&mut request).expect("read fixture request");
                let request = String::from_utf8_lossy(&request[..size]);
                let body = if request_number == 0 {
                    r#"{"error":"temporary"}"#
                } else if request.contains("eth_getLogs") {
                    r#"{"jsonrpc":"2.0","id":1,"result":[]}"#
                } else if request.contains("\"latest\"") {
                    r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x2","hash":"0xhead","parentHash":"0xf1"}}"#
                } else {
                    r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x1","hash":"0xf1","parentHash":"0xgenesis"}}"#
                };
                let status = if request_number == 0 {
                    "503 Service Unavailable"
                } else {
                    "200 OK"
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .expect("write fixture response");
            }
        });

        let provider = super::super::JsonRpcFinalizedProvider::new(
            endpoint,
            ProviderIdentity::new("fixture-rpc"),
            ChainId::POLYGON,
        )
        .expect("provider configuration")
        .with_config(super::super::RpcProviderConfig {
            max_range_blocks: 10,
            max_retries: 1,
            retry_delay: std::time::Duration::ZERO,
            max_concurrent_requests: 1,
        });
        let batch = provider
            .fetch_finalized_logs(&FinalizedBlockRange::new(ChainId::POLYGON, 1, 1).expect("range"))
            .await
            .expect("retry should recover the request");
        server.join().expect("fixture server joins");

        assert!(batch.logs.is_empty());
    }
}
