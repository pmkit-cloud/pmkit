use std::{fmt, time::Duration};

use reqwest::StatusCode;
use serde::Deserialize;
use thiserror::Error;

use crate::{MaterializedMarketSegments, StoreError};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_RETRIES: u8 = 2;

/// Failure returned while publishing materialized segments to Cloud.
#[derive(Debug, Error)]
pub enum CloudPublishError {
    /// The bounded HTTP client could not be configured or used.
    #[error("cloud HTTP client configuration failed")]
    Client(#[from] reqwest::Error),
    /// Cloud rejected the publication or artifact upload.
    #[error("cloud request failed with status {status}")]
    Http {
        /// HTTP status returned by Cloud.
        status: StatusCode,
    },
    /// The Cloud endpoint was not HTTPS or an explicit local development endpoint.
    #[error("cloud publisher endpoint must use HTTPS or local HTTP")]
    InvalidEndpoint,
    /// The materialized result did not contain the fields required to publish it.
    #[error("materialized segment is missing a declaration field")]
    InvalidMaterialization,
}

#[derive(Debug, Deserialize)]
struct PublishResponse {
    release_id: String,
    status: PublishStatus,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PublishStatus {
    Staging,
    Published,
}

/// Publishes a materialized bundle and uploads its exact declared segment bodies.
#[derive(Clone)]
pub struct CloudPublisher {
    client: reqwest::Client,
    endpoint: String,
    debug_endpoint: String,
    storage_token: String,
}

impl fmt::Debug for CloudPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CloudPublisher")
            .field("endpoint", &self.debug_endpoint)
            .field("storage_token", &"[redacted]")
            .finish_non_exhaustive()
    }
}

/// Publication progress for one materialized segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudPublishProgress {
    /// Catalog release returned by the manifest publication.
    pub release_id: String,
    /// Number of segment bodies successfully uploaded so far.
    pub uploaded_segments: usize,
    /// Total number of segment bodies in the materialization.
    pub total_segments: usize,
}

impl CloudPublisher {
    /// Creates a publisher with a bounded HTTP timeout.
    ///
    /// HTTPS endpoints are accepted. Local HTTP is accepted only for the
    /// exact development hosts `localhost`, `127.0.0.1`, and `[::1]`.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be configured.
    pub fn new(
        endpoint: impl Into<String>,
        storage_token: impl Into<String>,
    ) -> Result<Self, CloudPublishError> {
        let endpoint = endpoint.into();
        let parsed =
            reqwest::Url::parse(endpoint.trim()).map_err(|_| CloudPublishError::InvalidEndpoint)?;
        let local_http = parsed.scheme() == "http"
            && parsed
                .host_str()
                .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "[::1]"));
        if parsed.scheme() != "https" && !local_http
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(CloudPublishError::InvalidEndpoint);
        }
        let mut debug_endpoint = parsed.clone();
        debug_endpoint.set_query(None);
        debug_endpoint.set_fragment(None);
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            endpoint: parsed.as_str().trim_end_matches('/').to_owned(),
            debug_endpoint: debug_endpoint.as_str().trim_end_matches('/').to_owned(),
            storage_token: storage_token.into(),
        })
    }

    /// Publishes the manifest, then uploads every digest-verified segment body.
    ///
    /// # Errors
    ///
    /// Returns an error when local validation fails or Cloud rejects a request.
    pub async fn publish(
        &self,
        bundle_id: &str,
        materialized: &MaterializedMarketSegments,
    ) -> Result<String, CloudPublishError> {
        let mut release_id = None;
        self.publish_with_progress(bundle_id, materialized, |progress| {
            release_id = Some(progress.release_id);
        })
        .await?;
        release_id.ok_or(CloudPublishError::InvalidMaterialization)
    }

    /// Publishes a materialization and reports each completed segment upload.
    ///
    /// # Errors
    ///
    /// Returns an error when local validation fails or Cloud rejects a request.
    pub async fn publish_with_progress<F>(
        &self,
        bundle_id: &str,
        materialized: &MaterializedMarketSegments,
        mut on_progress: F,
    ) -> Result<(), CloudPublishError>
    where
        F: FnMut(CloudPublishProgress),
    {
        validate_materialization(materialized)?;
        let manifest = serde_json::to_vec(&materialized.manifest)
            .map_err(|_| CloudPublishError::InvalidMaterialization)?;
        let publish_url = format!("{}/internal/processor/bundles", self.endpoint);
        let (status, body) = self
            .send_with_retry(|| {
                self.client
                    .post(&publish_url)
                    .bearer_auth(&self.storage_token)
                    .header("x-pmkit-bundle-id", bundle_id)
                    .header("idempotency-key", bundle_id)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(manifest.clone())
            })
            .await?;
        if !status.is_success() {
            return Err(CloudPublishError::Http { status });
        }
        let published: PublishResponse =
            serde_json::from_str(&body).map_err(|_| CloudPublishError::InvalidMaterialization)?;
        if published.status == PublishStatus::Published {
            on_progress(CloudPublishProgress {
                release_id: published.release_id,
                uploaded_segments: materialized.segments.len(),
                total_segments: materialized.segments.len(),
            });
            return Ok(());
        }

        for (uploaded_segments, segment) in materialized.segments.iter().enumerate() {
            let artifact_key = segment.declaration["artifact_key"]
                .as_str()
                .ok_or(CloudPublishError::InvalidMaterialization)?;
            let upload_url = format!(
                "{}/internal/processor/bundles/{}/artifacts/{}",
                self.endpoint,
                percent_encode(bundle_id),
                artifact_key
                    .split('/')
                    .map(percent_encode)
                    .collect::<Vec<_>>()
                    .join("/")
            );
            let (status, _) = self
                .send_with_retry(|| {
                    self.client
                        .put(&upload_url)
                        .bearer_auth(&self.storage_token)
                        .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
                        .body(segment.bytes.clone())
                })
                .await?;
            if !status.is_success() {
                return Err(CloudPublishError::Http { status });
            }
            on_progress(CloudPublishProgress {
                release_id: published.release_id.clone(),
                uploaded_segments: uploaded_segments + 1,
                total_segments: materialized.segments.len(),
            });
        }

        let finalize_url = format!(
            "{}/internal/processor/bundles/{}/finalize",
            self.endpoint,
            percent_encode(bundle_id)
        );
        let (status, body) = self
            .send_with_retry(|| {
                self.client
                    .post(&finalize_url)
                    .bearer_auth(&self.storage_token)
            })
            .await?;
        if !status.is_success() {
            return Err(CloudPublishError::Http { status });
        }
        let finalized: PublishResponse =
            serde_json::from_str(&body).map_err(|_| CloudPublishError::InvalidMaterialization)?;
        if finalized.status != PublishStatus::Published
            || finalized.release_id != published.release_id
        {
            return Err(CloudPublishError::InvalidMaterialization);
        }
        Ok(())
    }

    async fn send_with_retry<F>(
        &self,
        mut build: F,
    ) -> Result<(StatusCode, String), CloudPublishError>
    where
        F: FnMut() -> reqwest::RequestBuilder,
    {
        for attempt in 0..=MAX_RETRIES {
            match build().send().await {
                Ok(response) => {
                    let status = response.status();
                    if !is_transient(status) || attempt == MAX_RETRIES {
                        let body = if status.is_success() {
                            response.text().await?
                        } else {
                            String::new()
                        };
                        return Ok((status, body));
                    }
                }
                Err(error) if attempt < MAX_RETRIES => {
                    let _ = error;
                }
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(100 * 2_u64.pow(u32::from(attempt)))).await;
        }
        unreachable!()
    }
}

fn is_transient(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn validate_materialization(
    materialized: &MaterializedMarketSegments,
) -> Result<(), CloudPublishError> {
    if materialized.segments.is_empty() {
        return Err(CloudPublishError::InvalidMaterialization);
    }
    for segment in &materialized.segments {
        let declaration = &segment.declaration;
        let bytes = declaration["bytes"]
            .as_u64()
            .ok_or(CloudPublishError::InvalidMaterialization)?;
        let sha256 = declaration["sha256"]
            .as_str()
            .ok_or(CloudPublishError::InvalidMaterialization)?;
        if bytes != segment.bytes.len() as u64
            || crate::integrity::sha256_hex(&segment.bytes) != sha256
        {
            return Err(CloudPublishError::InvalidMaterialization);
        }
        if declaration["artifact_key"].as_str().is_none() {
            return Err(CloudPublishError::InvalidMaterialization);
        }
    }
    Ok(())
}

fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            byte => format!("%{byte:02X}"),
        })
        .collect()
}

impl From<CloudPublishError> for StoreError {
    fn from(error: CloudPublishError) -> Self {
        Self::Storage {
            message: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex as StdMutex};

    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };

    use super::*;
    use crate::MaterializedMarketSegment;

    #[tokio::test]
    async fn publishes_then_uploads_exact_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let server = tokio::spawn(async move {
            for index in 0..3 {
                let (mut stream, _) = listener.accept().await?;
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut buffer).await?;
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..read]);
                    let header_end = request
                        .windows(4)
                        .position(|window| window == b"\r\n\r\n")
                        .map(|position| position + 4);
                    if let Some(header_end) = header_end {
                        let content_length = String::from_utf8_lossy(&request[..header_end])
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .and_then(|value| value.parse::<usize>().ok())
                            .unwrap_or(0);
                        if request.len() >= header_end + content_length {
                            break;
                        }
                    }
                }
                seen.lock().await.push(request);
                let (status, body) = [
                    (
                        "201 Created",
                        r#"{"release_id":"rel-1","status":"staging"}"#,
                    ),
                    ("200 OK", "{}"),
                    ("200 OK", r#"{"release_id":"rel-1","status":"published"}"#),
                ][index];
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await?;
            }
            Ok::<_, std::io::Error>(())
        });

        let bytes = b"{\"source_timestamp_ms\":1}\n".to_vec();
        let materialized = MaterializedMarketSegments {
            manifest: json!({"schema_version": 1, "artifact_sha256": "a"}),
            segments: vec![MaterializedMarketSegment {
                declaration: json!({
                    "artifact_key": "segments/token id/part.jsonl",
                    "bytes": 26,
                    "sha256": "07ea46fc5ae0411a7779cde60cb1a8d0b6610b10099a43543439fbb302d4b3b0"
                }),
                bytes: bytes.clone(),
            }],
        };
        let publisher = CloudPublisher::new(format!("http://{address}"), "secret")?;
        let progress = Arc::new(StdMutex::new(Vec::new()));
        let progress_seen = Arc::clone(&progress);
        publisher
            .publish_with_progress("bundle id", &materialized, |event| {
                if let Ok(mut progress) = progress_seen.lock() {
                    progress.push(event);
                }
            })
            .await?;
        server.await??;

        let captured = requests.lock().await;
        assert_publication_requests(&captured, &bytes);
        drop(captured);
        let expected = CloudPublishProgress {
            release_id: "rel-1".into(),
            uploaded_segments: 1,
            total_segments: 1,
        };
        assert_eq!(
            progress
                .lock()
                .map_err(|_| "progress mutex poisoned")?
                .as_slice(),
            &[expected]
        );
        Ok(())
    }

    #[tokio::test]
    async fn succeeds_without_artifact_upload_or_finalize_when_already_published()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4);
                if let Some(header_end) = header_end {
                    let content_length = String::from_utf8_lossy(&request[..header_end])
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length: "))
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(0);
                    if request.len() >= header_end + content_length {
                        break;
                    }
                }
            }
            let body = r#"{"release_id":"rel-1","status":"published"}"#;
            let response = format!(
                "HTTP/1.1 201 Created\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await?;
            Ok::<_, std::io::Error>(request)
        });

        let bytes = b"{\"source_timestamp_ms\":1}\n".to_vec();
        let materialized = MaterializedMarketSegments {
            manifest: json!({"schema_version": 1, "artifact_sha256": "a"}),
            segments: vec![MaterializedMarketSegment {
                declaration: json!({
                    "artifact_key": "segments/token id/part.jsonl",
                    "bytes": 26,
                    "sha256": "07ea46fc5ae0411a7779cde60cb1a8d0b6610b10099a43543439fbb302d4b3b0"
                }),
                bytes,
            }],
        };
        let publisher = CloudPublisher::new(format!("http://{address}"), "secret")?;

        assert_eq!(
            publisher.publish("bundle id", &materialized).await?,
            "rel-1"
        );

        let request = server.await??;
        let request = String::from_utf8_lossy(&request);
        assert!(request.starts_with("POST /internal/processor/bundles HTTP/1.1"));
        Ok(())
    }

    #[test]
    fn permits_only_explicit_local_http_development_hosts() {
        // Given: HTTP endpoints for the exact local development hosts and nearby impostors.
        for endpoint in [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
            "http://[::1]:3000",
        ] {
            // When: a publisher is configured for the approved local host.
            let result = CloudPublisher::new(endpoint, "secret");

            // Then: local development is allowed only for that exact host.
            assert!(result.is_ok(), "{endpoint}");
        }
        for endpoint in [
            "http://pmkit.cloud",
            "http://localhost.evil",
            "http://127.0.0.2",
            "http://[::2]",
        ] {
            assert!(
                CloudPublisher::new(endpoint, "secret").is_err(),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn rejects_endpoint_queries_and_fragments() {
        // Given: an otherwise valid HTTPS endpoint has a query or fragment.
        for endpoint in [
            "https://pmkit.cloud?access_token=endpoint-secret",
            "https://pmkit.cloud#endpoint-secret",
        ] {
            // When: a publisher is configured for the endpoint.
            let result = CloudPublisher::new(endpoint, "storage-secret");

            // Then: no request can be built from an ambiguous endpoint.
            assert!(result.is_err(), "{endpoint}");
        }
    }

    #[test]
    fn publisher_debug_redacts_storage_tokens() -> Result<(), CloudPublishError> {
        // Given: a storage token is configured for a valid endpoint.
        let publisher = CloudPublisher::new("https://pmkit.cloud", "storage-secret")?;

        // When: diagnostics render the publisher.
        let debug = format!("{publisher:?}");

        // Then: the storage secret is not exposed through the Debug implementation.
        assert!(!debug.contains("storage-secret"));
        Ok(())
    }

    #[tokio::test]
    async fn failed_cloud_responses_do_not_expose_their_bodies()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let body = "upstream-secret";
            let response = format!(
                "HTTP/1.1 401 Unauthorized\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await?;
            Ok::<_, std::io::Error>(())
        });
        let materialized = MaterializedMarketSegments {
            manifest: json!({"schema_version": 1, "artifact_sha256": "a"}),
            segments: vec![MaterializedMarketSegment {
                declaration: json!({
                    "artifact_key": "segments/token/part.jsonl",
                    "bytes": 26,
                    "sha256": "07ea46fc5ae0411a7779cde60cb1a8d0b6610b10099a43543439fbb302d4b3b0"
                }),
                bytes: b"{\"source_timestamp_ms\":1}\n".to_vec(),
            }],
        };
        let publisher = CloudPublisher::new(format!("http://{address}"), "secret")?;
        let error = publisher
            .publish("bundle", &materialized)
            .await
            .err()
            .ok_or_else(|| std::io::Error::other("cloud publisher unexpectedly succeeded"))?;
        server.await??;

        assert!(!error.to_string().contains("upstream-secret"));
        assert!(!format!("{error:?}").contains("upstream-secret"));
        Ok(())
    }

    fn assert_publication_requests(captured: &[Vec<u8>], bytes: &[u8]) {
        assert_eq!(captured.len(), 3);
        let publish = String::from_utf8_lossy(&captured[0]);
        assert!(publish.starts_with("POST /internal/processor/bundles HTTP/1.1"));
        assert!(publish.contains("authorization: Bearer secret"));
        assert!(publish.contains("x-pmkit-bundle-id: bundle id"));
        let upload = String::from_utf8_lossy(&captured[1]);
        assert!(upload.starts_with(
            "PUT /internal/processor/bundles/bundle%20id/artifacts/segments/token%20id/part.jsonl HTTP/1.1"
        ));
        assert!(captured[1].ends_with(bytes));
        let finalize = String::from_utf8_lossy(&captured[2]);
        assert!(
            finalize.starts_with("POST /internal/processor/bundles/bundle%20id/finalize HTTP/1.1")
        );
        assert!(finalize.contains("authorization: Bearer secret"));
    }

    #[tokio::test]
    async fn rejects_invalid_segment_before_network_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let materialized = MaterializedMarketSegments {
            manifest: json!({"schema_version": 1}),
            segments: vec![MaterializedMarketSegment {
                declaration: json!({
                    "artifact_key": "segments/token/part.jsonl",
                    "bytes": 1,
                    "sha256": "00"
                }),
                bytes: b"not-the-declared-body".to_vec(),
            }],
        };
        let publisher = CloudPublisher::new("http://127.0.0.1:1", "secret")?;
        assert!(matches!(
            publisher.publish("bundle", &materialized).await,
            Err(CloudPublishError::InvalidMaterialization)
        ));
        Ok(())
    }
}
