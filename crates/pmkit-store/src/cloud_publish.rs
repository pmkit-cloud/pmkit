use std::time::Duration;

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
    #[error("cloud request failed with status {status}: {body}")]
    Http {
        /// HTTP status returned by Cloud.
        status: StatusCode,
        /// Response body returned by Cloud.
        body: String,
    },
    /// The materialized result did not contain the fields required to publish it.
    #[error("materialized segment is missing a declaration field")]
    InvalidMaterialization,
}

#[derive(Debug, Deserialize)]
struct PublishResponse {
    release_id: String,
}

/// Publishes a materialized bundle and uploads its exact declared segment bodies.
#[derive(Debug, Clone)]
pub struct CloudPublisher {
    client: reqwest::Client,
    endpoint: String,
    storage_token: String,
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
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be configured.
    pub fn new(
        endpoint: impl Into<String>,
        storage_token: impl Into<String>,
    ) -> Result<Self, CloudPublishError> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()?,
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
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
            return Err(CloudPublishError::Http { status, body });
        }
        let published: PublishResponse =
            serde_json::from_str(&body).map_err(|_| CloudPublishError::InvalidMaterialization)?;

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
            let (status, body) = self
                .send_with_retry(|| {
                    self.client
                        .put(&upload_url)
                        .bearer_auth(&self.storage_token)
                        .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
                        .body(segment.bytes.clone())
                })
                .await?;
            if !status.is_success() {
                return Err(CloudPublishError::Http { status, body });
            }
            on_progress(CloudPublishProgress {
                release_id: published.release_id.clone(),
                uploaded_segments: uploaded_segments + 1,
                total_segments: materialized.segments.len(),
            });
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
                    let body = response.text().await?;
                    if !is_transient(status) || attempt == MAX_RETRIES {
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
            for index in 0..2 {
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
                let response = if index == 0 {
                    "HTTP/1.1 201 Created\r\nContent-Length: 22\r\n\r\n{\"release_id\":\"rel-1\"}"
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"
                };
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

        {
            let captured = requests.lock().await;
            assert_eq!(captured.len(), 2);
            let publish = String::from_utf8_lossy(&captured[0]);
            assert!(publish.starts_with("POST /internal/processor/bundles HTTP/1.1"));
            assert!(publish.contains("authorization: Bearer secret"));
            assert!(publish.contains("x-pmkit-bundle-id: bundle id"));
            let upload = String::from_utf8_lossy(&captured[1]);
            assert!(upload.starts_with(
                "PUT /internal/processor/bundles/bundle%20id/artifacts/segments/token%20id/part.jsonl HTTP/1.1"
            ));
            assert!(captured[1].ends_with(&bytes));
            drop(captured);
        }
        assert_eq!(
            progress
                .lock()
                .map_err(|_| "progress mutex poisoned")?
                .as_slice(),
            &[CloudPublishProgress {
                release_id: "rel-1".into(),
                uploaded_segments: 1,
                total_segments: 1,
            }]
        );
        Ok(())
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
