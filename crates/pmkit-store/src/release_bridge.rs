use serde_json::Value;

use crate::{
    SealedClosedDayManifest, StoreError, TapeStore, decode_sealed_closed_day_manifest,
    integrity::sha256_hex,
};

/// Durable state of a Cloud materialization attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudMaterializationState {
    /// Publication has not received a confirmed finalization response.
    Pending,
    /// Cloud confirmed the immutable catalog release.
    Finalized,
    /// The partition cannot publish and has an operational reason.
    Terminal,
}

impl CloudMaterializationState {
    pub(crate) const fn as_sql(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Finalized => "finalized",
            Self::Terminal => "terminal",
        }
    }

    pub(crate) fn from_sql(value: &str) -> Result<Self, StoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "finalized" => Ok(Self::Finalized),
            "terminal" => Ok(Self::Terminal),
            _ => Err(StoreError::Storage {
                message: "invalid cloud materialization state".into(),
            }),
        }
    }
}

/// Identity and recovery record for one materialized Cloud partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudMaterialization {
    /// Deterministic Cloud publication identity.
    pub bundle_id: String,
    /// SHA-256 of the exact local inbox manifest.
    pub manifest_sha256: String,
    /// Closed market/day partition identifier.
    pub partition_id: String,
    /// Inbox manifest schema version.
    pub schema_version: u16,
    /// SHA-256 of the materialized artifact manifest.
    pub artifact_sha256: String,
    /// Current durable recovery state.
    pub state: CloudMaterializationState,
    /// Cloud release ID after finalization.
    pub release_id: Option<String>,
    /// Operational reason when no release is possible.
    pub terminal_reason: Option<String>,
}

/// Derives the stable publication ID from immutable materialization inputs.
#[must_use]
pub fn materialization_bundle_id(
    manifest_sha256: &str,
    partition_id: &str,
    schema_version: u16,
    artifact_sha256: &str,
) -> String {
    sha256_hex(
        format!("{manifest_sha256}\n{partition_id}\n{schema_version}\n{artifact_sha256}")
            .as_bytes(),
    )
}

/// Parses one local inbox manifest into a pending materialization identity.
///
/// # Errors
///
/// Returns [`StoreError`] when the manifest schema version or identity is invalid.
pub fn cloud_materialization_from_inbox(
    inbox_manifest: &Value,
    partition_id: &str,
    artifact_sha256: &str,
) -> Result<CloudMaterialization, StoreError> {
    let manifest = decode_sealed_closed_day_manifest(inbox_manifest.clone())?;
    cloud_materialization_from_sealed_manifest(&manifest, partition_id, artifact_sha256)
}

/// Builds a pending Cloud identity from a decoded sealed closed-day manifest.
///
/// # Errors
///
/// Returns [`StoreError`] when the partition or artifact digest is invalid.
pub fn cloud_materialization_from_sealed_manifest(
    inbox_manifest: &SealedClosedDayManifest,
    partition_id: &str,
    artifact_sha256: &str,
) -> Result<CloudMaterialization, StoreError> {
    if partition_id.is_empty() || artifact_sha256.len() != 64 {
        return Err(StoreError::Storage {
            message: "inbox manifest materialization identity is invalid".into(),
        });
    }
    let manifest_sha256 = sha256_hex(inbox_manifest.document().to_string().as_bytes());
    Ok(CloudMaterialization {
        bundle_id: materialization_bundle_id(
            &manifest_sha256,
            partition_id,
            inbox_manifest.schema_version(),
            artifact_sha256,
        ),
        manifest_sha256,
        partition_id: partition_id.into(),
        schema_version: inbox_manifest.schema_version(),
        artifact_sha256: artifact_sha256.into(),
        state: CloudMaterializationState::Pending,
        release_id: None,
        terminal_reason: None,
    })
}

/// Publishes a pending materialization and leaves it pending after an indeterminate response.
///
/// # Errors
///
/// Returns [`StoreError`] when durable materialization state cannot be read or written.
pub async fn reconcile_materialization<F, Fut>(
    store: &dyn TapeStore,
    materialization: &CloudMaterialization,
    publish: F,
) -> Result<CloudMaterialization, StoreError>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<String, StoreError>>,
{
    let pending = store
        .store_cloud_materialization_pending(materialization)
        .await?;
    if pending.state != CloudMaterializationState::Pending {
        return Ok(pending);
    }
    match publish(pending.bundle_id.clone()).await {
        Ok(release_id) => {
            store
                .transition_cloud_materialization(
                    &pending.bundle_id,
                    CloudMaterializationState::Finalized,
                    Some(&release_id),
                    None,
                )
                .await
        }
        Err(_) => Ok(pending),
    }
}

/// Records a gapped partition as terminal without creating a Cloud release.
///
/// # Errors
///
/// Returns [`StoreError`] when durable materialization state cannot be read or written.
pub async fn terminalize_operational_gap(
    store: &dyn TapeStore,
    materialization: &CloudMaterialization,
) -> Result<CloudMaterialization, StoreError> {
    let pending = store
        .store_cloud_materialization_pending(materialization)
        .await?;
    if pending.state != CloudMaterializationState::Pending {
        return Ok(pending);
    }
    store
        .transition_cloud_materialization(
            &pending.bundle_id,
            CloudMaterializationState::Terminal,
            None,
            Some("operational_gap"),
        )
        .await
}
