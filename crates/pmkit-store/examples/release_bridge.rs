//! Drives the restart-safe Cloud materialization bridge against a local store.

use pmkit_store::{
    CloudMaterializationState, StoreError, TursoTapeStore, cloud_materialization_from_inbox,
    reconcile_materialization, terminalize_operational_gap,
};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("release-bridge.db");
    let (happy, pending, reconciled, gap) = {
        let store = TursoTapeStore::open_local(&path).await?;
        let inbox = json!({"version": 2, "day": "1970-01-02", "day_seal": "sealed"});
        let happy = cloud_materialization_from_inbox(&inbox, "m1:86400000", &"a".repeat(64))?;
        let lost = cloud_materialization_from_inbox(&inbox, "m2:86400000", &"b".repeat(64))?;
        let gap = cloud_materialization_from_inbox(&inbox, "m3:86400000", &"c".repeat(64))?;

        let happy =
            reconcile_materialization(&store, &happy, |_| async { Ok("release-happy".into()) })
                .await?;
        let pending = reconcile_materialization(&store, &lost, |_| async {
            Err(StoreError::Storage {
                message: "lost_finalize_response".into(),
            })
        })
        .await?;
        let reconciled =
            reconcile_materialization(&store, &lost, |_| async { Ok("release-reconciled".into()) })
                .await?;
        let gap = terminalize_operational_gap(&store, &gap).await?;
        drop(store);
        (happy, pending, reconciled, gap)
    };

    assert_eq!(happy.state, CloudMaterializationState::Finalized);
    assert_eq!(pending.state, CloudMaterializationState::Pending);
    assert_eq!(reconciled.state, CloudMaterializationState::Finalized);
    assert_eq!(gap.state, CloudMaterializationState::Terminal);
    println!(
        "happy={:?} lost={:?}->{:?} gap={:?}",
        happy.state, pending.state, reconciled.state, gap.state
    );
    Ok(())
}
