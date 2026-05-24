use std::collections::BTreeMap;

use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use crate::storage::manifest::{
    LifecycleRule, LifecycleStatus, Manifest, ManifestKey, ManifestKind, ManifestState,
    UploadMode, VersioningState,
};
use crate::storage::MetaError;

use super::state::AppState;

const NULL_VERSION_ID: &str = "null";

/// Summary of what one sweep pass did. Surfaced for tests and observability.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    pub expired_current: u32,
    pub expired_noncurrent: u32,
    pub aborted_multipart: u32,
    pub gc_parts_removed: u32,
}

/// Single sweep cycle: evaluate lifecycle rules then GC orphan parts.
/// `now` is parameterized so tests can simulate the passage of time without
/// monkey-patching the system clock.
pub async fn sweep_at(state: &AppState, now: OffsetDateTime) -> Result<SweepReport, MetaError> {
    let mut report = sweep_lifecycle_at(state, now).await?;
    let gc = sweep_gc(state).await?;
    report.gc_parts_removed = gc;
    Ok(report)
}

/// Production entry point — uses the current wall clock.
pub async fn sweep_once(state: &AppState) -> Result<SweepReport, MetaError> {
    sweep_at(state, OffsetDateTime::now_utc()).await
}

/// Spawn a tokio task that calls `sweep_once` on the given cadence until the
/// returned cancellation handle is dropped. Used by the server entrypoint.
pub fn spawn_daemon(state: AppState, interval: std::time::Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = sweep_once(&state).await {
                tracing::warn!(error = %e, "maintenance sweep failed");
            }
        }
    })
}

// ---------------- lifecycle ----------------

async fn sweep_lifecycle_at(
    state: &AppState,
    now: OffsetDateTime,
) -> Result<SweepReport, MetaError> {
    let mut report = SweepReport::default();
    let buckets = state.meta.list_buckets().await?;
    for (bucket_name, cfg) in buckets {
        let active_rules: Vec<&LifecycleRule> = cfg
            .lifecycle_rules
            .iter()
            .filter(|r| matches!(r.status, LifecycleStatus::Enabled))
            .collect();
        if active_rules.is_empty() {
            continue;
        }
        let manifests = state
            .meta
            .list_all_manifests_in_bucket(&bucket_name)
            .await?;
        // Group by key for "is this version current?" determination
        let mut by_key: BTreeMap<String, Vec<Manifest>> = BTreeMap::new();
        for m in manifests {
            by_key.entry(m.key.key.clone()).or_default().push(m);
        }
        for (_key, mut versions) in by_key {
            // Newest committed object/tombstone is "current"
            versions.sort_by(|a, b| b.last_modified.cmp(&a.last_modified));
            let current_version_id: Option<String> = versions
                .iter()
                .find(|m| matches!(m.state, ManifestState::Committed))
                .map(|m| m.key.version_id.clone());
            for m in versions.iter() {
                evaluate_manifest(
                    state,
                    &bucket_name,
                    &cfg.versioning,
                    m,
                    current_version_id.as_deref(),
                    &active_rules,
                    now,
                    &mut report,
                )
                .await?;
            }
        }
    }
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_manifest(
    state: &AppState,
    bucket: &str,
    versioning: &VersioningState,
    manifest: &Manifest,
    current_version_id: Option<&str>,
    rules: &[&LifecycleRule],
    now: OffsetDateTime,
    report: &mut SweepReport,
) -> Result<(), MetaError> {
    // First matching rule with the relevant action wins. Filter is prefix-only.
    let matching = |rule: &LifecycleRule| -> bool {
        match &rule.filter_prefix {
            Some(p) => manifest.key.key.starts_with(p.as_str()),
            None => true,
        }
    };

    match manifest.state {
        ManifestState::InProgress => {
            for rule in rules {
                if !matching(rule) {
                    continue;
                }
                if let Some(a) = &rule.abort_incomplete_multipart_upload {
                    let age = now - manifest.created_at;
                    if age >= Duration::days(a.days_after_initiation as i64) {
                        let effect = state
                            .meta
                            .delete_manifest(manifest.key.clone())
                            .await?;
                        // Inline GC opportunity, but sweep_gc will pick stragglers
                        // up regardless.
                        let _ = effect;
                        report.aborted_multipart += 1;
                        return Ok(());
                    }
                }
            }
        }
        ManifestState::Committed => {
            let is_current = current_version_id == Some(manifest.key.version_id.as_str());
            // Object Lock takes precedence over lifecycle for object-kind manifests.
            if let Some(lock) = &manifest.object_lock
                && lock.forbids_delete(now)
            {
                return Ok(());
            }
            for rule in rules {
                if !matching(rule) {
                    continue;
                }
                if is_current
                    && matches!(manifest.kind, ManifestKind::Object)
                    && let Some(exp) = &rule.expiration
                {
                    let age = now - manifest.created_at;
                    if age >= Duration::days(exp.days as i64) {
                        expire_current(state, bucket, versioning, manifest, now).await?;
                        report.expired_current += 1;
                        return Ok(());
                    }
                }
                if !is_current
                    && let Some(n) = &rule.noncurrent_version_expiration
                {
                    let age = now - manifest.created_at;
                    if age >= Duration::days(n.noncurrent_days as i64) {
                        let _ = state.meta.delete_manifest(manifest.key.clone()).await?;
                        report.expired_noncurrent += 1;
                        return Ok(());
                    }
                }
            }
        }
        ManifestState::Aborted => {
            // Aborted multipart manifests should be cleaned up; treat them as
            // perpetually expired.
            let _ = state.meta.delete_manifest(manifest.key.clone()).await?;
            report.aborted_multipart += 1;
        }
    }
    Ok(())
}

/// Expire the current version: under Enabled versioning, insert a fresh
/// tombstone; under Off, hard-delete the "null" manifest.
async fn expire_current(
    state: &AppState,
    bucket: &str,
    versioning: &VersioningState,
    manifest: &Manifest,
    now: OffsetDateTime,
) -> Result<(), MetaError> {
    match versioning {
        VersioningState::Off => {
            let _ = state.meta.delete_manifest(manifest.key.clone()).await?;
        }
        VersioningState::Enabled | VersioningState::Suspended => {
            let new_version = if matches!(versioning, VersioningState::Enabled) {
                Uuid::new_v4().simple().to_string()
            } else {
                NULL_VERSION_ID.to_string()
            };
            let tombstone = Manifest {
                key: ManifestKey::new(bucket, &manifest.key.key, &new_version),
                state: ManifestState::Committed,
                kind: ManifestKind::Tombstone,
                upload_mode: UploadMode::SinglePut,
                parts: Vec::new(),
                size: 0,
                content_type: None,
                user_metadata: BTreeMap::new(),
                tags: BTreeMap::new(),
                additional_checksum: None,
                storage_class: "STANDARD".into(),
                object_lock: None,
                created_at: now,
                last_modified: now,
                upload_id: None,
            };
            let _ = state.meta.put_manifest(tombstone).await?;
        }
    }
    Ok(())
}

// ---------------- GC ----------------

/// Walk every part with refcount=0 + state=Live and apply the 3-step
/// race-safe GC protocol: mark gc_pending (atomic recheck) → unlink the
/// data file → drop the row. Returns how many parts were actually removed.
pub async fn sweep_gc(state: &AppState) -> Result<u32, MetaError> {
    let orphans = state.meta.list_orphan_parts().await?;
    let mut removed = 0u32;
    for hash in orphans {
        match state.meta.mark_part_gc_pending(hash).await {
            Ok(true) => {
                if let Err(e) = state.parts.delete(&hash).await {
                    tracing::warn!(hash = %hex::encode(hash), error = %e, "gc unlink failed");
                    continue;
                }
                if let Err(e) = state.meta.remove_part(hash).await {
                    tracing::warn!(hash = %hex::encode(hash), error = %e, "gc remove row failed");
                    continue;
                }
                removed += 1;
            }
            Ok(false) => {} // refcount climbed back up — left alone
            Err(e) => tracing::warn!(hash = %hex::encode(hash), error = %e, "gc mark failed"),
        }
    }
    Ok(removed)
}
