//! Integration tests for the offline admin functions: backup, restore,
//! scan-rebuild. Drives the library directly without spawning the binary.

use std::collections::BTreeMap;

use s3lite::admin;
use s3lite::storage::manifest::{
    BucketConfig, Manifest, ManifestKey, ManifestKind, ManifestState, PartRef, UploadMode,
};
use s3lite::storage::{MetaStore, PartStore};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::io::AsyncReadExt;

/// Seed a data_dir with one bucket and one single-part object. Returns
/// the part hash for later assertions.
async fn seed_data_dir(dir: &std::path::Path) -> [u8; 32] {
    let meta = MetaStore::open(dir.join("meta.redb")).await.unwrap();
    let parts = PartStore::open(dir).await.unwrap();
    meta.create_bucket("backup-test", BucketConfig::new("us-east-1"))
        .await
        .unwrap();
    let r = parts.write_bytes(bytes::Bytes::from_static(b"hello")).await.unwrap();
    let now = OffsetDateTime::now_utc();
    let manifest = Manifest {
        key: ManifestKey::new("backup-test", "k", "null"),
        state: ManifestState::Committed,
        kind: ManifestKind::Object,
        upload_mode: UploadMode::SinglePut,
        parts: vec![PartRef {
            part_number: 1,
            hash: r.hash,
            md5: r.md5,
            size: r.size,
        }],
        size: r.size,
        content_type: Some("text/plain".into()),
        user_metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        additional_checksum: None,
        storage_class: "STANDARD".into(),
        object_lock: None,
        created_at: now,
        last_modified: now,
        upload_id: None,
    };
    meta.put_manifest(manifest).await.unwrap();
    drop(meta);
    drop(parts);
    r.hash
}

#[tokio::test]
async fn backup_then_restore_round_trip() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = TempDir::new().unwrap();
    let hash = seed_data_dir(src.path()).await;

    let backup_report = admin::backup(src.path(), dst.path()).await.unwrap();
    assert_eq!(backup_report.buckets, 1);
    assert_eq!(backup_report.manifests, 1);
    assert_eq!(backup_report.parts_copied, 1);
    assert!(backup_report.parts_missing.is_empty());

    let restore_report = admin::restore(dst.path(), target.path()).unwrap();
    assert_eq!(restore_report.parts_copied, 1);

    // Open the restored data dir and read the object back.
    let meta = MetaStore::open(target.path().join("meta.redb")).await.unwrap();
    let parts = PartStore::open(target.path()).await.unwrap();
    let m = meta
        .get_manifest(ManifestKey::new("backup-test", "k", "null"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.parts.len(), 1);
    assert_eq!(m.parts[0].hash, hash);
    let mut file = parts.open_read(&hash).await.unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"hello");
}

#[tokio::test]
async fn restore_refuses_to_overwrite_existing_meta() {
    let snapshot = TempDir::new().unwrap();
    let target = TempDir::new().unwrap();
    // Place an empty meta.redb in the target so restore must refuse.
    std::fs::write(target.path().join("meta.redb"), b"existing").unwrap();
    // snapshot must have a meta.redb for the check to even start
    std::fs::write(snapshot.path().join("meta.redb"), b"snapshot").unwrap();
    let err = admin::restore(snapshot.path(), target.path()).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("meta.redb"), "expected meta.redb mention, got {msg}");
}

#[tokio::test]
async fn scan_rebuild_detects_corrupted_part() {
    let src = TempDir::new().unwrap();
    let hash = seed_data_dir(src.path()).await;

    // Intact scan first
    let ok = admin::scan_rebuild(src.path()).unwrap();
    assert_eq!(ok.parts_checked, 1);
    assert_eq!(ok.parts_passed, 1);
    assert!(ok.corrupted.is_empty());

    // Corrupt the part file
    let hex = hex::encode(hash);
    let path = src
        .path()
        .join("parts")
        .join(&hex[..2])
        .join(&hex[2..4])
        .join(&hex);
    std::fs::write(&path, b"tampered").unwrap();

    let report = admin::scan_rebuild(src.path()).unwrap();
    assert_eq!(report.parts_checked, 1);
    assert_eq!(report.parts_passed, 0);
    assert_eq!(report.corrupted, vec![hex]);
}
