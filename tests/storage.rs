use std::collections::BTreeMap;

use s3lite::storage::{
    BucketConfig, Hash, Manifest, ManifestKey, ManifestKind, ManifestState, MetaError, MetaStore,
    PartRef, PartState, PartStore, UploadMode,
};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::io::AsyncReadExt;

// ---------------- PartStore ----------------

#[tokio::test]
async fn part_write_read_roundtrip() {
    let dir = TempDir::new().unwrap();
    let store = PartStore::open(dir.path()).await.unwrap();

    let payload = b"hello world".to_vec();
    let result = store.write_stream(payload.as_slice()).await.unwrap();
    assert_eq!(result.size, payload.len() as u64);

    let expected_hash: Hash = blake3::hash(&payload).into();
    assert_eq!(result.hash, expected_hash);

    let mut expected_md5 = md5::Md5::new();
    use md5::Digest;
    expected_md5.update(&payload);
    let md5_digest: [u8; 16] = expected_md5.finalize().into();
    assert_eq!(result.md5, md5_digest);

    let mut reader = store.open_read(&result.hash).await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, payload);
}

#[tokio::test]
async fn part_dedup_same_bytes() {
    let dir = TempDir::new().unwrap();
    let store = PartStore::open(dir.path()).await.unwrap();

    let bytes = b"dedup me twice".to_vec();
    let a = store.write_stream(bytes.as_slice()).await.unwrap();
    let b = store.write_stream(bytes.as_slice()).await.unwrap();
    assert_eq!(a.hash, b.hash);

    // Only one file should live under parts/<aa>/<bb>/<hash>
    let hex = hex::encode(a.hash);
    let part_path = dir
        .path()
        .join("parts")
        .join(&hex[..2])
        .join(&hex[2..4])
        .join(&hex);
    assert!(part_path.exists());

    // No leftover tmp files
    let mut tmp_iter = std::fs::read_dir(dir.path().join("tmp")).unwrap();
    assert!(tmp_iter.next().is_none(), "tmp dir should be empty after dedup");
}

#[tokio::test]
async fn part_delete_idempotent() {
    let dir = TempDir::new().unwrap();
    let store = PartStore::open(dir.path()).await.unwrap();

    let r = store.write_stream(b"x".as_slice()).await.unwrap();
    assert!(store.exists(&r.hash).await);
    store.delete(&r.hash).await.unwrap();
    assert!(!store.exists(&r.hash).await);
    // second delete should not error
    store.delete(&r.hash).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn part_file_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new().unwrap();
    let store = PartStore::open(dir.path()).await.unwrap();

    let r = store.write_stream(b"perm-check".as_slice()).await.unwrap();
    let hex = hex::encode(r.hash);
    let part_path = dir
        .path()
        .join("parts")
        .join(&hex[..2])
        .join(&hex[2..4])
        .join(&hex);
    let meta = std::fs::metadata(&part_path).unwrap();
    let mode = meta.permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "part file should be 0600, got {:o}", mode);
}

// ---------------- MetaStore: buckets ----------------

async fn fresh_meta() -> (TempDir, MetaStore) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("meta.redb");
    let meta = MetaStore::open(path).await.unwrap();
    (dir, meta)
}

#[tokio::test]
async fn bucket_crud_roundtrip() {
    let (_dir, meta) = fresh_meta().await;
    let cfg = BucketConfig::new("us-east-1");

    meta.create_bucket("alpha", cfg.clone()).await.unwrap();
    let got = meta.get_bucket("alpha").await.unwrap().unwrap();
    assert_eq!(got.region, "us-east-1");

    let listed = meta.list_buckets().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].0, "alpha");

    meta.delete_bucket("alpha").await.unwrap();
    assert!(meta.get_bucket("alpha").await.unwrap().is_none());
}

#[tokio::test]
async fn bucket_create_duplicate_rejected() {
    let (_dir, meta) = fresh_meta().await;
    meta.create_bucket("dup", BucketConfig::new("r")).await.unwrap();
    let err = meta
        .create_bucket("dup", BucketConfig::new("r"))
        .await
        .unwrap_err();
    assert!(matches!(err, MetaError::BucketExists(_)));
}

#[tokio::test]
async fn bucket_delete_missing_rejected() {
    let (_dir, meta) = fresh_meta().await;
    let err = meta.delete_bucket("ghost").await.unwrap_err();
    assert!(matches!(err, MetaError::BucketNotFound(_)));
}

#[tokio::test]
async fn bucket_delete_non_empty_rejected() {
    let (_dir, meta) = fresh_meta().await;
    meta.create_bucket("b", BucketConfig::new("r")).await.unwrap();
    let manifest = make_manifest("b", "k", "v1", &[fake_part(1, 7)], UploadMode::Multipart);
    meta.put_manifest(manifest).await.unwrap();
    let err = meta.delete_bucket("b").await.unwrap_err();
    assert!(matches!(err, MetaError::BucketNotEmpty(_)));
}

// ---------------- MetaStore: manifests + refcount ----------------

#[tokio::test]
async fn manifest_put_get_roundtrip() {
    let (_dir, meta) = fresh_meta().await;
    let parts = [fake_part(1, 100)];
    let m = make_manifest("b", "k", "v1", parts.as_slice(), UploadMode::SinglePut);
    let effect = meta.put_manifest(m.clone()).await.unwrap();
    assert!(effect.freed_parts.is_empty());

    let got = meta
        .get_manifest(ManifestKey::new("b", "k", "v1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts.len(), 1);
    assert_eq!(got.parts[0].hash, parts[0].hash);

    let entry = meta.get_part(parts[0].hash).await.unwrap().unwrap();
    assert_eq!(entry.refcount, 1);
    assert_eq!(entry.state, PartState::Live);
}

#[tokio::test]
async fn manifest_replace_frees_old_parts() {
    let (_dir, meta) = fresh_meta().await;
    let p1 = fake_part(1, 10);
    let p2 = fake_part(2, 20);

    let m1 = make_manifest("b", "k", "v1", std::slice::from_ref(&p1), UploadMode::SinglePut);
    meta.put_manifest(m1).await.unwrap();
    assert_eq!(meta.get_part(p1.hash).await.unwrap().unwrap().refcount, 1);

    let m2 = make_manifest("b", "k", "v1", std::slice::from_ref(&p2), UploadMode::SinglePut);
    let effect = meta.put_manifest(m2).await.unwrap();
    assert_eq!(effect.freed_parts, vec![p1.hash]);

    // p1 row still exists with refcount=0 (waiting for GC)
    let p1_entry = meta.get_part(p1.hash).await.unwrap().unwrap();
    assert_eq!(p1_entry.refcount, 0);
    assert_eq!(p1_entry.state, PartState::Live);

    let p2_entry = meta.get_part(p2.hash).await.unwrap().unwrap();
    assert_eq!(p2_entry.refcount, 1);
}

#[tokio::test]
async fn manifest_delete_frees_all_parts() {
    let (_dir, meta) = fresh_meta().await;
    let p1 = fake_part(1, 10);
    let p2 = fake_part(2, 20);

    let m = make_manifest("b", "k", "v1", &[p1.clone(), p2.clone()], UploadMode::Multipart);
    meta.put_manifest(m).await.unwrap();

    let effect = meta
        .delete_manifest(ManifestKey::new("b", "k", "v1"))
        .await
        .unwrap();
    let mut freed = effect.freed_parts.clone();
    freed.sort();
    let mut expected = vec![p1.hash, p2.hash];
    expected.sort();
    assert_eq!(freed, expected);

    assert!(meta
        .get_manifest(ManifestKey::new("b", "k", "v1"))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn part_gc_pending_lifecycle() {
    let (_dir, meta) = fresh_meta().await;
    let p = fake_part(1, 10);
    let m = make_manifest("b", "k", "v1", std::slice::from_ref(&p), UploadMode::SinglePut);
    meta.put_manifest(m).await.unwrap();

    // refcount > 0 → mark_gc_pending refused
    let marked = meta.mark_part_gc_pending(p.hash).await.unwrap();
    assert!(!marked);

    // After deleting manifest, refcount = 0
    let effect = meta
        .delete_manifest(ManifestKey::new("b", "k", "v1"))
        .await
        .unwrap();
    assert_eq!(effect.freed_parts, vec![p.hash]);

    let marked = meta.mark_part_gc_pending(p.hash).await.unwrap();
    assert!(marked);
    let entry = meta.get_part(p.hash).await.unwrap().unwrap();
    assert_eq!(entry.state, PartState::GcPending);

    let pending = meta.list_gc_pending_parts().await.unwrap();
    assert_eq!(pending, vec![p.hash]);

    meta.remove_part(p.hash).await.unwrap();
    assert!(meta.get_part(p.hash).await.unwrap().is_none());
}

#[tokio::test]
async fn put_referencing_gc_pending_part_rejected() {
    let (_dir, meta) = fresh_meta().await;
    let p = fake_part(1, 10);

    let m = make_manifest("b", "k", "v1", std::slice::from_ref(&p), UploadMode::SinglePut);
    meta.put_manifest(m).await.unwrap();
    meta.delete_manifest(ManifestKey::new("b", "k", "v1"))
        .await
        .unwrap();
    let marked = meta.mark_part_gc_pending(p.hash).await.unwrap();
    assert!(marked);

    let m2 = make_manifest("b", "k2", "v1", std::slice::from_ref(&p), UploadMode::SinglePut);
    let err = meta.put_manifest(m2).await.unwrap_err();
    assert!(matches!(err, MetaError::PartGcPending(_)));
}

#[tokio::test]
async fn append_part_then_complete_multipart() {
    let (_dir, meta) = fresh_meta().await;
    // Create in_progress manifest with no parts
    let mut m = make_manifest("b", "k", "u123", &[], UploadMode::Multipart);
    m.state = ManifestState::InProgress;
    meta.put_manifest(m).await.unwrap();

    let p1 = fake_part(1, 5);
    let p2 = fake_part(2, 7);
    meta.append_part(ManifestKey::new("b", "k", "u123"), p1.clone())
        .await
        .unwrap();
    meta.append_part(ManifestKey::new("b", "k", "u123"), p2.clone())
        .await
        .unwrap();

    let got = meta
        .get_manifest(ManifestKey::new("b", "k", "u123"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts.len(), 2);
    assert_eq!(got.size, 12);
    assert_eq!(got.parts[0].part_number, 1);
    assert_eq!(got.parts[1].part_number, 2);

    meta.update_manifest_state(
        ManifestKey::new("b", "k", "u123"),
        ManifestState::Committed,
    )
    .await
    .unwrap();

    let after = meta
        .get_manifest(ManifestKey::new("b", "k", "u123"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.state, ManifestState::Committed);
}

#[tokio::test]
async fn append_part_replaces_same_part_number() {
    let (_dir, meta) = fresh_meta().await;
    let mut m = make_manifest("b", "k", "u", &[], UploadMode::Multipart);
    m.state = ManifestState::InProgress;
    meta.put_manifest(m).await.unwrap();

    let p1a = fake_part(1, 5);
    let p1b = fake_part_with_seed(1, 5, 99); // same number, different hash
    meta.append_part(ManifestKey::new("b", "k", "u"), p1a.clone())
        .await
        .unwrap();
    let effect = meta
        .append_part(ManifestKey::new("b", "k", "u"), p1b.clone())
        .await
        .unwrap();
    assert_eq!(effect.freed_parts, vec![p1a.hash]);

    let got = meta
        .get_manifest(ManifestKey::new("b", "k", "u"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.parts.len(), 1);
    assert_eq!(got.parts[0].hash, p1b.hash);
}

// ---------------- ETag derivation ----------------

#[tokio::test]
async fn etag_single_put_is_part_md5() {
    let p = fake_part(1, 10);
    let m = make_manifest("b", "k", "v", std::slice::from_ref(&p), UploadMode::SinglePut);
    assert_eq!(m.etag(), format!("\"{}\"", hex::encode(p.md5)));
}

#[tokio::test]
async fn etag_multipart_has_dash_count() {
    let parts: Vec<PartRef> = (1..=3).map(|i| fake_part(i, 10)).collect();
    let m = make_manifest("b", "k", "v", &parts, UploadMode::Multipart);

    use md5::Digest;
    let mut h = md5::Md5::new();
    for p in &parts {
        h.update(p.md5);
    }
    let digest: [u8; 16] = h.finalize().into();
    assert_eq!(m.etag(), format!("\"{}-3\"", hex::encode(digest)));
}

// ---------------- helpers ----------------

fn make_manifest(
    bucket: &str,
    key: &str,
    version_id: &str,
    parts: &[PartRef],
    mode: UploadMode,
) -> Manifest {
    let now = OffsetDateTime::now_utc();
    Manifest {
        key: ManifestKey::new(bucket, key, version_id),
        state: ManifestState::Committed,
        kind: ManifestKind::Object,
        upload_mode: mode,
        parts: parts.to_vec(),
        size: parts.iter().map(|p| p.size).sum(),
        content_type: Some("application/octet-stream".into()),
        user_metadata: BTreeMap::new(),
        tags: BTreeMap::new(),
        additional_checksum: None,
        storage_class: "STANDARD".into(),
        object_lock: None,
        created_at: now,
        last_modified: now,
        upload_id: None,
    }
}

fn fake_part(part_number: u32, size: u64) -> PartRef {
    fake_part_with_seed(part_number, size, 0)
}

fn fake_part_with_seed(part_number: u32, size: u64, seed: u8) -> PartRef {
    let mut hash_bytes = [0u8; 32];
    hash_bytes[0] = part_number as u8;
    hash_bytes[1] = seed;
    let mut md5 = [0u8; 16];
    md5[0] = part_number as u8;
    md5[1] = seed;
    PartRef {
        part_number,
        hash: hash_bytes,
        md5,
        size,
    }
}
