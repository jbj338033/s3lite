use std::collections::BTreeMap;

use s3lite::storage::{
    Hash, Manifest, ManifestKey, ManifestKind, ManifestState, PartRef, PartStore, UploadMode,
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

    let hex = hex::encode(a.hash);
    let part_path = dir
        .path()
        .join("parts")
        .join(&hex[..2])
        .join(&hex[2..4])
        .join(&hex);
    assert!(part_path.exists());

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

// ---------------- ETag derivation ----------------

#[test]
fn etag_single_put_is_part_md5() {
    let p = fake_part(1, 10);
    let m = make_manifest("b", "k", "v", std::slice::from_ref(&p), UploadMode::SinglePut);
    assert_eq!(m.etag(), format!("\"{}\"", hex::encode(p.md5)));
}

#[test]
fn etag_multipart_has_dash_count() {
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
    let mut hash_bytes = [0u8; 32];
    hash_bytes[0] = part_number as u8;
    let mut md5 = [0u8; 16];
    md5[0] = part_number as u8;
    PartRef {
        part_number,
        hash: hash_bytes,
        md5,
        size,
    }
}
