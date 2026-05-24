use std::collections::BTreeMap;

use s3lite::storage::{
    Manifest, ManifestKey, ManifestKind, ManifestState, PartRef, UploadMode,
};
use time::OffsetDateTime;

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
