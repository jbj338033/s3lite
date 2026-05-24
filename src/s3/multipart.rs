use std::collections::BTreeMap;

use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::{
    Manifest, ManifestKey, ManifestKind, ManifestState, PartRef, UploadMode,
};
use crate::storage::MetaError;

use super::bucket::map_meta_err;
use super::checksum;
use super::state::AppState;
use super::xml::{
    CompleteMultipartUploadRequest, CompleteMultipartUploadResult, InitiateMultipartUploadResult,
    ListPartsPart, ListPartsResult, XmlBody,
};

const NULL_VERSION_ID: &str = "null";

/// `POST /bucket/key?uploads` — start a multipart upload, return upload_id.
pub async fn create_multipart_upload(
    state: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    require_bucket(&state, bucket).await?;

    let upload_id = Uuid::new_v4().simple().to_string();
    let now = OffsetDateTime::now_utc();
    let manifest = Manifest {
        key: ManifestKey::new(bucket, key, &upload_id),
        state: ManifestState::InProgress,
        kind: ManifestKind::Object,
        upload_mode: UploadMode::Multipart,
        parts: Vec::new(),
        size: 0,
        content_type: header_string(headers, &header::CONTENT_TYPE),
        user_metadata: extract_user_metadata(headers),
        tags: BTreeMap::new(),
        additional_checksum: None,
        storage_class: "STANDARD".into(),
        object_lock: None,
        created_at: now,
        last_modified: now,
        upload_id: Some(upload_id.clone()),
    };

    let effect = state
        .meta
        .put_manifest(manifest)
        .await
        .map_err(map_meta_err)?;
    gc_freed_parts(&state, &effect.freed_parts).await;

    let body = InitiateMultipartUploadResult {
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id,
    };
    Ok(XmlBody(body).into_response())
}

/// `PUT /bucket/key?partNumber=N&uploadId=...` — write one part for an
/// in-progress upload, return that part's ETag header.
pub async fn upload_part(
    state: AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: u32,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, S3Error> {
    if part_number == 0 || part_number > 10_000 {
        return Err(invalid("part-number must be between 1 and 10000")
            .with_resource(format!("/{bucket}/{key}")));
    }

    // Optional Content-MD5 (mirrors single PUT)
    if let Some(expected) = parse_content_md5(headers)? {
        let actual = compute_md5(&body);
        if expected != actual {
            return Err(
                S3Error::new(S3ErrorCode::BadDigest, "Content-MD5 mismatch")
                    .with_resource(format!("/{bucket}/{key}")),
            );
        }
    }

    let part_result = state
        .parts
        .write_bytes(body)
        .await
        .map_err(|e| S3Error::new(S3ErrorCode::InternalError, format!("part write: {e}")))?;

    let new_part = PartRef {
        part_number,
        hash: part_result.hash,
        md5: part_result.md5,
        size: part_result.size,
    };
    let effect = state
        .meta
        .append_part(ManifestKey::new(bucket, key, upload_id), new_part)
        .await
        .map_err(|e| map_upload_meta_err(e, bucket, key, upload_id))?;
    gc_freed_parts(&state, &effect.freed_parts).await;

    let part_etag = format!("\"{}\"", hex::encode(part_result.md5));
    let mut resp = StatusCode::OK.into_response();
    set_header(resp.headers_mut(), "etag", &part_etag);
    Ok(resp)
}

/// `POST /bucket/key?uploadId=...` with `<CompleteMultipartUpload>` body —
/// finalize the upload. Parts not listed in the body are dropped.
pub async fn complete_multipart_upload(
    state: AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: Bytes,
) -> Result<Response, S3Error> {
    let request: CompleteMultipartUploadRequest = quick_xml::de::from_reader(body.as_ref())
        .map_err(|e| invalid(format!("malformed CompleteMultipartUpload body: {e}")))?;

    if request.parts.is_empty() {
        return Err(invalid("CompleteMultipartUpload must list at least one part"));
    }
    let mut last_n = 0u32;
    for p in &request.parts {
        if p.part_number <= last_n {
            return Err(invalid("CompleteMultipartUpload parts must be sorted by part number"));
        }
        last_n = p.part_number;
    }

    let in_progress_key = ManifestKey::new(bucket, key, upload_id);
    let in_progress = state
        .meta
        .get_manifest(in_progress_key.clone())
        .await
        .map_err(map_meta_err)?
        .filter(|m| matches!(m.state, ManifestState::InProgress))
        .ok_or_else(|| no_such_upload(bucket, key, upload_id))?;

    // Verify every requested part exists and its ETag matches the stored
    // part_md5. Build the final manifest's parts vec in body order.
    let mut selected = Vec::with_capacity(request.parts.len());
    for req_part in &request.parts {
        let stored = in_progress
            .parts
            .iter()
            .find(|p| p.part_number == req_part.part_number)
            .ok_or_else(|| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    format!("part {} not found in upload", req_part.part_number),
                )
                .with_resource(format!("/{bucket}/{key}"))
            })?;
        let stored_etag = format!("\"{}\"", hex::encode(stored.md5));
        if req_part.etag.trim() != stored_etag {
            return Err(S3Error::new(
                S3ErrorCode::InvalidArgument,
                format!("ETag mismatch on part {}", req_part.part_number),
            )
            .with_resource(format!("/{bucket}/{key}")));
        }
        selected.push(stored.clone());
    }

    let total_size: u64 = selected.iter().map(|p| p.size).sum();
    let now = OffsetDateTime::now_utc();
    let committed = Manifest {
        key: ManifestKey::new(bucket, key, NULL_VERSION_ID),
        state: ManifestState::Committed,
        kind: ManifestKind::Object,
        upload_mode: UploadMode::Multipart,
        parts: selected,
        size: total_size,
        content_type: in_progress.content_type.clone(),
        user_metadata: in_progress.user_metadata.clone(),
        tags: in_progress.tags.clone(),
        additional_checksum: in_progress.additional_checksum.clone(),
        storage_class: in_progress.storage_class.clone(),
        object_lock: in_progress.object_lock.clone(),
        created_at: in_progress.created_at,
        last_modified: now,
        upload_id: None,
    };
    let etag = committed.etag();

    let effect = state
        .meta
        .complete_multipart_upload(in_progress_key, committed)
        .await
        .map_err(map_meta_err)?;
    gc_freed_parts(&state, &effect.freed_parts).await;

    let body = CompleteMultipartUploadResult {
        location: format!("/{bucket}/{key}"),
        bucket: bucket.to_string(),
        key: key.to_string(),
        etag,
    };
    Ok(XmlBody(body).into_response())
}

/// `DELETE /bucket/key?uploadId=...` — discard an in-progress upload.
pub async fn abort_multipart_upload(
    state: AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Response, S3Error> {
    let in_progress_key = ManifestKey::new(bucket, key, upload_id);
    // Verify it's in-progress before destroying — guards against deleting an
    // unrelated committed manifest that happens to share the storage key.
    let manifest = state
        .meta
        .get_manifest(in_progress_key.clone())
        .await
        .map_err(map_meta_err)?
        .filter(|m| matches!(m.state, ManifestState::InProgress))
        .ok_or_else(|| no_such_upload(bucket, key, upload_id))?;
    let _ = manifest;

    let effect = state
        .meta
        .delete_manifest(in_progress_key)
        .await
        .map_err(map_meta_err)?;
    gc_freed_parts(&state, &effect.freed_parts).await;

    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `GET /bucket/key?uploadId=...` — list parts uploaded so far for the given
/// upload id. Phase 6: returns all parts (no pagination); max-parts is echoed
/// but not enforced.
pub async fn list_parts(
    state: AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Response, S3Error> {
    let in_progress_key = ManifestKey::new(bucket, key, upload_id);
    let manifest = state
        .meta
        .get_manifest(in_progress_key)
        .await
        .map_err(map_meta_err)?
        .filter(|m| matches!(m.state, ManifestState::InProgress))
        .ok_or_else(|| no_such_upload(bucket, key, upload_id))?;

    let parts: Vec<ListPartsPart> = manifest
        .parts
        .iter()
        .map(|p| ListPartsPart {
            part_number: p.part_number,
            etag: format!("\"{}\"", hex::encode(p.md5)),
            size: p.size,
            last_modified: manifest
                .last_modified
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
        })
        .collect();

    let body = ListPartsResult {
        bucket: bucket.to_string(),
        key: key.to_string(),
        upload_id: upload_id.to_string(),
        max_parts: 1000,
        is_truncated: false,
        parts,
    };
    Ok(XmlBody(body).into_response())
}

// ---------------- helpers ----------------

async fn require_bucket(state: &AppState, bucket: &str) -> Result<(), S3Error> {
    if state
        .meta
        .get_bucket(bucket)
        .await
        .map_err(map_meta_err)?
        .is_none()
    {
        return Err(S3Error::new(S3ErrorCode::NoSuchBucket, "bucket does not exist")
            .with_resource(format!("/{bucket}")));
    }
    Ok(())
}

fn no_such_upload(bucket: &str, key: &str, upload_id: &str) -> S3Error {
    S3Error::new(
        S3ErrorCode::NoSuchUpload,
        format!("upload id {upload_id} not found"),
    )
    .with_resource(format!("/{bucket}/{key}"))
}

fn invalid(msg: impl Into<String>) -> S3Error {
    S3Error::new(S3ErrorCode::InvalidArgument, msg)
}

fn map_upload_meta_err(e: MetaError, bucket: &str, key: &str, upload_id: &str) -> S3Error {
    match e {
        MetaError::ManifestNotFound(_) => no_such_upload(bucket, key, upload_id),
        other => map_meta_err(other),
    }
}

async fn gc_freed_parts(state: &AppState, freed: &[crate::storage::Hash]) {
    for hash in freed {
        match state.meta.mark_part_gc_pending(*hash).await {
            Ok(true) => {
                let _ = state.parts.delete(hash).await;
                let _ = state.meta.remove_part(*hash).await;
            }
            Ok(false) => {}
            Err(_) => {}
        }
    }
}

fn header_string(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

fn extract_user_metadata(headers: &HeaderMap) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (k, v) in headers {
        let name = k.as_str();
        if let Some(suffix) = name.strip_prefix("x-amz-meta-")
            && let Ok(value) = v.to_str()
        {
            out.insert(suffix.to_string(), value.to_string());
        }
    }
    out
}

fn parse_content_md5(headers: &HeaderMap) -> Result<Option<[u8; 16]>, S3Error> {
    let Some(v) = headers.get("content-md5") else {
        return Ok(None);
    };
    let v = v.to_str().map_err(|_| invalid("invalid Content-MD5 header"))?;
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(v)
        .map_err(|_| invalid("Content-MD5 not valid base64"))?;
    bytes
        .as_slice()
        .try_into()
        .map(Some)
        .map_err(|_| invalid("Content-MD5 must be 16 bytes"))
}

fn compute_md5(data: &[u8]) -> [u8; 16] {
    use md5::Digest;
    let mut h = md5::Md5::new();
    h.update(data);
    h.finalize().into()
}

fn set_header(h: &mut HeaderMap, name: &str, value: &str) {
    if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::from_str(value)) {
        h.insert(n, v);
    }
}

// Silence unused-import warning until additional_checksum is parsed at
// CreateMultipartUpload time (Phase 6 keeps it None; revisit when checksum-
// over-trailer support arrives in later phases).
#[allow(dead_code)]
fn _checksum_module_used() {
    let _ = checksum::header_name_for;
}
