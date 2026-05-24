use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::{
    BucketConfig, Manifest, ManifestKey, ManifestKind, ManifestState, PartRef, UploadMode,
    VersioningState,
};

use super::bucket::map_meta_err;
use super::checksum;
use super::state::AppState;
use super::tagging::parse_x_amz_tagging;

const NULL_VERSION_ID: &str = "null";

/// Generate a new version id based on bucket versioning state.
/// Enabled → uuid v4 (32 hex chars). Off/Suspended → literal "null".
fn next_version_id(state: VersioningState) -> String {
    match state {
        VersioningState::Enabled => Uuid::new_v4().simple().to_string(),
        VersioningState::Off | VersioningState::Suspended => NULL_VERSION_ID.to_string(),
    }
}

pub async fn put_object(
    state: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, S3Error> {
    let bucket_cfg = require_bucket(&state, bucket).await?;
    check_put_preconditions(&state, bucket, key, headers).await?;

    // Legacy Content-MD5 verification (S3 returns BadDigest on mismatch).
    if let Some(expected) = parse_content_md5(headers)? {
        let actual = compute_md5(&body);
        if expected != actual {
            return Err(S3Error::new(
                S3ErrorCode::BadDigest,
                "Content-MD5 header did not match computed digest",
            )
            .with_resource(format!("/{bucket}/{key}")));
        }
    }

    // Additional checksum (x-amz-checksum-*). Parsed, verified, persisted,
    // and echoed back in the response so SDKs can confirm integrity.
    let additional_checksum = checksum::parse_request_checksum(headers).map_err(|e| {
        e.with_resource(format!("/{bucket}/{key}"))
    })?;
    if let Some(ac) = &additional_checksum {
        checksum::verify(ac, &body).map_err(|e| e.with_resource(format!("/{bucket}/{key}")))?;
    }

    let part_result = state
        .parts
        .write_bytes(body)
        .await
        .map_err(|e| S3Error::new(S3ErrorCode::InternalError, format!("part write: {e}")))?;

    let version_id = next_version_id(bucket_cfg.versioning);
    let tags = parse_x_amz_tagging(headers)
        .map_err(|e| e.with_resource(format!("/{bucket}/{key}")))?;
    let now = OffsetDateTime::now_utc();
    let manifest = Manifest {
        key: ManifestKey::new(bucket, key, &version_id),
        state: ManifestState::Committed,
        kind: ManifestKind::Object,
        upload_mode: UploadMode::SinglePut,
        parts: vec![PartRef {
            part_number: 1,
            hash: part_result.hash,
            md5: part_result.md5,
            size: part_result.size,
        }],
        size: part_result.size,
        content_type: header_string(headers, &header::CONTENT_TYPE),
        user_metadata: extract_user_metadata(headers),
        tags,
        additional_checksum: additional_checksum.clone(),
        storage_class: "STANDARD".into(),
        object_lock: None,
        created_at: now,
        last_modified: now,
        upload_id: None,
    };
    let etag = manifest.etag();
    let effect = state
        .meta
        .put_manifest(manifest)
        .await
        .map_err(map_meta_err)?;

    gc_freed_parts(&state, &effect.freed_parts).await;

    let mut resp = StatusCode::OK.into_response();
    let resp_headers = resp.headers_mut();
    set_header(resp_headers, "etag", &etag);
    if matches!(bucket_cfg.versioning, VersioningState::Enabled) {
        set_header(resp_headers, "x-amz-version-id", &version_id);
    }
    if let Some(ac) = &additional_checksum {
        set_header(
            resp_headers,
            checksum::header_name_for(ac.algorithm),
            &checksum::encode_value(&ac.value),
        );
    }
    Ok(resp)
}

pub async fn get_or_head_object(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    headers: &HeaderMap,
    body_only_headers: bool,
) -> Result<Response, S3Error> {
    require_bucket(&state, bucket).await?;
    let manifest = match version_id {
        Some(vid) => state
            .meta
            .get_manifest(ManifestKey::new(bucket, key, vid))
            .await
            .map_err(map_meta_err)?
            .filter(|m| matches!(m.state, ManifestState::Committed))
            .ok_or_else(|| no_such_key(bucket, key))?,
        None => state
            .meta
            .get_latest_version(bucket, key)
            .await
            .map_err(map_meta_err)?
            .ok_or_else(|| no_such_key(bucket, key))?,
    };
    // Tombstone = latest delete marker, surface as NoSuchKey (without versionId)
    // or DeleteMarker response (with versionId targeting the marker itself).
    if matches!(manifest.kind, ManifestKind::Tombstone) {
        if version_id.is_none() {
            return Err(no_such_key(bucket, key));
        } else {
            // Targeted GET on a delete marker — S3 returns 405 MethodNotAllowed
            // with x-amz-delete-marker: true. Map to a clear error for now.
            return Err(S3Error::new(
                S3ErrorCode::InvalidRequest,
                "the specified version is a delete marker",
            )
            .with_resource(format!("/{bucket}/{key}")));
        }
    }

    let etag = manifest.etag();
    if let Some(short_circuit) =
        evaluate_preconditions(headers, &etag, manifest.last_modified, bucket, key)
    {
        return Ok(short_circuit);
    }

    let range = parse_range_header(headers, manifest.size)?;
    let body_bytes = if body_only_headers {
        Bytes::new()
    } else {
        // Multipart objects: concatenate parts in part_number order. For
        // small/medium objects (Phase 6's scope), buffer everything then
        // slice. Streaming-multipart-GET that reads one part at a time
        // belongs in a later optimization pass.
        let mut buf = Vec::with_capacity(manifest.size as usize);
        for part_ref in &manifest.parts {
            let mut file = state.parts.open_read(&part_ref.hash).await.map_err(|e| {
                S3Error::new(S3ErrorCode::InternalError, format!("part open: {e}"))
            })?;
            file.read_to_end(&mut buf).await.map_err(|e| {
                S3Error::new(S3ErrorCode::InternalError, format!("part read: {e}"))
            })?;
        }
        match &range {
            Some((start, end)) => Bytes::copy_from_slice(&buf[*start as usize..=*end as usize]),
            None => Bytes::from(buf),
        }
    };

    let mut resp_builder = Response::builder();
    if range.is_some() {
        resp_builder = resp_builder.status(StatusCode::PARTIAL_CONTENT);
    }
    let mut resp = resp_builder
        .body(Body::from(body_bytes))
        .map_err(|e| S3Error::new(S3ErrorCode::InternalError, format!("response: {e}")))?;

    let h = resp.headers_mut();
    set_header(h, "etag", &etag);
    set_header(h, "last-modified", &format_http_date(manifest.last_modified));
    set_header(
        h,
        "x-amz-version-id",
        manifest.key.version_id.as_str(),
    );
    if let Some(ct) = &manifest.content_type {
        set_header(h, header::CONTENT_TYPE.as_str(), ct);
    } else {
        set_header(h, header::CONTENT_TYPE.as_str(), "binary/octet-stream");
    }
    if let Some((start, end)) = range {
        set_header(
            h,
            "content-range",
            &format!("bytes {start}-{end}/{}", manifest.size),
        );
        set_header(h, header::CONTENT_LENGTH.as_str(), &(end - start + 1).to_string());
    } else {
        set_header(
            h,
            header::CONTENT_LENGTH.as_str(),
            &manifest.size.to_string(),
        );
    }
    set_header(h, "accept-ranges", "bytes");
    set_header(h, "x-amz-storage-class", &manifest.storage_class);
    // Checksums apply to the full object; do not echo them on partial-content
    // responses (SDKs validate against received bytes and would mismatch).
    if range.is_none()
        && let Some(ac) = &manifest.additional_checksum
    {
        set_header(
            h,
            checksum::header_name_for(ac.algorithm),
            &checksum::encode_value(&ac.value),
        );
    }
    for (k, v) in &manifest.user_metadata {
        let header_name = format!("x-amz-meta-{k}");
        set_header(h, &header_name, v);
    }
    Ok(resp)
}

pub async fn delete_object(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<Response, S3Error> {
    let bucket_cfg = require_bucket(&state, bucket).await?;

    // Targeted version delete is always a permanent removal — bypasses
    // tombstone semantics.
    if let Some(vid) = version_id {
        return delete_specific_version(&state, bucket, key, vid).await;
    }

    match bucket_cfg.versioning {
        VersioningState::Off => {
            match state
                .meta
                .delete_manifest(ManifestKey::new(bucket, key, NULL_VERSION_ID))
                .await
            {
                Ok(effect) => {
                    gc_freed_parts(&state, &effect.freed_parts).await;
                    Ok(StatusCode::NO_CONTENT.into_response())
                }
                Err(crate::storage::MetaError::ManifestNotFound(_)) => {
                    Ok(StatusCode::NO_CONTENT.into_response())
                }
                Err(e) => Err(map_meta_err(e)),
            }
        }
        VersioningState::Enabled | VersioningState::Suspended => {
            // Insert a delete marker (tombstone). Enabled gets a fresh uuid;
            // Suspended overwrites the "null" version slot.
            let tombstone_version = next_version_id(bucket_cfg.versioning);
            let now = OffsetDateTime::now_utc();
            let tombstone = Manifest {
                key: ManifestKey::new(bucket, key, &tombstone_version),
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
            let effect = state.meta.put_manifest(tombstone).await.map_err(map_meta_err)?;
            gc_freed_parts(&state, &effect.freed_parts).await;
            let mut resp = StatusCode::NO_CONTENT.into_response();
            set_header(resp.headers_mut(), "x-amz-delete-marker", "true");
            if matches!(bucket_cfg.versioning, VersioningState::Enabled) {
                set_header(resp.headers_mut(), "x-amz-version-id", &tombstone_version);
            }
            Ok(resp)
        }
    }
}

async fn delete_specific_version(
    state: &AppState,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Response, S3Error> {
    let target = ManifestKey::new(bucket, key, version_id);
    let maybe = state.meta.get_manifest(target.clone()).await.map_err(map_meta_err)?;
    let is_delete_marker = matches!(
        maybe.as_ref().map(|m| m.kind),
        Some(ManifestKind::Tombstone)
    );
    match state.meta.delete_manifest(target).await {
        Ok(effect) => {
            gc_freed_parts(state, &effect.freed_parts).await;
            let mut resp = StatusCode::NO_CONTENT.into_response();
            set_header(resp.headers_mut(), "x-amz-version-id", version_id);
            if is_delete_marker {
                set_header(resp.headers_mut(), "x-amz-delete-marker", "true");
            }
            Ok(resp)
        }
        Err(crate::storage::MetaError::ManifestNotFound(_)) => {
            // S3 returns 204 for delete of non-existent version too.
            let mut resp = StatusCode::NO_CONTENT.into_response();
            set_header(resp.headers_mut(), "x-amz-version-id", version_id);
            Ok(resp)
        }
        Err(e) => Err(map_meta_err(e)),
    }
}

// ---------------- helpers ----------------

async fn require_bucket(state: &AppState, bucket: &str) -> Result<BucketConfig, S3Error> {
    state
        .meta
        .get_bucket(bucket)
        .await
        .map_err(map_meta_err)?
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::NoSuchBucket, "bucket does not exist")
                .with_resource(format!("/{bucket}"))
        })
}

fn no_such_key(bucket: &str, key: &str) -> S3Error {
    S3Error::new(S3ErrorCode::NoSuchKey, "key does not exist")
        .with_resource(format!("/{bucket}/{key}"))
}

/// Run the 3-step race-safe GC protocol synchronously for the given parts.
/// Inline now; a background sweeper takes over in Phase 11.
async fn gc_freed_parts(state: &AppState, freed: &[crate::storage::Hash]) {
    for hash in freed {
        match state.meta.mark_part_gc_pending(*hash).await {
            Ok(true) => {
                if let Err(e) = state.parts.delete(hash).await {
                    tracing::warn!(hash = %hex::encode(hash), error = %e, "gc part file delete failed");
                }
                if let Err(e) = state.meta.remove_part(*hash).await {
                    tracing::warn!(hash = %hex::encode(hash), error = %e, "gc part row remove failed");
                }
            }
            Ok(false) => {} // resurrected by concurrent writer — leave alone
            Err(e) => tracing::warn!(hash = %hex::encode(hash), error = %e, "gc mark failed"),
        }
    }
}

fn header_string(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

fn parse_content_md5(headers: &HeaderMap) -> Result<Option<[u8; 16]>, S3Error> {
    let Some(v) = headers.get("content-md5") else {
        return Ok(None);
    };
    let v = v.to_str().map_err(|_| {
        S3Error::new(S3ErrorCode::InvalidRequest, "invalid Content-MD5 header")
    })?;
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(v)
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidRequest, "Content-MD5 not valid base64"))?;
    bytes
        .as_slice()
        .try_into()
        .map(Some)
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidRequest, "Content-MD5 must be 16 bytes"))
}

fn compute_md5(data: &[u8]) -> [u8; 16] {
    use md5::Digest;
    let mut h = md5::Md5::new();
    h.update(data);
    h.finalize().into()
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

fn parse_range_header(
    headers: &HeaderMap,
    object_size: u64,
) -> Result<Option<(u64, u64)>, S3Error> {
    let Some(v) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let v = v
        .to_str()
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument, "invalid Range header"))?;
    let spec = v
        .strip_prefix("bytes=")
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument, "Range must use bytes unit"))?;
    if spec.contains(',') {
        return Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            "multi-range requests are not supported",
        ));
    }
    let (start_str, end_str) = spec.split_once('-').ok_or_else(|| {
        S3Error::new(S3ErrorCode::InvalidArgument, "Range must contain '-'")
    })?;
    let last_index = object_size.saturating_sub(1);
    let (start, end) = match (start_str.is_empty(), end_str.is_empty()) {
        (false, false) => {
            let s: u64 = start_str
                .parse()
                .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument, "bad Range start"))?;
            let e: u64 = end_str
                .parse()
                .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument, "bad Range end"))?;
            (s, e.min(last_index))
        }
        (false, true) => {
            let s: u64 = start_str
                .parse()
                .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument, "bad Range start"))?;
            (s, last_index)
        }
        (true, false) => {
            let suffix: u64 = end_str
                .parse()
                .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument, "bad suffix Range"))?;
            if suffix == 0 || object_size == 0 {
                return Ok(None);
            }
            let s = object_size.saturating_sub(suffix);
            (s, last_index)
        }
        (true, true) => {
            return Err(S3Error::new(
                S3ErrorCode::InvalidArgument,
                "Range must have at least one bound",
            ));
        }
    };
    if object_size == 0 || start > last_index || start > end {
        // Treat as if no range; S3 also accepts 416 here, but for Phase 3
        // simplicity we return the whole object.
        return Ok(None);
    }
    Ok(Some((start, end)))
}

/// Match an `If-Match` / `If-None-Match` header value (single ETag, `*`, or
/// comma-separated list) against an object's current ETag.
fn etag_matches(raw: &str, etag: &str) -> bool {
    let trimmed = raw.trim();
    trimmed == "*" || trimmed.split(',').any(|p| p.trim() == etag)
}

/// Pre-check conditional headers on PUT.
///
/// `If-Match` — replace only when the current object's ETag matches (or `*`
/// means "any existing"); missing/mismatch → 412.
/// `If-None-Match` — create only when no current object matches; existing or
/// `*` with existing → 412.
async fn check_put_preconditions(
    state: &AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<(), S3Error> {
    let if_match = headers.get("if-match").and_then(|v| v.to_str().ok());
    let if_none_match = headers.get("if-none-match").and_then(|v| v.to_str().ok());
    if if_match.is_none() && if_none_match.is_none() {
        return Ok(());
    }

    let existing = state
        .meta
        .get_manifest(ManifestKey::new(bucket, key, NULL_VERSION_ID))
        .await
        .map_err(map_meta_err)?
        .filter(|m| {
            matches!(m.state, ManifestState::Committed) && matches!(m.kind, ManifestKind::Object)
        });

    let resource = format!("/{bucket}/{key}");

    if let Some(im) = if_match {
        let pass = existing
            .as_ref()
            .is_some_and(|m| etag_matches(im, &m.etag()));
        if !pass {
            return Err(
                S3Error::new(S3ErrorCode::PreconditionFailed, "If-Match failed")
                    .with_resource(resource),
            );
        }
    }
    if let Some(inm) = if_none_match {
        let blocks = existing
            .as_ref()
            .is_some_and(|m| etag_matches(inm, &m.etag()));
        if blocks {
            return Err(
                S3Error::new(S3ErrorCode::PreconditionFailed, "If-None-Match failed")
                    .with_resource(resource),
            );
        }
    }
    Ok(())
}

/// Returns Some(response) for short-circuit precondition results (304 / 412).
fn evaluate_preconditions(
    headers: &HeaderMap,
    etag: &str,
    last_modified: OffsetDateTime,
    bucket: &str,
    key: &str,
) -> Option<Response> {
    let if_match = headers.get("if-match").and_then(|v| v.to_str().ok());
    let if_none_match = headers.get("if-none-match").and_then(|v| v.to_str().ok());
    let if_modified = headers
        .get("if-modified-since")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_http_date);
    let if_unmodified = headers
        .get("if-unmodified-since")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_http_date);

    if let Some(im) = if_match
        && !etag_matches(im, etag)
    {
        return Some(
            S3Error::new(S3ErrorCode::PreconditionFailed, "If-Match failed")
                .with_resource(format!("/{bucket}/{key}"))
                .into_response(),
        );
    }
    if let Some(inm) = if_none_match
        && etag_matches(inm, etag)
    {
        // 304 Not Modified
        return Some(StatusCode::NOT_MODIFIED.into_response());
    }
    if let Some(t) = if_unmodified
        && last_modified > t
    {
        return Some(
            S3Error::new(S3ErrorCode::PreconditionFailed, "If-Unmodified-Since failed")
                .with_resource(format!("/{bucket}/{key}"))
                .into_response(),
        );
    }
    if let Some(t) = if_modified
        && last_modified <= t
    {
        return Some(StatusCode::NOT_MODIFIED.into_response());
    }
    None
}

fn format_http_date(t: OffsetDateTime) -> String {
    let fmt = format_description!(
        "[weekday repr:short], [day padding:zero] [month repr:short] [year] [hour]:[minute]:[second] GMT"
    );
    t.to_offset(time::UtcOffset::UTC)
        .format(&fmt)
        .unwrap_or_default()
}

fn parse_http_date(s: &str) -> Option<OffsetDateTime> {
    // IMF-fixdate (RFC 7231): "Sun, 06 Nov 1994 08:49:37 GMT"
    let fmt = format_description!(
        "[weekday repr:short], [day padding:zero] [month repr:short] [year] [hour]:[minute]:[second] GMT"
    );
    OffsetDateTime::parse(s, &fmt).ok().or_else(|| {
        // Fallback: try RFC 3339 (some clients send ISO-8601)
        OffsetDateTime::parse(s, &Rfc3339).ok()
    })
}

fn set_header(h: &mut HeaderMap, name: &str, value: &str) {
    if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::from_str(value)) {
        h.insert(n, v);
    }
}
