use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use tokio::io::AsyncReadExt;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::{
    Manifest, ManifestKey, ManifestKind, ManifestState, PartRef, UploadMode,
};

use super::bucket::map_meta_err;
use super::checksum;
use super::state::AppState;

const NULL_VERSION_ID: &str = "null";

pub async fn put_object(
    state: AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response, S3Error> {
    require_bucket(&state, bucket).await?;
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

    let now = OffsetDateTime::now_utc();
    let manifest = Manifest {
        key: ManifestKey::new(bucket, key, NULL_VERSION_ID),
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
        tags: BTreeMap::new(),
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
    headers: &HeaderMap,
    body_only_headers: bool,
) -> Result<Response, S3Error> {
    require_bucket(&state, bucket).await?;
    let manifest = state
        .meta
        .get_manifest(ManifestKey::new(bucket, key, NULL_VERSION_ID))
        .await
        .map_err(map_meta_err)?
        .filter(|m| {
            matches!(m.state, ManifestState::Committed) && matches!(m.kind, ManifestKind::Object)
        })
        .ok_or_else(|| no_such_key(bucket, key))?;

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
        // Phase 3: single-part objects only. Open the lone part and slice if range.
        let part_ref = manifest
            .parts
            .first()
            .ok_or_else(|| S3Error::new(S3ErrorCode::InternalError, "empty manifest"))?;
        let mut file = state
            .parts
            .open_read(&part_ref.hash)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::InternalError, format!("part open: {e}")))?;
        let mut buf = Vec::with_capacity(part_ref.size as usize);
        file.read_to_end(&mut buf)
            .await
            .map_err(|e| S3Error::new(S3ErrorCode::InternalError, format!("part read: {e}")))?;
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
) -> Result<Response, S3Error> {
    require_bucket(&state, bucket).await?;
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
            // S3 returns 204 even on deleting a non-existent key.
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        Err(e) => Err(map_meta_err(e)),
    }
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
