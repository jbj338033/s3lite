use std::collections::BTreeMap;

use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncReadExt;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::{
    Manifest, ManifestKey, ManifestKind, ManifestState, PartRef, VersioningState,
};

use super::bucket::map_meta_err;
use super::state::AppState;
use super::xml::{CopyObjectResult, CopyPartResult, XmlBody};

const NULL_VERSION_ID: &str = "null";

#[derive(Debug, Clone)]
pub struct CopySource {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

/// Parse the `x-amz-copy-source` header. Accepts `bucket/key` or `/bucket/key`,
/// URL-encoded bytes in both segments, and optional `?versionId=<vid>` suffix.
pub fn parse_copy_source(raw: &str) -> Result<CopySource, S3Error> {
    let trimmed = raw.strip_prefix('/').unwrap_or(raw);
    let (path_part, version_part) = match trimmed.split_once('?') {
        Some((p, v)) => (p, Some(v)),
        None => (trimmed, None),
    };
    let (bucket_raw, key_raw) = path_part.split_once('/').ok_or_else(|| {
        S3Error::new(
            S3ErrorCode::InvalidArgument,
            "x-amz-copy-source must be bucket/key",
        )
    })?;
    if bucket_raw.is_empty() || key_raw.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            "x-amz-copy-source bucket and key must be non-empty",
        ));
    }
    let bucket = percent_decode(bucket_raw);
    let key = percent_decode(key_raw);
    let version_id = version_part.and_then(|v| {
        v.split('&').find_map(|pair| {
            let (k, val) = pair.split_once('=')?;
            if k == "versionId" {
                Some(percent_decode(val))
            } else {
                None
            }
        })
    });
    Ok(CopySource {
        bucket,
        key,
        version_id,
    })
}

/// `PUT /dst-bucket/dst-key` with `x-amz-copy-source` — server-side copy.
/// Reuses the source manifest's part hashes (natural dedup, no byte rewrite).
pub async fn copy_object(
    state: AppState,
    dest_bucket: &str,
    dest_key: &str,
    source: CopySource,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let dest_cfg = state
        .meta
        .get_bucket(dest_bucket)
        .await
        .map_err(map_meta_err)?
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::NoSuchBucket, "destination bucket does not exist")
                .with_resource(format!("/{dest_bucket}"))
        })?;

    let source_manifest = resolve_source(&state, &source).await?;

    let directive = header_string(headers, "x-amz-metadata-directive")
        .unwrap_or_else(|| "COPY".to_string());
    let tagging_directive = header_string(headers, "x-amz-tagging-directive")
        .unwrap_or_else(|| "COPY".to_string());

    let (content_type, user_metadata) = if directive == "REPLACE" {
        (
            header_string(headers, header::CONTENT_TYPE.as_str()),
            extract_user_metadata(headers),
        )
    } else {
        (
            source_manifest.content_type.clone(),
            source_manifest.user_metadata.clone(),
        )
    };
    let tags = if tagging_directive == "REPLACE" {
        BTreeMap::new()
    } else {
        source_manifest.tags.clone()
    };

    let dest_version_id = next_version_id(dest_cfg.versioning);
    let now = OffsetDateTime::now_utc();
    let manifest = Manifest {
        key: ManifestKey::new(dest_bucket, dest_key, &dest_version_id),
        state: ManifestState::Committed,
        kind: ManifestKind::Object,
        upload_mode: source_manifest.upload_mode,
        parts: source_manifest.parts.clone(),
        size: source_manifest.size,
        content_type,
        user_metadata,
        tags,
        additional_checksum: source_manifest.additional_checksum.clone(),
        storage_class: source_manifest.storage_class.clone(),
        // Lock is not inherited on copy — destination starts fresh.
        object_lock: None,
        created_at: now,
        last_modified: now,
        upload_id: None,
    };
    let etag = manifest.etag();
    let last_modified = manifest.last_modified;
    let effect = state
        .meta
        .put_manifest(manifest)
        .await
        .map_err(map_meta_err)?;
    gc_freed_parts(&state, &effect.freed_parts).await;

    let body = CopyObjectResult {
        etag,
        last_modified: format_iso8601(last_modified),
    };
    let mut resp = XmlBody(body).into_response();
    if matches!(dest_cfg.versioning, VersioningState::Enabled) {
        set_header(resp.headers_mut(), "x-amz-version-id", &dest_version_id);
    }
    if let Some(vid) = &source.version_id {
        set_header(resp.headers_mut(), "x-amz-copy-source-version-id", vid);
    }
    Ok(resp)
}

/// `PUT /dst-bucket/dst-key?partNumber=N&uploadId=...` with `x-amz-copy-source`
/// (and optional `x-amz-copy-source-range`). The byte slice (possibly across
/// multiple source parts) is hashed afresh and stored as a new part for the
/// destination upload.
pub async fn upload_part_copy(
    state: AppState,
    dest_bucket: &str,
    dest_key: &str,
    upload_id: &str,
    part_number: u32,
    source: CopySource,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    if part_number == 0 || part_number > 10_000 {
        return Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            "part-number must be between 1 and 10000",
        )
        .with_resource(format!("/{dest_bucket}/{dest_key}")));
    }

    let source_manifest = resolve_source(&state, &source).await?;

    let (start, end) = match header_string(headers, "x-amz-copy-source-range") {
        Some(r) => parse_copy_range(&r, source_manifest.size)?,
        None => {
            if source_manifest.size == 0 {
                return Err(S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    "cannot copy a zero-byte source as a part",
                )
                .with_resource(format!("/{dest_bucket}/{dest_key}")));
            }
            (0, source_manifest.size - 1)
        }
    };

    let payload = read_source_range(&state, &source_manifest, start, end).await?;
    let part_result = state
        .parts
        .write_bytes(Bytes::from(payload))
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
        .append_part(
            ManifestKey::new(dest_bucket, dest_key, upload_id),
            new_part,
        )
        .await
        .map_err(|e| map_upload_meta_err(e, dest_bucket, dest_key, upload_id))?;
    gc_freed_parts(&state, &effect.freed_parts).await;

    let body = CopyPartResult {
        etag: format!("\"{}\"", hex::encode(part_result.md5)),
        last_modified: format_iso8601(OffsetDateTime::now_utc()),
    };
    let mut resp = XmlBody(body).into_response();
    if let Some(vid) = &source.version_id {
        set_header(resp.headers_mut(), "x-amz-copy-source-version-id", vid);
    }
    Ok(resp)
}

// ---------------- helpers ----------------

async fn resolve_source(state: &AppState, source: &CopySource) -> Result<Manifest, S3Error> {
    let resolved = match &source.version_id {
        Some(vid) => state
            .meta
            .get_manifest(ManifestKey::new(&source.bucket, &source.key, vid))
            .await
            .map_err(map_meta_err)?
            .filter(|m| {
                matches!(m.state, ManifestState::Committed) && matches!(m.kind, ManifestKind::Object)
            }),
        None => state
            .meta
            .get_latest_version(&source.bucket, &source.key)
            .await
            .map_err(map_meta_err)?
            .filter(|m| {
                matches!(m.state, ManifestState::Committed) && matches!(m.kind, ManifestKind::Object)
            }),
    };
    resolved.ok_or_else(|| {
        S3Error::new(S3ErrorCode::NoSuchKey, "copy source does not exist")
            .with_resource(format!("/{}/{}", source.bucket, source.key))
    })
}

async fn read_source_range(
    state: &AppState,
    manifest: &Manifest,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, S3Error> {
    let mut out = Vec::with_capacity((end - start + 1) as usize);
    let mut cursor = 0u64;
    for p in &manifest.parts {
        let part_start = cursor;
        let part_end = cursor + p.size - 1;
        cursor += p.size;

        if part_end < start || part_start > end {
            continue;
        }

        let local_start = if part_start < start {
            (start - part_start) as usize
        } else {
            0
        };
        let local_end_exclusive = if part_end > end {
            (end - part_start + 1) as usize
        } else {
            p.size as usize
        };

        let mut file = state.parts.open_read(&p.hash).await.map_err(|e| {
            S3Error::new(S3ErrorCode::InternalError, format!("source part open: {e}"))
        })?;
        let mut buf = Vec::with_capacity(p.size as usize);
        file.read_to_end(&mut buf).await.map_err(|e| {
            S3Error::new(S3ErrorCode::InternalError, format!("source part read: {e}"))
        })?;
        out.extend_from_slice(&buf[local_start..local_end_exclusive]);
    }
    Ok(out)
}

fn parse_copy_range(value: &str, source_size: u64) -> Result<(u64, u64), S3Error> {
    let spec = value
        .strip_prefix("bytes=")
        .ok_or_else(|| invalid("x-amz-copy-source-range must use bytes unit"))?;
    let (start_str, end_str) = spec
        .split_once('-')
        .ok_or_else(|| invalid("x-amz-copy-source-range must contain '-'"))?;
    if start_str.is_empty() || end_str.is_empty() {
        return Err(invalid("x-amz-copy-source-range must be inclusive bytes=N-M"));
    }
    let start: u64 = start_str
        .parse()
        .map_err(|_| invalid("invalid copy-source-range start"))?;
    let end: u64 = end_str
        .parse()
        .map_err(|_| invalid("invalid copy-source-range end"))?;
    if source_size == 0 {
        return Err(invalid("source object is empty"));
    }
    if start > end || end >= source_size {
        return Err(invalid("copy-source-range out of bounds"));
    }
    Ok((start, end))
}

fn invalid(msg: impl Into<String>) -> S3Error {
    S3Error::new(S3ErrorCode::InvalidArgument, msg)
}

fn next_version_id(state: VersioningState) -> String {
    match state {
        VersioningState::Enabled => uuid::Uuid::new_v4().simple().to_string(),
        VersioningState::Off | VersioningState::Suspended => NULL_VERSION_ID.to_string(),
    }
}

fn map_upload_meta_err(
    e: crate::storage::MetaError,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> S3Error {
    match e {
        crate::storage::MetaError::ManifestNotFound(_) => S3Error::new(
            S3ErrorCode::NoSuchUpload,
            format!("upload id {upload_id} not found"),
        )
        .with_resource(format!("/{bucket}/{key}")),
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

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(String::from)
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

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_nibble(bytes[i + 1]);
            let lo = hex_nibble(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn format_iso8601(t: OffsetDateTime) -> String {
    t.to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn set_header(h: &mut HeaderMap, name: &str, value: &str) {
    if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::from_str(value)) {
        h.insert(n, v);
    }
}

// Status code constant — kept for symmetry with other handler modules.
#[allow(dead_code)]
fn _ok() -> StatusCode {
    StatusCode::OK
}
