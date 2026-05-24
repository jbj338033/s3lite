use std::collections::BTreeMap;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::MetaError;
use crate::storage::manifest::{ManifestKey, ManifestKind, ManifestState};

use super::bucket::map_meta_err;
use super::state::AppState;
use super::xml::{Tag, TagSet, Tagging, XmlBody};

/// `GET /bucket/key?tagging` — fetch the current tag set for an object.
pub async fn get_object_tagging(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<Response, S3Error> {
    let manifest = load_target(&state, bucket, key, version_id).await?;
    let tags: Vec<Tag> = manifest
        .tags
        .iter()
        .map(|(k, v)| Tag {
            key: k.clone(),
            value: v.clone(),
        })
        .collect();
    let body = Tagging {
        tag_set: TagSet { tags },
    };
    Ok(XmlBody(body).into_response())
}

/// `PUT /bucket/key?tagging` with `<Tagging>` body — replace tags.
pub async fn put_object_tagging(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    body: Bytes,
) -> Result<Response, S3Error> {
    let manifest = load_target(&state, bucket, key, version_id).await?;
    let parsed: Tagging = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        S3Error::new(
            S3ErrorCode::InvalidRequest,
            format!("malformed Tagging body: {e}"),
        )
        .with_resource(format!("/{bucket}/{key}"))
    })?;
    let tags: BTreeMap<String, String> = parsed
        .tag_set
        .tags
        .into_iter()
        .map(|t| (t.key, t.value))
        .collect();
    state
        .meta
        .update_manifest_tags(manifest.key.clone(), tags)
        .await
        .map_err(map_meta_err)?;
    Ok(StatusCode::OK.into_response())
}

/// `DELETE /bucket/key?tagging` — clear all tags.
pub async fn delete_object_tagging(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<Response, S3Error> {
    let manifest = load_target(&state, bucket, key, version_id).await?;
    state
        .meta
        .update_manifest_tags(manifest.key.clone(), BTreeMap::new())
        .await
        .map_err(map_meta_err)?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// Parse the `x-amz-tagging` header (URL-encoded query-string form,
/// e.g. `k1=v1&k2=v2`) into a tag map. Used by `PutObject` to set tags at
/// creation time.
pub fn parse_x_amz_tagging(headers: &HeaderMap) -> Result<BTreeMap<String, String>, S3Error> {
    let Some(raw) = headers.get("x-amz-tagging") else {
        return Ok(BTreeMap::new());
    };
    let raw = raw
        .to_str()
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidRequest, "invalid x-amz-tagging header"))?;
    let mut out = BTreeMap::new();
    if raw.is_empty() {
        return Ok(out);
    }
    for pair in raw.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(k);
        let value = percent_decode(v);
        if key.is_empty() {
            continue;
        }
        out.insert(key, value);
    }
    Ok(out)
}

// ---------------- helpers ----------------

async fn load_target(
    state: &AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<crate::storage::manifest::Manifest, S3Error> {
    let result = match version_id {
        Some(vid) => state
            .meta
            .get_manifest(ManifestKey::new(bucket, key, vid))
            .await
            .map_err(map_meta_err)?,
        None => state
            .meta
            .get_latest_version(bucket, key)
            .await
            .map_err(map_meta_err)?,
    };
    result
        .filter(|m| {
            matches!(m.state, ManifestState::Committed) && matches!(m.kind, ManifestKind::Object)
        })
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::NoSuchKey, "key does not exist")
                .with_resource(format!("/{bucket}/{key}"))
        })
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_nibble(bytes[i + 1]);
            let lo = hex_nibble(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
            out.push(bytes[i]);
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
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

#[allow(dead_code)]
fn _meta_err_used() -> std::marker::PhantomData<MetaError> {
    std::marker::PhantomData
}
