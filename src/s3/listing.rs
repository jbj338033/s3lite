use std::collections::HashMap;

use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::{ListCursor, ListItem, ListObjectsRequest, MetaError};

use super::bucket::map_meta_err;
use super::state::AppState;
use super::xml::{
    CommonPrefix, ListBucketResultV1, ListBucketResultV2, ObjectContent, XmlBody,
};

const DEFAULT_MAX_KEYS: u32 = 1000;
const MAX_MAX_KEYS: u32 = 1000;

/// Entry point for `GET /bucket` with neither `?location` nor any other
/// per-bucket subresource. Picks V1 vs V2 by the `list-type` query value.
pub async fn list_objects(
    state: AppState,
    bucket: &str,
    query: &str,
) -> Result<Response, S3Error> {
    require_bucket(&state, bucket).await?;
    let params = parse_query(query);
    if params.get("list-type").map(String::as_str) == Some("2") {
        list_v2(state, bucket, &params).await
    } else {
        list_v1(state, bucket, &params).await
    }
}

async fn list_v2(
    state: AppState,
    bucket: &str,
    params: &HashMap<String, String>,
) -> Result<Response, S3Error> {
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned().filter(|s| !s.is_empty());
    let max_keys = parse_max_keys(params.get("max-keys"))?;
    let encoding = parse_encoding(params.get("encoding-type"))?;
    let continuation = params.get("continuation-token").cloned();
    let start_after = params.get("start-after").cloned();

    // ContinuationToken (if any) overrides StartAfter.
    let cursor = if let Some(token) = &continuation {
        Some(decode_continuation_token(token)?)
    } else {
        start_after.clone().map(ListCursor::AfterKey)
    };

    let page = state
        .meta
        .list_objects(ListObjectsRequest {
            bucket: bucket.to_string(),
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
            cursor,
            limit: max_keys as usize,
        })
        .await
        .map_err(map_meta_err)?;

    let (contents, common_prefixes) = split_items(page.items, encoding.as_deref());
    let key_count = (contents.len() + common_prefixes.len()) as u32;
    let next_token = page
        .next_cursor
        .as_ref()
        .map(encode_continuation_token)
        .transpose()?;

    let body = ListBucketResultV2 {
        name: bucket.to_string(),
        prefix: encode_value(&prefix, encoding.as_deref()),
        key_count,
        max_keys,
        delimiter: delimiter.map(|d| encode_value(&d, encoding.as_deref())),
        is_truncated: page.truncated,
        encoding_type: encoding,
        continuation_token: continuation,
        next_continuation_token: next_token,
        start_after,
        contents,
        common_prefixes,
    };
    Ok(XmlBody(body).into_response())
}

async fn list_v1(
    state: AppState,
    bucket: &str,
    params: &HashMap<String, String>,
) -> Result<Response, S3Error> {
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned().filter(|s| !s.is_empty());
    let max_keys = parse_max_keys(params.get("max-keys"))?;
    let encoding = parse_encoding(params.get("encoding-type"))?;
    let marker = params.get("marker").cloned().unwrap_or_default();

    let cursor = if marker.is_empty() {
        None
    } else {
        Some(ListCursor::AfterKey(marker.clone()))
    };

    let page = state
        .meta
        .list_objects(ListObjectsRequest {
            bucket: bucket.to_string(),
            prefix: prefix.clone(),
            delimiter: delimiter.clone(),
            cursor,
            limit: max_keys as usize,
        })
        .await
        .map_err(map_meta_err)?;

    let (contents, common_prefixes) = split_items(page.items, encoding.as_deref());

    // V1 NextMarker rule: only set when IsTruncated AND a delimiter is present;
    // for non-delimited V1 results, the client uses the last returned key as
    // the next marker themselves.
    let next_marker = if page.truncated && delimiter.is_some() {
        page.next_cursor.as_ref().map(|c| match c {
            ListCursor::AfterKey(k) => encode_value(k, encoding.as_deref()),
            ListCursor::AfterPrefix(p) => encode_value(p, encoding.as_deref()),
        })
    } else {
        None
    };

    let body = ListBucketResultV1 {
        name: bucket.to_string(),
        prefix: encode_value(&prefix, encoding.as_deref()),
        marker,
        max_keys,
        delimiter: delimiter.map(|d| encode_value(&d, encoding.as_deref())),
        is_truncated: page.truncated,
        encoding_type: encoding,
        next_marker,
        contents,
        common_prefixes,
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

fn parse_query(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if query.is_empty() {
        return map;
    }
    for pair in query.split('&') {
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        let key = url_percent_decode(key);
        let value = url_percent_decode(value);
        map.insert(key, value);
    }
    map
}

fn url_percent_decode(s: &str) -> String {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'+' {
            out.push(b' ');
            i += 1;
        } else if b == b'%' && i + 2 < bytes.len() {
            let hi = hex_nibble(bytes[i + 1]);
            let lo = hex_nibble(bytes[i + 2]);
            match (hi, lo) {
                (Some(h), Some(l)) => {
                    out.push((h << 4) | l);
                    i += 3;
                }
                _ => {
                    out.push(b);
                    i += 1;
                }
            }
        } else {
            out.push(b);
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

fn parse_max_keys(value: Option<&String>) -> Result<u32, S3Error> {
    let Some(v) = value else {
        return Ok(DEFAULT_MAX_KEYS);
    };
    let n: u32 = v
        .parse()
        .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument, "max-keys is not a number"))?;
    if n == 0 {
        return Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            "max-keys must be >= 1",
        ));
    }
    Ok(n.min(MAX_MAX_KEYS))
}

fn parse_encoding(value: Option<&String>) -> Result<Option<String>, S3Error> {
    let Some(v) = value else { return Ok(None) };
    match v.as_str() {
        "url" => Ok(Some("url".to_string())),
        other => Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            format!("unsupported encoding-type '{other}'"),
        )),
    }
}

fn split_items(
    items: Vec<ListItem>,
    encoding: Option<&str>,
) -> (Vec<ObjectContent>, Vec<CommonPrefix>) {
    let mut contents = Vec::new();
    let mut prefixes = Vec::new();
    for item in items {
        match item {
            ListItem::Object(m) => {
                contents.push(ObjectContent {
                    key: encode_value(&m.key.key, encoding),
                    last_modified: format_iso8601(m.last_modified),
                    etag: m.etag(),
                    size: m.size,
                    storage_class: m.storage_class.clone(),
                });
            }
            ListItem::CommonPrefix(p) => {
                prefixes.push(CommonPrefix {
                    prefix: encode_value(&p, encoding),
                });
            }
        }
    }
    (contents, prefixes)
}

fn format_iso8601(t: OffsetDateTime) -> String {
    t.to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn encode_value(value: &str, encoding: Option<&str>) -> String {
    if encoding == Some("url") {
        url_percent_encode(value)
    } else {
        value.to_string()
    }
}

/// Minimal RFC 3986 unreserved set: A-Z a-z 0-9 `-_.~`. Everything else,
/// including `/`, is percent-encoded — matches AWS `encoding-type=url` output.
fn url_percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if unreserved {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn encode_continuation_token(cursor: &ListCursor) -> Result<String, S3Error> {
    let bytes = bincode::serde::encode_to_vec(cursor, bincode::config::standard())
        .map_err(|e| S3Error::new(S3ErrorCode::InternalError, format!("token encode: {e}")))?;
    Ok(B64.encode(bytes))
}

fn decode_continuation_token(token: &str) -> Result<ListCursor, S3Error> {
    let bytes = B64.decode(token).map_err(|_| {
        S3Error::new(
            S3ErrorCode::InvalidArgument,
            "continuation-token is not valid base64",
        )
    })?;
    let (cursor, _) =
        bincode::serde::decode_from_slice::<ListCursor, _>(&bytes, bincode::config::standard())
            .map_err(|_| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    "continuation-token is malformed",
                )
            })?;
    Ok(cursor)
}

// Quiet unused-import warning when only MetaError is referenced via map_meta_err.
#[allow(dead_code)]
fn _unused() -> std::marker::PhantomData<MetaError> {
    std::marker::PhantomData
}
