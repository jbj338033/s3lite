use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::http::error::{S3Error, S3ErrorCode};
use crate::http::request_context::RequestId;

use super::addressing::{Addressing, extract};
use super::state::AppState;
use super::{bucket, copy, cors, listing, lock, multipart, object, tagging};

/// Single entry point for every S3-shaped request. Dispatches by
/// (method, addressing, query) to the appropriate bucket/object handler.
pub async fn dispatch(State(state): State<AppState>, req: Request) -> Response {
    let request_id = req.extensions().get::<RequestId>().cloned();
    let (parts, body) = req.into_parts();
    let host_header = parts
        .headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok());
    let uri_path = parts.uri.path();
    let query = parts.uri.query().unwrap_or("");

    let addressing = match extract(host_header, uri_path, &state.config) {
        Ok(a) => a,
        Err(e) => return decorate(e, request_id, uri_path).into_response(),
    };

    let body_bytes = match axum::body::to_bytes(body, state.config.max_signed_body_bytes).await {
        Ok(b) => b,
        Err(_) => {
            return decorate(
                S3Error::new(S3ErrorCode::InvalidRequest, "request body too large"),
                request_id,
                uri_path,
            )
            .into_response();
        }
    };

    let result = handle(
        state,
        parts.method.clone(),
        addressing,
        query,
        &parts.headers,
        body_bytes,
    )
    .await;
    match result {
        Ok(r) => r,
        Err(e) => decorate(e, request_id, uri_path).into_response(),
    }
}

async fn handle(
    state: AppState,
    method: Method,
    addressing: Addressing,
    query: &str,
    headers: &axum::http::HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, S3Error> {
    let upload_id = query_value(query, "uploadId");
    let part_number = query_value(query, "partNumber");

    match (method, addressing.bucket, addressing.key) {
        (Method::GET, None, _) => bucket::list_buckets(state).await,

        // Bucket-level subresources (must precede the catch-all bucket arms)
        (Method::PUT, Some(b), None) if has_query_flag(query, "versioning") => {
            bucket::put_bucket_versioning(state, &b, body.clone()).await
        }
        (Method::GET, Some(b), None) if has_query_flag(query, "versioning") => {
            bucket::get_bucket_versioning(state, &b).await
        }
        (Method::GET, Some(b), None) if has_query_flag(query, "location") => {
            bucket::get_bucket_location(state, &b).await
        }
        (Method::GET, Some(b), None) if has_query_flag(query, "versions") => {
            listing::list_object_versions(state, &b, query).await
        }
        (Method::PUT, Some(b), None) if has_query_flag(query, "cors") => {
            cors::put_bucket_cors(state, &b, body.clone()).await
        }
        (Method::GET, Some(b), None) if has_query_flag(query, "cors") => {
            cors::get_bucket_cors(state, &b).await
        }
        (Method::DELETE, Some(b), None) if has_query_flag(query, "cors") => {
            cors::delete_bucket_cors(state, &b).await
        }
        (Method::PUT, Some(b), None) if has_query_flag(query, "object-lock") => {
            lock::put_bucket_object_lock_config(state, &b, body.clone()).await
        }
        (Method::GET, Some(b), None) if has_query_flag(query, "object-lock") => {
            lock::get_bucket_object_lock_config(state, &b).await
        }
        (Method::OPTIONS, Some(b), _) => cors::preflight(state, &b, headers).await,
        // Bucket-level catch-alls
        (Method::PUT, Some(b), None) => bucket::create_bucket(state, &b, headers).await,
        (Method::DELETE, Some(b), None) => bucket::delete_bucket(state, &b).await,
        (Method::HEAD, Some(b), None) => bucket::head_bucket(state, &b).await,
        (Method::GET, Some(b), None) => listing::list_objects(state, &b, query).await,

        // Multipart upload
        (Method::POST, Some(b), Some(k)) if has_query_flag(query, "uploads") => {
            multipart::create_multipart_upload(state, &b, &k, headers).await
        }
        (Method::POST, Some(b), Some(k)) if upload_id.is_some() => {
            multipart::complete_multipart_upload(
                state,
                &b,
                &k,
                upload_id.as_deref().unwrap(),
                body,
            )
            .await
        }
        (Method::PUT, Some(b), Some(k))
            if upload_id.is_some()
                && part_number.is_some()
                && headers.get("x-amz-copy-source").is_some() =>
        {
            let pn: u32 = part_number.as_deref().unwrap().parse().map_err(|_| {
                S3Error::new(S3ErrorCode::InvalidArgument, "partNumber must be a number")
            })?;
            let source = parse_copy_source_header(headers)?;
            copy::upload_part_copy(
                state,
                &b,
                &k,
                upload_id.as_deref().unwrap(),
                pn,
                source,
                headers,
            )
            .await
        }
        (Method::PUT, Some(b), Some(k))
            if upload_id.is_some() && part_number.is_some() =>
        {
            let pn: u32 = part_number.as_deref().unwrap().parse().map_err(|_| {
                S3Error::new(S3ErrorCode::InvalidArgument, "partNumber must be a number")
            })?;
            multipart::upload_part(
                state,
                &b,
                &k,
                upload_id.as_deref().unwrap(),
                pn,
                headers,
                body,
            )
            .await
        }
        (Method::DELETE, Some(b), Some(k)) if upload_id.is_some() => {
            multipart::abort_multipart_upload(state, &b, &k, upload_id.as_deref().unwrap()).await
        }
        (Method::GET, Some(b), Some(k)) if upload_id.is_some() => {
            multipart::list_parts(state, &b, &k, upload_id.as_deref().unwrap()).await
        }

        // Object-level subresources
        (Method::PUT, Some(b), Some(k)) if has_query_flag(query, "tagging") => {
            let vid = query_value(query, "versionId");
            tagging::put_object_tagging(state, &b, &k, vid.as_deref(), body.clone()).await
        }
        (Method::GET, Some(b), Some(k)) if has_query_flag(query, "tagging") => {
            let vid = query_value(query, "versionId");
            tagging::get_object_tagging(state, &b, &k, vid.as_deref()).await
        }
        (Method::DELETE, Some(b), Some(k)) if has_query_flag(query, "tagging") => {
            let vid = query_value(query, "versionId");
            tagging::delete_object_tagging(state, &b, &k, vid.as_deref()).await
        }
        (Method::PUT, Some(b), Some(k)) if has_query_flag(query, "retention") => {
            let vid = query_value(query, "versionId");
            lock::put_object_retention(state, &b, &k, vid.as_deref(), body.clone()).await
        }
        (Method::GET, Some(b), Some(k)) if has_query_flag(query, "retention") => {
            let vid = query_value(query, "versionId");
            lock::get_object_retention(state, &b, &k, vid.as_deref()).await
        }
        (Method::PUT, Some(b), Some(k)) if has_query_flag(query, "legal-hold") => {
            let vid = query_value(query, "versionId");
            lock::put_object_legal_hold(state, &b, &k, vid.as_deref(), body.clone()).await
        }
        (Method::GET, Some(b), Some(k)) if has_query_flag(query, "legal-hold") => {
            let vid = query_value(query, "versionId");
            lock::get_object_legal_hold(state, &b, &k, vid.as_deref()).await
        }

        // Object-level
        (Method::PUT, Some(b), Some(k)) if headers.get("x-amz-copy-source").is_some() => {
            let source = parse_copy_source_header(headers)?;
            copy::copy_object(state, &b, &k, source, headers).await
        }
        (Method::PUT, Some(b), Some(k)) => object::put_object(state, &b, &k, headers, body).await,
        (Method::GET, Some(b), Some(k)) => {
            let vid = query_value(query, "versionId");
            object::get_or_head_object(state, &b, &k, vid.as_deref(), headers, false).await
        }
        (Method::HEAD, Some(b), Some(k)) => {
            let vid = query_value(query, "versionId");
            let mut resp =
                object::get_or_head_object(state, &b, &k, vid.as_deref(), headers, true).await?;
            *resp.body_mut() = Body::empty();
            Ok(resp)
        }
        (Method::DELETE, Some(b), Some(k)) => {
            let vid = query_value(query, "versionId");
            object::delete_object(state, &b, &k, vid.as_deref()).await
        }

        _ => Err(S3Error::new(
            S3ErrorCode::NotImplemented,
            "operation not implemented",
        )
        .with_resource(String::new())),
    }
}

fn parse_copy_source_header(headers: &axum::http::HeaderMap) -> Result<copy::CopySource, S3Error> {
    let raw = headers
        .get("x-amz-copy-source")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument, "missing x-amz-copy-source header")
        })?;
    copy::parse_copy_source(raw)
}

fn query_value(query: &str, key: &str) -> Option<String> {
    if query.is_empty() {
        return None;
    }
    for pair in query.split('&') {
        let (name, value) = match pair.split_once('=') {
            Some((n, v)) => (n, v),
            None => (pair, ""),
        };
        if name == key {
            return Some(percent_decode(value));
        }
    }
    None
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_nib(bytes[i + 1]);
            let lo = hex_nib(bytes[i + 2]);
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

fn hex_nib(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn has_query_flag(query: &str, flag: &str) -> bool {
    if query.is_empty() {
        return false;
    }
    query.split('&').any(|p| {
        let name = p.split_once('=').map(|(n, _)| n).unwrap_or(p);
        name == flag
    })
}

fn decorate(mut e: S3Error, request_id: Option<RequestId>, uri_path: &str) -> S3Error {
    if e.resource.is_none() {
        e = e.with_resource(uri_path.to_string());
    }
    if let Some(id) = request_id {
        e = e.with_request_id(id);
    }
    e
}

/// Status code helper kept for symmetry with the rest of the module — not yet
/// used directly but referenced by future phases that build responses outside
/// of the standard `IntoResponse` path.
#[allow(dead_code)]
pub(crate) fn ok() -> StatusCode {
    StatusCode::OK
}
