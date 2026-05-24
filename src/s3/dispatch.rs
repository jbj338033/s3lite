use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::http::error::{S3Error, S3ErrorCode};
use crate::http::request_context::RequestId;

use super::addressing::{Addressing, extract};
use super::state::AppState;
use super::{bucket, object};

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
    match (method, addressing.bucket, addressing.key) {
        (Method::GET, None, _) => bucket::list_buckets(state).await,

        // Bucket-level
        (Method::PUT, Some(b), None) => bucket::create_bucket(state, &b).await,
        (Method::DELETE, Some(b), None) => bucket::delete_bucket(state, &b).await,
        (Method::HEAD, Some(b), None) => bucket::head_bucket(state, &b).await,
        (Method::GET, Some(b), None) if has_query_flag(query, "location") => {
            bucket::get_bucket_location(state, &b).await
        }
        (Method::GET, Some(_b), None) => {
            // ListObjects(V2) — Phase 5
            Err(S3Error::new(
                S3ErrorCode::NotImplemented,
                "ListObjects arrives in Phase 5",
            ))
        }

        // Object-level
        (Method::PUT, Some(b), Some(k)) => object::put_object(state, &b, &k, headers, body).await,
        (Method::GET, Some(b), Some(k)) => {
            object::get_or_head_object(state, &b, &k, headers, false).await
        }
        (Method::HEAD, Some(b), Some(k)) => {
            // For HEAD we still call the same path but suppress the body.
            let mut resp = object::get_or_head_object(state, &b, &k, headers, true).await?;
            *resp.body_mut() = Body::empty();
            Ok(resp)
        }
        (Method::DELETE, Some(b), Some(k)) => object::delete_object(state, &b, &k).await,

        _ => Err(S3Error::new(
            S3ErrorCode::NotImplemented,
            "operation not implemented",
        )
        .with_resource(String::new())),
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
