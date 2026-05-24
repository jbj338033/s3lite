use axum::extract::Request;
use axum::http::HeaderValue;
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand::RngCore;

use crate::http::request_context::RequestId;

/// Middleware: generate `x-amz-request-id` (16 hex chars) and `x-amz-id-2`
/// (base64 of 32 random bytes) for every request, insert the pair into
/// `Request::extensions` so handlers can reference them, and echo on response
/// headers. Mirrors AWS S3's response-id conventions.
pub async fn request_id_mw(mut req: Request, next: Next) -> Response {
    let id = generate();
    req.extensions_mut().insert(id.clone());

    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    if !headers.contains_key("x-amz-request-id")
        && let Ok(v) = HeaderValue::from_str(&id.request_id)
    {
        headers.insert("x-amz-request-id", v);
    }
    if !headers.contains_key("x-amz-id-2")
        && let Ok(v) = HeaderValue::from_str(&id.host_id)
    {
        headers.insert("x-amz-id-2", v);
    }
    response
}

fn generate() -> RequestId {
    let mut rng = rand::rng();
    let mut req_bytes = [0u8; 8];
    rng.fill_bytes(&mut req_bytes);
    let request_id = hex::encode(req_bytes); // 16 hex chars

    let mut host_bytes = [0u8; 32];
    rng.fill_bytes(&mut host_bytes);
    let host_id = BASE64.encode(host_bytes);

    RequestId { request_id, host_id }
}
