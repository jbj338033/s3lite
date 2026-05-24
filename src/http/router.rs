use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::get;

use crate::config::ServerConfig;
use crate::http::error::{S3Error, S3ErrorCode};
use crate::http::middleware::auth::sigv4_mw;
use crate::http::middleware::request_id::request_id_mw;
use crate::http::request_context::RequestId;

/// Build the full axum app:
/// * `/health` — public, unauthenticated, returns `200 OK`
/// * everything else — Sigv4-verified S3 catch-all (501 NotImplemented until
///   Phase 3 wires actual handlers)
/// * global `request_id` middleware on all responses
pub fn build_app(config: Arc<ServerConfig>) -> Router {
    let public = Router::new().route("/health", get(health_handler));

    let s3 = Router::new()
        .fallback(s3_not_implemented)
        .layer(middleware::from_fn_with_state(config.clone(), sigv4_mw));

    public
        .merge(s3)
        .layer(middleware::from_fn(request_id_mw))
        .with_state(config)
}

async fn health_handler() -> &'static str {
    "ok"
}

/// Placeholder for the entire S3 surface — returns 501 until Phase 3+.
async fn s3_not_implemented(req: Request) -> impl IntoResponse {
    let request_id = req.extensions().get::<RequestId>().cloned();
    let mut err = S3Error::new(
        S3ErrorCode::NotImplemented,
        "s3lite: handler not yet implemented",
    );
    err.resource = Some(req.uri().path().to_string());
    if let Some(id) = request_id {
        err = err.with_request_id(id);
    }
    err
}
