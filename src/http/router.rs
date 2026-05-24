use axum::Router;
use axum::middleware;
use axum::routing::get;

use crate::http::middleware::auth::sigv4_mw;
use crate::http::middleware::request_id::request_id_mw;
use crate::s3::AppState;
use crate::s3::dispatch::dispatch;

/// Build the full axum app:
/// * `/health` — public, unauthenticated, returns `200 OK`
/// * everything else — Sigv4-verified S3 dispatch (bucket + object handlers)
/// * global `request_id` middleware on all responses
pub fn build_app(state: AppState) -> Router {
    let config = state.config.clone();

    let public = Router::new()
        .route("/health", get(health_handler))
        .with_state(());

    let s3 = Router::new()
        .fallback(dispatch)
        .layer(middleware::from_fn_with_state(config, sigv4_mw))
        .with_state(state);

    public
        .merge(s3)
        .layer(middleware::from_fn(request_id_mw))
}

async fn health_handler() -> &'static str {
    "ok"
}
