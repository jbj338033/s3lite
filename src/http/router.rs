use axum::Router;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::get;

use crate::http::middleware::auth::sigv4_mw;
use crate::http::middleware::request_id::request_id_mw;
use crate::s3::AppState;
use crate::s3::dispatch::dispatch;

/// Build the full axum app:
/// * `/health` — public, unauthenticated, returns `200 OK`
/// * `/metrics` — public, Prometheus text exposition of basic gauges
/// * everything else — Sigv4-verified S3 dispatch (bucket + object handlers)
/// * global `request_id` middleware on all responses
pub fn build_app(state: AppState) -> Router {
    let config = state.config.clone();

    let public = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone());

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

/// Prometheus text exposition format. Kept deliberately small and live —
/// each call queries the MetaStore so operators see current counts without
/// any caching staleness. Returns `text/plain; version=0.0.4` per Prometheus
/// spec.
async fn metrics_handler(State(state): State<AppState>) -> Response {
    let buckets = state.meta.list_buckets().await.map(|v| v.len() as u64).ok();
    let dlq = state.meta.list_dlq().await.map(|v| v.len() as u64).ok();
    let mut body = String::new();
    body.push_str("# HELP s3lite_buckets_total Number of buckets currently present.\n");
    body.push_str("# TYPE s3lite_buckets_total gauge\n");
    body.push_str(&format!(
        "s3lite_buckets_total {}\n",
        buckets.unwrap_or(0)
    ));
    body.push_str("# HELP s3lite_dlq_entries_total Number of failed webhook deliveries queued for inspection.\n");
    body.push_str("# TYPE s3lite_dlq_entries_total gauge\n");
    body.push_str(&format!(
        "s3lite_dlq_entries_total {}\n",
        dlq.unwrap_or(0)
    ));
    body.push_str("# HELP s3lite_build_info Build identification for the running binary.\n");
    body.push_str("# TYPE s3lite_build_info gauge\n");
    body.push_str(&format!(
        "s3lite_build_info{{version=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION")
    ));

    let mut resp = (StatusCode::OK, body).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    resp
}
