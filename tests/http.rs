use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use s3lite::config::ServerConfig;
use s3lite::http::build_app;
use tower::ServiceExt;

const TEST_REGION: &str = "us-east-1";
const TEST_AK: &str = "AKIAIOSFODNN7EXAMPLE";
const TEST_SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

fn test_config() -> Arc<ServerConfig> {
    ServerConfig::new(
        TEST_REGION,
        TEST_AK,
        TEST_SK,
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    )
}

#[tokio::test]
async fn health_returns_200_with_request_id() {
    let app = build_app(test_config());
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers().contains_key("x-amz-request-id"),
        "x-amz-request-id missing on /health response"
    );
    assert!(
        response.headers().contains_key("x-amz-id-2"),
        "x-amz-id-2 missing on /health response"
    );
}

#[tokio::test]
async fn unsigned_s3_request_returns_403_with_xml_body() {
    let app = build_app(test_config());
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/some-bucket/some-key")
                .header("host", "s3.us-east-1.amazonaws.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let ct = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(ct.starts_with("application/xml"), "content-type was {ct}");

    let body_bytes = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let body = std::str::from_utf8(&body_bytes).unwrap();
    assert!(body.contains("<Code>SignatureDoesNotMatch</Code>"), "body: {body}");
    assert!(body.contains("<Resource>"), "body missing Resource: {body}");
    assert!(body.contains("<RequestId>"), "body missing RequestId: {body}");
}

#[tokio::test]
async fn signed_s3_request_passes_auth_returns_not_implemented() {
    let app = build_app(test_config());

    let signed = build_signed_get(
        "http://s3.us-east-1.amazonaws.com/foo/bar",
        "s3.us-east-1.amazonaws.com",
    );

    let response = app.oneshot(signed).await.unwrap();

    // Should have passed auth and reached the catch-all 501.
    assert_eq!(
        response.status(),
        StatusCode::NOT_IMPLEMENTED,
        "status was {} — auth probably failed; body: {}",
        response.status(),
        body_to_string(response.into_body()).await,
    );
}

#[tokio::test]
async fn signed_virtual_hosted_addressing_passes_auth() {
    let app = build_app(test_config());

    let signed = build_signed_get(
        "http://my-bucket.s3.us-east-1.amazonaws.com/some-key",
        "my-bucket.s3.us-east-1.amazonaws.com",
    );

    let response = app.oneshot(signed).await.unwrap();

    assert_eq!(
        response.status(),
        StatusCode::NOT_IMPLEMENTED,
        "virtual-hosted style failed auth; body: {}",
        body_to_string(response.into_body()).await,
    );
}

// ---------------- helpers ----------------

fn build_signed_get(url: &str, host_header: &str) -> Request<Body> {
    let identity = Credentials::new(TEST_AK, TEST_SK, None, None, "test").into();
    let settings = SigningSettings::default();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(TEST_REGION)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .unwrap()
        .into();

    let signable = SignableRequest::new(
        "GET",
        url,
        std::iter::once(("host", host_header)),
        SignableBody::Bytes(&[]),
    )
    .expect("valid signable request");

    let (instructions, _sig) = sign(signable, &signing_params).expect("sign").into_parts();

    // Build the http::Request that we'll feed to our app.
    let mut request = http::Request::builder()
        .method("GET")
        .uri(url)
        .header("host", host_header)
        .body(())
        .unwrap();
    instructions.apply_to_request_http1x(&mut request);

    let (parts, _) = request.into_parts();
    Request::from_parts(parts, Body::empty())
}

async fn body_to_string(body: Body) -> String {
    let bytes = to_bytes(body, 64 * 1024).await.unwrap_or_default();
    String::from_utf8_lossy(&bytes).to_string()
}
