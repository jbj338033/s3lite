use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::Request as HttpRequest;
use axum::middleware::Next;
use axum::response::Response;
use chrono::Utc;
use scratchstack_aws_signature::principal::Principal;
use scratchstack_aws_signature::{
    GetSigningKeyRequest, GetSigningKeyResponse, KSecretKey, NO_ADDITIONAL_SIGNED_HEADERS,
    SignatureOptions, sigv4_validate_request,
};
use subtle::ConstantTimeEq;
use tower::{BoxError, Service};

use crate::config::ServerConfig;
use crate::http::error::{S3Error, S3ErrorCode};
use crate::http::request_context::{AuthenticatedIdentity, RequestId};

const S3_SERVICE: &str = "s3";

/// Single-root-key signing-key lookup. Implemented as an explicit `tower::Service`
/// (rather than a closure) because scratchstack-aws-signature's combinator wraps
/// the function in `tower::service_fn`, which demands an `FnMut` closure — closures
/// that capture state and return an `async move` block confound that inference.
/// A named struct sidesteps the issue.
#[derive(Clone)]
struct RootKeyService {
    expected_access_key: Arc<String>,
    expected_secret: Arc<String>,
}

impl Service<GetSigningKeyRequest> for RootKeyService {
    type Response = GetSigningKeyResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetSigningKeyRequest) -> Self::Future {
        let expected_access_key = Arc::clone(&self.expected_access_key);
        let expected_secret = Arc::clone(&self.expected_secret);
        Box::pin(async move {
            let matches = req
                .access_key()
                .as_bytes()
                .ct_eq(expected_access_key.as_bytes());
            if !bool::from(matches) {
                return Err::<GetSigningKeyResponse, BoxError>("unknown access key".into());
            }
            let k_secret = KSecretKey::from_str(&expected_secret)?;
            let k_signing = k_secret.to_ksigning(req.request_date(), req.region(), req.service());
            let resp = GetSigningKeyResponse::builder()
                .signing_key(k_signing)
                .principal(Principal::from(vec![]))
                .build()?;
            Ok(resp)
        })
    }
}

/// Verify the incoming request's AWS Sigv4 signature against the configured
/// root key, then forward to the next layer with `AuthenticatedIdentity`
/// inserted into the request extensions.
///
/// Body is buffered up to `max_signed_body_bytes` for canonical request
/// reconstruction. Streaming-signed / unsigned-payload optimization lands
/// in Phase 6+.
pub async fn sigv4_mw(
    State(config): State<Arc<ServerConfig>>,
    req: Request,
    next: Next,
) -> Result<Response, S3Error> {
    let request_id_ext = req.extensions().get::<RequestId>().cloned();
    let resource_path = req.uri().path().to_string();
    let decorate = |mut e: S3Error| {
        e = e.with_resource(resource_path.clone());
        if let Some(id) = &request_id_ext {
            e = e.with_request_id(id.clone());
        }
        e
    };
    let attach_id = decorate;

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, config.max_signed_body_bytes)
        .await
        .map_err(|_| {
            attach_id(S3Error::new(
                S3ErrorCode::InvalidRequest,
                "request body too large to verify signature",
            ))
        })?;

    let http_req = HttpRequest::from_parts(parts, body_bytes);

    let mut signing_svc = RootKeyService {
        expected_access_key: Arc::new(config.root_key.access_key_id.clone()),
        expected_secret: Arc::new(config.root_key.secret_access_key.clone()),
    };

    let result = sigv4_validate_request(
        http_req,
        &config.region,
        S3_SERVICE,
        &mut signing_svc,
        Utc::now(),
        &NO_ADDITIONAL_SIGNED_HEADERS,
        SignatureOptions::S3,
    )
    .await;

    let (parts, body, _auth) = match result {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "sigv4 verification failed");
            // TODO Phase 6+: downcast SignatureError variants to richer mapping
            // (RequestTimeTooSkewed, MissingSecurityHeader, etc.).
            return Err(attach_id(S3Error::new(
                S3ErrorCode::SignatureDoesNotMatch,
                "signature verification failed",
            )));
        }
    };

    let mut authed = Request::from_parts(parts, Body::from(body));
    authed.extensions_mut().insert(AuthenticatedIdentity {
        access_key_id: config.root_key.access_key_id.clone(),
    });

    Ok(next.run(authed).await)
}
