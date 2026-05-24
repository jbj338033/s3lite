use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::request_context::RequestId;

/// S3 error codes — a curated subset. AWS defines ~70; we add codes as they
/// become needed by handlers. Each variant maps to a fixed HTTP status and a
/// human-readable message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ErrorCode {
    AccessDenied,
    SignatureDoesNotMatch,
    InvalidAccessKeyId,
    RequestTimeTooSkewed,
    AuthorizationHeaderMalformed,
    MissingSecurityHeader,
    InvalidRequest,
    NotImplemented,
    InternalError,
    NoSuchBucket,
    BucketAlreadyExists,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    NoSuchKey,
    NoSuchUpload,
    InvalidArgument,
    BadDigest,
    PreconditionFailed,
    NotModified,
    InvalidBucketName,
    NoSuchCorsConfiguration,
    NoSuchLifecycleConfiguration,
    AccessForbidden,
}

impl S3ErrorCode {
    pub fn status(self) -> StatusCode {
        use S3ErrorCode::*;
        match self {
            AccessDenied
            | SignatureDoesNotMatch
            | InvalidAccessKeyId
            | RequestTimeTooSkewed
            | AuthorizationHeaderMalformed
            | MissingSecurityHeader => StatusCode::FORBIDDEN,
            InvalidRequest
            | InvalidArgument
            | BadDigest
            | InvalidBucketName => StatusCode::BAD_REQUEST,
            NotImplemented => StatusCode::NOT_IMPLEMENTED,
            InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            NoSuchBucket | NoSuchKey | NoSuchUpload => StatusCode::NOT_FOUND,
            BucketAlreadyExists | BucketAlreadyOwnedByYou | BucketNotEmpty => StatusCode::CONFLICT,
            PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            NotModified => StatusCode::NOT_MODIFIED,
            NoSuchCorsConfiguration => StatusCode::NOT_FOUND,
            NoSuchLifecycleConfiguration => StatusCode::NOT_FOUND,
            AccessForbidden => StatusCode::FORBIDDEN,
        }
    }

    pub fn as_str(self) -> &'static str {
        use S3ErrorCode::*;
        match self {
            AccessDenied => "AccessDenied",
            SignatureDoesNotMatch => "SignatureDoesNotMatch",
            InvalidAccessKeyId => "InvalidAccessKeyId",
            RequestTimeTooSkewed => "RequestTimeTooSkewed",
            AuthorizationHeaderMalformed => "AuthorizationHeaderMalformed",
            MissingSecurityHeader => "MissingSecurityHeader",
            InvalidRequest => "InvalidRequest",
            NotImplemented => "NotImplemented",
            InternalError => "InternalError",
            NoSuchBucket => "NoSuchBucket",
            BucketAlreadyExists => "BucketAlreadyExists",
            BucketAlreadyOwnedByYou => "BucketAlreadyOwnedByYou",
            BucketNotEmpty => "BucketNotEmpty",
            NoSuchKey => "NoSuchKey",
            NoSuchUpload => "NoSuchUpload",
            InvalidArgument => "InvalidArgument",
            BadDigest => "BadDigest",
            PreconditionFailed => "PreconditionFailed",
            NotModified => "NotModified",
            InvalidBucketName => "InvalidBucketName",
            NoSuchCorsConfiguration => "NoSuchCORSConfiguration",
            NoSuchLifecycleConfiguration => "NoSuchLifecycleConfiguration",
            AccessForbidden => "AccessForbidden",
        }
    }
}

/// S3-shaped error returned by handlers and middleware. Carries optional
/// resource path; request/host ids are injected from `RequestId` extension
/// at response time.
#[derive(Debug, Clone)]
pub struct S3Error {
    pub code: S3ErrorCode,
    pub message: String,
    pub resource: Option<String>,
    pub request_id: Option<RequestId>,
}

impl S3Error {
    pub fn new(code: S3ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            resource: None,
            request_id: None,
        }
    }

    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    pub fn with_request_id(mut self, id: RequestId) -> Self {
        self.request_id = Some(id);
        self
    }
}

#[derive(Serialize)]
#[serde(rename = "Error")]
struct ErrorXml {
    #[serde(rename = "Code")]
    code: String,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "Resource")]
    resource: String,
    #[serde(rename = "RequestId")]
    request_id: String,
    #[serde(rename = "HostId")]
    host_id: String,
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let status = self.code.status();
        let (request_id, host_id) = match &self.request_id {
            Some(r) => (r.request_id.clone(), r.host_id.clone()),
            None => (String::new(), String::new()),
        };
        let xml_payload = ErrorXml {
            code: self.code.as_str().to_string(),
            message: self.message,
            resource: self.resource.unwrap_or_default(),
            request_id: request_id.clone(),
            host_id: host_id.clone(),
        };
        let mut body = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        if let Err(e) = quick_xml::se::to_writer(&mut body, &xml_payload) {
            tracing::error!(error = %e, "failed to serialize S3 error XML");
            body.push_str("<Error><Code>InternalError</Code></Error>");
        }

        let mut response = (status, body).into_response();
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/xml"),
        );
        if let Ok(v) = HeaderValue::from_str(&request_id) {
            headers.insert("x-amz-request-id", v);
        }
        if let Ok(v) = HeaderValue::from_str(&host_id) {
            headers.insert("x-amz-id-2", v);
        }
        response
    }
}
