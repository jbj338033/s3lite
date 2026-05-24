use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::{BucketConfig, VersioningState};
use crate::storage::MetaError;

use super::state::AppState;
use super::xml::{
    BucketEntry, Buckets, GetVersioningConfiguration, ListAllMyBucketsResult, LocationConstraint,
    Owner, PutVersioningConfiguration, XmlBody,
};

const OWNER_ID: &str = "s3lite";
const OWNER_DISPLAY_NAME: &str = "s3lite";

pub async fn list_buckets(state: AppState) -> Result<Response, S3Error> {
    let listed = state.meta.list_buckets().await.map_err(map_meta_err)?;
    let bucket = listed
        .into_iter()
        .map(|(name, cfg)| BucketEntry {
            name,
            creation_date: cfg
                .created_at
                .format(&Rfc3339)
                .unwrap_or_else(|_| String::new()),
        })
        .collect();
    let body = ListAllMyBucketsResult {
        owner: Owner {
            id: OWNER_ID.into(),
            display_name: OWNER_DISPLAY_NAME.into(),
        },
        buckets: Buckets { bucket },
    };
    Ok(XmlBody(body).into_response())
}

pub async fn create_bucket(
    state: AppState,
    bucket: &str,
    headers: &axum::http::HeaderMap,
) -> Result<Response, S3Error> {
    let object_lock_enabled = headers
        .get("x-amz-bucket-object-lock-enabled")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let object_lock = if object_lock_enabled {
        Some(crate::storage::manifest::ObjectLockConfig {
            enabled: true,
            default_retention: None,
        })
    } else {
        None
    };
    // S3 enabling object lock at creation also implicitly enables versioning.
    let versioning = if object_lock_enabled {
        crate::storage::manifest::VersioningState::Enabled
    } else {
        crate::storage::manifest::VersioningState::Off
    };
    let cfg = BucketConfig {
        created_at: OffsetDateTime::now_utc(),
        versioning,
        region: state.config.region.clone(),
        cors_rules: Vec::new(),
        object_lock,
        lifecycle_rules: Vec::new(),
    };
    match state.meta.create_bucket(bucket, cfg).await {
        Ok(()) => {
            let mut resp = StatusCode::OK.into_response();
            // S3 sets Location: /<bucket> on successful create.
            if let Ok(loc) = axum::http::HeaderValue::from_str(&format!("/{bucket}")) {
                resp.headers_mut().insert("location", loc);
            }
            Ok(resp)
        }
        Err(MetaError::BucketExists(_)) => Err(S3Error::new(
            S3ErrorCode::BucketAlreadyOwnedByYou,
            "bucket already exists",
        )
        .with_resource(format!("/{bucket}"))),
        Err(e) => Err(map_meta_err(e)),
    }
}

pub async fn delete_bucket(state: AppState, bucket: &str) -> Result<Response, S3Error> {
    match state.meta.delete_bucket(bucket).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT.into_response()),
        Err(MetaError::BucketNotFound(_)) => Err(no_such_bucket(bucket)),
        Err(MetaError::BucketNotEmpty(_)) => Err(S3Error::new(
            S3ErrorCode::BucketNotEmpty,
            "bucket is not empty",
        )
        .with_resource(format!("/{bucket}"))),
        Err(e) => Err(map_meta_err(e)),
    }
}

pub async fn head_bucket(state: AppState, bucket: &str) -> Result<Response, S3Error> {
    match state.meta.get_bucket(bucket).await.map_err(map_meta_err)? {
        Some(_) => Ok(StatusCode::OK.into_response()),
        None => Err(no_such_bucket(bucket)),
    }
}

pub async fn get_bucket_location(state: AppState, bucket: &str) -> Result<Response, S3Error> {
    let cfg = state
        .meta
        .get_bucket(bucket)
        .await
        .map_err(map_meta_err)?
        .ok_or_else(|| no_such_bucket(bucket))?;
    Ok(XmlBody(LocationConstraint { region: cfg.region }).into_response())
}

fn no_such_bucket(bucket: &str) -> S3Error {
    S3Error::new(S3ErrorCode::NoSuchBucket, "bucket does not exist")
        .with_resource(format!("/{bucket}"))
}

pub fn map_meta_err(e: MetaError) -> S3Error {
    tracing::error!(error = %e, "meta store error");
    S3Error::new(S3ErrorCode::InternalError, format!("meta: {e}"))
}

/// `PUT /bucket?versioning` with `<VersioningConfiguration>` body —
/// switch the bucket between Off/Enabled/Suspended.
pub async fn put_bucket_versioning(
    state: AppState,
    bucket: &str,
    body: Bytes,
) -> Result<Response, S3Error> {
    let cfg: PutVersioningConfiguration = quick_xml::de::from_reader(body.as_ref())
        .map_err(|e| {
            S3Error::new(
                S3ErrorCode::InvalidRequest,
                format!("malformed VersioningConfiguration body: {e}"),
            )
            .with_resource(format!("/{bucket}"))
        })?;
    let new_state = match cfg.status.as_deref() {
        Some("Enabled") => VersioningState::Enabled,
        Some("Suspended") => VersioningState::Suspended,
        Some(other) => {
            return Err(S3Error::new(
                S3ErrorCode::InvalidArgument,
                format!("invalid versioning Status '{other}'"),
            )
            .with_resource(format!("/{bucket}")));
        }
        None => {
            return Err(S3Error::new(
                S3ErrorCode::InvalidRequest,
                "VersioningConfiguration must include a Status",
            )
            .with_resource(format!("/{bucket}")));
        }
    };
    match state.meta.update_bucket_versioning(bucket, new_state).await {
        Ok(()) => Ok(StatusCode::OK.into_response()),
        Err(MetaError::BucketNotFound(_)) => Err(no_such_bucket(bucket)),
        Err(e) => Err(map_meta_err(e)),
    }
}

/// `GET /bucket?versioning` — return the current versioning state.
pub async fn get_bucket_versioning(
    state: AppState,
    bucket: &str,
) -> Result<Response, S3Error> {
    let cfg = state
        .meta
        .get_bucket(bucket)
        .await
        .map_err(map_meta_err)?
        .ok_or_else(|| no_such_bucket(bucket))?;
    let status = match cfg.versioning {
        VersioningState::Off => None,
        VersioningState::Enabled => Some("Enabled".to_string()),
        VersioningState::Suspended => Some("Suspended".to_string()),
    };
    Ok(XmlBody(GetVersioningConfiguration { status }).into_response())
}
