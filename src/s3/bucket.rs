use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::BucketConfig;
use crate::storage::MetaError;

use super::state::AppState;
use super::xml::{
    BucketEntry, Buckets, ListAllMyBucketsResult, LocationConstraint, Owner, XmlBody,
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

pub async fn create_bucket(state: AppState, bucket: &str) -> Result<Response, S3Error> {
    let cfg = BucketConfig {
        created_at: OffsetDateTime::now_utc(),
        versioning: crate::storage::manifest::VersioningState::Off,
        region: state.config.region.clone(),
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
