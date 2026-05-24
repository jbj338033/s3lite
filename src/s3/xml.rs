use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use crate::http::error::{S3Error, S3ErrorCode};

/// Wrap a serializable type so it renders as `application/xml`.
pub struct XmlBody<T>(pub T);

impl<T: Serialize> IntoResponse for XmlBody<T> {
    fn into_response(self) -> Response {
        match render_xml(&self.0) {
            Ok(body) => {
                let mut resp = (StatusCode::OK, body).into_response();
                resp.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/xml"),
                );
                resp
            }
            Err(e) => S3Error::new(S3ErrorCode::InternalError, e).into_response(),
        }
    }
}

pub fn render_xml<T: Serialize>(value: &T) -> Result<String, String> {
    let mut body = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    quick_xml::se::to_writer(&mut body, value)
        .map_err(|e| format!("xml serialize: {e}"))?;
    Ok(body)
}

// ---------------- Bucket listing ----------------

#[derive(Debug, Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
pub struct ListAllMyBucketsResult {
    #[serde(rename = "Owner")]
    pub owner: Owner,
    #[serde(rename = "Buckets")]
    pub buckets: Buckets,
}

#[derive(Debug, Serialize)]
pub struct Owner {
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

#[derive(Debug, Serialize)]
pub struct Buckets {
    #[serde(rename = "Bucket", default)]
    pub bucket: Vec<BucketEntry>,
}

#[derive(Debug, Serialize)]
pub struct BucketEntry {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "CreationDate")]
    pub creation_date: String,
}

#[derive(Debug, Serialize)]
#[serde(rename = "LocationConstraint")]
pub struct LocationConstraint {
    #[serde(rename = "$text")]
    pub region: String,
}

// ---------------- ListObjects (V1 + V2) ----------------

#[derive(Debug, Serialize)]
pub struct ObjectContent {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

#[derive(Debug, Serialize)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

/// Response body for `ListObjectsV2`. Optional fields are omitted from XML
/// when `None` so the wire output matches AWS exactly.
#[derive(Debug, Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListBucketResultV2 {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Prefix")]
    pub prefix: String,
    #[serde(rename = "KeyCount")]
    pub key_count: u32,
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "EncodingType", skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
    #[serde(rename = "ContinuationToken", skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    #[serde(rename = "NextContinuationToken", skip_serializing_if = "Option::is_none")]
    pub next_continuation_token: Option<String>,
    #[serde(rename = "StartAfter", skip_serializing_if = "Option::is_none")]
    pub start_after: Option<String>,
    #[serde(rename = "Contents", default)]
    pub contents: Vec<ObjectContent>,
    #[serde(rename = "CommonPrefixes", default)]
    pub common_prefixes: Vec<CommonPrefix>,
}

/// Response body for `ListObjects` (V1).
#[derive(Debug, Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListBucketResultV1 {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Prefix")]
    pub prefix: String,
    #[serde(rename = "Marker")]
    pub marker: String,
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "EncodingType", skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
    #[serde(rename = "NextMarker", skip_serializing_if = "Option::is_none")]
    pub next_marker: Option<String>,
    #[serde(rename = "Contents", default)]
    pub contents: Vec<ObjectContent>,
    #[serde(rename = "CommonPrefixes", default)]
    pub common_prefixes: Vec<CommonPrefix>,
}

// ---------------- Multipart upload ----------------

#[derive(Debug, Serialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
pub struct InitiateMultipartUploadResult {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

/// Request body for `CompleteMultipartUpload`. AWS sends the parts list the
/// client decided to keep (etag + part number), in ascending part_number order.
#[derive(Debug, Deserialize)]
#[serde(rename = "CompleteMultipartUpload")]
pub struct CompleteMultipartUploadRequest {
    #[serde(rename = "Part", default)]
    pub parts: Vec<CompletePart>,
}

#[derive(Debug, Deserialize)]
pub struct CompletePart {
    #[serde(rename = "PartNumber")]
    pub part_number: u32,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Debug, Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
pub struct CompleteMultipartUploadResult {
    #[serde(rename = "Location")]
    pub location: String,
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "ETag")]
    pub etag: String,
}

#[derive(Debug, Serialize)]
pub struct ListPartsPart {
    #[serde(rename = "PartNumber")]
    pub part_number: u32,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
}

#[derive(Debug, Serialize)]
#[serde(rename = "ListPartsResult")]
pub struct ListPartsResult {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
    #[serde(rename = "MaxParts")]
    pub max_parts: u32,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "Part", default)]
    pub parts: Vec<ListPartsPart>,
}

// ---------------- Versioning ----------------

/// Request body for `PutBucketVersioning`. Only `Status` is supported in
/// Phase 7 (MfaDelete left out — single-key auth model has no MFA concept).
#[derive(Debug, Deserialize)]
#[serde(rename = "VersioningConfiguration")]
pub struct PutVersioningConfiguration {
    #[serde(rename = "Status", default)]
    pub status: Option<String>,
}

/// Response body for `GetBucketVersioning`. Empty body means "Off" in S3 —
/// we emit a Status only when versioning has been touched.
#[derive(Debug, Serialize)]
#[serde(rename = "VersioningConfiguration")]
pub struct GetVersioningConfiguration {
    #[serde(rename = "Status", skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// Single entry in `ListVersionsResult` — either an object version or a
/// delete marker (tombstone). Both share the same shape; serde renames
/// dispatch via untagged is awkward, so they're separate vecs in the parent.
#[derive(Debug, Serialize)]
pub struct ObjectVersion {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId")]
    pub version_id: String,
    #[serde(rename = "IsLatest")]
    pub is_latest: bool,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "Size")]
    pub size: u64,
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

#[derive(Debug, Serialize)]
pub struct DeleteMarkerEntry {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "VersionId")]
    pub version_id: String,
    #[serde(rename = "IsLatest")]
    pub is_latest: bool,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
}

// ---------------- Copy ----------------

/// Response body for `CopyObject`.
#[derive(Debug, Serialize)]
#[serde(rename = "CopyObjectResult")]
pub struct CopyObjectResult {
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
}

/// Response body for `UploadPartCopy`.
#[derive(Debug, Serialize)]
#[serde(rename = "CopyPartResult")]
pub struct CopyPartResult {
    #[serde(rename = "ETag")]
    pub etag: String,
    #[serde(rename = "LastModified")]
    pub last_modified: String,
}

#[derive(Debug, Serialize)]
#[serde(rename = "ListVersionsResult")]
pub struct ListVersionsResult {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Prefix")]
    pub prefix: String,
    #[serde(rename = "KeyMarker")]
    pub key_marker: String,
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    #[serde(rename = "EncodingType", skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
    #[serde(rename = "NextKeyMarker", skip_serializing_if = "Option::is_none")]
    pub next_key_marker: Option<String>,
    #[serde(rename = "Version", default)]
    pub versions: Vec<ObjectVersion>,
    #[serde(rename = "DeleteMarker", default)]
    pub delete_markers: Vec<DeleteMarkerEntry>,
}
