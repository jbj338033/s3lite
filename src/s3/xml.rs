use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

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
