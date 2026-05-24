//! Server-side validation of AWS Sigv4 presigned URLs.
//!
//! `scratchstack-aws-signature` is used for header-signed requests, but its
//! 0.11.4 release does not URL-decode the `/` separators in `X-Amz-Credential`
//! before splitting (the AWS SDK percent-encodes them as `%2F`). Rather than
//! patch a third-party crate, presigned URLs are verified inline here.
//!
//! Verification follows the AWS Sigv4 spec exactly:
//! * `X-Amz-Algorithm`, `X-Amz-Credential`, `X-Amz-Date`, `X-Amz-Expires`,
//!   `X-Amz-SignedHeaders`, `X-Amz-Signature` are required.
//! * `X-Amz-Expires` must be in `[1, 604800]` (7 days).
//! * Server time must be within `±5 min` of `X-Amz-Date`; the URL expires at
//!   `X-Amz-Date + X-Amz-Expires`.
//! * Canonical request uses the path / query verbatim from the wire (the
//!   client signed them in that form); payload hash is `UNSIGNED-PAYLOAD`.

use std::collections::BTreeMap;

use axum::http::{HeaderMap, Method, Uri};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::{Duration, OffsetDateTime, PrimitiveDateTime, format_description};

use crate::config::ServerConfig;
use crate::http::error::{S3Error, S3ErrorCode};

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const TERMINATOR: &str = "aws4_request";
const MAX_EXPIRES: i64 = 604_800;
const CLOCK_SKEW_SECS: i64 = 300;

/// True if the request looks like a presigned URL (has `X-Amz-Signature` in
/// the query). Cheap pre-check used by the dispatcher to decide which
/// verification path to take.
pub fn is_presigned(uri: &Uri) -> bool {
    uri.query()
        .map(|q| {
            q.split('&').any(|p| {
                p.split_once('=')
                    .map(|(k, _)| k.eq_ignore_ascii_case("X-Amz-Signature"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

pub fn verify(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    config: &ServerConfig,
) -> Result<(), S3Error> {
    let Some(query) = uri.query() else {
        return Err(S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "presigned URL has no query string",
        ));
    };
    let params = parse_query_pairs(query);

    // Required parameter presence ---------------------------------------
    let alg = get_required(&params, "X-Amz-Algorithm")?;
    if alg != ALGORITHM {
        return Err(S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            format!("unsupported X-Amz-Algorithm '{alg}'"),
        ));
    }
    let credential_raw = get_required(&params, "X-Amz-Credential")?;
    let credential = percent_decode_str(&credential_raw);
    let cred_parts: Vec<&str> = credential.split('/').collect();
    if cred_parts.len() != 5 {
        return Err(S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "X-Amz-Credential must have 5 slash-delimited components",
        ));
    }
    let (access_key, date_scope, region, service, terminator) = (
        cred_parts[0],
        cred_parts[1],
        cred_parts[2],
        cred_parts[3],
        cred_parts[4],
    );
    if terminator != TERMINATOR {
        return Err(S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "X-Amz-Credential must end with aws4_request",
        ));
    }
    if service != "s3" {
        return Err(S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "X-Amz-Credential service must be s3",
        ));
    }

    // Access-key check, constant-time
    let ak_match = access_key
        .as_bytes()
        .ct_eq(config.root_key.access_key_id.as_bytes());
    if !bool::from(ak_match) {
        return Err(S3Error::new(
            S3ErrorCode::InvalidAccessKeyId,
            "unknown access key id",
        ));
    }

    // Date + expires --------------------------------------------------
    let date_full_raw = get_required(&params, "X-Amz-Date")?;
    let date_full = percent_decode_str(&date_full_raw);
    let signed_at = parse_amz_date(&date_full).ok_or_else(|| {
        S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "X-Amz-Date is not ISO-8601 basic (yyyyMMddTHHmmssZ)",
        )
    })?;
    let signed_date = date_full.get(..8).unwrap_or_default();
    if signed_date != date_scope {
        return Err(S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "X-Amz-Credential date does not match X-Amz-Date",
        ));
    }

    let expires_raw = get_required(&params, "X-Amz-Expires")?;
    let expires: i64 = expires_raw.parse().map_err(|_| {
        S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "X-Amz-Expires must be a non-negative integer",
        )
    })?;
    if !(1..=MAX_EXPIRES).contains(&expires) {
        return Err(S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            format!("X-Amz-Expires must be in 1..={MAX_EXPIRES}"),
        ));
    }

    let now = OffsetDateTime::now_utc();
    if (signed_at - now).abs() > Duration::seconds(CLOCK_SKEW_SECS) {
        return Err(S3Error::new(
            S3ErrorCode::RequestTimeTooSkewed,
            "request time is outside the allowed clock skew",
        ));
    }
    if now > signed_at + Duration::seconds(expires) {
        return Err(S3Error::new(
            S3ErrorCode::AccessForbidden,
            "presigned URL has expired",
        ));
    }

    let signed_headers_raw = get_required(&params, "X-Amz-SignedHeaders")?;
    let signed_headers_list: Vec<String> = percent_decode_str(&signed_headers_raw)
        .split(';')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    if signed_headers_list.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::AuthorizationHeaderMalformed,
            "X-Amz-SignedHeaders is empty",
        ));
    }

    let provided_sig = get_required(&params, "X-Amz-Signature")?;

    // Build canonical request -----------------------------------------
    let canonical_uri = canonical_uri(uri.path());
    let canonical_query = canonical_query(query);
    let canonical_headers = canonical_headers(headers, &signed_headers_list)?;
    let signed_headers_joined = signed_headers_list.join(";");

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        canonical_uri,
        canonical_query,
        canonical_headers,
        signed_headers_joined,
        "UNSIGNED-PAYLOAD",
    );

    let scope = format!("{date_scope}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "{ALGORITHM}\n{date_full}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes())),
    );

    // Derive signing key chain ---------------------------------------
    let k_secret = format!("AWS4{}", config.root_key.secret_access_key);
    let k_date = hmac_sha256(k_secret.as_bytes(), date_scope.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    let k_signing = hmac_sha256(&k_service, b"aws4_request");
    let computed = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

    if !bool::from(computed.as_bytes().ct_eq(provided_sig.as_bytes())) {
        return Err(S3Error::new(
            S3ErrorCode::SignatureDoesNotMatch,
            "presigned signature did not match",
        ));
    }

    Ok(())
}

// ---------------- canonicalization helpers ----------------

/// Canonical query string per Sigv4: every parameter except `X-Amz-Signature`,
/// keys URL-encoded then sorted byte-wise, values as-they-appeared on the
/// wire (the client signed them in that form).
fn canonical_query(raw_query: &str) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for token in raw_query.split('&') {
        if token.is_empty() {
            continue;
        }
        let (k, v) = match token.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (token.to_string(), String::new()),
        };
        if k.eq_ignore_ascii_case("X-Amz-Signature") {
            continue;
        }
        pairs.push((k, v));
    }
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// S3 canonical URI: leave the path verbatim (no double-encoding). axum
/// gives us the wire form already.
fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        path.to_string()
    }
}

/// Canonical headers: name lowercased, value trimmed, joined by `:` then
/// terminated with `\n`. Output ends with a trailing `\n` for each header.
fn canonical_headers(headers: &HeaderMap, signed: &[String]) -> Result<String, S3Error> {
    let mut by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for name in signed {
        by_name.insert(name.clone(), Vec::new());
    }
    for (name, value) in headers {
        let lower = name.as_str().to_ascii_lowercase();
        if let Some(slot) = by_name.get_mut(&lower) {
            let v = value
                .to_str()
                .map_err(|_| {
                    S3Error::new(
                        S3ErrorCode::InvalidRequest,
                        format!("non-ascii header {lower}"),
                    )
                })?
                .trim()
                .to_string();
            slot.push(v);
        }
    }
    let mut out = String::new();
    for (name, values) in by_name {
        if values.is_empty() {
            return Err(S3Error::new(
                S3ErrorCode::AuthorizationHeaderMalformed,
                format!("signed header '{name}' missing"),
            ));
        }
        out.push_str(&name);
        out.push(':');
        out.push_str(&values.join(","));
        out.push('\n');
    }
    Ok(out)
}

fn parse_query_pairs(query: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for token in query.split('&') {
        if token.is_empty() {
            continue;
        }
        let (k, v) = match token.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (token.to_string(), String::new()),
        };
        map.insert(k, v);
    }
    map
}

fn get_required(params: &BTreeMap<String, String>, name: &str) -> Result<String, S3Error> {
    // Case-insensitive lookup over canonical key names.
    params
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
        .ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::AuthorizationHeaderMalformed,
                format!("presigned URL is missing {name}"),
            )
        })
}

fn percent_decode_str(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_nibble(bytes[i + 1]);
            let lo = hex_nibble(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn parse_amz_date(s: &str) -> Option<OffsetDateTime> {
    let fmt = format_description::parse("[year][month][day]T[hour][minute][second]Z").ok()?;
    let pdt = PrimitiveDateTime::parse(s, &fmt).ok()?;
    Some(pdt.assume_utc())
}

type HmacSha256 = Hmac<Sha256>;

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decode_handles_slashes() {
        assert_eq!(
            percent_decode_str("AKID%2F20240101%2Fus-east-1%2Fs3%2Faws4_request"),
            "AKID/20240101/us-east-1/s3/aws4_request"
        );
    }

    #[test]
    fn parses_amz_date_basic_format() {
        let dt = parse_amz_date("20260524T144602Z").unwrap();
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month() as u8, 5);
        assert_eq!(dt.day(), 24);
        assert_eq!(dt.hour(), 14);
    }

    #[test]
    fn is_presigned_detects_query_signature() {
        let uri: Uri = "http://h/p?X-Amz-Signature=abc&X-Amz-Date=20260101T000000Z"
            .parse()
            .unwrap();
        assert!(is_presigned(&uri));
        let no_sig: Uri = "http://h/p?foo=bar".parse().unwrap();
        assert!(!is_presigned(&no_sig));
    }
}
