use crate::config::ServerConfig;
use crate::http::error::{S3Error, S3ErrorCode};

/// Result of parsing `(Host, URI)` into an S3 bucket/key reference.
///
/// `bucket = None, key = None` → service-level operation (e.g. `ListBuckets`).
/// `bucket = Some, key = None` → bucket-level operation.
/// `bucket = Some, key = Some` → object-level operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addressing {
    pub bucket: Option<String>,
    pub key: Option<String>,
}

/// Extract bucket/key from the request's `Host` header and URI path,
/// honoring virtual-hosted vs path-style addressing.
///
/// Virtual-hosted: `<bucket>.<config.endpoint_host>` → bucket comes from the
/// subdomain, key is the URI path with the leading `/` stripped.
/// Path-style: bucket and key come from the URI path.
pub fn extract(
    host_header: Option<&str>,
    uri_path: &str,
    config: &ServerConfig,
) -> Result<Addressing, S3Error> {
    let host_no_port = host_header.map(strip_port).unwrap_or("");

    // 1. Try virtual-hosted addressing if an endpoint host is configured.
    if let Some(endpoint) = config.endpoint_host.as_deref()
        && !endpoint.is_empty()
        && let Some(bucket) = virtual_hosted_bucket(host_no_port, endpoint)
    {
        validate_bucket_name(&bucket)?;
        let key = path_to_key(uri_path);
        return Ok(Addressing {
            bucket: Some(bucket),
            key,
        });
    }

    // 2. Path-style: split first path segment as bucket.
    path_style(uri_path)
}

fn strip_port(host: &str) -> &str {
    match host.rsplit_once(':') {
        Some((h, _port)) if !h.is_empty() => h,
        _ => host,
    }
}

fn virtual_hosted_bucket(host: &str, endpoint: &str) -> Option<String> {
    let suffix = format!(".{endpoint}");
    let prefix = host.strip_suffix(&suffix)?;
    // The bucket label must not itself contain a dot — preserves the
    // unambiguous `<bucket>.<endpoint>` parse.
    if prefix.is_empty() || prefix.contains('.') {
        return None;
    }
    Some(prefix.to_string())
}

fn path_style(uri_path: &str) -> Result<Addressing, S3Error> {
    let trimmed = uri_path.strip_prefix('/').unwrap_or(uri_path);
    if trimmed.is_empty() {
        return Ok(Addressing {
            bucket: None,
            key: None,
        });
    }
    let (bucket_raw, key_raw) = match trimmed.find('/') {
        Some(i) => (&trimmed[..i], Some(&trimmed[i + 1..])),
        None => (trimmed, None),
    };

    validate_bucket_name(bucket_raw)?;

    let key = key_raw
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    Ok(Addressing {
        bucket: Some(bucket_raw.to_string()),
        key,
    })
}

fn path_to_key(uri_path: &str) -> Option<String> {
    let trimmed = uri_path.strip_prefix('/').unwrap_or(uri_path);
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// AWS S3 bucket name rules (subset enforced in Phase 3):
/// * 3-63 characters
/// * lowercase ASCII letters, digits, and hyphens
/// * must start and end with a letter or digit
/// * may not be formatted as an IPv4 address
pub fn validate_bucket_name(name: &str) -> Result<(), S3Error> {
    fn invalid(reason: &str) -> S3Error {
        S3Error::new(
            S3ErrorCode::InvalidBucketName,
            format!("invalid bucket name: {reason}"),
        )
    }

    if !(3..=63).contains(&name.len()) {
        return Err(invalid("length must be 3-63 characters"));
    }
    let bytes = name.as_bytes();
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    if !is_alnum(first) || !is_alnum(last) {
        return Err(invalid("must start and end with a letter or digit"));
    }
    for &b in bytes {
        if !(is_alnum(b) || b == b'-') {
            return Err(invalid(
                "only lowercase letters, digits, and hyphens are allowed",
            ));
        }
    }
    // Disallow IPv4-like (a.b.c.d numeric); since dots are already rejected,
    // this is implicit. Kept explicit here for future when dots are allowed.
    if is_ipv4_like(name) {
        return Err(invalid("must not be formatted as an IPv4 address"));
    }

    Ok(())
}

fn is_ipv4_like(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() != 4 {
        return false;
    }
    parts
        .iter()
        .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn cfg(endpoint: Option<&str>) -> ServerConfig {
        let mut c = (*ServerConfig::new(
            "us-east-1",
            "ak",
            "sk",
            "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        ))
        .clone();
        c.endpoint_host = endpoint.map(str::to_string);
        c
    }

    #[test]
    fn path_style_root_lists_buckets() {
        let a = extract(Some("s3.example.com"), "/", &cfg(Some("s3.example.com"))).unwrap();
        assert_eq!(a.bucket, None);
        assert_eq!(a.key, None);
    }

    #[test]
    fn path_style_bucket_only() {
        let a = extract(Some("s3.example.com"), "/my-bucket", &cfg(None)).unwrap();
        assert_eq!(a.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(a.key, None);
    }

    #[test]
    fn path_style_bucket_and_key() {
        let a = extract(Some("s3.example.com"), "/bkt/some/nested/key", &cfg(None)).unwrap();
        assert_eq!(a.bucket.as_deref(), Some("bkt"));
        assert_eq!(a.key.as_deref(), Some("some/nested/key"));
    }

    #[test]
    fn virtual_hosted_extracts_bucket_from_host() {
        let a = extract(
            Some("my-bucket.s3.example.com"),
            "/some-key",
            &cfg(Some("s3.example.com")),
        )
        .unwrap();
        assert_eq!(a.bucket.as_deref(), Some("my-bucket"));
        assert_eq!(a.key.as_deref(), Some("some-key"));
    }

    #[test]
    fn virtual_hosted_strips_port() {
        let a = extract(
            Some("my-bucket.s3.example.com:9000"),
            "/k",
            &cfg(Some("s3.example.com")),
        )
        .unwrap();
        assert_eq!(a.bucket.as_deref(), Some("my-bucket"));
    }

    #[test]
    fn invalid_bucket_name_rejected() {
        let err = extract(None, "/AB", &cfg(None)).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidBucketName);
    }

    #[test]
    fn ipv4_like_name_rejected() {
        assert!(validate_bucket_name("192.168.1.1").is_err());
    }
}
