use axum::http::HeaderMap;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::{AdditionalChecksum, ChecksumAlgorithm};

/// Parse a client-supplied `x-amz-checksum-<algo>` header into an
/// `AdditionalChecksum`. Multiple algorithm headers in one request are
/// rejected — S3 itself accepts at most one per request.
pub fn parse_request_checksum(headers: &HeaderMap) -> Result<Option<AdditionalChecksum>, S3Error> {
    let mut found: Option<(ChecksumAlgorithm, String)> = None;
    for (algo, header_name) in HEADER_BINDINGS {
        let Some(v) = headers.get(*header_name) else {
            continue;
        };
        let s = v
            .to_str()
            .map_err(|_| {
                S3Error::new(
                    S3ErrorCode::InvalidRequest,
                    format!("invalid {header_name} header"),
                )
            })?
            .to_string();
        if found.is_some() {
            return Err(S3Error::new(
                S3ErrorCode::InvalidRequest,
                "more than one x-amz-checksum-* header supplied",
            ));
        }
        found = Some((*algo, s));
    }

    let Some((algo, raw)) = found else {
        return Ok(None);
    };
    let bytes = BASE64.decode(&raw).map_err(|_| {
        S3Error::new(
            S3ErrorCode::InvalidRequest,
            format!("{} checksum is not valid base64", header_name_for(algo)),
        )
    })?;
    let expected_len = expected_len(algo);
    if bytes.len() != expected_len {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            format!(
                "{} checksum must be {} bytes",
                header_name_for(algo),
                expected_len
            ),
        ));
    }
    Ok(Some(AdditionalChecksum {
        algorithm: algo,
        value: bytes,
    }))
}

pub fn compute(algo: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    match algo {
        ChecksumAlgorithm::Crc32 => {
            let v = crc32fast::hash(data);
            v.to_be_bytes().to_vec()
        }
        ChecksumAlgorithm::Crc32c => {
            let v = crc32c::crc32c(data);
            v.to_be_bytes().to_vec()
        }
        ChecksumAlgorithm::Sha1 => {
            use sha1::Digest;
            let mut h = sha1::Sha1::new();
            h.update(data);
            h.finalize().to_vec()
        }
        ChecksumAlgorithm::Sha256 => {
            use sha2::Digest;
            let mut h = sha2::Sha256::new();
            h.update(data);
            h.finalize().to_vec()
        }
    }
}

pub fn verify(expected: &AdditionalChecksum, data: &[u8]) -> Result<(), S3Error> {
    let actual = compute(expected.algorithm, data);
    if actual != expected.value {
        return Err(S3Error::new(
            S3ErrorCode::BadDigest,
            format!(
                "{} checksum mismatch",
                header_name_for(expected.algorithm)
            ),
        ));
    }
    Ok(())
}

pub fn header_name_for(algo: ChecksumAlgorithm) -> &'static str {
    match algo {
        ChecksumAlgorithm::Crc32 => "x-amz-checksum-crc32",
        ChecksumAlgorithm::Crc32c => "x-amz-checksum-crc32c",
        ChecksumAlgorithm::Sha1 => "x-amz-checksum-sha1",
        ChecksumAlgorithm::Sha256 => "x-amz-checksum-sha256",
    }
}

pub fn encode_value(value: &[u8]) -> String {
    BASE64.encode(value)
}

fn expected_len(algo: ChecksumAlgorithm) -> usize {
    match algo {
        ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32c => 4,
        ChecksumAlgorithm::Sha1 => 20,
        ChecksumAlgorithm::Sha256 => 32,
    }
}

const HEADER_BINDINGS: &[(ChecksumAlgorithm, &str)] = &[
    (ChecksumAlgorithm::Crc32, "x-amz-checksum-crc32"),
    (ChecksumAlgorithm::Crc32c, "x-amz-checksum-crc32c"),
    (ChecksumAlgorithm::Sha1, "x-amz-checksum-sha1"),
    (ChecksumAlgorithm::Sha256, "x-amz-checksum-sha256"),
];

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderName, HeaderValue};

    fn build_headers(name: &'static str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static(name),
            HeaderValue::from_str(value).unwrap(),
        );
        h
    }

    #[test]
    fn parse_sha256_header_roundtrip() {
        let data = b"hello";
        let raw = compute(ChecksumAlgorithm::Sha256, data);
        let b64 = encode_value(&raw);
        let headers = build_headers("x-amz-checksum-sha256", &b64);
        let parsed = parse_request_checksum(&headers).unwrap().unwrap();
        assert_eq!(parsed.algorithm, ChecksumAlgorithm::Sha256);
        assert_eq!(parsed.value, raw);
        verify(&parsed, data).unwrap();
    }

    #[test]
    fn multiple_checksum_headers_rejected() {
        let mut h = HeaderMap::new();
        h.insert(
            HeaderName::from_static("x-amz-checksum-crc32"),
            HeaderValue::from_static("AAAAAA=="),
        );
        h.insert(
            HeaderName::from_static("x-amz-checksum-sha256"),
            HeaderValue::from_static("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        );
        let err = parse_request_checksum(&h).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn invalid_base64_rejected() {
        let h = build_headers("x-amz-checksum-sha1", "@@@@");
        let err = parse_request_checksum(&h).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn wrong_length_rejected() {
        // base64 of 3 bytes — not 4
        let h = build_headers("x-amz-checksum-crc32", "AAAA");
        let err = parse_request_checksum(&h).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn verify_mismatch_returns_bad_digest() {
        let provided = AdditionalChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            value: vec![0u8; 32],
        };
        let err = verify(&provided, b"some-other-data").unwrap_err();
        assert_eq!(err.code, S3ErrorCode::BadDigest);
    }
}
