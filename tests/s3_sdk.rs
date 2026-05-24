//! End-to-end Phase 3 verification: drive a live s3lite server with the
//! official `aws-sdk-s3` client. Each test spins up the server on an
//! ephemeral TCP port and runs the SDK against `http://127.0.0.1:<port>`.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use s3lite::config::ServerConfig;
use s3lite::http::build_app;
use s3lite::s3::AppState;
use s3lite::storage::{MetaStore, PartStore};
use s3lite::s3::maintenance::{sweep_at, sweep_gc};
use tempfile::TempDir;
use tokio::net::TcpListener;

const REGION: &str = "us-east-1";
const AK: &str = "AKIAIOSFODNN7EXAMPLE";
const SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

struct Harness {
    _dir: TempDir,
    _server: tokio::task::JoinHandle<()>,
    client: Client,
    endpoint: String,
    state: AppState,
}

async fn start_server() -> Harness {
    let dir = TempDir::new().unwrap();
    let meta = Arc::new(
        MetaStore::open(dir.path().join("meta.redb"))
            .await
            .unwrap(),
    );
    let parts = Arc::new(PartStore::open(dir.path()).await.unwrap());
    let config = ServerConfig::new(
        REGION,
        AK,
        SK,
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    );
    let state = AppState::new(meta, parts, config);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_app(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let endpoint = format!("http://{addr}");
    let endpoint_str = endpoint.clone();
    let creds = Credentials::new(AK, SK, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(REGION))
        .credentials_provider(creds)
        .endpoint_url(endpoint)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    let client = Client::from_conf(s3_config);

    Harness {
        _dir: dir,
        _server: server,
        client,
        endpoint: endpoint_str,
        state,
    }
}

#[tokio::test]
async fn full_object_lifecycle() {
    let h = start_server().await;
    let bucket = "test-bucket";
    let key = "hello.txt";
    let payload = b"hello, s3lite!".to_vec();

    // ListBuckets — should be empty
    let listed = h.client.list_buckets().send().await.unwrap();
    assert!(listed.buckets().is_empty(), "fresh server should have no buckets");

    // CreateBucket
    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create_bucket");

    // ListBuckets — should contain the new bucket
    let listed = h.client.list_buckets().send().await.unwrap();
    let names: Vec<&str> = listed
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .collect();
    assert_eq!(names, vec![bucket]);

    // HeadBucket
    h.client.head_bucket().bucket(bucket).send().await.unwrap();

    // PutObject
    let put = h
        .client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect("put_object");
    assert!(put.e_tag().is_some());

    // HeadObject
    let head = h
        .client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("head_object");
    assert_eq!(head.content_length(), Some(payload.len() as i64));

    // GetObject — full body
    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("get_object");
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), payload.as_slice());

    // GetObject — range
    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range("bytes=0-4")
        .send()
        .await
        .expect("get_object range");
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), &payload[0..=4]);

    // DeleteObject
    h.client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    // GetObject after delete — NoSuchKey
    let err = h
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("get_object should fail after delete");
    let msg = format!("{err:?}");
    assert!(msg.contains("NoSuchKey"), "expected NoSuchKey, got {msg}");

    // DeleteBucket
    h.client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn duplicate_create_bucket_rejected() {
    let h = start_server().await;
    let bucket = "duplicated";
    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let err = h
        .client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect_err("duplicate create_bucket should fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("BucketAlreadyOwnedByYou"),
        "expected BucketAlreadyOwnedByYou, got {msg}"
    );
}

#[tokio::test]
async fn delete_non_empty_bucket_rejected() {
    let h = start_server().await;
    let bucket = "nonempty";
    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();
    let err = h
        .client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect_err("non-empty delete should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("BucketNotEmpty"), "expected BucketNotEmpty, got {msg}");
}

#[tokio::test]
async fn etag_matches_md5_for_single_put() {
    let h = start_server().await;
    let bucket = "etagchk";
    let key = "k";
    let payload = b"the quick brown fox".to_vec();

    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let put = h
        .client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .unwrap();

    use md5::Digest;
    let mut h_md5 = md5::Md5::new();
    h_md5.update(&payload);
    let digest: [u8; 16] = h_md5.finalize().into();
    let expected_etag = format!("\"{}\"", hex::encode(digest));
    assert_eq!(put.e_tag().unwrap(), expected_etag);
}

#[tokio::test]
async fn put_with_bad_content_md5_returns_bad_digest() {
    let h = start_server().await;
    let bucket = "md5chk";
    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();

    // Build a deliberately mismatched Content-MD5
    let payload = b"actual content".to_vec();
    use md5::Digest;
    let mut h_md5 = md5::Md5::new();
    h_md5.update(b"different content");
    let bad_digest: [u8; 16] = h_md5.finalize().into();
    use base64::Engine as _;
    let bad_b64 = base64::engine::general_purpose::STANDARD.encode(bad_digest);

    let err = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .content_md5(bad_b64)
        .body(ByteStream::from(payload))
        .send()
        .await
        .expect_err("PutObject with mismatched Content-MD5 should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("BadDigest"), "expected BadDigest, got {msg}");
}

#[tokio::test]
async fn if_none_match_star_blocks_overwrite() {
    let h = start_server().await;
    let bucket = "cond";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();

    // If-None-Match: * on GET means "give me the object unless it exists"
    // → for existing object, returns 304.
    let res = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .if_none_match("*")
        .send()
        .await;
    // SDK maps 304 to a NotModified error variant.
    let err = res.expect_err("expected NotModified");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("NotModified") || msg.contains("304"),
        "expected NotModified, got {msg}"
    );
}

// ---------------- Phase 4: additional checksums ----------------

#[tokio::test]
async fn put_with_sha256_checksum_stored_and_echoed() {
    use base64::Engine as _;
    use sha2::Digest;

    let h = start_server().await;
    let bucket = "sha256-bucket";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let payload = b"sha256 checksum body".to_vec();
    let mut hasher = sha2::Sha256::new();
    hasher.update(&payload);
    let digest = hasher.finalize();
    let b64 = base64::engine::general_purpose::STANDARD.encode(digest);

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .checksum_sha256(b64.clone())
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect("put_object with sha256 checksum");

    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .expect("get_object with checksum mode");
    assert_eq!(got.checksum_sha256().unwrap(), b64);
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn put_with_mismatched_sha256_returns_bad_digest() {
    use base64::Engine as _;
    let h = start_server().await;
    let bucket = "bad-sha256";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let bad_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    let err = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .checksum_sha256(bad_b64)
        .body(ByteStream::from(b"actual body".to_vec()))
        .send()
        .await
        .expect_err("mismatched sha256 should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("BadDigest"), "expected BadDigest, got {msg}");
}

#[tokio::test]
async fn put_with_crc32_checksum_succeeds() {
    use base64::Engine as _;
    let h = start_server().await;
    let bucket = "crc32-bucket";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let payload = b"crc32 body content".to_vec();
    let digest = crc32fast::hash(&payload).to_be_bytes();
    let b64 = base64::engine::general_purpose::STANDARD.encode(digest);

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .checksum_crc32(b64.clone())
        .body(ByteStream::from(payload))
        .send()
        .await
        .expect("put_object with crc32 checksum");

    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(got.checksum_crc32().unwrap(), b64);
}

#[tokio::test]
async fn put_with_sha1_checksum_succeeds() {
    use base64::Engine as _;
    use sha1::Digest;
    let h = start_server().await;
    let bucket = "sha1-bucket";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let payload = b"sha1 content".to_vec();
    let mut hasher = sha1::Sha1::new();
    hasher.update(&payload);
    let b64 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .checksum_sha1(b64.clone())
        .body(ByteStream::from(payload))
        .send()
        .await
        .expect("put_object with sha1 checksum");
}

// ---------------- Phase 4: conditional PUT ----------------

#[tokio::test]
async fn put_if_none_match_star_blocks_overwrite() {
    let h = start_server().await;
    let bucket = "cond-put";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"first".to_vec()))
        .send()
        .await
        .unwrap();

    let err = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .if_none_match("*")
        .body(ByteStream::from(b"second".to_vec()))
        .send()
        .await
        .expect_err("If-None-Match: * should block overwrite");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("PreconditionFailed") || msg.contains("412"),
        "expected PreconditionFailed, got {msg}"
    );

    // original content unchanged
    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"first");
}

#[tokio::test]
async fn put_if_none_match_star_allows_create() {
    let h = start_server().await;
    let bucket = "cond-create";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("new-key")
        .if_none_match("*")
        .body(ByteStream::from(b"created".to_vec()))
        .send()
        .await
        .expect("If-None-Match: * should allow create when absent");
}

#[tokio::test]
async fn put_if_match_with_wrong_etag_returns_412() {
    let h = start_server().await;
    let bucket = "cond-cas";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();

    let err = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .if_match("\"deadbeef\"")
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .expect_err("If-Match with wrong ETag should fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("PreconditionFailed") || msg.contains("412"),
        "expected PreconditionFailed, got {msg}"
    );
}

#[tokio::test]
async fn put_if_match_with_correct_etag_replaces() {
    let h = start_server().await;
    let bucket = "cond-cas-ok";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    let put1 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();
    let etag = put1.e_tag().unwrap().to_string();

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .if_match(etag)
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .expect("If-Match with correct ETag should replace");

    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"v2");
}

// ---------------- Phase 5: ListObjects ----------------

async fn seed_objects(h: &Harness, bucket: &str, keys: &[&str]) {
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    for k in keys {
        h.client
            .put_object()
            .bucket(bucket)
            .key(*k)
            .body(ByteStream::from(format!("body-{k}").into_bytes()))
            .send()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn list_v2_returns_all_objects_lexicographic_order() {
    let h = start_server().await;
    let bucket = "list-basic";
    seed_objects(&h, bucket, &["b.txt", "a.txt", "c.txt"]).await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap();

    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys, vec!["a.txt", "b.txt", "c.txt"]);
    assert_eq!(resp.key_count(), Some(3));
    assert_eq!(resp.is_truncated(), Some(false));
}

#[tokio::test]
async fn list_v2_prefix_filter() {
    let h = start_server().await;
    let bucket = "list-prefix";
    seed_objects(
        &h,
        bucket,
        &["photos/a.jpg", "photos/b.jpg", "docs/r.pdf", "z.bin"],
    )
    .await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .prefix("photos/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys, vec!["photos/a.jpg", "photos/b.jpg"]);
}

#[tokio::test]
async fn list_v2_delimiter_produces_common_prefixes() {
    let h = start_server().await;
    let bucket = "list-delim";
    seed_objects(
        &h,
        bucket,
        &[
            "photos/2024/jan/01.jpg",
            "photos/2024/jan/02.jpg",
            "photos/2024/feb/01.jpg",
            "photos/2023/dec/31.jpg",
            "videos/v.mp4",
            "root.txt",
        ],
    )
    .await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(prefixes, vec!["photos/", "videos/"]);
    assert_eq!(keys, vec!["root.txt"]);
}

#[tokio::test]
async fn list_v2_prefix_with_delimiter_lists_subdirs() {
    let h = start_server().await;
    let bucket = "list-subdir";
    seed_objects(
        &h,
        bucket,
        &[
            "photos/2023/dec/31.jpg",
            "photos/2024/feb/01.jpg",
            "photos/2024/jan/01.jpg",
            "photos/index.html",
        ],
    )
    .await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .prefix("photos/")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(prefixes, vec!["photos/2023/", "photos/2024/"]);
    assert_eq!(keys, vec!["photos/index.html"]);
}

#[tokio::test]
async fn list_v2_pagination_via_continuation_token() {
    let h = start_server().await;
    let bucket = "list-page";
    let all: Vec<String> = (0..5).map(|i| format!("k{i:02}")).collect();
    let refs: Vec<&str> = all.iter().map(String::as_str).collect();
    seed_objects(&h, bucket, &refs).await;

    let mut collected: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = h
            .client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(2);
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let resp = req.send().await.unwrap();
        for c in resp.contents() {
            collected.push(c.key().unwrap().to_string());
        }
        if resp.is_truncated() != Some(true) {
            break;
        }
        token = resp.next_continuation_token().map(String::from);
        assert!(token.is_some(), "truncated response must include next-token");
    }
    assert_eq!(collected, all);
}

#[tokio::test]
async fn list_v2_start_after_skips_to_resume_point() {
    let h = start_server().await;
    let bucket = "list-start-after";
    seed_objects(&h, bucket, &["a", "b", "c", "d", "e"]).await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .start_after("b")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys, vec!["c", "d", "e"]);
}

#[tokio::test]
async fn list_v2_encoding_type_url_encodes_keys_and_prefixes() {
    let h = start_server().await;
    let bucket = "list-enc";
    seed_objects(&h, bucket, &["pictures/cat dog.jpg"]).await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .delimiter("/")
        .encoding_type(aws_sdk_s3::types::EncodingType::Url)
        .send()
        .await
        .unwrap();
    // The SDK passes through encoded values; "/" becomes "%2F".
    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(prefixes, vec!["pictures%2F"]);
    assert_eq!(resp.encoding_type(), Some(&aws_sdk_s3::types::EncodingType::Url));
}

#[tokio::test]
async fn list_v1_marker_pagination() {
    let h = start_server().await;
    let bucket = "list-v1";
    seed_objects(&h, bucket, &["a", "b", "c", "d"]).await;

    let first = h
        .client
        .list_objects()
        .bucket(bucket)
        .max_keys(2)
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = first.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys, vec!["a", "b"]);
    assert_eq!(first.is_truncated(), Some(true));

    let second = h
        .client
        .list_objects()
        .bucket(bucket)
        .marker("b")
        .max_keys(2)
        .send()
        .await
        .unwrap();
    let keys2: Vec<&str> = second.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys2, vec!["c", "d"]);
}

// ---------------- Phase 6: multipart upload ----------------

#[tokio::test]
async fn multipart_full_round_trip() {
    let h = start_server().await;
    let bucket = "mpu-bucket";
    let key = "big.bin";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let init = h
        .client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("create_multipart_upload");
    let upload_id = init.upload_id().unwrap().to_string();

    let part1 = vec![b'a'; 5 * 1024 * 1024]; // 5 MiB
    let part2 = vec![b'b'; 1024];
    let up1 = h
        .client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part1.clone()))
        .send()
        .await
        .expect("upload_part 1");
    let up2 = h
        .client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(part2.clone()))
        .send()
        .await
        .expect("upload_part 2");

    let completed_parts = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(1)
                .e_tag(up1.e_tag().unwrap())
                .build(),
        )
        .parts(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(2)
                .e_tag(up2.e_tag().unwrap())
                .build(),
        )
        .build();
    let completed = h
        .client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(completed_parts)
        .send()
        .await
        .expect("complete_multipart_upload");

    // Final ETag must follow the multipart "md5(of-concat-md5s)-N" form.
    let final_etag = completed.e_tag().unwrap();
    assert!(
        final_etag.ends_with("-2\""),
        "expected multipart ETag with -2 suffix, got {final_etag}"
    );

    // GetObject returns concatenated bytes.
    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let content_length = got.content_length();
    let returned_etag = got.e_tag().map(String::from);
    let body = got.body.collect().await.unwrap().into_bytes();
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    assert_eq!(body.as_ref(), expected.as_slice());
    assert_eq!(content_length, Some(expected.len() as i64));
    assert_eq!(returned_etag.unwrap(), final_etag);
}

#[tokio::test]
async fn multipart_abort_releases_parts() {
    let h = start_server().await;
    let bucket = "mpu-abort";
    let key = "k";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let init = h
        .client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = init.upload_id().unwrap().to_string();

    h.client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(b"abc".to_vec()))
        .send()
        .await
        .unwrap();

    h.client
        .abort_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await
        .expect("abort_multipart_upload");

    // Subsequent Complete on the same upload_id should fail (no such upload).
    let bogus_parts = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(1)
                .e_tag("\"deadbeef\"")
                .build(),
        )
        .build();
    let err = h
        .client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(bogus_parts)
        .send()
        .await
        .expect_err("complete should fail after abort");
    let msg = format!("{err:?}");
    assert!(msg.contains("NoSuchUpload"), "expected NoSuchUpload, got {msg}");
}

#[tokio::test]
async fn multipart_complete_with_wrong_etag_rejected() {
    let h = start_server().await;
    let bucket = "mpu-etag";
    let key = "k";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let init = h
        .client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = init.upload_id().unwrap().to_string();

    h.client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(b"hello".to_vec()))
        .send()
        .await
        .unwrap();

    let completed_parts = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(1)
                .e_tag("\"badbadbadbadbadbadbadbadbadbadba\"")
                .build(),
        )
        .build();
    let err = h
        .client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(completed_parts)
        .send()
        .await
        .expect_err("complete with wrong etag should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("InvalidArgument"), "expected InvalidArgument, got {msg}");
}

#[tokio::test]
async fn multipart_list_parts_shows_uploaded_parts() {
    let h = start_server().await;
    let bucket = "mpu-list";
    let key = "k";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let init = h
        .client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = init.upload_id().unwrap().to_string();
    for n in 1..=3 {
        h.client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(n)
            .body(ByteStream::from(vec![n as u8; 10]))
            .send()
            .await
            .unwrap();
    }
    let parts = h
        .client
        .list_parts()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .send()
        .await
        .expect("list_parts");
    let nums: Vec<i32> = parts.parts().iter().filter_map(|p| p.part_number()).collect();
    assert_eq!(nums, vec![1, 2, 3]);
}

#[tokio::test]
async fn multipart_replaces_existing_committed_object() {
    let h = start_server().await;
    let bucket = "mpu-replace";
    let key = "obj";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    // First, write a single-part object.
    h.client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(b"version-one".to_vec()))
        .send()
        .await
        .unwrap();

    // Now upload a multipart object to the same key.
    let init = h
        .client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_id = init.upload_id().unwrap().to_string();
    let p1 = vec![b'x'; 8];
    let up = h
        .client
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(p1.clone()))
        .send()
        .await
        .unwrap();
    h.client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(up.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let got = h.client.get_object().bucket(bucket).key(key).send().await.unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), p1.as_slice());
}

#[tokio::test]
async fn list_v2_etag_and_size_in_contents() {
    let h = start_server().await;
    let bucket = "list-meta";
    seed_objects(&h, bucket, &["hello"]).await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let entry = resp.contents().first().unwrap();
    assert_eq!(entry.key(), Some("hello"));
    assert!(entry.e_tag().is_some());
    // "body-hello" is 10 bytes
    assert_eq!(entry.size(), Some(10));
    assert_eq!(
        entry.storage_class(),
        Some(&aws_sdk_s3::types::ObjectStorageClass::Standard)
    );
}

// ---------------- Phase 7: versioning ----------------

async fn enable_versioning(h: &Harness, bucket: &str) {
    h.client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("put_bucket_versioning Enabled");
}

#[tokio::test]
async fn versioning_get_returns_off_initially() {
    let h = start_server().await;
    h.client.create_bucket().bucket("vbk").send().await.unwrap();
    let resp = h.client.get_bucket_versioning().bucket("vbk").send().await.unwrap();
    assert!(
        resp.status().is_none(),
        "expected no Status on fresh bucket, got {:?}",
        resp.status()
    );
}

#[tokio::test]
async fn versioning_put_get_roundtrip() {
    let h = start_server().await;
    h.client.create_bucket().bucket("vbk").send().await.unwrap();
    enable_versioning(&h, "vbk").await;
    let resp = h.client.get_bucket_versioning().bucket("vbk").send().await.unwrap();
    assert_eq!(
        resp.status(),
        Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled)
    );
}

#[tokio::test]
async fn enabled_put_returns_distinct_version_ids() {
    let h = start_server().await;
    let bucket = "ver-puts";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    enable_versioning(&h, bucket).await;

    let r1 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();
    let r2 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .unwrap();

    let id1 = r1.version_id().unwrap();
    let id2 = r2.version_id().unwrap();
    assert_ne!(id1, id2, "Enabled PUTs must yield distinct version ids");

    // GET (no version) returns the latest
    let got = h.client.get_object().bucket(bucket).key("k").send().await.unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"v2");

    // GET versionId targets the older one
    let got_v1 = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .version_id(id1)
        .send()
        .await
        .unwrap();
    let body = got_v1.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"v1");
}

#[tokio::test]
async fn enabled_delete_creates_marker_and_hides_latest() {
    let h = start_server().await;
    let bucket = "ver-del";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    enable_versioning(&h, bucket).await;

    let r1 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();
    let v1_id = r1.version_id().unwrap().to_string();

    let del = h.client.delete_object().bucket(bucket).key("k").send().await.unwrap();
    assert_eq!(del.delete_marker(), Some(true));
    let marker_id = del.version_id().unwrap().to_string();
    assert_ne!(marker_id, v1_id);

    // GET (no version) now 404 — latest is a tombstone
    let err = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .expect_err("GET after tombstone should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("NoSuchKey"), "expected NoSuchKey, got {msg}");

    // GET versionId on the original still returns v1
    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .version_id(&v1_id)
        .send()
        .await
        .unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"v1");

    // ListObjectsV2 hides the key entirely
    let listed = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    assert!(listed.contents().is_empty(), "tombstoned key must not appear");
}

#[tokio::test]
async fn delete_version_id_permanently_removes() {
    let h = start_server().await;
    let bucket = "ver-perm";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    enable_versioning(&h, bucket).await;

    let r1 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();
    let v1_id = r1.version_id().unwrap().to_string();
    let r2 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .unwrap();
    let v2_id = r2.version_id().unwrap().to_string();

    // Permanently delete v2 → latest becomes v1
    h.client
        .delete_object()
        .bucket(bucket)
        .key("k")
        .version_id(&v2_id)
        .send()
        .await
        .unwrap();

    let got = h.client.get_object().bucket(bucket).key("k").send().await.unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"v1");

    // v2 specifically is gone
    let err = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .version_id(&v2_id)
        .send()
        .await
        .expect_err("v2 should be unreachable");
    let _ = err;

    // v1 still present
    let _ = h.client.get_object().bucket(bucket).key("k").version_id(&v1_id).send().await.unwrap();
}

#[tokio::test]
async fn list_object_versions_shows_all_with_is_latest() {
    let h = start_server().await;
    let bucket = "ver-list";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    enable_versioning(&h, bucket).await;

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .unwrap();
    h.client.delete_object().bucket(bucket).key("k").send().await.unwrap();

    let resp = h
        .client
        .list_object_versions()
        .bucket(bucket)
        .send()
        .await
        .expect("list_object_versions");

    let versions = resp.versions();
    let markers = resp.delete_markers();
    assert_eq!(versions.len(), 2, "expected 2 versions, got {versions:?}");
    assert_eq!(markers.len(), 1, "expected 1 delete marker");
    // The delete marker should be the latest
    assert_eq!(markers[0].is_latest(), Some(true));
    // Exactly one of the two object versions should NOT be latest
    let latest_versions = versions.iter().filter(|v| v.is_latest() == Some(true)).count();
    assert_eq!(latest_versions, 0, "no object version should be latest when delete marker is latest");
}

#[tokio::test]
async fn suspended_writes_to_null_keeps_old_versions() {
    let h = start_server().await;
    let bucket = "ver-susp";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    enable_versioning(&h, bucket).await;

    let r1 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"e1".to_vec()))
        .send()
        .await
        .unwrap();
    let enabled_id = r1.version_id().unwrap().to_string();

    // Suspend
    h.client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // PUT under Suspended → version_id "null"
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"s1".to_vec()))
        .send()
        .await
        .unwrap();

    // GET returns the most-recent (suspended write)
    let got = h.client.get_object().bucket(bucket).key("k").send().await.unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"s1");

    // Old enabled-mode version still accessible
    let got_e1 = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .version_id(&enabled_id)
        .send()
        .await
        .unwrap();
    let body = got_e1.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"e1");
}

// ---------------- Phase 8: copy ----------------

#[tokio::test]
async fn copy_object_same_bucket_dedups_bytes() {
    let h = start_server().await;
    let bucket = "cp-same";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let payload = b"to be copied".to_vec();
    let put = h
        .client
        .put_object()
        .bucket(bucket)
        .key("src")
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .unwrap();
    let src_etag = put.e_tag().unwrap().to_string();

    let cp = h
        .client
        .copy_object()
        .bucket(bucket)
        .key("dst")
        .copy_source(format!("{bucket}/src"))
        .send()
        .await
        .expect("copy_object");
    let result_etag = cp.copy_object_result().and_then(|r| r.e_tag()).unwrap();
    assert_eq!(result_etag, src_etag, "copied ETag must equal source ETag");

    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("dst")
        .send()
        .await
        .unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn copy_object_cross_bucket() {
    let h = start_server().await;
    h.client.create_bucket().bucket("src-b").send().await.unwrap();
    h.client.create_bucket().bucket("dst-b").send().await.unwrap();

    h.client
        .put_object()
        .bucket("src-b")
        .key("k")
        .body(ByteStream::from(b"cross".to_vec()))
        .send()
        .await
        .unwrap();

    h.client
        .copy_object()
        .bucket("dst-b")
        .key("k2")
        .copy_source("src-b/k")
        .send()
        .await
        .expect("cross-bucket copy");

    let got = h.client.get_object().bucket("dst-b").key("k2").send().await.unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"cross");
}

#[tokio::test]
async fn copy_object_metadata_directive_replace() {
    let h = start_server().await;
    let bucket = "cp-meta";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("src")
        .metadata("origin", "original")
        .body(ByteStream::from(b"data".to_vec()))
        .send()
        .await
        .unwrap();

    h.client
        .copy_object()
        .bucket(bucket)
        .key("dst")
        .copy_source(format!("{bucket}/src"))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .metadata("origin", "copy")
        .send()
        .await
        .unwrap();

    let head = h.client.head_object().bucket(bucket).key("dst").send().await.unwrap();
    let meta = head.metadata().unwrap();
    assert_eq!(meta.get("origin").map(String::as_str), Some("copy"));
}

#[tokio::test]
async fn copy_object_with_explicit_version_id() {
    let h = start_server().await;
    let bucket = "cp-ver";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    enable_versioning(&h, bucket).await;

    let v1 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("src")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();
    let v1_id = v1.version_id().unwrap().to_string();
    h.client
        .put_object()
        .bucket(bucket)
        .key("src")
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .unwrap();

    h.client
        .copy_object()
        .bucket(bucket)
        .key("dst")
        .copy_source(format!("{bucket}/src?versionId={v1_id}"))
        .send()
        .await
        .unwrap();

    let got = h.client.get_object().bucket(bucket).key("dst").send().await.unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"v1");
}

#[tokio::test]
async fn upload_part_copy_whole_object() {
    let h = start_server().await;
    let bucket = "upc-whole";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    // Source is a 5 MiB object so it can be used as a multipart "part 1".
    let payload = vec![b'a'; 5 * 1024 * 1024];
    h.client
        .put_object()
        .bucket(bucket)
        .key("src")
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .unwrap();

    let init = h
        .client
        .create_multipart_upload()
        .bucket(bucket)
        .key("dst")
        .send()
        .await
        .unwrap();
    let upload_id = init.upload_id().unwrap().to_string();

    let upc = h
        .client
        .upload_part_copy()
        .bucket(bucket)
        .key("dst")
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source(format!("{bucket}/src"))
        .send()
        .await
        .expect("upload_part_copy");
    let part_etag = upc.copy_part_result().and_then(|r| r.e_tag()).unwrap().to_string();

    let tail = b"tail".to_vec();
    let up2 = h
        .client
        .upload_part()
        .bucket(bucket)
        .key("dst")
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(tail.clone()))
        .send()
        .await
        .unwrap();

    h.client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("dst")
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(&part_etag)
                        .build(),
                )
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(2)
                        .e_tag(up2.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let got = h.client.get_object().bucket(bucket).key("dst").send().await.unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    let mut expected = payload.clone();
    expected.extend_from_slice(&tail);
    assert_eq!(body.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn upload_part_copy_with_range() {
    let h = start_server().await;
    let bucket = "upc-range";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    let payload = b"abcdefghijklmnopqrstuvwxyz".to_vec();
    h.client
        .put_object()
        .bucket(bucket)
        .key("src")
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .unwrap();

    let init = h
        .client
        .create_multipart_upload()
        .bucket(bucket)
        .key("dst")
        .send()
        .await
        .unwrap();
    let upload_id = init.upload_id().unwrap().to_string();

    // Inclusive byte range — copies "cdefg"
    let upc = h
        .client
        .upload_part_copy()
        .bucket(bucket)
        .key("dst")
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source(format!("{bucket}/src"))
        .copy_source_range("bytes=2-6")
        .send()
        .await
        .expect("ranged upload_part_copy");
    let etag = upc.copy_part_result().and_then(|r| r.e_tag()).unwrap().to_string();

    h.client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("dst")
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(&etag)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let got = h.client.get_object().bucket(bucket).key("dst").send().await.unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"cdefg");
}

#[tokio::test]
async fn copy_object_missing_source_returns_no_such_key() {
    let h = start_server().await;
    h.client.create_bucket().bucket("cp-mss").send().await.unwrap();
    let err = h
        .client
        .copy_object()
        .bucket("cp-mss")
        .key("dst")
        .copy_source("cp-mss/does-not-exist")
        .send()
        .await
        .expect_err("copy from missing source should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("NoSuchKey"), "expected NoSuchKey, got {msg}");
}

// ---------------- Phase 9: tagging ----------------

#[tokio::test]
async fn put_get_object_tagging_roundtrip() {
    let h = start_server().await;
    let bucket = "tag-rt";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();

    h.client
        .put_object_tagging()
        .bucket(bucket)
        .key("k")
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("env")
                        .value("prod")
                        .build()
                        .unwrap(),
                )
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("team")
                        .value("infra")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("put_object_tagging");

    let resp = h
        .client
        .get_object_tagging()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .expect("get_object_tagging");
    let mut pairs: Vec<(String, String)> = resp
        .tag_set()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("env".to_string(), "prod".to_string()),
            ("team".to_string(), "infra".to_string()),
        ]
    );
}

#[tokio::test]
async fn x_amz_tagging_header_on_put_sets_tags() {
    let h = start_server().await;
    let bucket = "tag-hdr";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .tagging("env=staging&owner=alice")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();

    let resp = h
        .client
        .get_object_tagging()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    let mut pairs: Vec<(String, String)> = resp
        .tag_set()
        .iter()
        .map(|t| (t.key().to_string(), t.value().to_string()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("env".to_string(), "staging".to_string()),
            ("owner".to_string(), "alice".to_string()),
        ]
    );
}

#[tokio::test]
async fn delete_object_tagging_clears_all() {
    let h = start_server().await;
    let bucket = "tag-del";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .tagging("a=1&b=2")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();

    h.client
        .delete_object_tagging()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .expect("delete_object_tagging");

    let resp = h
        .client
        .get_object_tagging()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    assert!(resp.tag_set().is_empty());
}

// ---------------- Phase 9: CORS ----------------

#[tokio::test]
async fn cors_put_get_roundtrip() {
    let h = start_server().await;
    let bucket = "cors-rt";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    let rule = aws_sdk_s3::types::CorsRule::builder()
        .allowed_origins("https://example.com")
        .allowed_methods("GET")
        .allowed_methods("PUT")
        .allowed_headers("Content-Type")
        .max_age_seconds(3600)
        .build()
        .unwrap();
    h.client
        .put_bucket_cors()
        .bucket(bucket)
        .cors_configuration(
            aws_sdk_s3::types::CorsConfiguration::builder()
                .cors_rules(rule)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("put_bucket_cors");

    let resp = h
        .client
        .get_bucket_cors()
        .bucket(bucket)
        .send()
        .await
        .expect("get_bucket_cors");
    let rules = resp.cors_rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].allowed_origins(), &["https://example.com"]);
    assert_eq!(rules[0].max_age_seconds(), Some(3600));
}

#[tokio::test]
async fn cors_preflight_returns_allow_headers() {
    let h = start_server().await;
    let bucket = "cors-pre";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_bucket_cors()
        .bucket(bucket)
        .cors_configuration(
            aws_sdk_s3::types::CorsConfiguration::builder()
                .cors_rules(
                    aws_sdk_s3::types::CorsRule::builder()
                        .allowed_origins("https://app.example")
                        .allowed_methods("GET")
                        .allowed_methods("PUT")
                        .allowed_headers("authorization")
                        .max_age_seconds(60)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Send OPTIONS via reqwest (the SDK doesn't expose preflight directly).
    let endpoint = h.endpoint.clone();
    let client = reqwest::Client::new();
    let resp = client
        .request(reqwest::Method::OPTIONS, format!("{endpoint}/{bucket}/key"))
        .header("Origin", "https://app.example")
        .header("Access-Control-Request-Method", "PUT")
        .header("Access-Control-Request-Headers", "authorization")
        .send()
        .await
        .expect("preflight HTTP");
    assert!(resp.status().is_success(), "preflight returned {}", resp.status());
    assert_eq!(
        resp.headers().get("access-control-allow-origin").and_then(|h| h.to_str().ok()),
        Some("https://app.example")
    );
    let methods = resp
        .headers()
        .get("access-control-allow-methods")
        .and_then(|h| h.to_str().ok())
        .unwrap();
    assert!(methods.contains("PUT"));
    assert_eq!(
        resp.headers()
            .get("access-control-max-age")
            .and_then(|h| h.to_str().ok()),
        Some("60")
    );
}

#[tokio::test]
async fn cors_preflight_rejected_without_matching_rule() {
    let h = start_server().await;
    let bucket = "cors-deny";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_bucket_cors()
        .bucket(bucket)
        .cors_configuration(
            aws_sdk_s3::types::CorsConfiguration::builder()
                .cors_rules(
                    aws_sdk_s3::types::CorsRule::builder()
                        .allowed_origins("https://allowed.example")
                        .allowed_methods("GET")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let endpoint = h.endpoint.clone();
    let client = reqwest::Client::new();
    let resp = client
        .request(reqwest::Method::OPTIONS, format!("{endpoint}/{bucket}/key"))
        .header("Origin", "https://elsewhere.example")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

// ---------------- Phase 10: object lock ----------------

#[tokio::test]
async fn create_bucket_with_object_lock_enables_versioning() {
    let h = start_server().await;
    let bucket = "lock-create";
    h.client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let cfg = h
        .client
        .get_object_lock_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("get_object_lock_configuration");
    let lock_cfg = cfg.object_lock_configuration().unwrap();
    assert_eq!(
        lock_cfg.object_lock_enabled(),
        Some(&aws_sdk_s3::types::ObjectLockEnabled::Enabled)
    );

    // Lock-enabled bucket auto-enables versioning
    let v = h.client.get_bucket_versioning().bucket(bucket).send().await.unwrap();
    assert_eq!(
        v.status(),
        Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled)
    );
}

#[tokio::test]
async fn put_object_retention_blocks_delete_until_expiry() {
    let h = start_server().await;
    let bucket = "lock-ret";
    h.client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let r = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();
    let vid = r.version_id().unwrap().to_string();

    // Set retention 1 day in the future
    let future = aws_sdk_s3::primitives::DateTime::from_secs(
        (time::OffsetDateTime::now_utc() + time::Duration::days(1)).unix_timestamp(),
    );
    h.client
        .put_object_retention()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .retention(
            aws_sdk_s3::types::ObjectLockRetention::builder()
                .mode(aws_sdk_s3::types::ObjectLockRetentionMode::Compliance)
                .retain_until_date(future)
                .build(),
        )
        .send()
        .await
        .expect("put_object_retention");

    // Versioned DELETE on the protected version must fail
    let err = h
        .client
        .delete_object()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .send()
        .await
        .expect_err("retention should block delete");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessForbidden") || msg.contains("AccessDenied") || msg.contains("403"),
        "expected access denied, got {msg}"
    );

    // Reading retention back
    let got = h
        .client
        .get_object_retention()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.retention().and_then(|r| r.mode()),
        Some(&aws_sdk_s3::types::ObjectLockRetentionMode::Compliance)
    );
}

#[tokio::test]
async fn legal_hold_on_blocks_delete_off_allows() {
    let h = start_server().await;
    let bucket = "lock-lh";
    h.client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let r = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();
    let vid = r.version_id().unwrap().to_string();

    h.client
        .put_object_legal_hold()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await
        .expect("put_object_legal_hold ON");

    let err = h
        .client
        .delete_object()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .send()
        .await
        .expect_err("legal hold ON should block delete");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessForbidden") || msg.contains("AccessDenied") || msg.contains("403"),
        "expected access denied, got {msg}"
    );

    // Turn legal hold OFF
    h.client
        .put_object_legal_hold()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::Off)
                .build(),
        )
        .send()
        .await
        .unwrap();

    h.client
        .delete_object()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .send()
        .await
        .expect("delete after legal hold OFF");
}

#[tokio::test]
async fn put_object_with_per_request_lock_headers() {
    let h = start_server().await;
    let bucket = "lock-hdr";
    h.client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let future = aws_sdk_s3::primitives::DateTime::from_secs(
        (time::OffsetDateTime::now_utc() + time::Duration::days(1)).unix_timestamp(),
    );
    let r = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"x".to_vec()))
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Compliance)
        .object_lock_retain_until_date(future)
        .send()
        .await
        .unwrap();
    let vid = r.version_id().unwrap().to_string();

    let got = h
        .client
        .get_object_retention()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .send()
        .await
        .unwrap();
    assert!(got.retention().is_some());

    // Versioned DELETE must fail due to retention applied at PUT time
    let err = h
        .client
        .delete_object()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .send()
        .await
        .expect_err("retention applied at PUT should block delete");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessForbidden") || msg.contains("AccessDenied") || msg.contains("403"),
        "expected access denied, got {msg}"
    );
}

#[tokio::test]
async fn compliance_retention_cannot_be_shortened() {
    let h = start_server().await;
    let bucket = "lock-shorten";
    h.client
        .create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();

    let r = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();
    let vid = r.version_id().unwrap().to_string();

    let now = time::OffsetDateTime::now_utc();
    let far = aws_sdk_s3::primitives::DateTime::from_secs(
        (now + time::Duration::days(10)).unix_timestamp(),
    );
    let near = aws_sdk_s3::primitives::DateTime::from_secs(
        (now + time::Duration::days(2)).unix_timestamp(),
    );

    h.client
        .put_object_retention()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .retention(
            aws_sdk_s3::types::ObjectLockRetention::builder()
                .mode(aws_sdk_s3::types::ObjectLockRetentionMode::Compliance)
                .retain_until_date(far)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let err = h
        .client
        .put_object_retention()
        .bucket(bucket)
        .key("k")
        .version_id(&vid)
        .retention(
            aws_sdk_s3::types::ObjectLockRetention::builder()
                .mode(aws_sdk_s3::types::ObjectLockRetentionMode::Compliance)
                .retain_until_date(near)
                .build(),
        )
        .send()
        .await
        .expect_err("shortening compliance retention should fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessForbidden") || msg.contains("AccessDenied") || msg.contains("403"),
        "expected access denied, got {msg}"
    );
}


// ---------------- Phase 11: lifecycle ----------------

#[tokio::test]
async fn lifecycle_put_get_roundtrip() {
    let h = start_server().await;
    let bucket = "lc-rt";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    let rule = aws_sdk_s3::types::LifecycleRule::builder()
        .id("rule-1")
        .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
        .filter(
            aws_sdk_s3::types::LifecycleRuleFilter::builder()
                .prefix("logs/")
                .build(),
        )
        .expiration(
            aws_sdk_s3::types::LifecycleExpiration::builder()
                .days(30)
                .build(),
        )
        .build()
        .unwrap();
    h.client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(rule)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("put_bucket_lifecycle_configuration");

    let resp = h
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
        .expect("get_bucket_lifecycle_configuration");
    let rules = resp.rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id(), Some("rule-1"));
    assert_eq!(rules[0].expiration().and_then(|e| e.days()), Some(30));
}

#[tokio::test]
async fn lifecycle_expiration_tombstones_current_version() {
    let h = start_server().await;
    let bucket = "lc-exp";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    enable_versioning(&h, bucket).await;
    h.client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    aws_sdk_s3::types::LifecycleRule::builder()
                        .id("expire-1d")
                        .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
                        .filter(
                            aws_sdk_s3::types::LifecycleRuleFilter::builder()
                                .prefix(String::new())
                                .build(),
                        )
                        .expiration(
                            aws_sdk_s3::types::LifecycleExpiration::builder().days(1).build(),
                        )
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"data".to_vec()))
        .send()
        .await
        .unwrap();

    let future = time::OffsetDateTime::now_utc() + time::Duration::days(2);
    let report = sweep_at(&h.state, future).await.expect("sweep_at");
    assert_eq!(report.expired_current, 1, "should have expired one current version");

    let err = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .expect_err("after expiration GET should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("NoSuchKey"), "expected NoSuchKey, got {msg}");
}

#[tokio::test]
async fn lifecycle_aborts_old_multipart_uploads() {
    let h = start_server().await;
    let bucket = "lc-abort";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    aws_sdk_s3::types::LifecycleRule::builder()
                        .id("abort-1d")
                        .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
                        .filter(
                            aws_sdk_s3::types::LifecycleRuleFilter::builder()
                                .prefix(String::new())
                                .build(),
                        )
                        .abort_incomplete_multipart_upload(
                            aws_sdk_s3::types::AbortIncompleteMultipartUpload::builder()
                                .days_after_initiation(1)
                                .build(),
                        )
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let init = h
        .client
        .create_multipart_upload()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    let upload_id = init.upload_id().unwrap().to_string();
    h.client
        .upload_part()
        .bucket(bucket)
        .key("k")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(b"data".to_vec()))
        .send()
        .await
        .unwrap();

    let future = time::OffsetDateTime::now_utc() + time::Duration::days(2);
    let report = sweep_at(&h.state, future).await.expect("sweep_at");
    assert_eq!(report.aborted_multipart, 1, "should have aborted one upload");

    let err = h
        .client
        .complete_multipart_upload()
        .bucket(bucket)
        .key("k")
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag("\"deadbeef\"")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("complete after abort should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("NoSuchUpload"), "expected NoSuchUpload, got {msg}");
}

#[tokio::test]
async fn sweep_gc_idempotent_after_inline_gc() {
    let h = start_server().await;
    let bucket = "gc-orphans";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"abc".to_vec()))
        .send()
        .await
        .unwrap();
    h.client.delete_object().bucket(bucket).key("k").send().await.unwrap();
    let first = sweep_gc(&h.state).await.unwrap();
    let second = sweep_gc(&h.state).await.unwrap();
    assert_eq!(first, 0, "inline GC already cleaned this part");
    assert_eq!(second, 0);
}

// ---------------- Phase 12: webhook event notifications ----------------

use std::sync::Mutex;
use s3lite::config::{EventType, WebhookSubscription};
use s3lite::s3::events::decode_dlq_entry;

/// Start a tiny HTTP server that records every received body. Used as the
/// destination of webhook events under test.
async fn spawn_webhook_sink() -> (String, std::sync::Arc<Mutex<Vec<String>>>) {
    use axum::routing::post;
    use axum::Router;
    use tokio::net::TcpListener;

    let received: std::sync::Arc<Mutex<Vec<String>>> = std::sync::Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let app = Router::new().route(
        "/sink",
        post(move |body: String| {
            let received = received_clone.clone();
            async move {
                received.lock().unwrap().push(body);
                axum::http::StatusCode::OK
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}/sink"), received)
}

async fn start_server_with_webhooks(subs: Vec<WebhookSubscription>) -> Harness {
    let dir = TempDir::new().unwrap();
    let meta = Arc::new(MetaStore::open(dir.path().join("meta.redb")).await.unwrap());
    let parts = Arc::new(PartStore::open(dir.path()).await.unwrap());
    let mut config = (*s3lite::config::ServerConfig::new(
        REGION,
        AK,
        SK,
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    ))
    .clone();
    config.webhook_subscriptions = subs;
    config.allow_loopback_webhooks = true;
    let config = std::sync::Arc::new(config);
    let state = AppState::new(meta, parts, config);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_app(state.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let endpoint = format!("http://{addr}");
    let creds = aws_sdk_s3::config::Credentials::new(AK, SK, None, None, "test");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .credentials_provider(creds)
        .endpoint_url(endpoint.clone())
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    let client = Client::from_conf(s3_config);
    Harness {
        _dir: dir,
        _server: server,
        client,
        endpoint,
        state,
    }
}

#[tokio::test]
async fn webhook_delivered_on_put_object() {
    let (sink_url, received) = spawn_webhook_sink().await;
    let h = start_server_with_webhooks(vec![WebhookSubscription {
        bucket: "wh-bk".to_string(),
        events: vec![EventType::ObjectCreatedPut],
        prefix: None,
        suffix: None,
        url: sink_url,
    }])
    .await;
    h.client.create_bucket().bucket("wh-bk").send().await.unwrap();
    h.client
        .put_object()
        .bucket("wh-bk")
        .key("k")
        .body(ByteStream::from(b"payload".to_vec()))
        .send()
        .await
        .unwrap();

    // Webhook is fire-and-forget; give it a moment.
    for _ in 0..20 {
        if !received.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let bodies = received.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1, "expected exactly one webhook delivery");
    let body = &bodies[0];
    assert!(body.contains("s3:ObjectCreated:Put"), "body: {body}");
    assert!(body.contains("\"name\":\"wh-bk\""), "body: {body}");
    assert!(body.contains("\"key\":\"k\""), "body: {body}");
    assert!(body.contains("\"size\":7"), "body: {body}");
}

#[tokio::test]
async fn webhook_prefix_filter_excludes_nonmatching() {
    let (sink_url, received) = spawn_webhook_sink().await;
    let h = start_server_with_webhooks(vec![WebhookSubscription {
        bucket: "wh-pf".to_string(),
        events: vec![],
        prefix: Some("photos/".to_string()),
        suffix: None,
        url: sink_url,
    }])
    .await;
    h.client.create_bucket().bucket("wh-pf").send().await.unwrap();
    h.client
        .put_object()
        .bucket("wh-pf")
        .key("docs/a.pdf")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();
    h.client
        .put_object()
        .bucket("wh-pf")
        .key("photos/a.jpg")
        .body(ByteStream::from(b"y".to_vec()))
        .send()
        .await
        .unwrap();
    for _ in 0..20 {
        if !received.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let bodies = received.lock().unwrap().clone();
    assert_eq!(bodies.len(), 1, "only the matching key should fire");
    assert!(bodies[0].contains("\"key\":\"photos/a.jpg\""), "{:?}", bodies);
}

#[tokio::test]
async fn webhook_to_link_local_blocked_and_dlq_recorded() {
    let h = start_server_with_webhooks(vec![WebhookSubscription {
        bucket: "wh-ssrf".to_string(),
        events: vec![EventType::ObjectCreatedPut],
        prefix: None,
        suffix: None,
        // Disable loopback again for this test by overriding via a manual config — but
        // since our helper turns loopback on, use a clearly-private address that's
        // still blocked even with loopback allowed.
        url: "http://169.254.169.254/latest/meta-data/".to_string(),
    }])
    .await;
    h.client.create_bucket().bucket("wh-ssrf").send().await.unwrap();
    h.client
        .put_object()
        .bucket("wh-ssrf")
        .key("k")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();

    // Wait for retries + DLQ insertion to settle.
    for _ in 0..40 {
        if !h.state.meta.list_dlq().await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let dlq = h.state.meta.list_dlq().await.unwrap();
    assert!(!dlq.is_empty(), "expected DLQ entry for SSRF-blocked webhook");
    let (_, bytes) = &dlq[0];
    let record = decode_dlq_entry(bytes).expect("decode dlq");
    assert!(record.url.contains("169.254"));
    assert!(record.last_error.contains("SSRF"));
}
