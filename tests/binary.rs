//! Binary-level integration tests — spawn the actual compiled `s3lite`
//! executable (via the `CARGO_BIN_EXE_s3lite` env var cargo injects at test
//! build time) and drive it the way an operator would: `init`, `serve`,
//! `kill -HUP`, `backup`, `restore`. These cover what library tests can't —
//! CLI parsing, file permissions, signal wiring, and the real axum_server
//! bind path.

use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command as TokioCommand};
use tokio::time::{Instant, sleep};

const REGION: &str = "us-east-1";

fn s3lite_bin() -> &'static str {
    env!("CARGO_BIN_EXE_s3lite")
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Parse the credentials block that `s3lite init` prints to stdout.
fn parse_init_credentials(stdout: &str) -> (String, String) {
    let mut ak = None;
    let mut sk = None;
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("access_key_id") {
            ak = Some(
                rest.trim_start_matches([' ', '=']).trim().to_string(),
            );
        } else if let Some(rest) = line.strip_prefix("secret_access_key") {
            sk = Some(
                rest.trim_start_matches([' ', '=']).trim().to_string(),
            );
        }
    }
    (ak.expect("access_key_id"), sk.expect("secret_access_key"))
}

async fn sdk_client(endpoint: &str, ak: &str, sk: &str) -> Client {
    let creds = Credentials::new(ak, sk, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(REGION))
        .credentials_provider(creds)
        .endpoint_url(endpoint.to_string())
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    Client::from_conf(s3_config)
}

/// Spawn `s3lite serve --config <path>` and wait until `/health` answers OK
/// (so subsequent SDK calls don't race the listener).
async fn spawn_serve(config_path: &Path, endpoint: &str) -> (Child, BufReader<tokio::process::ChildStderr>) {
    let mut child = TokioCommand::new(s3lite_bin())
        .arg("serve")
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn s3lite serve");
    let stderr = BufReader::new(child.stderr.take().expect("stderr piped"));

    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let health = format!("{endpoint}/health");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Instant::now() >= deadline {
            panic!("server did not become healthy in time");
        }
        if let Ok(resp) = client.get(&health).send().await
            && resp.status() == 200
        {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    (child, stderr)
}

fn send_signal(pid: u32, sig: &str) {
    let status = Command::new("kill")
        .arg(format!("-{sig}"))
        .arg(pid.to_string())
        .status()
        .expect("invoke kill");
    assert!(status.success(), "kill -{sig} {pid} failed");
}

/// Wait for a stderr log line matching `needle` so SIGHUP-driven reloads
/// can be observed deterministically instead of polled with a sleep.
async fn wait_for_log(
    stderr: &mut BufReader<tokio::process::ChildStderr>,
    needle: &str,
    deadline: Duration,
) -> bool {
    let timeout = tokio::time::timeout(deadline, async {
        let mut line = String::new();
        loop {
            line.clear();
            let n = stderr.read_line(&mut line).await.unwrap_or(0);
            if n == 0 {
                return false;
            }
            if line.contains(needle) {
                return true;
            }
        }
    });
    timeout.await.unwrap_or(false)
}

fn write_config(
    path: &Path,
    data_dir: &Path,
    listen_addr: SocketAddr,
    ak: &str,
    sk: &str,
) {
    let body = format!(
        r#"region = "us-east-1"
listen_addr = "{listen_addr}"
data_dir = "{data}"
access_key_id = "{ak}"
secret_access_key = "{sk}"
"#,
        data = data_dir.display(),
    );
    std::fs::write(path, body).unwrap();
}

// ---------------- Tests ----------------

#[test]
fn init_writes_config_and_locks_down_permissions() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");

    let output = Command::new(s3lite_bin())
        .args(["init", "--data-dir"])
        .arg(&data_dir)
        .arg("--listen-addr")
        .arg("127.0.0.1:9999")
        .output()
        .expect("run s3lite init");
    assert!(
        output.status.success(),
        "init failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let (ak, sk) = parse_init_credentials(&stdout);
    assert!(!ak.is_empty() && !sk.is_empty());

    let config_path = data_dir.join("config.toml");
    assert!(config_path.exists(), "config.toml not written");

    let config_text = std::fs::read_to_string(&config_path).unwrap();
    assert!(config_text.contains(&ak));
    assert!(config_text.contains(&sk));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let data_mode = std::fs::metadata(&data_dir).unwrap().permissions().mode() & 0o777;
        let cfg_mode = std::fs::metadata(&config_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(data_mode, 0o700, "data_dir perms should be 0700");
        assert_eq!(cfg_mode, 0o600, "config.toml perms should be 0600");
    }
}

#[tokio::test]
async fn serve_handles_sdk_round_trip_and_reloads_credentials_on_sighup() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let config_path = tmp.path().join("config.toml");
    let port = free_port();
    let endpoint = format!("http://127.0.0.1:{port}");
    let listen: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let ak1 = "AKIAINTEGRATION00001";
    let sk1 = "secret1secret1secret1secret1secret1secre";
    std::fs::create_dir_all(&data_dir).unwrap();
    write_config(&config_path, &data_dir, listen, ak1, sk1);

    let (mut child, mut stderr) = spawn_serve(&config_path, &endpoint).await;
    let pid = child.id().expect("child pid");

    // SDK round-trip with the original credentials.
    let client = sdk_client(&endpoint, ak1, sk1).await;
    client
        .create_bucket()
        .bucket("integ")
        .send()
        .await
        .expect("create_bucket");
    client
        .put_object()
        .bucket("integ")
        .key("hello.txt")
        .body(ByteStream::from(b"hi".to_vec()))
        .send()
        .await
        .expect("put_object");
    let got = client
        .get_object()
        .bucket("integ")
        .key("hello.txt")
        .send()
        .await
        .expect("get_object");
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&bytes[..], b"hi");

    // Rotate credentials in the config file and SIGHUP the server.
    let ak2 = "AKIAINTEGRATION00002";
    let sk2 = "secret2secret2secret2secret2secret2secre";
    write_config(&config_path, &data_dir, listen, ak2, sk2);
    send_signal(pid, "HUP");
    assert!(
        wait_for_log(&mut stderr, "config reloaded", Duration::from_secs(5)).await,
        "expected 'config reloaded' log line after SIGHUP"
    );

    // Old credentials must be rejected, new ones accepted.
    let old_client = sdk_client(&endpoint, ak1, sk1).await;
    let err = old_client
        .list_buckets()
        .send()
        .await
        .expect_err("old creds must fail after rotation");
    let raw = err.into_service_error();
    assert_eq!(raw.meta().code().unwrap_or(""), "InvalidAccessKeyId");

    let new_client = sdk_client(&endpoint, ak2, sk2).await;
    let listed = new_client
        .list_buckets()
        .send()
        .await
        .expect("new creds must work");
    let names: Vec<String> = listed
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(|s| s.to_string()))
        .collect();
    assert_eq!(names, vec!["integ".to_string()]);

    // Clean shutdown via SIGTERM.
    send_signal(pid, "TERM");
    let exit = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("server should exit after SIGTERM")
        .expect("wait");
    assert!(exit.success(), "server exited with non-success status: {exit:?}");
}

#[test]
fn backup_then_restore_via_cli_round_trips_metadata() {
    // Use a synchronous flow: init, briefly serve to write an object, kill,
    // backup, restore into a fresh dir, then serve from the restored data
    // and confirm the object is still listable.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        let snapshot_dir = tmp.path().join("snapshot");
        let restored_dir = tmp.path().join("restored");
        let config_path = tmp.path().join("config.toml");
        let port = free_port();
        let endpoint = format!("http://127.0.0.1:{port}");
        let listen: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let ak = "AKIABACKUPTEST000000";
        let sk = "backupsecretbackupsecretbackupsecretback";

        std::fs::create_dir_all(&data_dir).unwrap();
        write_config(&config_path, &data_dir, listen, ak, sk);

        // 1. Serve, write an object, shut down so the data dir is quiescent.
        {
            let (mut child, _stderr) = spawn_serve(&config_path, &endpoint).await;
            let client = sdk_client(&endpoint, ak, sk).await;
            client
                .create_bucket()
                .bucket("backup-it")
                .send()
                .await
                .expect("create_bucket");
            client
                .put_object()
                .bucket("backup-it")
                .key("snap.txt")
                .body(ByteStream::from(b"snapshot-me".to_vec()))
                .send()
                .await
                .expect("put_object");
            let pid = child.id().expect("pid");
            send_signal(pid, "TERM");
            tokio::time::timeout(Duration::from_secs(10), child.wait())
                .await
                .expect("server exits after SIGTERM")
                .unwrap();
        }

        // 2. Backup the now-quiet data dir.
        let backup_out = Command::new(s3lite_bin())
            .arg("backup")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--output")
            .arg(&snapshot_dir)
            .output()
            .expect("backup");
        assert!(
            backup_out.status.success(),
            "backup failed: {}",
            String::from_utf8_lossy(&backup_out.stderr)
        );

        // 3. Restore into a fresh data dir.
        let restore_out = Command::new(s3lite_bin())
            .arg("restore")
            .arg("--snapshot")
            .arg(&snapshot_dir)
            .arg("--data-dir")
            .arg(&restored_dir)
            .output()
            .expect("restore");
        assert!(
            restore_out.status.success(),
            "restore failed: {}",
            String::from_utf8_lossy(&restore_out.stderr)
        );

        // 4. Serve from restored dir and verify the object is still there.
        let port2 = free_port();
        let endpoint2 = format!("http://127.0.0.1:{port2}");
        let listen2: SocketAddr = format!("127.0.0.1:{port2}").parse().unwrap();
        let config2 = tmp.path().join("config-restored.toml");
        write_config(&config2, &restored_dir, listen2, ak, sk);
        let (mut child, _stderr) = spawn_serve(&config2, &endpoint2).await;
        let pid = child.id().expect("pid");

        let client = sdk_client(&endpoint2, ak, sk).await;
        let got = client
            .get_object()
            .bucket("backup-it")
            .key("snap.txt")
            .send()
            .await
            .expect("get_object after restore");
        let bytes = got.body.collect().await.unwrap().into_bytes();
        assert_eq!(&bytes[..], b"snapshot-me");

        send_signal(pid, "TERM");
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("restored server exits")
            .unwrap();
    });
}
