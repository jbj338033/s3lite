use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use clap::{Parser, Subcommand};
use rand::RngCore;
use s3lite::admin;
use s3lite::config::load_config;
use s3lite::http::build_app;
use s3lite::s3::AppState;
use s3lite::s3::maintenance::spawn_daemon;
use s3lite::storage::{MetaStore, PartStore};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

const DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Parser)]
#[command(name = "s3lite", version, about = "Lightweight S3-compatible storage")]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a new data directory and generate a config file with a
    /// fresh access key. Prints the credentials to stdout exactly once.
    Init {
        /// Where to create the data directory.
        #[arg(long)]
        data_dir: PathBuf,
        /// Where to write the config file. Defaults to <data_dir>/config.toml.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Listen address recorded in the config.
        #[arg(long, default_value = "127.0.0.1:9000")]
        listen_addr: String,
        /// AWS region recorded in the config.
        #[arg(long, default_value = "us-east-1")]
        region: String,
    },
    /// Run the HTTP server from a config file.
    Serve {
        #[arg(long)]
        config: PathBuf,
    },
    /// Snapshot a stopped data dir into another directory.
    Backup {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Restore a backup snapshot into a new data dir.
    Restore {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Verify every part file's blake3 hash matches its filename.
    ScanRebuild {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Container entrypoint: bootstrap config from env vars then serve.
    /// Designed for the distroless Docker image, which has no shell to wrap
    /// the binary in. Reads `S3LITE_DATA_DIR`, `S3LITE_LISTEN_ADDR`,
    /// `S3LITE_REGION`, `S3LITE_ACCESS_KEY_ID`, `S3LITE_SECRET_ACCESS_KEY`,
    /// `S3LITE_ENDPOINT_HOST`, `S3LITE_TLS_CERT_PATH`, `S3LITE_TLS_KEY_PATH`.
    /// Creates `<data_dir>/config.toml` on first start (printing generated
    /// credentials to stdout once if AK/SK env vars are absent).
    Auto,
}

fn main() -> ExitCode {
    init_tracing();
    install_crypto_provider();
    let cli = Cli::parse();
    match cli.cmd {
        Command::Init {
            data_dir,
            config,
            listen_addr,
            region,
        } => match init_command(data_dir, config, listen_addr, region) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("init failed: {e}");
                ExitCode::FAILURE
            }
        },
        Command::Serve { config } => {
            let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("failed to construct tokio runtime: {e}");
                    return ExitCode::FAILURE;
                }
            };
            match runtime.block_on(serve_command(config)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("serve failed: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Backup { data_dir, output } => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            match runtime.block_on(admin::backup(&data_dir, &output)) {
                Ok(report) => {
                    println!(
                        "backup complete: buckets={} manifests={} parts_copied={} missing_parts={}",
                        report.buckets,
                        report.manifests,
                        report.parts_copied,
                        report.parts_missing.len(),
                    );
                    if !report.parts_missing.is_empty() {
                        eprintln!("warning: {} part(s) referenced by manifests are missing on disk", report.parts_missing.len());
                    }
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("backup failed: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Restore { snapshot, data_dir } => match admin::restore(&snapshot, &data_dir) {
            Ok(report) => {
                println!("restore complete: parts_copied={}", report.parts_copied);
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("restore failed: {e}");
                ExitCode::FAILURE
            }
        },
        Command::Auto => {
            let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("failed to construct tokio runtime: {e}");
                    return ExitCode::FAILURE;
                }
            };
            match runtime.block_on(auto_command()) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("auto failed: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::ScanRebuild { data_dir } => match admin::scan_rebuild(&data_dir) {
            Ok(report) => {
                println!(
                    "scan complete: checked={} passed={} corrupted={}",
                    report.parts_checked,
                    report.parts_passed,
                    report.corrupted.len(),
                );
                for name in &report.corrupted {
                    eprintln!("corrupted: {name}");
                }
                if report.corrupted.is_empty() {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
            Err(e) => {
                eprintln!("scan failed: {e}");
                ExitCode::FAILURE
            }
        },
    }
}

/// rustls 0.23 dropped its built-in default crypto provider; pick `ring`
/// once at process start so both the inbound TLS listener and any outbound
/// reqwest clients agree on the same backend. Safe to call multiple times.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_thread_names(true)
        // Stream logs to stderr so stdout stays clean for command output
        // (e.g. the credentials block `init` prints, or future `--json`
        // subcommands that emit machine-readable payloads).
        .with_writer(std::io::stderr);
    tracing_subscriber::registry()
        .with(filter)
        .with(json)
        .init();
}

/// Container entrypoint: derive everything from env, bootstrap `config.toml`
/// on first start, then hand off to `serve_command`. Idempotent — re-running
/// against an already-initialized data dir just re-launches the server with
/// the existing config.
async fn auto_command() -> Result<(), String> {
    let data_dir = std::env::var("S3LITE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/data"));
    let config_path = data_dir.join("config.toml");

    if !config_path.exists() {
        let preexisted = data_dir.exists();
        std::fs::create_dir_all(&data_dir).map_err(|e| format!("create data_dir: {e}"))?;
        #[cfg(unix)]
        if !preexisted {
            // Only tighten perms on a freshly-created dir. A pre-existing dir
            // (Docker volume mount, bind mount, operator-prepared path) is the
            // operator's responsibility — we don't own the perms there.
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700))
                .map_err(|e| format!("chmod data_dir: {e}"))?;
        }

        let listen_addr = std::env::var("S3LITE_LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:9000".to_string());
        let region = std::env::var("S3LITE_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let endpoint_host = std::env::var("S3LITE_ENDPOINT_HOST").ok();
        let tls_cert = std::env::var("S3LITE_TLS_CERT_PATH").ok();
        let tls_key = std::env::var("S3LITE_TLS_KEY_PATH").ok();
        if tls_cert.is_some() != tls_key.is_some() {
            return Err(
                "S3LITE_TLS_CERT_PATH and S3LITE_TLS_KEY_PATH must both be set or both omitted"
                    .into(),
            );
        }

        let env_ak = std::env::var("S3LITE_ACCESS_KEY_ID").ok();
        let env_sk = std::env::var("S3LITE_SECRET_ACCESS_KEY").ok();
        let (access_key_id, secret_access_key, generated) = match (env_ak, env_sk) {
            (Some(ak), Some(sk)) => (ak, sk, false),
            (None, None) => (generate_access_key(), generate_secret_key(), true),
            _ => {
                return Err(
                    "S3LITE_ACCESS_KEY_ID and S3LITE_SECRET_ACCESS_KEY must both be set or both omitted"
                        .into(),
                );
            }
        };

        let mut content = format!(
            r#"region = "{region}"
listen_addr = "{listen_addr}"
data_dir = "{data}"
access_key_id = "{access_key_id}"
secret_access_key = "{secret_access_key}"
"#,
            data = data_dir.display(),
        );
        if let Some(host) = endpoint_host {
            content.push_str(&format!("endpoint_host = \"{host}\"\n"));
        }
        if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
            content.push_str(&format!("tls_cert_path = \"{cert}\"\ntls_key_path = \"{key}\"\n"));
        }
        std::fs::write(&config_path, content).map_err(|e| format!("write config: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| format!("chmod config: {e}"))?;
        }

        if generated {
            println!("s3lite auto-init at {}", data_dir.display());
            println!();
            println!("save these credentials — they will not be shown again:");
            println!("  access_key_id     = {access_key_id}");
            println!("  secret_access_key = {secret_access_key}");
        } else {
            tracing::info!(
                data_dir = %data_dir.display(),
                "s3lite auto-init from env credentials"
            );
        }
    }

    serve_command(config_path).await
}

fn init_command(
    data_dir: PathBuf,
    config_path: Option<PathBuf>,
    listen_addr: String,
    region: String,
) -> Result<(), String> {
    std::fs::create_dir_all(&data_dir).map_err(|e| format!("create data_dir: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("chmod data_dir: {e}"))?;
    }

    let config_path = config_path.unwrap_or_else(|| data_dir.join("config.toml"));
    if config_path.exists() {
        return Err(format!(
            "config already exists at {}; remove it first or use --config",
            config_path.display()
        ));
    }

    let access_key_id = generate_access_key();
    let secret_access_key = generate_secret_key();
    let content = format!(
        r#"# s3lite config file — keep this readable only to the s3lite user.
region = "{region}"
listen_addr = "{listen_addr}"
data_dir = "{data}"
access_key_id = "{access_key_id}"
secret_access_key = "{secret_access_key}"

# Optional virtual-hosted addressing — uncomment to enable.
# endpoint_host = "s3.example.com"

# Webhook subscriptions for object events.
# [[webhook]]
# url = "https://example.com/hook"
# bucket = "my-bucket"
# events = ["s3:ObjectCreated:Put"]
"#,
        data = data_dir.display(),
    );
    std::fs::write(&config_path, content).map_err(|e| format!("write config: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&config_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("chmod config: {e}"))?;
    }

    println!("s3lite initialized at {}", data_dir.display());
    println!("config written to {}", config_path.display());
    println!();
    println!("save these credentials — they will not be shown again:");
    println!("  access_key_id     = {access_key_id}");
    println!("  secret_access_key = {secret_access_key}");
    Ok(())
}

async fn serve_command(config_path: PathBuf) -> Result<(), String> {
    let (config, data_dir) =
        load_config(&config_path).map_err(|e| format!("load config: {e}"))?;
    std::fs::create_dir_all(&data_dir).map_err(|e| format!("create data_dir: {e}"))?;

    let meta = Arc::new(
        MetaStore::open(data_dir.join("meta.redb"))
            .await
            .map_err(|e| format!("open meta store: {e}"))?,
    );
    let parts = Arc::new(
        PartStore::open(&data_dir)
            .await
            .map_err(|e| format!("open part store: {e}"))?,
    );
    let state = AppState::new(meta, parts, config.clone());

    let _daemon = spawn_daemon(state.clone(), DEFAULT_MAINTENANCE_INTERVAL);

    let addr = config.listen_addr;
    let tls_config = &config.tls;

    let rustls_handle = if let Some(tls) = tls_config {
        Some(
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_path, &tls.key_path)
                .await
                .map_err(|e| format!("load TLS cert/key: {e}"))?,
        )
    } else {
        None
    };

    // SIGHUP: re-read config.toml + (if TLS is enabled) re-load cert/key.
    // `listen_addr`, `data_dir`, and the TLS termination toggle are bound at
    // boot — only the in-memory `ServerConfig` and the TLS material rotate.
    tokio::spawn(reload_loop(
        config_path.clone(),
        state.clone(),
        rustls_handle.clone(),
    ));

    let app = build_app(state);

    if let Some(rustls_config) = rustls_handle {
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
        });
        tracing::info!(addr = %addr, tls = true, "s3lite listening");
        axum_server::bind_rustls(addr, rustls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| format!("server: {e}"))?;
    } else {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| format!("bind {addr}: {e}"))?;
        let local_addr = listener
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| addr.to_string());
        tracing::info!(addr = %local_addr, tls = false, "s3lite listening");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| format!("server: {e}"))?;
    }
    Ok(())
}

async fn reload_loop(
    config_path: PathBuf,
    state: AppState,
    rustls_handle: Option<axum_server::tls_rustls::RustlsConfig>,
) {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "SIGHUP handler unavailable; reload disabled");
            return;
        }
    };
    while sighup.recv().await.is_some() {
        match load_config(&config_path) {
            Ok((new_cfg, _)) => {
                state.config.store(new_cfg);
                tracing::info!("config reloaded from {}", config_path.display());
            }
            Err(e) => {
                tracing::error!(error = %e, "config reload failed; keeping previous");
            }
        }
        if let Some(handle) = &rustls_handle {
            // We only reach this branch when TLS was enabled at boot, so the
            // current config (whether old or freshly reloaded) is guaranteed
            // to have `tls = Some(_)`.
            let snapshot = state.config_snapshot();
            if let Some(tls) = &snapshot.tls {
                match handle.reload_from_pem_file(&tls.cert_path, &tls.key_path).await {
                    Ok(()) => tracing::info!("TLS cert reloaded"),
                    Err(e) => tracing::error!(error = %e, "TLS cert reload failed"),
                }
            }
        }
    }
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = sigint.recv() => tracing::info!("received SIGINT, shutting down"),
        _ = sigterm.recv() => tracing::info!("received SIGTERM, shutting down"),
    }
}

fn generate_access_key() -> String {
    // 20 chars, AWS-style alphabet (uppercase + digits)
    const ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut rng = rand::rng();
    let mut bytes = [0u8; 20];
    for slot in bytes.iter_mut() {
        let mut b = [0u8; 1];
        rng.fill_bytes(&mut b);
        *slot = ALPHA[(b[0] as usize) % ALPHA.len()];
    }
    String::from_utf8(bytes.to_vec()).expect("ascii")
}

fn generate_secret_key() -> String {
    // 30 raw bytes → base64 = 40 chars (matches AWS secret length so scratchstack's
    // `KSecretKey` size cap can't reject it).
    let mut bytes = [0u8; 30];
    rand::rng().fill_bytes(&mut bytes);
    BASE64.encode(bytes)
}
