use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use clap::{Parser, Subcommand};
use rand::RngCore;
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
}

fn main() -> ExitCode {
    init_tracing();
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
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_thread_names(true);
    tracing_subscriber::registry()
        .with(filter)
        .with(json)
        .init();
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

    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .map_err(|e| format!("bind {}: {e}", config.listen_addr))?;
    let local_addr = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| config.listen_addr.to_string());
    tracing::info!(addr = %local_addr, "s3lite listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| format!("server: {e}"))?;
    Ok(())
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
    // 40-byte raw → base64 (60 chars) — comfortably longer than AWS's 40-char
    // secrets but legal as a Sigv4 secret. Discoverable from the config file
    // anyway, the entropy is what matters.
    let mut bytes = [0u8; 30];
    rand::rng().fill_bytes(&mut bytes);
    BASE64.encode(bytes)
}
