use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::config::ServerConfig;
use crate::storage::{MetaStore, PartStore};

/// Bundle of long-lived handles shared by every S3 handler.
/// Cloning is cheap (all fields are `Arc`).
///
/// `config` is held inside an `ArcSwap` so a SIGHUP-triggered reload in
/// `main` can atomically publish a fresh `ServerConfig`; in-flight requests
/// keep operating on the snapshot they loaded at entry.
#[derive(Clone)]
pub struct AppState {
    pub meta: Arc<MetaStore>,
    pub parts: Arc<PartStore>,
    pub config: Arc<ArcSwap<ServerConfig>>,
}

impl AppState {
    pub fn new(
        meta: Arc<MetaStore>,
        parts: Arc<PartStore>,
        config: Arc<ServerConfig>,
    ) -> Self {
        Self {
            meta,
            parts,
            config: Arc::new(ArcSwap::new(config)),
        }
    }

    /// Snapshot the live config — handlers and middleware should call this
    /// once per request and operate on the returned `Arc` for consistency.
    pub fn config_snapshot(&self) -> Arc<ServerConfig> {
        self.config.load_full()
    }
}
