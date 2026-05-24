use std::sync::Arc;

use crate::config::ServerConfig;
use crate::storage::{MetaStore, PartStore};

/// Bundle of long-lived handles shared by every S3 handler.
/// Cloning is cheap (all fields are `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub meta: Arc<MetaStore>,
    pub parts: Arc<PartStore>,
    pub config: Arc<ServerConfig>,
}

impl AppState {
    pub fn new(
        meta: Arc<MetaStore>,
        parts: Arc<PartStore>,
        config: Arc<ServerConfig>,
    ) -> Self {
        Self { meta, parts, config }
    }
}
