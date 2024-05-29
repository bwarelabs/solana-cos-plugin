use serde::{Deserialize, Serialize};

/// The Configuration
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GeyserPluginCosConfig {
    /// The folder path were intermediate files are stored.
    pub workspace: String,
    pub file_prefix: String,
    pub max_file_size_mb: u64,
    pub slot_range: u64,
}
