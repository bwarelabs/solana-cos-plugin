use serde::{Deserialize, Serialize};

/// The Configuration
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GeyserPluginCosConfig {
    /// The folder path were block information is saved.
    pub workspace: String,
    /// The maximum file size in MB for a log file (e.g. were notifications are backed up).
    pub max_file_size_mb: u64,
    /// The maximum number of slots to include in a slot range.
    pub slot_range: u64,
    /// Commit slot delay in number of slots.
    pub commit_slot_delay: u64,
}
