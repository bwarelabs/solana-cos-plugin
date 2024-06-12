use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::event::{Event, EventReceiver};
use crate::geyser_plugin_cos_config::GeyserPluginCosConfig;

pub struct LogManager {
    workspace: PathBuf,
    max_file_size: u64,
    current_file_size: u64,
    file_index: u64,
    current_file: File,
}

impl LogManager {
    pub fn new(config: &GeyserPluginCosConfig) -> io::Result<Self> {
        let workspace = PathBuf::from(config.workspace.to_string()).join("logs");

        // Ensure the log directory exists
        std::fs::create_dir_all(&workspace)?;

        let file_index = Self::find_latest_file_index(&workspace)?;
        let current_file = Self::new_log_file(&workspace, file_index)?;
        let current_file_size = Self::get_file_size(&current_file)?;
        let max_file_size = config.max_file_size_mb * 1024 * 1024;

        Ok(LogManager {
            workspace,
            max_file_size,
            current_file,
            current_file_size,
            file_index,
        })
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.current_file.sync_all()
    }

    fn find_latest_file_index(workspace: &Path) -> io::Result<u64> {
        let mut file_index = 0;

        for entry in fs::read_dir(workspace)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(index) = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|s| s.strip_prefix("log_"))
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    if index >= file_index {
                        file_index = index;
                    }
                }
            }
        }
        Ok(file_index)
    }

    fn new_log_file(workspace: &Path, file_index: u64) -> io::Result<File> {
        let file_path = Self::get_file_path(workspace, file_index);
        OpenOptions::new().create(true).append(true).open(file_path)
    }

    fn get_file_size(file: &File) -> io::Result<u64> {
        Ok(file.metadata()?.len())
    }

    fn get_file_path(workspace: &Path, file_index: u64) -> PathBuf {
        workspace.join(format!("log_{file_index}.bin"))
    }

    fn change_file_if_needed(&mut self) -> io::Result<()> {
        if self.current_file_size >= self.max_file_size {
            // Time to change log file
            // Make sure to commit any cached data
            self.current_file.sync_all()?;

            // Previous log file is not needed anymore
            let prev_file_path = Self::get_file_path(&self.workspace, self.file_index - 1);
            if prev_file_path.exists() {
                fs::remove_file(prev_file_path)?;
            }

            // Move to a new log file, leaving only one history log file
            self.file_index += 1;
            self.current_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(Self::get_file_path(&self.workspace, self.file_index))?;
            self.current_file_size = 0;
        }
        Ok(())
    }

    pub fn append_event(&mut self, event: &Event) -> io::Result<()> {
        self.change_file_if_needed()?;

        let serialized = bincode::serialize(event).unwrap();
        self.current_file.write_all(&serialized)?;

        self.current_file_size += serialized.len() as u64;

        Ok(())
    }

    fn read_events_from_file(
        event_receiver: &mut dyn EventReceiver,
        file_path: &Path,
    ) -> io::Result<()> {
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();

        file.read_to_end(&mut buffer)?;
        let mut pos = 0;

        while pos < buffer.len() {
            let event: Event = bincode::deserialize(&buffer[pos..]).unwrap();
            pos += bincode::serialized_size(&event).unwrap() as usize;
            event_receiver.receive(event)?;
        }
        Ok(())
    }

    pub fn read_all_events(&self, event_receiver: &mut dyn EventReceiver) -> io::Result<()> {
        let mut entries = fs::read_dir(&self.workspace)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;
        entries.sort();

        for entry_path in entries {
            if entry_path.is_file() {
                Self::read_events_from_file(event_receiver, &entry_path)?;
            }
        }
        Ok(())
    }
}
