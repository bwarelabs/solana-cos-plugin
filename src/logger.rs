use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::event::{Event, EventReceiver};
use crate::geyser_plugin_cos_config::GeyserPluginCosConfig;

pub struct LogManager {
    workspace: PathBuf,
    max_file_size: u64,
    current_file: Option<File>,
    current_file_size: u64,
    file_index: u64,
}

impl LogManager {
    pub fn new(config: &GeyserPluginCosConfig) -> io::Result<Self> {
        let log_workspace = PathBuf::from(config.workspace.to_string()).join("logs");

        // Ensure the log directory exists
        std::fs::create_dir_all(&log_workspace)?;

        let last_file_index = Self::find_latest_file_index(&log_workspace)?;
        let new_file = Self::new_log_file(&log_workspace, last_file_index + 1)?;

        Ok(LogManager {
            workspace: log_workspace,
            max_file_size: config.max_file_size_mb * 1024 * 1024,
            current_file: new_file,
            current_file_size: 0,
            file_index: last_file_index + 1,
        })
    }

    pub fn close(&mut self) -> io::Result<()> {
        if let Some(file) = self.current_file.take() {
            file.sync_all()?;
        }
        Ok(())
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

    fn new_log_file(workspace: &Path, file_index: u64) -> io::Result<Option<File>> {
        let file_path = Self::get_current_file_path(workspace, file_index);
        Ok(Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path)?,
        ))
    }

    fn get_current_file_path(workspace: &Path, file_index: u64) -> PathBuf {
        workspace.join(format!("log_{file_index}.bin"))
    }

    fn change_file_if_needed(&mut self) -> io::Result<()> {
        if self.current_file_size >= self.max_file_size {
            self.file_index += 1;
            self.current_file = Some(OpenOptions::new().create(true).append(true).open(
                Self::get_current_file_path(&self.workspace, self.file_index),
            )?);
            self.current_file_size = 0;
        }
        Ok(())
    }

    pub fn append_event(&mut self, event: &Event) -> io::Result<()> {
        self.change_file_if_needed()?;

        let file = self.current_file.as_mut().unwrap();
        let serialized = bincode::serialize(event).unwrap();
        file.write_all(&serialized)?;

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
