/// Main entry for the Tencent COS plugin
use {
    crate::{
        cos_types::{BlockInfoEvent, EntryEvent, TransactionEvent},
        datastore::Datastore,
        errors::GeyserPluginCosError,
        geyser_plugin_cos_config::GeyserPluginCosConfig,
        storage::{Storage, StorageManager},
    },
    log, serde_json,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaBlockInfoVersions, ReplicaEntryInfoVersions,
        ReplicaTransactionInfoVersions, Result, SlotStatus,
    },
    solana_sdk::clock::Slot,
    solana_transaction_status::{EntrySummary, VersionedTransactionWithStatusMeta},
    std::{
        fs::File,
        io::Read,
        sync::{Arc, Mutex},
    },
};

#[derive(Default)]
pub struct GeyserPluginCos {
    /// In memory storage for finalized slots
    datastore: Arc<Mutex<Datastore>>,
    /// On disk storage for finalized slots.
    storage: StorageManager,
}

impl std::fmt::Debug for GeyserPluginCos {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl GeyserPlugin for GeyserPluginCos {
    fn name(&self) -> &'static str {
        "GeyserPluginCos"
    }

    /// Do initialization for the COS plugin.
    ///
    /// # Format of the config file:
    /// * The `workspace` sets the destination folder of intermediate files.
    /// "workspace" : "/path/to/workspace"
    ///
    /// # Examples
    ///
    /// {
    ///    "libpath": "/home/solana/target/release/libgayser_plugin_cos.so",
    ///    "workspace": "/path/to/workspace",
    ///    "slot_range": 1000
    ///    "commit_slot_delay": 100
    /// }
    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");

        let plugin_name = self.name();
        log::info!("COS: Loading plugin {plugin_name} from config_file {config_file}");

        let mut file = File::open(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: GeyserPluginCosConfig = serde_json::from_str(&contents).map_err(|err| {
            log::error!("COS: The config file is not in the JSON format expected: {err:?}");
            GeyserPluginError::ConfigFileReadError {
                msg: format!("COS: The config file is not in the JSON format expected: {err:?}"),
            }
        })?;

        self.datastore = Arc::new(Mutex::new(Datastore::new(&config)));
        self.storage = StorageManager::new(&config)?;

        Ok(())
    }

    fn on_unload(&mut self) {
        let plugin_name = self.name();
        log::info!("COS: Unloading plugin: {plugin_name}");
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<()> {
        log::info!("COS: Slot {slot} status: {status:?}");
        self.on_slot_status(slot, status)
    }

    fn notify_transaction(
        &self,
        transaction_info: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        match transaction_info {
            ReplicaTransactionInfoVersions::V0_0_1(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaTransactionV001NotSupported,
            ))),
            ReplicaTransactionInfoVersions::V0_0_2(transaction_info) => {
                let transaction = TransactionEvent {
                    slot,
                    transaction: transaction_info.into(),
                };
                log::debug!(
                    "COS: Slot {slot} index = {} transaction = {}",
                    transaction.transaction.meta.index,
                    transaction.transaction.transaction.signatures[0]
                );
                self.on_transaction(transaction)
            }
        }
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions) -> Result<()> {
        match block_info {
            ReplicaBlockInfoVersions::V0_0_1(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaBlockV001NotSupported,
            ))),
            ReplicaBlockInfoVersions::V0_0_2(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaBlockV001NotSupported,
            ))),
            ReplicaBlockInfoVersions::V0_0_3(block_info) => {
                let block_info_event: BlockInfoEvent = block_info.into();
                log::debug!(
                    "COS: Slot {} metadata tx_count = {}",
                    block_info_event.slot,
                    block_info_event.executed_transaction_count
                );
                log::debug!(
                    "COS: Slot {} metadata entry_count = {}",
                    block_info_event.slot,
                    block_info_event.entry_count
                );
                self.on_block_info(block_info_event)
            }
        }
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions<'_>) -> Result<()> {
        match entry {
            ReplicaEntryInfoVersions::V0_0_1(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaEntryV001NotSupported,
            ))),
            ReplicaEntryInfoVersions::V0_0_2(entry) => {
                let entry_event: EntryEvent = entry.into();
                log::debug!(
                    "COS: Slot {} entry index = {} hash = {}",
                    entry_event.slot,
                    entry_event.index,
                    entry_event.hash
                );
                self.on_entry(entry_event)
            }
        }
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}

impl GeyserPluginCos {
    pub fn new() -> Self {
        Self::default()
    }

    fn on_transaction(&self, tx_event: TransactionEvent) -> Result<()> {
        let mut datastore = self.datastore.lock().unwrap();
        datastore.check_first_slot(tx_event.slot)?;

        let block_with_entries = datastore.get_mut_entry(tx_event.slot);

        let index = tx_event.transaction.meta.index;
        if index >= block_with_entries.block.transactions.len() {
            block_with_entries.block.transactions.resize(
                tx_event.transaction.meta.index + 1,
                VersionedTransactionWithStatusMeta {
                    transaction: Default::default(),
                    meta: Default::default(),
                },
            );
        }
        block_with_entries.block.transactions[index] = tx_event.transaction.into();

        Ok(())
    }

    fn on_block_info(&self, block_info_event: BlockInfoEvent) -> Result<()> {
        let mut datastore = self.datastore.lock().unwrap();
        datastore.check_first_slot(block_info_event.slot)?;

        let block_with_entries = datastore.get_mut_entry(block_info_event.slot);

        log::debug!("COS: BlockInfoEvent {block_info_event:?}");

        block_with_entries.block.previous_blockhash = block_info_event.parent_blockhash;
        block_with_entries.block.blockhash = block_info_event.blockhash;
        block_with_entries.block.parent_slot = block_info_event.parent_slot;
        block_with_entries.block.rewards = block_info_event.rewards;
        block_with_entries.block.block_time = block_info_event.block_time;
        block_with_entries.block.block_height = block_info_event.block_height;
        block_with_entries.executed_transaction_count = block_info_event.executed_transaction_count;
        block_with_entries.entry_count = block_info_event.entry_count;

        Ok(())
    }

    fn on_entry(&self, entry_event: EntryEvent) -> Result<()> {
        let mut datastore = self.datastore.lock().unwrap();
        datastore.check_first_slot(entry_event.slot)?;

        let block_with_entries = datastore.get_mut_entry(entry_event.slot);

        let index = entry_event.index;
        if index >= block_with_entries.entries.len() {
            block_with_entries
                .entries
                .resize_with(entry_event.index + 1, || EntrySummary {
                    num_hashes: Default::default(),
                    hash: Default::default(),
                    num_transactions: Default::default(),
                    starting_transaction_index: Default::default(),
                });
        }
        block_with_entries.entries[index] = entry_event.into();

        Ok(())
    }

    fn on_slot_status(&self, slot: Slot, status: SlotStatus) -> Result<()> {
        {
            let mut datastore = self.datastore.lock().unwrap();
            datastore.check_first_slot(slot)?;

            let block_with_entries = datastore.get_mut_entry(slot);

            block_with_entries.slot_status = status;
        }
        match status {
            SlotStatus::Rooted => self.on_slot_rooted(slot),
            _ => Ok(()),
        }
    }

    fn on_slot_rooted(&self, slot: Slot) -> Result<()> {
        // NOTE: We have no guaranteed order of events for current slot.
        // (e.g. it might be that we still need to process some transactions for the current slot
        // when we receive slot status rooted).
        // NOTE: We only save rooted slots to storage. All non rooted slots are skipped in solana
        // and we don't need to save them, just make sure to cleanup the cache.
        //
        // But we can safely assume that all previous slots are complete.
        let first_slot = if slot >= 100 { slot - 100 } else { 0 };
        let last_slot = if slot >= 10 { slot - 10 } else { 0 };

        if last_slot > 0 {
            for prev_slot in first_slot..=last_slot {
                let block_with_entries;
                {
                    // Unlock mutex as soon as possible
                    let mut datastore = self.datastore.lock().unwrap();
                    block_with_entries = datastore.remove_entry(prev_slot);
                }
                if let Some(block_with_entries) = block_with_entries {
                    if block_with_entries.slot_status != SlotStatus::Rooted {
                        log::debug!("COS: Slot {prev_slot} is not rooted, discarding");
                    } else {
                        log::debug!("COS: Saving slot {prev_slot} to storage");

                        self.storage.save(prev_slot, &block_with_entries)?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the GeyserPluginCos pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = GeyserPluginCos::new();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
