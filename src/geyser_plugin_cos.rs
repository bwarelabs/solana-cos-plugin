/// Main entry for the Tencent COS plugin
use {
    crate::{
        cos_types::{
            BlockInfoEvent, CosVersionedConfirmedBlockWithEntries, EntryEvent, Events,
            SlotFinalizedEvent, TransactionEvent,
        },
        errors::GeyserPluginCosError,
        event::{Event, EventReceiver},
        geyser_plugin_cos_config::GeyserPluginCosConfig,
        logger::LogManager,
        storage::StorageManager,
    },
    log, serde_json,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaBlockInfoVersions, ReplicaEntryInfoVersions,
        ReplicaTransactionInfoVersions, Result, SlotStatus,
    },
    solana_sdk::clock::Slot,
    solana_transaction_status::{EntrySummary, VersionedTransactionWithStatusMeta},
    std::{
        collections::HashMap,
        fs::File,
        io::Read,
        sync::{Arc, Mutex},
    },
};

#[derive(Default)]
pub struct GeyserPluginCos {
    datastore: Arc<Mutex<HashMap<Slot, CosVersionedConfirmedBlockWithEntries>>>,
    logger: Arc<Mutex<Option<LogManager>>>,
    storage: Arc<Mutex<Option<StorageManager>>>,
}

impl std::fmt::Debug for GeyserPluginCos {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl EventReceiver for GeyserPluginCos {
    fn receive(&mut self, event: Event) -> std::io::Result<()> {
        if let Err(err) = self.receive_inner(event) {
            log::error!("Error processing event: {err:?}");
        }
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
    /// }
    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> Result<()> {
        solana_logger::setup_with_default("info");

        let plugin_name = self.name();
        log::info!("Loading plugin {plugin_name} from config_file {config_file}");

        let mut file = File::open(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: GeyserPluginCosConfig = serde_json::from_str(&contents).map_err(|err| {
            log::error!("The config file is not in the JSON format expected: {err:?}");
            GeyserPluginError::ConfigFileReadError {
                msg: format!("The config file is not in the JSON format expected: {err:?}"),
            }
        })?;

        self.datastore = Arc::new(Mutex::new(HashMap::new()));

        let logger_instance = LogManager::new(&config)?;
        logger_instance.read_all_events(self)?;

        let storage_instance = StorageManager::new(&config)?;

        self.logger = Arc::new(Mutex::new(Some(logger_instance)));
        self.storage = Arc::new(Mutex::new(Some(storage_instance)));

        Ok(())
    }

    fn on_unload(&mut self) {
        let plugin_name = self.name();
        log::info!("Unloading plugin: {plugin_name}");

        let mut logger = self.logger.lock().unwrap();
        if let Some(logger) = logger.as_mut() {
            if let Err(err) = logger.close() {
                log::error!("Error closing the logger: {err:?}");
            }
        }
        self.datastore.lock().unwrap().clear();
    }

    fn notify_end_of_startup(&self) -> Result<()> {
        log::info!("End of startup");
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: u64,
        _parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<()> {
        log::debug!("EVENT: Slot {slot} status: {status:?}");

        let mut logger_lock = self.logger.lock().unwrap();
        let mut logger_opt = logger_lock.as_mut();
        let logger = logger_opt.as_mut().unwrap();

        if let Err(err) = match status {
            SlotStatus::Rooted => {
                let slot_finalized_event = SlotFinalizedEvent { slot };
                match bincode::serialize(&Events::SlotFinalized(slot_finalized_event.clone())) {
                    Ok(event_data) => {
                        let event = Event::new(event_data);
                        logger.append_event(&event)?;

                        self.on_slot_rooted(slot)?;
                        self.on_slot_finalized(slot_finalized_event)
                    }
                    Err(err) => Err(GeyserPluginError::Custom(err)),
                }
            }
            _ => Ok(()),
        } {
            panic!("Error updating slot status: {err:?}")
        }
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction_info: ReplicaTransactionInfoVersions,
        slot: u64,
    ) -> Result<()> {
        if let Err(err) = match transaction_info {
            ReplicaTransactionInfoVersions::V0_0_1(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaTransactionV001NotSupported,
            ))),
            ReplicaTransactionInfoVersions::V0_0_2(transaction_info) => {
                let mut logger_lock = self.logger.lock().unwrap();
                let mut logger_opt = logger_lock.as_mut();
                let logger = logger_opt.as_mut().unwrap();

                let transaction = TransactionEvent {
                    slot,
                    transaction: transaction_info.into(),
                };
                log::debug!(
                    "EVENT: Slot {slot} index = {} transaction = {}",
                    transaction.transaction.meta.index,
                    transaction.transaction.transaction.signatures[0]
                );

                match bincode::serialize(&Events::Transaction(transaction.clone())) {
                    Ok(event_data) => {
                        let event = Event::new(event_data);
                        logger.append_event(&event)?;

                        self.on_transaction(transaction)
                    }
                    Err(err) => Err(GeyserPluginError::Custom(err)),
                }
            }
        } {
            panic!("Error processing transaction: {err:?}")
        }
        Ok(())
    }

    fn notify_block_metadata(&self, block_info: ReplicaBlockInfoVersions) -> Result<()> {
        if let Err(err) = match block_info {
            ReplicaBlockInfoVersions::V0_0_1(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaBlockV001NotSupported,
            ))),
            ReplicaBlockInfoVersions::V0_0_2(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaBlockV001NotSupported,
            ))),
            ReplicaBlockInfoVersions::V0_0_3(block_info) => {
                let block_info_event: BlockInfoEvent = block_info.into();
                log::debug!(
                    "EVENT: Slot {} metadata tx_count = {}",
                    block_info_event.slot,
                    block_info_event.executed_transaction_count
                );
                log::debug!(
                    "EVENT: Slot {} metadata entry_count = {}",
                    block_info_event.slot,
                    block_info_event.entry_count
                );

                let mut logger_lock = self.logger.lock().unwrap();
                let mut logger_opt = logger_lock.as_mut();
                let logger = logger_opt.as_mut().unwrap();

                match bincode::serialize(&Events::BlockInfo(block_info_event.clone())) {
                    Ok(event_data) => {
                        let event = Event::new(event_data);
                        logger.append_event(&event)?;

                        self.on_block_info(block_info_event)
                    }
                    Err(err) => Err(GeyserPluginError::Custom(err)),
                }
            }
        } {
            panic!("Error processing block metadata: {err:?}")
        }
        Ok(())
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions<'_>) -> Result<()> {
        if let Err(err) = match entry {
            ReplicaEntryInfoVersions::V0_0_1(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaEntryV001NotSupported,
            ))),
            ReplicaEntryInfoVersions::V0_0_2(entry) => {
                let entry_event: EntryEvent = entry.into();
                log::debug!(
                    "EVENT: Slot {} entry index = {} hash = {}",
                    entry_event.slot,
                    entry_event.index,
                    entry_event.hash
                );

                let mut logger_lock = self.logger.lock().unwrap();
                let mut logger_opt = logger_lock.as_mut();
                let logger = logger_opt.as_mut().unwrap();

                match bincode::serialize(&Events::Entry(entry_event.clone())) {
                    Ok(event_data) => {
                        let event = Event::new(event_data);
                        logger.append_event(&event)?;

                        self.on_entry(entry_event)
                    }
                    Err(err) => Err(GeyserPluginError::Custom(err)),
                }
            }
        } {
            panic!("Error processing entry: {err:?}")
        }
        Ok(())
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

    fn receive_inner(&mut self, event: Event) -> Result<()> {
        match bincode::deserialize(&event.data).unwrap() {
            Events::SlotFinalized(finalized_event) => self.on_slot_finalized(finalized_event),
            Events::Transaction(tx_event) => self.on_transaction(tx_event),
            Events::BlockInfo(block_info_event) => self.on_block_info(block_info_event),
            Events::Entry(entry_event) => self.on_entry(entry_event),
        }
    }

    fn on_transaction(&self, tx_event: TransactionEvent) -> Result<()> {
        let mut datastore = self.datastore.lock().unwrap();
        let block_with_entries = datastore.entry(tx_event.slot).or_default();

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
        let block_with_entries = datastore.entry(block_info_event.slot).or_default();

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
        let block_with_entries = datastore.entry(entry_event.slot).or_default();

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

    fn on_slot_rooted(&self, slot: Slot) -> Result<()> {
        let datastore = self.datastore.lock().unwrap();
        if let Some(block_with_entries) = datastore.get(&slot) {
            log::debug!("Saving slot {slot} to storage");
            // Self::check_slot_complete(slot, block_with_entries)?;

            let mut storage_lock = self.storage.lock().unwrap();
            let mut storage_opt = storage_lock.as_mut();
            storage_opt
                .as_mut()
                .unwrap()
                .save(slot, block_with_entries)?;
        }
        Ok(())
    }

    fn on_slot_finalized(&self, slot_finalized_event: SlotFinalizedEvent) -> Result<()> {
        self.datastore
            .lock()
            .unwrap()
            .remove(&slot_finalized_event.slot);
        Ok(())
    }

    fn check_slot_complete(
        slot: Slot,
        block_with_entries: &CosVersionedConfirmedBlockWithEntries,
    ) -> Result<()> {
        if block_with_entries.executed_transaction_count
            != block_with_entries.block.transactions.len() as u64
            || block_with_entries.entry_count != block_with_entries.entries.len() as u64
        {
            return Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::InternalError {
                    msg: format!("Slot {slot} is not complete"),
                },
            )));
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
