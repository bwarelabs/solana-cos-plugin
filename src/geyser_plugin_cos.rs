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
        let mut logger_lock = self.logger.lock().unwrap();
        let mut logger_opt = logger_lock.as_mut();
        let logger = logger_opt.as_mut().unwrap();

        match status {
            SlotStatus::Confirmed => {
                let slot_finalized_event = SlotFinalizedEvent { slot };
                match bincode::serialize(&Events::SlotFinalized(slot_finalized_event.clone())) {
                    Ok(event_data) => {
                        let event = Event::new(event_data);
                        logger.append_event(&event)?;

                        self.on_slot_confirmed(slot)?;
                        self.on_slot_finalized(slot_finalized_event)
                    }
                    Err(err) => Err(GeyserPluginError::Custom(err)),
                }
            }
            _ => Ok(()),
        }
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
                let mut logger_lock = self.logger.lock().unwrap();
                let mut logger_opt = logger_lock.as_mut();
                let logger = logger_opt.as_mut().unwrap();

                let transaction = TransactionEvent {
                    slot,
                    transaction: transaction_info.into(),
                };
                match bincode::serialize(&Events::Transaction(transaction.clone())) {
                    Ok(event_data) => {
                        let event = Event::new(event_data);
                        logger.append_event(&event)?;

                        self.on_transaction(transaction)
                    }
                    Err(err) => Err(GeyserPluginError::Custom(err)),
                }
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
                let mut logger_lock = self.logger.lock().unwrap();
                let mut logger_opt = logger_lock.as_mut();
                let logger = logger_opt.as_mut().unwrap();

                let block_info_event: BlockInfoEvent = block_info.into();
                match bincode::serialize(&Events::BlockInfo(block_info_event.clone())) {
                    Ok(event_data) => {
                        let event = Event::new(event_data);
                        logger.append_event(&event)?;

                        self.on_block_info(block_info_event)
                    }
                    Err(err) => Err(GeyserPluginError::Custom(err)),
                }
            }
        }
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions<'_>) -> Result<()> {
        match entry {
            ReplicaEntryInfoVersions::V0_0_1(_) => Err(GeyserPluginError::Custom(Box::new(
                GeyserPluginCosError::ReplicaEntryV001NotSupported,
            ))),
            ReplicaEntryInfoVersions::V0_0_2(entry) => {
                let mut logger_lock = self.logger.lock().unwrap();
                let mut logger_opt = logger_lock.as_mut();
                let logger = logger_opt.as_mut().unwrap();

                let entry_event: EntryEvent = entry.into();
                match bincode::serialize(&Events::Entry(entry_event.clone())) {
                    Ok(event_data) => {
                        let event = Event::new(event_data);
                        logger.append_event(&event)?;

                        self.on_entry(entry_event)
                    }
                    Err(err) => Err(GeyserPluginError::Custom(err)),
                }
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

        block_with_entries
            .block
            .transactions
            .push(tx_event.transaction.into());

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

        Ok(())
    }

    fn on_entry(&self, entry_event: EntryEvent) -> Result<()> {
        let mut datastore = self.datastore.lock().unwrap();
        let block_with_entries = datastore.entry(entry_event.slot).or_default();
        block_with_entries.entries.push(entry_event.into());
        Ok(())
    }

    fn on_slot_confirmed(&self, slot: Slot) -> Result<()> {
        let datastore = self.datastore.lock().unwrap();
        if let Some(block_with_entries) = datastore.get(&slot) {
            if GeyserPluginCos::check_slot_complete(block_with_entries) {
                log::info!("Slot {slot} is complete");
                self.save_confirmed_block_with_entries(slot, block_with_entries)?;
            } else {
                log::warn!("Slot {slot} is not complete, skipping")
            }
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

    fn check_slot_complete(_block_with_entries: &CosVersionedConfirmedBlockWithEntries) -> bool {
        true
    }

    fn save_confirmed_block_with_entries(
        &self,
        slot: Slot,
        confirmed_block: &CosVersionedConfirmedBlockWithEntries,
    ) -> Result<()> {
        let mut storage_lock = self.storage.lock().unwrap();
        let mut storage_opt = storage_lock.as_mut();
        storage_opt.as_mut().unwrap().save(slot, confirmed_block)?;
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
