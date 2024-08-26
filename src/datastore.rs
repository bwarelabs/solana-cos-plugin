use {
    crate::{
        cos_types::CosVersionedConfirmedBlockWithEntries, errors::GeyserPluginCosError,
        geyser_plugin_cos_config::GeyserPluginCosConfig,
    },
    solana_geyser_plugin_interface::geyser_plugin_interface::{GeyserPluginError, Result},
    solana_sdk::clock::Slot,
    std::collections::HashMap,
};

#[derive(Default)]
pub struct Datastore {
    /// In memory cache for finalized slots.
    cache: HashMap<Slot, CosVersionedConfirmedBlockWithEntries>,
    /// First valid slot.
    first_slot: Option<u64>,
    /// The number of slots in each range.
    slot_range: u64,
}

impl Datastore {
    pub fn new(config: &GeyserPluginCosConfig) -> Self {
        let slot_range = config.slot_range;
        Self {
            slot_range,
            ..Default::default()
        }
    }

    pub fn check_first_slot(&mut self, slot: Slot) -> Result<()> {
        if self.first_slot.is_none() {
            let first_slot = if slot % self.slot_range == 0 {
                slot
            } else {
                slot + (self.slot_range - slot % self.slot_range)
            };
            self.first_slot = Some(first_slot);
            log::info!("Setting first slot to {first_slot}");
        }
        if let Some(first_slot) = self.first_slot {
            if first_slot > slot {
                return Err(GeyserPluginError::Custom(Box::new(
                    GeyserPluginCosError::SkipIncompleteBlockRange,
                )));
            }
        }
        Ok(())
    }

    pub fn get_mut_entry(&mut self, slot: Slot) -> &mut CosVersionedConfirmedBlockWithEntries {
        self.cache.entry(slot).or_default()
    }

    pub fn remove_entry(&mut self, slot: Slot) -> Option<CosVersionedConfirmedBlockWithEntries> {
        self.cache.remove(&slot)
    }
}
