use solana_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use solana_sdk::{
    clock::{Slot, UnixTimestamp},
    hash::Hash,
    message::{v0::LoadedAddresses, AccountKeys},
    transaction::{TransactionError, VersionedTransaction},
};
use solana_transaction_status::{EntrySummary, Rewards, VersionedConfirmedBlock};

#[derive(Serialize, Debug)]
pub struct CosTransactionInfo {
    pub slot: Slot, // The slot that contains the block with this transaction in it
    pub index: u32, // Where the transaction is located in the block
    pub err: Option<TransactionError>, // None if the transaction executed successfully
    pub memo: Option<String>, // Transaction memo
}

#[derive(Debug, Clone)]
pub struct CosTransactionStatusMeta {
    pub status: Option<TransactionError>,
    pub loaded_addresses: LoadedAddresses,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct CosVersionedTransactionWithStatusMeta {
    pub transaction: VersionedTransaction,
    pub meta: CosTransactionStatusMeta,
}

impl CosVersionedTransactionWithStatusMeta {
    pub fn account_keys(&self) -> AccountKeys {
        AccountKeys::new(
            self.transaction.message.static_account_keys(),
            Some(&self.meta.loaded_addresses),
        )
    }
}

pub struct CosVersionedConfirmedBlockWithEntries {
    pub block: VersionedConfirmedBlock,
    pub entries: Vec<EntrySummary>,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
    pub slot_status: SlotStatus,
}

impl Default for CosVersionedConfirmedBlockWithEntries {
    fn default() -> Self {
        let slot_status = SlotStatus::Processed;
        Self {
            block: VersionedConfirmedBlock {
                previous_blockhash: Default::default(),
                blockhash: Default::default(),
                parent_slot: Default::default(),
                transactions: Default::default(),
                rewards: Default::default(),
                block_time: Default::default(),
                block_height: Default::default(),
            },
            entries: Default::default(),
            executed_transaction_count: Default::default(),
            entry_count: Default::default(),
            slot_status,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionEvent {
    pub slot: Slot,
    pub transaction: CosVersionedTransactionWithStatusMeta,
}

#[derive(Debug, Clone)]
pub struct BlockInfoEvent {
    pub parent_slot: Slot,
    pub parent_blockhash: String,
    pub slot: Slot,
    pub blockhash: String,
    pub rewards: Rewards,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
}

#[derive(Debug, Clone)]
pub struct EntryEvent {
    pub slot: Slot,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Hash,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: usize,
}

pub type RowKey = String;
pub type RowType = String;
pub type RowData = Vec<u8>;
