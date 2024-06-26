use solana_sdk::{
    clock::{Slot, UnixTimestamp},
    hash::Hash,
    message::{v0::LoadedAddresses, AccountKeys},
    transaction::{TransactionError, VersionedTransaction},
};
use solana_transaction_status::{EntrySummary, Rewards, VersionedConfirmedBlock};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CosSlotStatus {
    Processed,
    Rooted,
    Confirmed,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CosTransactionInfo {
    pub slot: Slot, // The slot that contains the block with this transaction in it
    pub index: u32, // Where the transaction is located in the block
    pub err: Option<TransactionError>, // None if the transaction executed successfully
    pub memo: Option<String>, // Transaction memo
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CosTransactionStatusMeta {
    pub status: Option<TransactionError>,
    pub loaded_addresses: LoadedAddresses,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
}

impl Default for CosVersionedConfirmedBlockWithEntries {
    fn default() -> Self {
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
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SlotStatusEvent {
    pub slot: Slot,
    pub status: CosSlotStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransactionEvent {
    pub slot: Slot,
    pub transaction: CosVersionedTransactionWithStatusMeta,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EntryEvent {
    pub slot: Slot,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Hash,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Events {
    SlotStatus(SlotStatusEvent),
    Transaction(TransactionEvent),
    BlockInfo(BlockInfoEvent),
    Entry(EntryEvent),
}

pub struct SlotData {
    pub block_with_entries: CosVersionedConfirmedBlockWithEntries,
    pub status: CosSlotStatus,
}

pub type RowKey = String;
pub type RowType = String;
pub type RowData = Vec<u8>;
