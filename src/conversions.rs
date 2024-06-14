use solana_geyser_plugin_interface::geyser_plugin_interface::{
    ReplicaBlockInfoV3, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
};
use solana_sdk::{
    hash::Hash,
    message::{v0::Message, VersionedMessage},
    pubkey::Pubkey,
    transaction::{TransactionError, VersionedTransaction},
};
use solana_transaction_status::{
    EntrySummary, TransactionStatusMeta, VersionedTransactionWithStatusMeta,
};

use crate::cos_types::{
    BlockInfoEvent, CosTransactionStatusMeta, CosVersionedTransactionWithStatusMeta, EntryEvent,
};

impl From<&ReplicaBlockInfoV3<'_>> for BlockInfoEvent {
    fn from(block_info: &ReplicaBlockInfoV3) -> Self {
        BlockInfoEvent {
            parent_slot: block_info.parent_slot,
            parent_blockhash: block_info.parent_blockhash.to_string(),
            slot: block_info.slot,
            blockhash: block_info.parent_blockhash.to_string(),
            rewards: block_info.rewards.to_vec(),
            block_time: block_info.block_time,
            block_height: block_info.block_height,
            executed_transaction_count: block_info.executed_transaction_count,
            entry_count: block_info.entry_count,
        }
    }
}

impl From<&ReplicaEntryInfoV2<'_>> for EntryEvent {
    fn from(entry_info: &ReplicaEntryInfoV2) -> Self {
        EntryEvent {
            slot: entry_info.slot,
            index: entry_info.index,
            num_hashes: entry_info.num_hashes,
            hash: Hash::new(entry_info.hash),
            executed_transaction_count: entry_info.executed_transaction_count,
            starting_transaction_index: entry_info.starting_transaction_index,
        }
    }
}

impl From<&ReplicaTransactionInfoV2<'_>> for CosVersionedTransactionWithStatusMeta {
    fn from(transaction_info: &ReplicaTransactionInfoV2) -> Self {
        CosVersionedTransactionWithStatusMeta {
            transaction: VersionedTransaction {
                signatures: transaction_info.transaction.signatures().to_vec(),
                message: VersionedMessage::V0(Message {
                    header: *transaction_info.transaction.message().header(),
                    account_keys: transaction_info
                        .transaction
                        .message()
                        .account_keys()
                        .iter()
                        .map(ref_pubkey_to_pubkey)
                        .collect(),
                    recent_blockhash: *transaction_info.transaction.message().recent_blockhash(),
                    instructions: transaction_info
                        .transaction
                        .message()
                        .instructions()
                        .to_vec(),
                    address_table_lookups: transaction_info
                        .transaction
                        .message()
                        .message_address_table_lookups()
                        .to_vec(),
                }),
            },
            meta: CosTransactionStatusMeta {
                status: transaction_info
                    .transaction_status_meta
                    .status
                    .clone()
                    .err(),
                loaded_addresses: transaction_info
                    .transaction_status_meta
                    .loaded_addresses
                    .clone(),
            },
        }
    }
}

impl From<EntryEvent> for EntrySummary {
    fn from(entry_event: EntryEvent) -> Self {
        EntrySummary {
            num_hashes: entry_event.num_hashes,
            hash: entry_event.hash,
            num_transactions: entry_event.executed_transaction_count,
            starting_transaction_index: entry_event.starting_transaction_index,
        }
    }
}

pub fn ref_pubkey_to_pubkey(pubkey: &Pubkey) -> Pubkey {
    *pubkey
}

fn status_from_tx_error(err: Option<TransactionError>) -> Result<(), TransactionError> {
    match err {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

impl From<CosVersionedTransactionWithStatusMeta> for VersionedTransactionWithStatusMeta {
    fn from(transaction: CosVersionedTransactionWithStatusMeta) -> Self {
        VersionedTransactionWithStatusMeta {
            transaction: transaction.transaction,
            meta: TransactionStatusMeta {
                status: status_from_tx_error(transaction.meta.status),
                loaded_addresses: transaction.meta.loaded_addresses,
                // Below fields are not used in the context of the COS plugin
                ..Default::default()
            },
        }
    }
}
