use crate::compression::compress_best;
use solana_sdk::clock::Slot;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::AccountKeys;
use solana_sdk::pubkey::Pubkey;
use solana_storage_proto::convert::{entries, generated, tx_by_addr};
use solana_transaction_status::extract_memos::ExtractMemos;
use solana_transaction_status::{
    EntrySummary, TransactionByAddrInfo, VersionedTransactionWithStatusMeta,
};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Result, Write};
use std::path::{Path, PathBuf};

use crate::cos_types::{
    CosTransactionInfo, CosVersionedConfirmedBlockWithEntries,
    CosVersionedTransactionWithStatusMeta, RowData, RowKey, RowType,
};
use crate::geyser_plugin_cos_config::GeyserPluginCosConfig;

enum KeyType<'a> {
    MemoProgram,
    OtherProgram,
    Unknown(&'a Pubkey),
}

/// Manages storage of confirmed blocks and transactions
pub struct StorageManager {
    /// The directory where finalized block ranges are moved.
    ready_path: PathBuf,
    /// The directory where finalized block ranges are staged.
    staging_path: PathBuf,
    /// The number of slots in each range.
    slot_range: u64,
}

impl StorageManager {
    pub fn new(config: &GeyserPluginCosConfig) -> Result<Self> {
        let slot_range = config.slot_range;

        // Ensure the storage directory exists
        let ready_path = PathBuf::from(config.workspace.to_string()).join("storage");
        let staging_path = PathBuf::from(config.workspace.to_string()).join("staging");

        std::fs::create_dir_all(&ready_path)?;

        Ok(StorageManager {
            ready_path,
            staging_path,
            slot_range,
        })
    }

    /// Save a confirmed block and its transactions to storage
    ///
    /// Note that this code is copied from solana and should be kept in sync with the original.
    pub fn save(
        &self,
        slot: Slot,
        confirmed_block: &CosVersionedConfirmedBlockWithEntries,
    ) -> Result<()> {
        let mut by_addr: HashMap<&Pubkey, Vec<TransactionByAddrInfo>> = HashMap::new();
        let CosVersionedConfirmedBlockWithEntries {
            block: confirmed_block,
            entries,
            ..
        } = confirmed_block;

        let mut tx_cells = Vec::with_capacity(confirmed_block.transactions.len());
        for (index, transaction_with_meta) in confirmed_block.transactions.iter().enumerate() {
            let VersionedTransactionWithStatusMeta { meta, transaction } = transaction_with_meta;
            let err = meta.status.clone().err();
            let index = index as u32;
            let signature = transaction.signatures[0];
            let memo = solana_transaction_status::extract_and_fmt_memos(transaction_with_meta);

            for address in transaction_with_meta.account_keys().iter() {
                if !solana_program::sysvar::is_sysvar_id(address) {
                    by_addr
                        .entry(address)
                        .or_default()
                        .push(TransactionByAddrInfo {
                            signature,
                            err: err.clone(),
                            index,
                            memo: memo.clone(),
                            block_time: confirmed_block.block_time,
                        });
                }
            }

            tx_cells.push((
                signature.to_string(),
                CosTransactionInfo {
                    slot,
                    index,
                    err,
                    memo,
                },
            ));
        }

        let tx_by_addr_cells: Vec<_> = by_addr
            .into_iter()
            .map(|(address, transaction_info_by_addr)| {
                (
                    format!("{}_{}", address, Self::slot_to_tx_by_addr_key(slot)),
                    tx_by_addr::TransactionByAddr {
                        tx_by_addrs: transaction_info_by_addr
                            .into_iter()
                            .map(|by_addr| by_addr.into())
                            .collect(),
                    },
                )
            })
            .collect();

        if !entries.is_empty() {
            let entry_cell = (
                Self::slot_to_entries_key(slot),
                entries::Entries {
                    entries: entries
                        .iter()
                        .map(|entry| EntrySummary {
                            num_hashes: entry.num_hashes,
                            hash: entry.hash,
                            num_transactions: entry.num_transactions,
                            starting_transaction_index: entry.starting_transaction_index,
                        })
                        .enumerate()
                        .map(Into::into)
                        .collect(),
                },
            );
            self.put_protobuf_cells::<entries::Entries>(slot, "entries", &[entry_cell])?;
        }

        if !tx_cells.is_empty() {
            self.put_bincode_cells::<CosTransactionInfo>(slot, "tx", &tx_cells)?;
        }

        if !tx_by_addr_cells.is_empty() {
            self.put_protobuf_cells::<tx_by_addr::TransactionByAddr>(
                slot,
                "tx_by_addr",
                &tx_by_addr_cells,
            )?;
        }

        let blocks_cells = [(
            Self::slot_to_blocks_key(slot),
            confirmed_block.clone().into(),
        )];
        self.put_protobuf_cells::<generated::ConfirmedBlock>(slot, "blocks", &blocks_cells)?;

        if slot % self.slot_range == 0 {
            self.commit(slot - 1)?;
        }
        Ok(())
    }

    pub fn extract_memos(
        account_keys: &AccountKeys,
        instructions: &[CompiledInstruction],
    ) -> Vec<String> {
        let mut account_keys: Vec<KeyType> = account_keys.iter().map(KeyType::Unknown).collect();
        instructions
            .iter()
            .filter_map(|ix| {
                let index = ix.program_id_index as usize;
                let key_type = account_keys.get(index)?;
                let memo_data = match key_type {
                    KeyType::MemoProgram => Some(&ix.data),
                    KeyType::OtherProgram => None,
                    KeyType::Unknown(program_id) => {
                        if **program_id
                            == solana_transaction_status::extract_memos::spl_memo_id_v1()
                            || **program_id
                                == solana_transaction_status::extract_memos::spl_memo_id_v3()
                        {
                            account_keys[index] = KeyType::MemoProgram;
                            Some(&ix.data)
                        } else {
                            account_keys[index] = KeyType::OtherProgram;
                            None
                        }
                    }
                }?;
                Some(Self::extract_and_fmt_memo_data(memo_data))
            })
            .collect()
    }

    pub fn put_bincode_cells<T>(&self, slot: Slot, table: &str, cells: &[(RowKey, T)]) -> Result<()>
    where
        T: serde::ser::Serialize,
    {
        let mut new_row_data = vec![];
        for (row_key, data) in cells {
            let data = compress_best(&bincode::serialize(&data).unwrap())?;
            new_row_data.push((row_key, "bin".to_string(), data));
        }
        self.save_row_data(slot, table, &new_row_data)
    }

    pub fn put_protobuf_cells<T>(
        &self,
        slot: Slot,
        table: &str,
        cells: &[(RowKey, T)],
    ) -> Result<()>
    where
        T: prost::Message,
    {
        let mut new_row_data = vec![];
        for (row_key, data) in cells {
            let mut buf = Vec::with_capacity(data.encoded_len());
            data.encode(&mut buf).unwrap();
            let data = compress_best(&buf)?;
            new_row_data.push((row_key, "proto".to_string(), data));
        }
        self.save_row_data(slot, table, &new_row_data)
    }

    fn save_row_data(
        &self,
        slot: Slot,
        table_name: &str,
        row_data: &[(&RowKey, RowType, RowData)],
    ) -> Result<()> {
        let folder_path = self
            .staging_path
            .join(Self::format_slot_range(slot, self.slot_range))
            .join(Self::format_slot_single(slot))
            .join(table_name);

        // Ensure clean staging directory
        if Path::exists(&folder_path) {
            std::fs::remove_dir_all(&folder_path)?;
        }
        std::fs::create_dir_all(&folder_path)?;

        for (key, data_type, data) in row_data {
            Self::save_row(&folder_path, key, data_type, data)?;
        }
        Ok(())
    }

    /// Copy the interval containing "slot" from staging to ready folder.
    /// A timestamp will be appended to the folder name to ensure uniqueness.
    fn commit(&self, slot: Slot) -> Result<()> {
        let slot_range_str = Self::format_slot_range(slot, self.slot_range);
        let staging_folder_path = self.staging_path.join(slot_range_str.clone());

        if Path::exists(&staging_folder_path) {
            let timestamp = chrono::Utc::now().timestamp_millis();
            let storage_folder_path = self
                .ready_path
                .join(format!("{slot_range_str}_{timestamp:0>15}"));

            // Ensure clean storage directory
            if Path::exists(&storage_folder_path) {
                std::fs::remove_dir_all(&storage_folder_path)?;
            }
            std::fs::create_dir_all(&storage_folder_path)?;

            // Move the staging directory to the storage directory
            std::fs::rename(&staging_folder_path, &storage_folder_path)?;
        }
        Ok(())
    }

    fn save_row(folder_path: &Path, key: &str, data_type: &str, data: &[u8]) -> Result<()> {
        let file_path = folder_path.join(format!("{key}.{data_type}"));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(file_path)?;
        file.write_all(data)?;
        file.sync_all()
    }

    fn format_slot_range(slot: Slot, slot_range: u64) -> String {
        let start_slot = slot - (slot % slot_range);
        let start_slot_str = Self::format_slot(start_slot);
        let end_slot_str = Self::format_slot(start_slot + slot_range);
        format!("slot_range_{start_slot_str}_{end_slot_str}")
    }

    fn format_slot_single(slot: Slot) -> String {
        let slot_str = Self::format_slot(slot);
        format!("slot_{slot_str}")
    }

    fn format_slot(slot: Slot) -> String {
        format!("{slot:0>10}")
    }

    fn slot_to_blocks_key(slot: Slot) -> String {
        Self::slot_to_key(slot)
    }

    fn slot_to_entries_key(slot: Slot) -> String {
        Self::slot_to_key(slot)
    }

    fn slot_to_tx_by_addr_key(slot: Slot) -> String {
        Self::slot_to_key(!slot)
    }

    fn slot_to_key(slot: Slot) -> String {
        format!("{slot:016x}")
    }

    fn extract_and_fmt_memo_data(data: &[u8]) -> String {
        let memo_len = data.len();
        let parsed_memo = solana_transaction_status::parse_instruction::parse_memo_data(data)
            .unwrap_or_else(|_| "(unparseable)".to_string());
        format!("[{memo_len}] {parsed_memo}")
    }
}

impl ExtractMemos for CosVersionedTransactionWithStatusMeta {
    fn extract_memos(&self) -> Vec<String> {
        StorageManager::extract_memos(
            &self.account_keys(),
            self.transaction.message.instructions(),
        )
    }
}
