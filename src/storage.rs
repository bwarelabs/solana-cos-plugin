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

pub struct StorageManager {
    workspace: PathBuf,
    slot_range: u64,
}

impl StorageManager {
    pub fn new(config: &GeyserPluginCosConfig) -> Result<Self> {
        let slot_range = config.slot_range;

        // Ensure the storage directory exists
        let workspace = PathBuf::from(config.workspace.to_string()).join("storage");
        std::fs::create_dir_all(&workspace)?;

        Ok(StorageManager {
            workspace,
            slot_range,
        })
    }

    pub fn save(
        &self,
        slot: Slot,
        confirmed_block: &CosVersionedConfirmedBlockWithEntries,
    ) -> Result<()> {
        log::trace!("Save request received: {:?}", slot);

        let mut by_addr: HashMap<&Pubkey, Vec<TransactionByAddrInfo>> = HashMap::new();
        let CosVersionedConfirmedBlockWithEntries {
            block: confirmed_block,
            entries,
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

        Ok(())
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
            let data = bincode::serialize(&data).unwrap();
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
            new_row_data.push((row_key, "proto".to_string(), buf));
        }
        self.save_row_data(slot, table, &new_row_data)
    }

    fn save_row_data(
        &self,
        slot: Slot,
        table_name: &str,
        row_data: &[(&RowKey, RowType, RowData)],
    ) -> Result<()> {
        let start_slot = slot - (slot % self.slot_range);
        let end_slot = start_slot + self.slot_range;
        let folder_path = self
            .workspace
            .join(format!("slot_range_{start_slot:0>15}_{end_slot:0>15}"))
            .join(format!("slot_{slot:0>15}"))
            .join(table_name);

        // Ensure the storage directory exists
        std::fs::create_dir_all(&folder_path)?;

        for (key, data_type, data) in row_data {
            Self::save_row(&folder_path, key, data_type, data)?;
        }
        Ok(())
    }

    fn save_row(folder_path: &Path, key: &str, data_type: &str, data: &[u8]) -> Result<()> {
        let file_path = folder_path.join(format!("{key}.{data_type}"));
        log::info!("Saving to {:?}", file_path);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(file_path)?;
        file.write_all(data)?;
        file.sync_all()
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
