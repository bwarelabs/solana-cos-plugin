use crate::compression::compress_best;
use crate::cos_types::{
    CosTransactionInfo, CosVersionedConfirmedBlockWithEntries,
    CosVersionedTransactionWithStatusMeta, RowData, RowKey, RowType,
};
use crate::geyser_plugin_cos_config::GeyserPluginCosConfig;
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
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

enum KeyType<'a> {
    MemoProgram,
    OtherProgram,
    Unknown(&'a Pubkey),
}

pub trait Storage {
    fn save(
        &self,
        slot: Slot,
        confirmed_block: &CosVersionedConfirmedBlockWithEntries,
    ) -> std::io::Result<()>;
}

/// Manages storage of confirmed blocks and transactions
#[derive(Default)]
pub struct StorageManager {
    /// The number of slots in each range.
    slot_range: u64,
    /// Commit slot delay in number of slots.
    commit_slot_delay: u64,
    /// RWLock to ensure only one thread is writing to "storage" at a time.
    /// Multiple threads can write to "staging" concurrently.
    rw_lock: RwLock<(PathBuf, PathBuf)>,
}

impl Storage for StorageManager {
    /// Save a confirmed block and its transactions to storage
    fn save(
        &self,
        slot: Slot,
        confirmed_block: &CosVersionedConfirmedBlockWithEntries,
    ) -> std::io::Result<()> {
        self.save_to_staging(slot, confirmed_block)?;
        self.commit_to_storage(slot)
    }
}

impl StorageManager {
    pub fn new(config: &GeyserPluginCosConfig) -> std::io::Result<Self> {
        let slot_range = config.slot_range;

        // Ensure the storage directory exists
        let ready_path = PathBuf::from(config.workspace.to_string()).join("storage");
        let staging_path = PathBuf::from(config.workspace.to_string()).join("staging");
        let commit_slot_delay = config.commit_slot_delay;

        std::fs::create_dir_all(&ready_path)?;
        // Ensure clean staging directory
        if Path::exists(&staging_path) {
            std::fs::remove_dir_all(&staging_path)?;
        }
        std::fs::create_dir_all(&staging_path)?;

        let rw_lock = RwLock::new((ready_path, staging_path));

        Ok(StorageManager {
            slot_range,
            commit_slot_delay,
            rw_lock,
        })
    }

    /// Save a confirmed block and its transactions to staging in COS ready format.
    ///
    /// Note that this code is copied from solana and should be kept in sync with the original.
    fn save_to_staging(
        &self,
        slot: Slot,
        confirmed_block: &CosVersionedConfirmedBlockWithEntries,
    ) -> std::io::Result<()> {
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

        let entry_cells = [(
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
        )];

        let blocks_cells = [(
            Self::slot_to_blocks_key(slot),
            confirmed_block.clone().into(),
        )];

        let _r_lock = self.rw_lock.read().unwrap();
        let (_, staging_path) = &*_r_lock;

        if !tx_cells.is_empty() {
            self.put_bincode_cells::<CosTransactionInfo>(staging_path, slot, "tx", &tx_cells)?;
        }

        if !tx_by_addr_cells.is_empty() {
            self.put_protobuf_cells::<tx_by_addr::TransactionByAddr>(
                staging_path,
                slot,
                "tx_by_addr",
                &tx_by_addr_cells,
            )?;
        }

        if !entries.is_empty() {
            self.put_protobuf_cells::<entries::Entries>(
                staging_path,
                slot,
                "entries",
                &entry_cells,
            )?;
        }

        self.put_protobuf_cells::<generated::ConfirmedBlock>(
            staging_path,
            slot,
            "blocks",
            &blocks_cells,
        )
    }

    /// Copy the interval containing "slot" from staging to ready folder.
    /// A timestamp will be appended to the folder name to ensure uniqueness.
    fn commit_to_storage(&self, current_slot: Slot) -> std::io::Result<()> {
        // The trigger to copy the staging directory to the storage directory is to receive
        // a slot that is a multiple of the slot range plus a constant factor.
        // When this slot is received, we copy all previous slot ranges from staging to storage.
        if current_slot % self.slot_range != self.commit_slot_delay {
            return Ok(());
        }

        let _w_lock = self.rw_lock.write().unwrap();
        let (ready_path, staging_path) = &*_w_lock;

        // Read all slot ranges from staging and move them to storage,
        // all except the current one.
        let current_slot_range_str = Self::format_slot_range(current_slot, self.slot_range);
        for entry in std::fs::read_dir(staging_path)? {
            let slot_range_path = entry?.path();

            if let Some(folder_name) = slot_range_path.file_name() {
                if let Some(folder_name) = folder_name.to_str() {
                    if folder_name != current_slot_range_str {
                        let storage_folder_path = ready_path.join(folder_name);
                        // Move the staging directory to the storage directory
                        std::fs::rename(&slot_range_path, &storage_folder_path)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn extract_memos(
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

    fn put_bincode_cells<T>(
        &self,
        staging_path: &Path,
        slot: Slot,
        table_name: &str,
        cells: &[(RowKey, T)],
    ) -> std::io::Result<()>
    where
        T: serde::ser::Serialize,
    {
        let mut new_row_data = vec![];
        for (row_key, data) in cells {
            let data = compress_best(&bincode::serialize(&data).unwrap())?;
            new_row_data.push((row_key, "bin".to_string(), data));
        }
        self.save_row_data(staging_path, slot, table_name, &new_row_data)
    }

    fn put_protobuf_cells<T>(
        &self,
        staging_path: &Path,
        slot: Slot,
        table_name: &str,
        cells: &[(RowKey, T)],
    ) -> std::io::Result<()>
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
        self.save_row_data(staging_path, slot, table_name, &new_row_data)
    }

    fn save_row_data(
        &self,
        staging_path: &Path,
        slot: Slot,
        table_name: &str,
        row_data: &[(&RowKey, RowType, RowData)],
    ) -> std::io::Result<()> {
        let folder_path = staging_path
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

    fn save_row(
        folder_path: &Path,
        key: &str,
        data_type: &str,
        data: &[u8],
    ) -> std::io::Result<()> {
        let file_path = folder_path.join(format!("{key}.{data_type}"));
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(file_path)?;
        file.write_all(data)
    }

    fn format_slot_range(slot: Slot, slot_range: u64) -> String {
        let start_slot = slot - (slot % slot_range);
        let start_slot_str = Self::format_slot(start_slot);
        let end_slot_str = Self::format_slot(start_slot + slot_range);
        format!("range_{start_slot_str}_{end_slot_str}")
    }

    fn format_slot_single(slot: Slot) -> String {
        let slot_str = Self::format_slot(slot);
        format!("slot_{slot_str}")
    }

    fn format_slot(slot: Slot) -> String {
        format!("{slot:016x}")
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
