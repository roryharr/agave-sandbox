use {
    crate::{
        ObsoleteAccountItem, ObsoleteAccounts,
        account_info::Offset,
        account_storage_entry::AccountStorageEntry,
        accounts_db::AccountsFileId,
        accounts_file::{AccountsFile, AccountsFileError, StorageAccess},
    },
    agave_fs::FileInfo,
    rayon::iter::{IntoParallelRefIterator, ParallelIterator},
    serde::{Deserialize, Serialize},
    solana_clock::Slot,
    std::{io, str::FromStr, sync::Arc},
    thiserror::Error,
    wincode::{SchemaRead, SchemaWrite},
};

/// Directory name under the bank snapshot dir that holds hard-linked storage files.
pub const SNAPSHOT_ACCOUNTS_HARDLINKS: &str = "accounts_hardlinks";

/// Filename under the bank snapshot dir that holds the AppendVec obsolete-accounts sidecar.
pub const SNAPSHOT_OBSOLETE_ACCOUNTS_FILENAME: &str = "obsolete_accounts";

/// The serialized AccountsFileId type is fixed as usize for snapshot format stability.
pub type SerializedAccountsFileId = usize;

/// Error type for restoring a single account storage entry from disk.
#[derive(Error, Debug)]
pub enum StorageRestoreError {
    #[error("accounts file id mismatch: expected {expected}, found {found}")]
    MismatchedId {
        expected: AccountsFileId,
        found: AccountsFileId,
    },
    #[error("duplicate storage entry for slot {slot}")]
    DuplicateSlot { slot: Slot },
    #[error(transparent)]
    AccountsFile(#[from] AccountsFileError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Parse `"<slot>.<id>"` from an AppendVec filename. Returns `None` for non-AppendVec files.
pub fn parse_append_vec_filename(filename: &str) -> Option<(Slot, AccountsFileId)> {
    let mut parts = filename.splitn(2, '.');
    let slot = parts.next().and_then(|s| Slot::from_str(s).ok())?;
    let id = parts
        .next()
        .and_then(|s| AccountsFileId::from_str(s).ok())?;
    Some((slot, id))
}

/// Reconstruct a single [`AccountStorageEntry`] from a file on disk.
///
/// Storage files are always truncated to their written length at snapshot time,
/// so `file_info.size` is the authoritative length — no metadata needed.
pub fn reconstruct_single_storage(
    slot: Slot,
    file_info: FileInfo,
    id: AccountsFileId,
    storage_access: StorageAccess,
    obsolete_accounts: Option<(ObsoleteAccounts, AccountsFileId)>,
) -> Result<Arc<AccountStorageEntry>, StorageRestoreError> {
    let current_len = file_info.size as usize;

    let obsolete_accounts = if let Some((obs, obs_id)) = obsolete_accounts {
        if obs_id != id {
            return Err(StorageRestoreError::MismatchedId {
                expected: id,
                found: obs_id,
            });
        }
        obs
    } else {
        ObsoleteAccounts::default()
    };

    let accounts_file = AccountsFile::new_for_startup(file_info, current_len, storage_access)?;
    Ok(Arc::new(AccountStorageEntry::new_existing(
        slot,
        id,
        accounts_file,
        obsolete_accounts,
    )))
}

/// Serializable form of an [`AccountStorageEntry`] for inclusion in a snapshot.
///
/// Only the `id` field is meaningful; `accounts_current_len` is kept solely for snapshot
/// format stability. Storage length is now derived from file size at restore time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SerializableAccountStorageEntry {
    id: SerializedAccountsFileId,
    /// Retained for snapshot format compatibility only; not used during restore.
    accounts_current_len: usize,
}

impl SerializableAccountStorageEntry {
    pub fn new(
        accounts: &AccountStorageEntry,
        snapshot_slot: Slot,
    ) -> SerializableAccountStorageEntry {
        SerializableAccountStorageEntry {
            id: accounts.id() as SerializedAccountsFileId,
            accounts_current_len: accounts.accounts.len()
                - accounts.get_obsolete_bytes(Some(snapshot_slot)),
        }
    }
}

#[cfg(feature = "frozen-abi")]
impl solana_frozen_abi::abi_example::TransparentAsHelper for SerializableAccountStorageEntry {}

/// Serialized form of the obsolete accounts for a single storage entry.
#[derive(Debug, Default, Serialize, SchemaRead, SchemaWrite)]
pub struct SerdeObsoleteAccounts {
    /// The ID of the associated account file, for verification on restore.
    pub id: SerializedAccountsFileId,
    /// Total obsolete bytes (accounts removed during archive but present on disk).
    pub bytes: u64,
    /// (offset, data_len, obsolete_at_slot) for each obsolete account.
    pub accounts: Vec<(Offset, usize, Slot)>,
}

impl SerdeObsoleteAccounts {
    pub fn new_from_storage_entry(storage: &AccountStorageEntry, snapshot_slot: Slot) -> Self {
        let accounts = storage
            .obsolete_accounts_for_snapshots(snapshot_slot)
            .accounts
            .into_iter()
            .map(|item| (item.offset, item.data_len, item.slot))
            .collect();
        SerdeObsoleteAccounts {
            id: storage.id() as SerializedAccountsFileId,
            bytes: storage.get_obsolete_bytes(Some(snapshot_slot)) as u64,
            accounts,
        }
    }

    pub fn into_id_and_accounts(self) -> (ObsoleteAccounts, AccountsFileId) {
        let accounts = self
            .accounts
            .into_iter()
            .map(|(offset, data_len, slot)| ObsoleteAccountItem {
                offset,
                data_len,
                slot,
            })
            .collect();
        (ObsoleteAccounts { accounts }, self.id as AccountsFileId)
    }
}

/// Serialized obsolete-accounts map across all storages in a snapshot.
#[derive(Serialize, Debug, SchemaRead, SchemaWrite)]
pub struct SerdeObsoleteAccountsMap {
    pub map: Vec<(Slot, SerdeObsoleteAccounts)>,
}

impl SerdeObsoleteAccountsMap {
    pub fn new_from_storages(
        snapshot_storages: &[Arc<AccountStorageEntry>],
        snapshot_slot: Slot,
    ) -> Self {
        let map = snapshot_storages
            .par_iter()
            .map(|storage| {
                (
                    storage.slot(),
                    SerdeObsoleteAccounts::new_from_storage_entry(storage, snapshot_slot),
                )
            })
            .collect();
        SerdeObsoleteAccountsMap { map }
    }
}
