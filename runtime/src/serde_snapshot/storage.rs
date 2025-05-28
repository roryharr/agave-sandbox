use {
    serde::{Deserialize, Serialize},
    solana_accounts_db::accounts_db::AccountStorageEntry,
    solana_clock::Slot,
};

/// The serialized AccountsFileId type is fixed as usize
pub(crate) type SerializedAccountsFileId = usize;

// Serializable version of AccountStorageEntry for snapshot format
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SerializableAccountStorageEntry {
    id: SerializedAccountsFileId,
    accounts_current_len: usize,
}

pub(super) trait SerializableStorage {
    fn id(&self) -> SerializedAccountsFileId;
    fn current_len(&self) -> usize;
}

impl SerializableStorage for SerializableAccountStorageEntry {
    fn id(&self) -> SerializedAccountsFileId {
        self.id
    }
    fn current_len(&self) -> usize {
        self.accounts_current_len
    }
}

impl From<&AccountStorageEntry> for SerializableAccountStorageEntry {
    fn from(rhs: &AccountStorageEntry) -> Self {
        Self {
            id: rhs.id() as SerializedAccountsFileId,
            accounts_current_len: rhs.accounts.len(),
        }
    }
}

/// When obsolete accounts are enabled, the saved size is decreased by the
/// amount of obsolete bytes in the storage. The number of obsolete bytes is
/// determined by the snapshot slot, as an entry's obsolescence is dependant
/// on the slot that marked it as such.
impl From<(&AccountStorageEntry, Slot)> for SerializableAccountStorageEntry {
    fn from(value: (&AccountStorageEntry, Slot)) -> Self {
        Self {
            id: value.0.id() as SerializedAccountsFileId,
            accounts_current_len: value.0.accounts.len()
                - value.0.get_obsolete_bytes(Some(value.1)),
        }
    }
}

#[cfg(feature = "frozen-abi")]
impl solana_frozen_abi::abi_example::TransparentAsHelper for SerializableAccountStorageEntry {}
