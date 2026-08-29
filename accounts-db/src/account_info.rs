//! AccountInfo represents a reference to AccountSharedData in an AccountsFile
//! AccountInfo is not persisted anywhere between program runs.
//! AccountInfo is purely runtime state.
//! Note that AccountInfo is saved to disk buckets during runtime, but disk buckets are recreated at startup.
use {
    crate::{
        accounts_index::IndexValue,
        is_zero_lamport::IsZeroLamport,
    },
    modular_bitfield::prelude::*,
};

/// offset within an accounts file to account data
pub type Offset = u32;
pub const MAX_OFFSET: Offset = (1 << 24) - 1;

/// distinguishes the two storages a slot has while a shrink is in progress
pub type StorageGeneration = bool;

/// specify where account data is located
#[derive(Debug, PartialEq, Eq)]
pub enum StorageLocation {
    AccountsFile(StorageGeneration, Offset),
}

impl StorageLocation {
    pub fn is_offset_equal(&self, other: &StorageLocation) -> bool {
        match self {
            StorageLocation::AccountsFile(_, offset) => match other {
                StorageLocation::AccountsFile(_, other_offset) => other_offset == offset,
            },
        }
    }
    /// within a slot, the generation identifies which storage this refers to
    pub fn is_generation_equal(&self, other: &StorageLocation) -> bool {
        match self {
            StorageLocation::AccountsFile(generation, _) => match other {
                StorageLocation::AccountsFile(other_generation, _) => {
                    other_generation == generation
                }
            },
        }
    }
}

/// The account's location within its slot's storage, packed into 26 bits so that the index
/// entry can carry it alongside the slot and its `dirty` and `age` metadata in one 64 bit cell.
/// The slot itself lives in the index entry, which is where every caller already reads it from.
#[bitfield(bits = 26)]
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub struct PackedAccountInfo {
    /// offset = 'offset_reduced' * ALIGN_BOUNDARY_OFFSET into the storage.
    /// 2^24 * 8 = 128MiB, the size of an ancient storage
    offset_reduced: B24,
    /// use 1 bit to specify that the entry is zero lamport
    zero_lamport: bool,
    /// which of the slot's storages this refers to while a shrink is in progress
    generation: bool,
}

#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub struct AccountInfo {
    packed: PackedAccountInfo,
}

// Ensure the size of AccountInfo never changes unexpectedly
const _: () = assert!(size_of::<AccountInfo>() == 4);

impl IsZeroLamport for AccountInfo {
    fn is_zero_lamport(&self) -> bool {
        self.packed.zero_lamport()
    }
}

impl IndexValue for AccountInfo {
    fn to_bits(self) -> u64 {
        let bytes = self.packed.into_bytes();
        u64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], 0, 0, 0, 0])
    }
    fn from_bits(bits: u64) -> Self {
        let bytes = bits.to_le_bytes();
        Self {
            packed: PackedAccountInfo::from_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        }
    }
}

impl AccountInfo {
    pub fn new(storage_location: StorageLocation, is_zero_lamport: bool) -> Self {
        let mut packed = PackedAccountInfo::default();
        match storage_location {
            StorageLocation::AccountsFile(generation, offset) => {
                assert!(offset <= MAX_OFFSET, "illegal offset");
                packed.set_offset_reduced(offset);
                packed.set_generation(generation);
            }
        }
        packed.set_zero_lamport(is_zero_lamport);
        Self { packed }
    }

    pub fn generation(&self) -> StorageGeneration {
        self.packed.generation()
    }

    pub fn offset(&self) -> Offset {
        self.packed.offset_reduced()
    }

    pub fn storage_location(&self) -> StorageLocation {
        StorageLocation::AccountsFile(self.generation(), self.offset())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_limits() {
        for offset in [0, 1, MAX_OFFSET - 1, MAX_OFFSET] {
            let info = AccountInfo::new(StorageLocation::AccountsFile(false, offset), true);
            assert_eq!(info.offset(), offset);
        }
    }

    #[test]
    #[should_panic(expected = "illegal offset")]
    fn test_offset_too_large() {
        let offset = MAX_OFFSET + 1;
        AccountInfo::new(StorageLocation::AccountsFile(false, offset), true);
    }
}
