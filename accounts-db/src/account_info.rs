//! AccountInfo represents a reference to AccountSharedData in an AccountsFile
//! AccountInfo is not persisted anywhere between program runs.
//! AccountInfo is purely runtime state.
//! Note that AccountInfo is saved to disk buckets during runtime, but disk buckets are recreated at startup.
use {
    crate::{
        accounts_file::ALIGN_BOUNDARY_OFFSET, accounts_index::IndexValue,
        is_zero_lamport::IsZeroLamport,
    },
    modular_bitfield::prelude::*,
};

/// offset within an accounts file to account data
pub type Offset = usize;

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

/// how large the offset we store in AccountInfo is
/// Note this is a smaller datatype than 'Offset'
/// AppendVecs store accounts aligned to u64, so offset is always a multiple of 8 (sizeof(u64))
pub type OffsetReduced = u32;

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
                packed.set_offset_reduced(Self::get_reduced_offset(offset));
                assert_eq!(
                    Self::reduced_offset_to_offset(packed.offset_reduced()),
                    offset,
                    "illegal offset"
                );
                packed.set_generation(generation);
            }
        }
        packed.set_zero_lamport(is_zero_lamport);
        Self { packed }
    }

    pub fn get_reduced_offset(offset: usize) -> OffsetReduced {
        (offset / ALIGN_BOUNDARY_OFFSET) as OffsetReduced
    }

    pub fn generation(&self) -> StorageGeneration {
        self.packed.generation()
    }

    pub fn offset(&self) -> Offset {
        Self::reduced_offset_to_offset(self.packed.offset_reduced())
    }

    pub fn reduced_offset_to_offset(reduced_offset: OffsetReduced) -> Offset {
        (reduced_offset as Offset) * ALIGN_BOUNDARY_OFFSET
    }

    pub fn storage_location(&self) -> StorageLocation {
        StorageLocation::AccountsFile(self.generation(), self.offset())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    /// the largest storage the packed offset must address
    const ANCIENT_STORAGE_SIZE: u64 = 128 * 1024 * 1024;

    #[test]
    fn test_limits() {
        for offset in [
            // MAXIMUM_APPEND_VEC_FILE_SIZE is too big. That would be an offset at the first invalid byte in the max file size.
            // MAXIMUM_APPEND_VEC_FILE_SIZE - 8 bytes would reference the very last 8 bytes in the file size. It makes no sense to reference that since element sizes are always more than 8.
            // MAXIMUM_APPEND_VEC_FILE_SIZE - 16 bytes would reference the second to last 8 bytes in the max file size. This is still likely meaningless, but it is 'valid' as far as the index
            // is concerned.
            (ANCIENT_STORAGE_SIZE - 2 * (ALIGN_BOUNDARY_OFFSET as u64)) as Offset,
            0,
            ALIGN_BOUNDARY_OFFSET,
            4 * ALIGN_BOUNDARY_OFFSET,
        ] {
            let info = AccountInfo::new(StorageLocation::AccountsFile(false, offset), true);
            assert!(info.offset() == offset);
        }
    }

    #[test]
    #[should_panic(expected = "illegal offset")]
    fn test_alignment() {
        let offset = 1; // not aligned
        AccountInfo::new(StorageLocation::AccountsFile(false, offset), true);
    }
}
