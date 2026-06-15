//! AccountInfo represents a reference to AccountSharedData in either an AppendVec or the write cache.
//! AccountInfo is not persisted anywhere between program runs.
//! AccountInfo is purely runtime state.
//! Note that AccountInfo is saved to disk buckets during runtime, but disk buckets are recreated at startup.
//!
//! In addition to the storage location, AccountInfo carries enough metadata (lamports, owner,
//! executable, data length) and, for small accounts, the account data itself inline. When the data
//! is inlined, `do_load` can reconstruct the whole account directly from the index entry and skip
//! the storage read entirely. The storage location is retained regardless, so clean/shrink and all
//! other storage accounting continue to operate exactly as before -- the inline copy is purely a
//! read accelerator derived from the append-vec, which remains the source of truth.
use {
    crate::{
        accounts_db::AccountsFileId,
        accounts_file::ALIGN_BOUNDARY_OFFSET,
        accounts_index::{DiskIndexValue, IndexValue, IsCached},
        is_zero_lamport::IsZeroLamport,
    },
    solana_pubkey::Pubkey,
};

/// offset within an append vec to account data
pub type Offset = usize;

/// specify where account data is located
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum StorageLocation {
    AppendVec(AccountsFileId, Offset),
    Cached,
}

impl StorageLocation {
    pub fn is_offset_equal(&self, other: &StorageLocation) -> bool {
        match self {
            StorageLocation::Cached => {
                matches!(other, StorageLocation::Cached) // technically, 2 cached entries match in offset
            }
            StorageLocation::AppendVec(_, offset) => {
                match other {
                    StorageLocation::Cached => {
                        false // 1 cached, 1 not
                    }
                    StorageLocation::AppendVec(_, other_offset) => other_offset == offset,
                }
            }
        }
    }
    pub fn is_store_id_equal(&self, other: &StorageLocation) -> bool {
        match self {
            StorageLocation::Cached => {
                matches!(other, StorageLocation::Cached) // 2 cached entries are same store id
            }
            StorageLocation::AppendVec(store_id, _) => {
                match other {
                    StorageLocation::Cached => {
                        false // 1 cached, 1 not
                    }
                    StorageLocation::AppendVec(other_store_id, _) => other_store_id == store_id,
                }
            }
        }
    }
}

/// how large the offset we store in AccountInfo is
/// Note this is a smaller datatype than 'Offset'
/// AppendVecs store accounts aligned to u64, so offset is always a multiple of 8 (sizeof(u64))
pub type OffsetReduced = u32;

/// Sentinel `offset_reduced` value marking an AccountInfo that refers to the write cache rather
/// than an append vec. A real offset can never be this high: it would point at the last 8 bytes of
/// a maximally sized append vec, leaving room for an 8-byte account, which is impossible (a pubkey
/// alone is 32 bytes).
const CACHED_OFFSET: OffsetReduced = OffsetReduced::MAX;

/// Number of bytes of account data that can be stored inline in an AccountInfo.
///
/// Chosen so that the on-disk `bucket_map` `IndexEntry<AccountInfo>` is exactly 256 bytes
/// (4 cache lines): 32 (pubkey key) + size_of::<AccountInfo>() == 256, i.e. AccountInfo == 224.
/// 170 bytes covers a base SPL token account (165 bytes) with a few bytes to spare.
pub const INLINE_DATA_CAPACITY: usize = 170;

/// `flags` bit: the account has zero lamports.
const FLAG_ZERO_LAMPORT: u8 = 1 << 0;
/// `flags` bit: the account lives in the write cache (storage location is meaningless).
const FLAG_CACHED: u8 = 1 << 1;
/// `flags` bit: the first `data_len` bytes of `inline_data` hold the account's data, so the
/// account can be reconstructed without reading storage.
const FLAG_INLINE: u8 = 1 << 2;
/// `flags` bit (inline only): the account's rent_epoch is 0; otherwise it is u64::MAX. We only
/// inline accounts whose rent_epoch is one of these two values (see `new_inline`), so a single bit
/// reconstructs it exactly.
const FLAG_RENT_EPOCH_ZERO: u8 = 1 << 3;

#[repr(C)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct AccountInfo {
    /// the account's lamports (valid only when `FLAG_INLINE` is set)
    lamports: u64,
    /// the account's owner (valid only when `FLAG_INLINE` is set)
    owner: Pubkey,
    /// index identifying the append storage; meaningless when cached
    store_id: AccountsFileId,
    /// offset = `offset_reduced` * ALIGN_BOUNDARY_OFFSET into the storage. Note this is a smaller
    /// type than 'Offset'. Meaningless when cached.
    offset_reduced: OffsetReduced,
    /// full length of the account's data. The data is held inline iff `FLAG_INLINE` is set and
    /// `data_len <= INLINE_DATA_CAPACITY`.
    data_len: u32,
    /// the account's executable flag (valid only when `FLAG_INLINE` is set)
    executable: u8,
    /// FLAG_ZERO_LAMPORT | FLAG_CACHED | FLAG_INLINE
    flags: u8,
    /// the first `data_len` bytes are the account data when `FLAG_INLINE` is set; otherwise unused
    inline_data: [u8; INLINE_DATA_CAPACITY],
}

// Ensure the size of AccountInfo never changes unexpectedly. 224 + the 32-byte pubkey key makes the
// on-disk bucket_map IndexEntry exactly 256 bytes.
const _: () = assert!(size_of::<AccountInfo>() == 224);

impl Default for AccountInfo {
    fn default() -> Self {
        // `[u8; N]` has no `Default` impl for N > 32, so AccountInfo cannot derive Default.
        Self {
            lamports: 0,
            owner: Pubkey::default(),
            store_id: 0,
            offset_reduced: 0,
            data_len: 0,
            executable: 0,
            flags: 0,
            inline_data: [0u8; INLINE_DATA_CAPACITY],
        }
    }
}

impl IsZeroLamport for AccountInfo {
    fn is_zero_lamport(&self) -> bool {
        self.flags & FLAG_ZERO_LAMPORT != 0
    }
}

impl IsCached for AccountInfo {
    fn is_cached(&self) -> bool {
        self.flags & FLAG_CACHED != 0
    }
}

impl IndexValue for AccountInfo {}

impl DiskIndexValue for AccountInfo {}

impl IsCached for StorageLocation {
    fn is_cached(&self) -> bool {
        matches!(self, StorageLocation::Cached)
    }
}

/// We have to have SOME value for store_id when we are cached
const CACHE_VIRTUAL_STORAGE_ID: AccountsFileId = AccountsFileId::MAX;

impl AccountInfo {
    /// Construct an AccountInfo carrying only a storage location (no inline metadata/data). The
    /// account is read from storage on load. This is the legacy shape used wherever inline data is
    /// not available or not wanted.
    pub fn new(storage_location: StorageLocation, is_zero_lamport: bool) -> Self {
        let mut info = Self::default();
        match storage_location {
            StorageLocation::AppendVec(store_id, offset) => {
                let reduced_offset = Self::get_reduced_offset(offset);
                assert_ne!(
                    CACHED_OFFSET, reduced_offset,
                    "illegal offset for non-cached item"
                );
                assert_eq!(
                    Self::reduced_offset_to_offset(reduced_offset),
                    offset,
                    "illegal offset"
                );
                info.store_id = store_id;
                info.offset_reduced = reduced_offset;
            }
            StorageLocation::Cached => {
                info.store_id = CACHE_VIRTUAL_STORAGE_ID;
                info.flags |= FLAG_CACHED;
            }
        }
        if is_zero_lamport {
            info.flags |= FLAG_ZERO_LAMPORT;
        }
        info
    }

    /// Construct an AccountInfo that inlines the account so it can be reconstructed without reading
    /// storage. The storage location is retained so clean/shrink continue to operate. `data` must
    /// fit within `INLINE_DATA_CAPACITY`, and `rent_epoch` must be either 0 or u64::MAX (the only
    /// values we can reconstruct exactly from a single flag bit -- callers gate on this).
    pub fn new_inline(
        storage_location: StorageLocation,
        is_zero_lamport: bool,
        lamports: u64,
        owner: &Pubkey,
        executable: bool,
        rent_epoch: u64,
        data: &[u8],
    ) -> Self {
        assert!(
            data.len() <= INLINE_DATA_CAPACITY,
            "inline data too large: {} > {INLINE_DATA_CAPACITY}",
            data.len()
        );
        assert!(
            rent_epoch == 0 || rent_epoch == u64::MAX,
            "inline rent_epoch must be 0 or u64::MAX, got {rent_epoch}"
        );
        let mut info = Self::new(storage_location, is_zero_lamport);
        info.lamports = lamports;
        info.owner = *owner;
        info.executable = u8::from(executable);
        info.data_len = data.len() as u32;
        info.inline_data[..data.len()].copy_from_slice(data);
        info.flags |= FLAG_INLINE;
        if rent_epoch == 0 {
            info.flags |= FLAG_RENT_EPOCH_ZERO;
        }
        info
    }

    pub fn get_reduced_offset(offset: usize) -> OffsetReduced {
        (offset / ALIGN_BOUNDARY_OFFSET) as OffsetReduced
    }

    pub fn store_id(&self) -> AccountsFileId {
        // if the account is in a cached store, the store_id is meaningless
        assert!(!self.is_cached());
        self.store_id
    }

    pub fn offset(&self) -> Offset {
        Self::reduced_offset_to_offset(self.offset_reduced)
    }

    pub fn reduced_offset_to_offset(reduced_offset: OffsetReduced) -> Offset {
        (reduced_offset as Offset) * ALIGN_BOUNDARY_OFFSET
    }

    pub fn storage_location(&self) -> StorageLocation {
        if self.is_cached() {
            StorageLocation::Cached
        } else {
            StorageLocation::AppendVec(self.store_id, self.offset())
        }
    }

    /// True if the account data (and metadata) is stored inline and the account can be
    /// reconstructed without reading storage.
    pub fn is_inline(&self) -> bool {
        self.flags & FLAG_INLINE != 0
    }

    /// The account's data when stored inline; `None` otherwise.
    pub fn inline_data(&self) -> Option<&[u8]> {
        self.is_inline()
            .then(|| &self.inline_data[..self.data_len as usize])
    }

    /// The inlined lamports. Only meaningful when `is_inline()`.
    pub fn inline_lamports(&self) -> u64 {
        self.lamports
    }

    /// The inlined owner. Only meaningful when `is_inline()`.
    pub fn inline_owner(&self) -> &Pubkey {
        &self.owner
    }

    /// The inlined executable flag. Only meaningful when `is_inline()`.
    pub fn inline_executable(&self) -> bool {
        self.executable != 0
    }

    /// The inlined rent_epoch (0 or u64::MAX). Only meaningful when `is_inline()`.
    pub fn inline_rent_epoch(&self) -> u64 {
        if self.flags & FLAG_RENT_EPOCH_ZERO != 0 {
            0
        } else {
            u64::MAX
        }
    }
}

#[cfg(test)]
mod test {
    use {super::*, crate::append_vec::MAXIMUM_APPEND_VEC_FILE_SIZE};

    #[test]
    fn test_limits() {
        for offset in [
            // MAXIMUM_APPEND_VEC_FILE_SIZE is too big. That would be an offset at the first invalid byte in the max file size.
            // MAXIMUM_APPEND_VEC_FILE_SIZE - 8 bytes would reference the very last 8 bytes in the file size. It makes no sense to reference that since element sizes are always more than 8.
            // MAXIMUM_APPEND_VEC_FILE_SIZE - 16 bytes would reference the second to last 8 bytes in the max file size. This is still likely meaningless, but it is 'valid' as far as the index
            // is concerned.
            (MAXIMUM_APPEND_VEC_FILE_SIZE - 2 * (ALIGN_BOUNDARY_OFFSET as u64)) as Offset,
            0,
            ALIGN_BOUNDARY_OFFSET,
            4 * ALIGN_BOUNDARY_OFFSET,
        ] {
            let info = AccountInfo::new(StorageLocation::AppendVec(0, offset), true);
            assert!(info.offset() == offset);
        }
    }

    #[test]
    #[should_panic(expected = "illegal offset")]
    fn test_illegal_offset() {
        // An offset whose reduced value collides with the cached sentinel (u32::MAX) is illegal for
        // a non-cached entry. Such an offset (~34 GiB) is far larger than any real append vec, so a
        // valid offset can never hit it.
        let offset = CACHED_OFFSET as Offset * ALIGN_BOUNDARY_OFFSET;
        AccountInfo::new(StorageLocation::AppendVec(0, offset), true);
    }

    #[test]
    #[should_panic(expected = "illegal offset")]
    fn test_alignment() {
        let offset = 1; // not aligned
        AccountInfo::new(StorageLocation::AppendVec(0, offset), true);
    }
}
