//! Persistent accounts are stored at this path location:
//!  `<path>/<pid>/data/`
//!
//! The persistent store would allow for this mode of operation:
//!  - Concurrent single thread append with many concurrent readers.
//!
//! The underlying memory is memory mapped to a file. The accounts would be
//! stored across multiple files and the mappings of file and offset of a
//! particular account would be stored in a shared index. This will allow for
//! concurrent commits without blocking reads, which will sequentially write
//! to memory, ssd or disk, and should be as fast as the hardware allow for.
//! The only required in memory data structure with a write lock is the index,
//! which should be fast to update.
//!
//! [`AppendVec`]'s only store accounts for single slots.

pub(crate) mod ancient_append_vecs;
pub(crate) mod clean;
pub(crate) mod shrink;
pub(crate) mod storable_accounts_by_slot;
pub(crate) mod tests;

#[cfg(test)]
use super::ACCOUNTS_DB_CONFIG_FOR_TESTING;
#[cfg(feature = "frozen-abi")]
use super::AccountsBackend;
#[cfg(test)]
use crate::account_storage::stored_account_info::StoredAccountInfoWithoutData;
#[cfg(test)]
pub(crate) use clean::CleaningInfo;
#[cfg(any(test, feature = "dev-context-only-utils"))]
use qualifier_attr::qualifiers;
#[cfg(test)]
pub(crate) use shrink::AliveAccounts;
#[cfg(test)]
pub use shrink::GetUniqueAccountsResult;
use {
    super::{AccountsDb, AccountsDbConfig, LoadHint, PopulateReadCache},
    crate::{
        account_info::{AccountInfo, Offset, StorageLocation},
        account_storage::{
            AccountStorage, AccountStorageMap, AccountStoragesOrderer, ShrinkInProgress,
            stored_account_info::StoredAccountInfo,
        },
        account_storage_entry::AccountStorageEntry,
        account_storage_reader::AccountStorageReader,
        accounts_cache::CachedAccount,
        accounts_db::{
            backend::storable_accounts_by_slot::StorableAccountsBySlot,
            stats::{
                AppendVecStats, CleanAccountsStats, FlushStats, PurgeStats, ShrinkAncientStats,
                ShrinkStats, ShrinkStatsSub, StoreAccountsFrozenStats, StoreAccountsTiming,
            },
        },
        accounts_file::{AccountsFileProvider, StorageAccess},
        accounts_hash::{AccountLtHash, AccountsLtHash, ZERO_LAMPORT_ACCOUNT_LT_HASH},
        accounts_index::{
            AccountsIndex, IsCached, ReclaimsSlotList, RefCount, ScanFilter, SlotList,
            UpsertReclaim, in_mem_accounts_index::InsertIfNewerResult,
        },
        active_stats::ActiveStatItem,
        ancestors::Ancestors,
        append_vec::{self, AppendVec, STORE_META_OVERHEAD},
        contains::Contains,
        is_zero_lamport::IsZeroLamport,
        read_only_accounts_cache::ReadOnlyAccountsCache,
        snapshot_storage::{
            SNAPSHOT_ACCOUNTS_HARDLINKS, SNAPSHOT_OBSOLETE_ACCOUNTS_FILENAME,
            SerdeObsoleteAccountsMap,
        },
        storable_accounts::StorableAccounts,
        storage_roots::StorageRoots,
        u64_align,
        utils::{self, ACCOUNTS_SNAPSHOT_DIR, create_account_shared_data},
    },
    agave_fs::buffered_reader::RequiredLenBufFileRead,
    dashmap::{DashMap, DashSet},
    log::*,
    rand::{Rng, rng},
    rayon::{ThreadPool, prelude::*},
    seqlock::SeqLock,
    smallvec::SmallVec,
    solana_account::{Account, AccountSharedData, ReadableAccount},
    solana_clock::{Epoch, Slot},
    solana_epoch_schedule::EpochSchedule,
    solana_lattice_hash::lt_hash::LtHash,
    solana_measure::{measure::Measure, measure_us},
    solana_nohash_hasher::{BuildNoHashHasher, IntMap, IntSet},
    solana_pubkey::Pubkey,
    solana_rayon_threadlimit::get_thread_count,
    std::{
        borrow::Cow,
        boxed::Box,
        collections::{HashMap, HashSet, VecDeque},
        io, mem,
        num::Saturating,
        ops::RangeBounds,
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex, RwLock,
            atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
    symlink,
    tempfile::TempDir,
    wincode::{config::Configuration, io::std_write::WriteAdapter},
};

pub(crate) const DEFAULT_NUM_DIRS: u32 = 4;

// This value reflects recommended memory lock limit documented in the validator's
// setup instructions at docs/src/operations/guides/validator-start.md allowing use of
// several io_uring instances with fixed buffers for large disk IO operations.
#[allow(dead_code)]
pub const TOTAL_IO_URING_BUFFERS_SIZE_LIMIT: usize = 2_000_000_000;

// When getting accounts for shrinking from the index, this is the # of accounts to lookup per thread.
// This allows us to split up accounts index accesses across multiple threads.
const SHRINK_COLLECT_CHUNK_SIZE: usize = 50;

/// The number of shrink candidate slots that is small enough so that
/// additional storages from ancient slots can be added to the
/// candidates for shrinking.
const SHRINK_INSERT_ANCIENT_THRESHOLD: usize = 10;

/// reference an account found during scanning a storage.
#[derive(Debug, PartialEq, Copy, Clone)]
pub struct AccountFromStorage {
    pub index_info: AccountInfo,
    pub data_len: u64,
    pub pubkey: Pubkey,
}

impl IsZeroLamport for AccountFromStorage {
    fn is_zero_lamport(&self) -> bool {
        self.index_info.is_zero_lamport()
    }
}

impl AccountFromStorage {
    pub fn pubkey(&self) -> &Pubkey {
        &self.pubkey
    }
    pub fn stored_size(&self) -> usize {
        AppendVec::calculate_stored_size(self.data_len as usize)
    }
    pub fn data_len(&self) -> usize {
        self.data_len as usize
    }
    #[cfg(test)]
    pub(crate) fn new(offset: Offset, account: &StoredAccountInfoWithoutData) -> Self {
        // the id is irrelevant in this account info. This structure is only used DURING shrink operations.
        // In those cases, there is only 1 append vec id per slot when we read the accounts.
        // Any value of storage id in account info works fine when we want the 'normal' storage.
        let storage_id = 0;
        AccountFromStorage {
            index_info: AccountInfo::new(
                StorageLocation::AppendVec(storage_id, offset),
                account.is_zero_lamport(),
            ),
            pubkey: *account.pubkey(),
            data_len: account.data_len as u64,
        }
    }
}

/// Slots older the "number of slots in an epoch minus this number"
/// than max root are treated as ancient and subject to packing.
/// |  older  |<-          slots in an epoch          ->| max root
/// |  older  |<-    offset   ->|                       |
/// |          ancient          |        modern         |
///
/// If this is negative, this many slots older than the number of
/// slots in epoch are still treated as modern (ie. non-ancient).
/// |  older  |<- abs(offset) ->|<- slots in an epoch ->| max root
/// | ancient |                 modern                  |
///
/// Note that another constant DEFAULT_MAX_ANCIENT_STORAGES sets a
/// threshold for combining ancient storages so that their overall
/// number is under a certain limit, whereas this constant establishes
/// the distance from the max root slot beyond which storages holding
/// the account data for the slots are considered ancient by the
/// shrinking algorithm.
const ANCIENT_APPEND_VEC_DEFAULT_OFFSET: Option<i64> = Some(100_000);
/// The smallest size of ideal ancient storage.
/// The setting can be overridden on the command line
/// with --accounts-db-ancient-ideal-storage-size option.
const DEFAULT_ANCIENT_STORAGE_IDEAL_SIZE: u64 = 100_000;
/// Default value for the number of ancient storages the ancient slot
/// combining should converge to.
pub const DEFAULT_MAX_ANCIENT_STORAGES: usize = 100_000;

#[cfg(not(test))]
const ABSURD_CONSECUTIVE_FAILED_ITERATIONS: usize = 100;

#[derive(Debug, Clone, Copy)]
pub enum AccountShrinkThreshold {
    /// Measure the total space sparseness across all candidates
    /// And select the candidates by using the top sparse account storage entries to shrink.
    /// The value is the overall shrink threshold measured as ratio of the total live bytes
    /// over the total bytes.
    TotalSpace { shrink_ratio: f64 },
    /// Use the following option to shrink all stores whose alive ratio is below
    /// the specified threshold.
    IndividualStore { shrink_ratio: f64 },
}
#[allow(dead_code)]
pub const DEFAULT_ACCOUNTS_SHRINK_OPTIMIZE_TOTAL_SPACE: bool = true;
pub const DEFAULT_ACCOUNTS_SHRINK_RATIO: f64 = 0.80;
// The default extra account space in percentage from the ideal target
pub(crate) const DEFAULT_ACCOUNTS_SHRINK_THRESHOLD_OPTION: AccountShrinkThreshold =
    AccountShrinkThreshold::TotalSpace {
        shrink_ratio: DEFAULT_ACCOUNTS_SHRINK_RATIO,
    };

impl Default for AccountShrinkThreshold {
    fn default() -> AccountShrinkThreshold {
        DEFAULT_ACCOUNTS_SHRINK_THRESHOLD_OPTION
    }
}

#[cfg(test)]
pub enum ScanStorageResult<R, B> {
    Cached(Vec<R>),
    Stored(B),
}

#[derive(Debug)]
pub struct IndexGenerationInfo {
    pub accounts_data_len: u64,
    /// The accounts lt hash calculated during index generation.
    /// Will be used when verifying accounts, after rebuilding a Bank.
    pub calculated_accounts_lt_hash: AccountsLtHash,
    /// The capitalization, in lamports, calculated during index generation.
    pub calculated_capitalization: u64,
}

/// Accumulator for the values produced while generating the index
#[derive(Debug)]
pub(crate) struct IndexGenerationAccumulator {
    insert_time_us: u64,
    num_accounts: u64,
    accounts_data_len: u64,
    all_accounts_are_zero_lamports_slots: u64,
    /// List of slots with only zero lamports accounts and indices into `storages` used in `generate_index`
    slots_with_only_zero_lamport_accounts: Vec<(Slot, usize)>,
    storage_info: StorageSizeAndCountList,
    /// The accounts lt hash for the set of accounts processed using this accumulator
    lt_hash: LtHash,
    /// The capitalization for the set of accounts processed using this accumulator.
    capitalization: u128,
    /// The number of accounts in this slot that were skipped when generating the index as they
    /// were already marked obsolete in the account storage entry
    num_obsolete_accounts_skipped: u64,
    /// Accounts that were displaced from the index because a newer-slot version won.
    /// Each entry holds the slot, account info, and pubkey of the displaced account.
    /// After all threads finish, these are used to subtract the displaced accounts'
    /// contributions from capitalization/lt_hash/data_len and to mark their storage
    /// entries as dead.
    displaced_reclaims: Vec<(Slot, AccountInfo, Pubkey)>,
    slot_arena: IndexGenerationSlotArena,
}
impl IndexGenerationAccumulator {
    fn with_slots_capacity(num_slots: usize) -> Self {
        Self {
            insert_time_us: 0,
            num_accounts: 0,
            accounts_data_len: 0,
            all_accounts_are_zero_lamports_slots: 0,
            slots_with_only_zero_lamport_accounts: Vec::new(),
            storage_info: Vec::with_capacity(num_slots),
            lt_hash: LtHash::identity(),
            capitalization: 0,
            num_obsolete_accounts_skipped: 0,
            displaced_reclaims: Vec::new(),
            slot_arena: IndexGenerationSlotArena::default(),
        }
    }
    fn accumulate(&mut self, mut other: Self) {
        self.insert_time_us += other.insert_time_us;
        self.num_accounts += other.num_accounts;
        self.accounts_data_len += other.accounts_data_len;
        self.all_accounts_are_zero_lamports_slots += other.all_accounts_are_zero_lamports_slots;
        self.slots_with_only_zero_lamport_accounts
            .append(&mut other.slots_with_only_zero_lamport_accounts);
        self.lt_hash.mix_in(&other.lt_hash);
        self.capitalization = self
            .capitalization
            .checked_add(other.capitalization)
            .expect("capitalization cannot overflow");
        self.num_obsolete_accounts_skipped += other.num_obsolete_accounts_skipped;
        self.storage_info.append(&mut other.storage_info);
        self.displaced_reclaims
            .append(&mut other.displaced_reclaims);
    }
}

/// Auxiliary state populated and emptied per slot within `generate_index_for_slot`
///
/// Holds allocated memory across run of index generation thread for performance.
#[derive(Debug, Default)]
struct IndexGenerationSlotArena {
    zero_lamport_offsets: Vec<usize>,
}

impl IndexGenerationSlotArena {
    fn ensure_empty(&mut self) {
        self.zero_lamport_offsets.clear();
    }
}

#[derive(Default, Debug)]
struct GenerateIndexTimings {
    pub total_time_us: u64,
    pub index_time: u64,
    pub insertion_time_us: u64,
    pub storage_size_storages_us: u64,
    pub total_including_duplicates: u64,
    pub visit_duplicate_accounts_time_us: u64,
    pub total_duplicate_slot_keys: u64,
    pub total_num_unique_duplicate_keys: u64,
    pub num_duplicate_accounts: u64,
    pub populate_duplicate_keys_us: u64,
    pub total_slots: u64,
    pub all_accounts_are_zero_lamports_slots: u64,
    pub mark_obsolete_accounts_us: u64,
    pub num_obsolete_accounts_marked: u64,
    pub num_slots_removed_as_obsolete: u64,
    pub num_obsolete_accounts_skipped: u64,
}

#[derive(Default, Debug, PartialEq, Eq)]
struct StorageSizeAndCount {
    /// total size stored, including both alive and dead bytes
    pub stored_size: usize,
    /// number of accounts in the storage including both alive and dead accounts
    pub count: usize,
}
type StorageSizeAndCountList = Vec<(AccountsFileId, StorageSizeAndCount)>;

impl GenerateIndexTimings {
    pub fn report(&self) {
        datapoint_info!(
            "generate_index",
            ("overall_us", self.total_time_us, i64),
            ("index_time_us", self.index_time, i64),
            // we cannot accurately measure index insertion time because of many threads and lock contention
            ("insertion_time_us", self.insertion_time_us, i64),
            (
                "storage_size_storages_us",
                self.storage_size_storages_us,
                i64
            ),
            (
                "total_items_including_duplicates",
                self.total_including_duplicates,
                i64
            ),
            (
                "visit_duplicate_accounts_us",
                self.visit_duplicate_accounts_time_us,
                i64
            ),
            (
                "total_duplicate_slot_keys",
                self.total_duplicate_slot_keys,
                i64
            ),
            (
                "total_num_unique_duplicate_keys",
                self.total_num_unique_duplicate_keys,
                i64
            ),
            ("num_duplicate_accounts", self.num_duplicate_accounts, i64),
            (
                "populate_duplicate_keys_us",
                self.populate_duplicate_keys_us,
                i64
            ),
            ("total_slots", self.total_slots, i64),
            (
                "all_accounts_are_zero_lamports_slots",
                self.all_accounts_are_zero_lamports_slots,
                i64
            ),
            (
                "mark_obsolete_accounts_us",
                self.mark_obsolete_accounts_us,
                i64
            ),
            (
                "num_obsolete_accounts_marked",
                self.num_obsolete_accounts_marked,
                i64
            ),
            (
                "num_slots_removed_as_obsolete",
                self.num_slots_removed_as_obsolete,
                i64
            ),
            (
                "num_obsolete_accounts_skipped",
                self.num_obsolete_accounts_skipped,
                i64
            ),
        );
    }
}

impl IsZeroLamport for AccountSharedData {
    fn is_zero_lamport(&self) -> bool {
        self.lamports() == 0
    }
}

impl IsZeroLamport for Account {
    fn is_zero_lamport(&self) -> bool {
        self.lamports() == 0
    }
}

/// An offset into the AccountsDb::storage vector
pub type AtomicAccountsFileId = AtomicU32;
pub type AccountsFileId = u32;

type AccountSlots = HashMap<Pubkey, IntSet<Slot>>;
type SlotOffsets = IntMap<Slot, IntSet<Offset>>;
type ReclaimResult = (AccountSlots, SlotOffsets);
type PubkeysRemovedFromAccountsIndex = HashSet<Pubkey>;
type ShrinkCandidates = IntSet<Slot>;

#[derive(Debug)]
pub enum LoadedAccountAccessor<'a> {
    // StoredAccountInfo can't be held directly here due to its lifetime dependency on
    // AccountStorageEntry
    Stored(Option<(Arc<AccountStorageEntry>, usize)>),
    // None value in Cached variant means the cache was flushed
    #[allow(dead_code)]
    Cached(Option<Cow<'a, Arc<CachedAccount>>>),
}

impl LoadedAccountAccessor<'_> {
    pub(super) fn check_and_get_loaded_account_shared_data(&mut self) -> AccountSharedData {
        // all of these following .expect() and .unwrap() are like serious logic errors,
        // ideal for representing this as rust type system....

        match self {
            LoadedAccountAccessor::Stored(Some((maybe_storage_entry, offset))) => {
                // If we do find the storage entry, we can guarantee that the storage entry is
                // safe to read from because we grabbed a reference to the storage entry while it
                // was still in the storage map. This means even if the storage entry is removed
                // from the storage map after we grabbed the storage entry, the recycler should not
                // reset the storage entry until we drop the reference to the storage entry.
                maybe_storage_entry
                    .accounts
                    .get_account_shared_data(*offset)
                    .expect(
                        "If a storage entry was found in the storage map, it must not have been \
                         reset yet",
                    )
            }
            _ => self.check_and_get_loaded_account(|loaded_account| loaded_account.take_account()),
        }
    }

    fn check_and_get_loaded_account<T>(
        &mut self,
        callback: impl for<'local> FnMut(LoadedAccount<'local>) -> T,
    ) -> T {
        // all of these following .expect() and .unwrap() are like serious logic errors,
        // ideal for representing this as rust type system....

        match self {
            LoadedAccountAccessor::Cached(None) | LoadedAccountAccessor::Stored(None) => {
                panic!(
                    "Should have already been taken care of when creating this \
                     LoadedAccountAccessor"
                );
            }
            LoadedAccountAccessor::Cached(Some(_cached_account)) => {
                // Cached(Some(x)) variant always produces `Some` for get_loaded_account() since
                // it just returns the inner `x` without additional fetches
                self.get_loaded_account(callback).unwrap()
            }
            LoadedAccountAccessor::Stored(Some(_maybe_storage_entry)) => {
                // If we do find the storage entry, we can guarantee that the storage entry is
                // safe to read from because we grabbed a reference to the storage entry while it
                // was still in the storage map. This means even if the storage entry is removed
                // from the storage map after we grabbed the storage entry, the recycler should not
                // reset the storage entry until we drop the reference to the storage entry.
                self.get_loaded_account(callback).expect(
                    "If a storage entry was found in the storage map, it must not have been reset \
                     yet",
                )
            }
        }
    }

    pub(super) fn get_loaded_account<T>(
        &mut self,
        mut callback: impl for<'local> FnMut(LoadedAccount<'local>) -> T,
    ) -> Option<T> {
        match self {
            LoadedAccountAccessor::Cached(cached_account) => {
                let cached_account = cached_account.take().expect(
                    "Cache flushed/purged should be handled before trying to fetch account",
                );
                Some(callback(LoadedAccount::Cached(cached_account)))
            }
            LoadedAccountAccessor::Stored(maybe_storage_entry) => {
                // storage entry may not be present if slot was cleaned up in
                // between reading the accounts index and calling this function to
                // get account meta from the storage entry here
                maybe_storage_entry
                    .as_ref()
                    .and_then(|(storage_entry, offset)| {
                        storage_entry
                            .accounts
                            .get_stored_account_callback(*offset, |account| {
                                callback(LoadedAccount::Stored(account))
                            })
                    })
            }
        }
    }
}

pub enum LoadedAccount<'a> {
    Stored(StoredAccountInfo<'a>),
    Cached(Cow<'a, Arc<CachedAccount>>),
}

impl LoadedAccount<'_> {
    pub fn pubkey(&self) -> &Pubkey {
        match self {
            LoadedAccount::Stored(stored_account) => stored_account.pubkey(),
            LoadedAccount::Cached(cached_account) => cached_account.pubkey(),
        }
    }

    pub fn take_account(&self) -> AccountSharedData {
        match self {
            LoadedAccount::Stored(stored_account) => create_account_shared_data(stored_account),
            LoadedAccount::Cached(cached_account) => match cached_account {
                Cow::Owned(cached_account) => cached_account.account.clone(),
                Cow::Borrowed(cached_account) => cached_account.account.clone(),
            },
        }
    }

    pub fn is_cached(&self) -> bool {
        match self {
            LoadedAccount::Stored(_) => false,
            LoadedAccount::Cached(_) => true,
        }
    }

    /// data_len can be calculated without having access to `&data` in future implementations
    pub fn data_len(&self) -> usize {
        self.data().len()
    }
}

impl ReadableAccount for LoadedAccount<'_> {
    fn lamports(&self) -> u64 {
        match self {
            LoadedAccount::Stored(stored_account) => stored_account.lamports(),
            LoadedAccount::Cached(cached_account) => cached_account.account.lamports(),
        }
    }
    fn data(&self) -> &[u8] {
        match self {
            LoadedAccount::Stored(stored_account) => stored_account.data(),
            LoadedAccount::Cached(cached_account) => cached_account.account.data(),
        }
    }
    fn owner(&self) -> &Pubkey {
        match self {
            LoadedAccount::Stored(stored_account) => stored_account.owner(),
            LoadedAccount::Cached(cached_account) => cached_account.account.owner(),
        }
    }
    fn executable(&self) -> bool {
        match self {
            LoadedAccount::Stored(stored_account) => stored_account.executable(),
            LoadedAccount::Cached(cached_account) => cached_account.account.executable(),
        }
    }
    fn rent_epoch(&self) -> Epoch {
        match self {
            LoadedAccount::Stored(stored_account) => stored_account.rent_epoch(),
            LoadedAccount::Cached(cached_account) => cached_account.account.rent_epoch(),
        }
    }
}

pub fn get_temp_accounts_paths(count: u32) -> io::Result<(Vec<TempDir>, Vec<PathBuf>)> {
    let temp_dirs: io::Result<Vec<TempDir>> = (0..count).map(|_| TempDir::new()).collect();
    let temp_dirs = temp_dirs?;

    let paths: io::Result<Vec<_>> = temp_dirs
        .iter()
        .map(|temp_dir| {
            utils::create_accounts_run_and_snapshot_dirs(temp_dir)
                .map(|(run_dir, _snapshot_dir)| run_dir)
        })
        .collect();
    let paths = paths?;
    Ok((temp_dirs, paths))
}

type AccountInfoAccountsIndex = AccountsIndex<AccountInfo, AccountInfo>;

// This structure handles the AppendVec-specific load/store of the accounts.
// Backend-agnostic (generic) state lives in `AccountsDb` (wrapper).
#[derive(Debug)]
pub struct AppendVecBackend {
    /// Keeps tracks of index into AppendVec on a per slot basis
    pub accounts_index: AccountInfoAccountsIndex,

    /// Some(offset) iff we want to squash old append vecs together into 'ancient append vecs'
    /// Some(offset) means for slots up to (max_slot - (slots_per_epoch - 'offset')), put them in ancient append vecs
    pub ancient_append_vec_offset: Option<i64>,
    pub ancient_storage_ideal_size: u64,
    pub max_ancient_storages: usize,

    pub storage: AccountStorage,

    /// distribute the accounts across storage lists
    pub next_id: AtomicAccountsFileId,

    /// Set of shrinkable stores organized by map of slot to storage id
    pub shrink_candidate_slots: Mutex<ShrinkCandidates>,

    /// Set of storage paths to pick from
    pub paths: Vec<PathBuf>,

    shrink_paths: Vec<PathBuf>,

    /// Thread pool for background tasks, e.g. AccountsBackgroundService and flush/clean/shrink
    pub thread_pool_background: ThreadPool,

    clean_accounts_stats: CleanAccountsStats,

    pub shrink_stats: ShrinkStats,

    pub(crate) shrink_ancient_stats: ShrinkAncientStats,

    /// Set of unique keys per slot which is used
    /// to drive clean_accounts
    /// Populated when flushing the accounts write cache
    pub(crate) uncleaned_pubkeys: DashMap<Slot, Vec<Pubkey>, BuildNoHashHasher<Slot>>,

    pub(crate) store_accounts_frozen_stats: StoreAccountsFrozenStats,

    pub(crate) appendvec_stats: AppendVecStats,

    pub(crate) shrink_ratio: AccountShrinkThreshold,

    /// Set of stores which are recently rooted or had accounts removed
    /// such that potentially a 0-lamport account update could be present which
    /// means we can remove the account from the index entirely.
    pub(crate) dirty_stores: DashMap<Slot, Arc<AccountStorageEntry>, BuildNoHashHasher<Slot>>,

    /// Zero-lamport accounts that are *not* purged during clean because they need to stay alive
    /// for incremental snapshot support.
    pub(crate) zero_lamport_accounts_to_purge_after_full_snapshot: DashSet<(Slot, Pubkey)>,

    /// debug feature to scan every append vec and verify refcounts are equal
    pub(crate) exhaustively_verify_refcounts: bool,

    /// storage format to use for new storages
    pub(crate) accounts_file_provider: AccountsFileProvider,

    /// method to use for accessing storages
    pub(crate) storage_access: StorageAccess,

    /// index scan filtering for shrinking
    #[allow(dead_code)]
    pub(crate) scan_filter_for_shrinking: ScanFilter,

    /// The latest full snapshot slot dictates how to handle zero lamport accounts
    /// Note, this is None if we're told to *not* take snapshots
    pub(crate) latest_full_snapshot_slot: SeqLock<Option<Slot>>,

    /// These are the ancient storages that could be valuable to
    /// shrink, sorted by amount of dead bytes.  The elements
    /// are sorted from the largest dead bytes to the smallest.
    /// Members are Slot and capacity. If capacity is smaller, then
    /// that means the storage was already shrunk.
    pub(crate) best_ancient_slots_to_shrink: RwLock<VecDeque<(Slot, u64)>>,

    /// Read-only account cache. Avoids repeated mmap reads for recently loaded accounts.
    /// This is AppendVec-specific; RocksDB uses its own internal block cache.
    pub(crate) read_only_accounts_cache: ReadOnlyAccountsCache,

    /// Tracks which flushed slots have live storage. Used by shrink to find candidates.
    pub storage_roots: RwLock<StorageRoots>,

    #[cfg(test)]
    pub load_limit: AtomicU64,
    #[cfg(test)]
    pub load_delay: u64,
}

pub fn quarter_thread_count() -> usize {
    std::cmp::max(2, num_cpus::get() / 4)
}

pub fn default_num_foreground_threads() -> usize {
    get_thread_count()
}

#[cfg(feature = "frozen-abi")]
impl solana_frozen_abi::abi_example::AbiExample for AppendVecBackend {
    fn example() -> Self {
        let accounts_db = AccountsDb::new_single_for_tests();
        let key = Pubkey::default();
        let some_data_len = 5;
        let some_slot: Slot = 0;
        let account = AccountSharedData::new(1, some_data_len, &key);
        accounts_db.store_for_tests((some_slot, [(&key, &account)].as_slice()));
        accounts_db.add_root_and_flush_write_cache(0);
        let AccountsBackend::AppendVec(backend) = accounts_db.backend;
        backend
    }
}

impl AppendVecBackend {
    /// Returns the number of accounts persisted in `slot`'s storage entry,
    /// or zero if the backend has no record of `slot`. Test only.
    #[cfg(test)]
    pub(super) fn account_count_in_slot(&self, slot: Slot) -> usize {
        self.storage
            .get_slot_storage_entry(slot)
            .map(|s| s.count())
            .unwrap_or(0)
    }

    // The default high and low watermark sizes for the accounts read cache.
    // If the cache size exceeds MAX_SIZE_HI, it'll evict entries until the size is <= MAX_SIZE_LO.
    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    const DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_LO: usize = 3_000_000_000;
    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    const DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_HI: usize = 3_100_000_000;

    // See AccountsDbConfig::read_cache_evict_sample_size.
    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    const DEFAULT_READ_ONLY_CACHE_EVICT_SAMPLE_SIZE: usize = 8;

    /// Create the AppendVec-specific backend.
    /// Callers should normally use `AccountsDb::new_with_config` instead.
    pub(crate) fn new_backend(
        paths: Vec<PathBuf>,
        accounts_db_config: AccountsDbConfig,
        exit: Arc<AtomicBool>,
        storage: AccountStorageMap,
        next_id: AccountsFileId,
    ) -> Self {
        let accounts_index_config = accounts_db_config.index.unwrap_or_default();
        let accounts_index = AccountsIndex::new(&accounts_index_config, exit);

        let read_cache_size = accounts_db_config.read_cache_limit_bytes.unwrap_or((
            Self::DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_LO,
            Self::DEFAULT_MAX_READ_ONLY_CACHE_DATA_SIZE_HI,
        ));
        let read_cache_evict_sample_size = accounts_db_config
            .read_cache_evict_sample_size
            .unwrap_or(Self::DEFAULT_READ_ONLY_CACHE_EVICT_SAMPLE_SIZE);

        let shrink_paths = accounts_db_config
            .shrink_paths
            .clone()
            .unwrap_or_else(|| paths.clone());

        let num_background_threads = accounts_db_config
            .num_background_threads
            .map(Into::into)
            .unwrap_or_else(quarter_thread_count);
        let thread_pool_background = rayon::ThreadPoolBuilder::new()
            .thread_name(|i| format!("solAcctsDbBg{i:02}"))
            .num_threads(num_background_threads)
            .build()
            .expect("new rayon threadpool");

        let new = Self {
            accounts_index,
            paths,
            shrink_paths,
            ancient_append_vec_offset: accounts_db_config
                .ancient_append_vec_offset
                .or(ANCIENT_APPEND_VEC_DEFAULT_OFFSET),
            ancient_storage_ideal_size: accounts_db_config
                .ancient_storage_ideal_size
                .unwrap_or(DEFAULT_ANCIENT_STORAGE_IDEAL_SIZE),
            max_ancient_storages: accounts_db_config
                .max_ancient_storages
                .unwrap_or(DEFAULT_MAX_ANCIENT_STORAGES),
            shrink_ratio: accounts_db_config.shrink_ratio,
            exhaustively_verify_refcounts: accounts_db_config.exhaustively_verify_refcounts,
            storage_access: accounts_db_config.storage_access,
            scan_filter_for_shrinking: accounts_db_config.scan_filter_for_shrinking,
            thread_pool_background,
            storage: AccountStorage::new_with_map(storage),
            uncleaned_pubkeys: DashMap::default(),
            next_id: AtomicAccountsFileId::new(next_id),
            shrink_candidate_slots: Mutex::new(ShrinkCandidates::default()),
            clean_accounts_stats: CleanAccountsStats::default(),
            shrink_stats: ShrinkStats::default(),
            shrink_ancient_stats: ShrinkAncientStats::default(),
            dirty_stores: DashMap::default(),
            zero_lamport_accounts_to_purge_after_full_snapshot: DashSet::default(),
            accounts_file_provider: accounts_db_config.accounts_file_provider,
            latest_full_snapshot_slot: SeqLock::new(None),
            best_ancient_slots_to_shrink: RwLock::default(),
            store_accounts_frozen_stats: StoreAccountsFrozenStats::default(),
            appendvec_stats: AppendVecStats::default(),
            read_only_accounts_cache: ReadOnlyAccountsCache::new(
                read_cache_size.0,
                read_cache_size.1,
                read_cache_evict_sample_size,
            ),
            storage_roots: RwLock::new(StorageRoots::default()),
            #[cfg(test)]
            load_limit: AtomicU64::default(),
            #[cfg(test)]
            load_delay: u64::default(),
        };

        {
            for path in new.paths.iter() {
                std::fs::create_dir_all(path).expect("Create directory failed.");
            }
        }
        new
    }

    fn next_id(&self) -> AccountsFileId {
        let next_id = self.next_id.fetch_add(1, Ordering::AcqRel);
        assert!(
            next_id != AccountsFileId::MAX,
            "We've run out of storage ids!"
        );
        next_id
    }

    fn new_storage_entry(&self, slot: Slot, path: &Path, size: u64) -> AccountStorageEntry {
        AccountStorageEntry::new(
            path,
            slot,
            self.next_id(),
            size,
            self.accounts_file_provider,
            self.storage_access,
        )
    }

    fn create_store(
        &self,
        slot: Slot,
        size: u64,
        from: &str,
        paths: &[PathBuf],
    ) -> Arc<AccountStorageEntry> {
        self.appendvec_stats
            .create_store_count
            .fetch_add(1, Ordering::Relaxed);
        let path_index = rng().random_range(0..paths.len());
        let store = Arc::new(self.new_storage_entry(slot, Path::new(&paths[path_index]), size));
        debug!(
            "creating store: {} slot: {} len: {} size: {} from: {} path: {}",
            store.id(),
            slot,
            store.accounts.len(),
            store.accounts.capacity(),
            from,
            store.accounts.path().display(),
        );
        store
    }

    fn create_and_insert_store(
        &self,
        slot: Slot,
        size: u64,
        from: &str,
    ) -> Arc<AccountStorageEntry> {
        self.create_and_insert_store_with_paths(slot, size, from, &self.paths)
    }

    fn create_and_insert_store_with_paths(
        &self,
        slot: Slot,
        size: u64,
        from: &str,
        paths: &[PathBuf],
    ) -> Arc<AccountStorageEntry> {
        let store = self.create_store(slot, size, from, paths);
        self.storage.insert(store.clone());
        store
    }

    fn apply_offset_to_slot(slot: Slot, offset: i64) -> Slot {
        if offset > 0 {
            slot.saturating_add(offset as u64)
        } else {
            slot.saturating_sub(offset.unsigned_abs())
        }
    }

    /// Returns the oldest slot within one epoch of `max_root`, offset by `ancient_append_vec_offset`.
    fn get_oldest_non_ancient_slot(&self, epoch_schedule: &EpochSchedule, max_root: Slot) -> Slot {
        let mut result = max_root;
        if let Some(offset) = self.ancient_append_vec_offset {
            result = Self::apply_offset_to_slot(result, offset);
        }
        result = Self::apply_offset_to_slot(
            result,
            -((epoch_schedule.slots_per_epoch as i64).saturating_sub(1)),
        );
        result.min(max_root)
    }

    /// Purge the backing storage entries for the given slot, does not purge from the cache.
    fn purge_dead_slots_from_storage<'a>(
        &'a self,
        removed_slots: impl Iterator<Item = &'a Slot> + Clone,
        purge_stats: &PurgeStats,
    ) {
        let mut total_removed_stored_bytes = 0;
        let mut all_removed_slot_storages = vec![];
        let mut remove_storage_entries_elapsed = Measure::start("remove_storage_entries_elapsed");
        for remove_slot in removed_slots {
            if let Some(store) = self.storage.remove(remove_slot, false) {
                total_removed_stored_bytes += store.accounts.capacity();
                all_removed_slot_storages.push(store);
            }
        }
        remove_storage_entries_elapsed.stop();
        let num_stored_slots_removed = all_removed_slot_storages.len();
        let mut drop_storage_entries_elapsed = Measure::start("drop_storage_entries_elapsed");
        drop(all_removed_slot_storages);
        drop_storage_entries_elapsed.stop();
        purge_stats
            .remove_storage_entries_elapsed
            .fetch_add(remove_storage_entries_elapsed.as_us(), Ordering::Relaxed);
        purge_stats
            .drop_storage_entries_elapsed
            .fetch_add(drop_storage_entries_elapsed.as_us(), Ordering::Relaxed);
        purge_stats
            .num_stored_slots_removed
            .fetch_add(num_stored_slots_removed, Ordering::Relaxed);
        purge_stats
            .total_removed_storage_entries
            .fetch_add(num_stored_slots_removed, Ordering::Relaxed);
        purge_stats
            .total_removed_stored_bytes
            .fetch_add(total_removed_stored_bytes, Ordering::Relaxed);
        self.appendvec_stats
            .dropped_stores
            .fetch_add(num_stored_slots_removed as u64, Ordering::Relaxed);
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn set_storage_access(&mut self, storage_access: StorageAccess) {
        self.storage_access = storage_access;
    }

    /// Scan `account_paths` for AppendVec files and reconstruct storage entries.
    ///
    /// Files must follow the `"<slot>.<id>"` naming convention and be de-duplicated
    /// (one file per slot). Returns the storage map and the next available file ID.
    pub fn from_account_paths(
        account_paths: &[std::path::PathBuf],
        storage_access: StorageAccess,
        obsolete_accounts: Option<
            &dashmap::DashMap<Slot, (crate::ObsoleteAccounts, super::AccountsFileId)>,
        >,
    ) -> Result<
        (
            crate::account_storage::AccountStorageMap,
            super::AccountsFileId,
        ),
        crate::snapshot_storage::StorageRestoreError,
    > {
        use {
            crate::snapshot_storage::{
                StorageRestoreError, parse_append_vec_filename, reconstruct_single_storage,
            },
            agave_fs::FileInfo,
        };
        let storage = crate::account_storage::AccountStorageMap::default();
        let writable = matches!(storage_access, StorageAccess::Mmap);
        let mut next_id: super::AccountsFileId = 0;

        for path in account_paths {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let file_path = entry.path();
                if !file_path.is_file() {
                    continue;
                }
                let Some(filename) = file_path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                let Some((slot, id)) = parse_append_vec_filename(filename) else {
                    continue;
                };
                let obs = obsolete_accounts.and_then(|map| map.remove(&slot).map(|(_, v)| v));
                let file_info = FileInfo::new_from_path_writable(file_path, writable)?;
                let storage_entry =
                    reconstruct_single_storage(slot, file_info, id, storage_access, obs)?;
                if storage.insert(slot, storage_entry).is_some() {
                    return Err(StorageRestoreError::DuplicateSlot { slot });
                }
                next_id = next_id.max(id.saturating_add(1));
            }
        }

        Ok((storage, next_id))
    }

    /// returns (dead slots, reclaimed_offsets)
    fn remove_dead_accounts<'a, I>(
        &'a self,
        reclaims: I,
        expected_slot: Option<Slot>,
        mark_accounts_obsolete: MarkAccountsObsolete,
    ) -> (IntSet<Slot>, SlotOffsets)
    where
        I: Iterator<Item = &'a (Slot, AccountInfo)>,
    {
        let mut reclaimed_offsets = SlotOffsets::default();

        assert!(self.storage.no_shrink_in_progress());

        let mut dead_slots = IntSet::default();
        let mut new_shrink_candidates = ShrinkCandidates::default();
        let mut measure = Measure::start("remove");
        for (slot, account_info) in reclaims {
            // No cached accounts should make it here
            assert!(!account_info.is_cached());
            reclaimed_offsets
                .entry(*slot)
                .or_default()
                .insert(account_info.offset());
        }
        if let Some(expected_slot) = expected_slot {
            assert_eq!(reclaimed_offsets.len(), 1);
            assert!(reclaimed_offsets.contains_key(&expected_slot));
        }

        self.clean_accounts_stats
            .slots_cleaned
            .fetch_add(reclaimed_offsets.len() as u64, Ordering::Relaxed);

        reclaimed_offsets.iter().for_each(|(slot, offsets)| {
            if let Some(store) = self.storage.get_slot_storage_entry(*slot) {
                assert_eq!(
                    *slot,
                    store.slot(),
                    "AccountsDB::accounts_index corrupted. Storage pointed to: {}, expected: {}, \
                     should only point to one slot",
                    store.slot(),
                    *slot
                );

                let remaining_accounts = if offsets.len() == store.count() {
                    // all remaining alive accounts in the storage are being removed, so the entire storage/slot is dead
                    store.remove_accounts(store.alive_bytes(), offsets.len())
                } else {
                    // not all accounts are being removed, so figure out sizes of accounts we are removing and update the alive bytes and alive account count
                    let (remaining_accounts, us) = measure_us!({
                        let mut offsets = offsets.iter().cloned().collect::<Vec<_>>();
                        // sort so offsets are in order. This improves efficiency of loading the accounts.
                        offsets.sort_unstable();
                        let data_lens = store.accounts.get_account_data_lens(&offsets);
                        let dead_bytes = data_lens
                            .iter()
                            .map(|len| store.accounts.calculate_stored_size(*len))
                            .sum();
                        let remaining_accounts = store.remove_accounts(dead_bytes, offsets.len());

                        if let MarkAccountsObsolete::Yes(slot_marked_obsolete) =
                            mark_accounts_obsolete
                        {
                            store
                                .obsolete_accounts
                                .write()
                                .unwrap()
                                .mark_accounts_obsolete(
                                    offsets.into_iter().zip(data_lens),
                                    slot_marked_obsolete,
                                );
                        }
                        remaining_accounts
                    });
                    self.clean_accounts_stats
                        .get_account_sizes_us
                        .fetch_add(us, Ordering::Relaxed);
                    remaining_accounts
                };

                // Check if we have removed all accounts from the storage
                // This may be different from the check above as this
                // can be multithreaded
                if remaining_accounts == 0 {
                    self.dirty_stores.insert(*slot, store);
                    dead_slots.insert(*slot);
                } else if Self::is_shrinking_productive(&store)
                    && self.is_candidate_for_shrink(&store)
                {
                    // Checking that this single storage entry is ready for shrinking,
                    // should be a sufficient indication that the slot is ready to be shrunk
                    // because slots should only have one storage entry, namely the one that was
                    // created by `flush_slot_cache()`.
                    new_shrink_candidates.insert(*slot);
                };
            }
        });
        measure.stop();
        self.clean_accounts_stats
            .remove_dead_accounts_remove_us
            .fetch_add(measure.as_us(), Ordering::Relaxed);

        let mut measure = Measure::start("shrink");
        let mut shrink_candidate_slots = self.shrink_candidate_slots.lock().unwrap();
        for slot in new_shrink_candidates {
            shrink_candidate_slots.insert(slot);
        }
        drop(shrink_candidate_slots);
        measure.stop();
        self.clean_accounts_stats
            .remove_dead_accounts_shrink_us
            .fetch_add(measure.as_us(), Ordering::Relaxed);

        dead_slots.retain(|slot| {
            if let Some(slot_store) = self.storage.get_slot_storage_entry(*slot) {
                if slot_store.count() != 0 {
                    return false;
                }
            }
            true
        });

        (dead_slots, reclaimed_offsets)
    }

    pub(super) fn read_index_for_accessor_or_load_slow<'a>(
        &'a self,
        ancestors: &Ancestors,
        pubkey: &'a Pubkey,
        clone_in_lock: bool,
    ) -> Option<(Slot, StorageLocation, Option<LoadedAccountAccessor<'a>>)> {
        self.accounts_index
            .get(pubkey)
            .and_then(|(slot, account_info)| {
                let valid = ancestors.min_slot().is_none_or(|min| slot < min);
                let valid = valid || ancestors.contains_key(&slot);
                valid.then(|| {
                    let storage_location = account_info.storage_location();
                    let account_accessor = clone_in_lock
                        .then(|| self.get_account_accessor(slot, pubkey, &storage_location));
                    (slot, storage_location, account_accessor)
                })
            })
    }

    #[cfg_attr(test, qualifiers(pub(crate)))]
    pub(super) fn get_account_accessor<'a>(
        &'a self,
        slot: Slot,
        _pubkey: &'a Pubkey,
        storage_location: &StorageLocation,
    ) -> LoadedAccountAccessor<'a> {
        match storage_location {
            StorageLocation::Cached => {
                // The accounts index never stores cached entries on this branch;
                // storage_location is always AppendVec when reading from the index.
                unreachable!("index entries are never cached on this branch")
            }
            StorageLocation::AppendVec(store_id, offset) => {
                let maybe_storage_entry = self
                    .storage
                    .get_account_storage_entry(slot, *store_id)
                    .map(|account_storage_entry| (account_storage_entry, *offset));
                LoadedAccountAccessor::Stored(maybe_storage_entry)
            }
        }
    }

    fn write_accounts_to_storage<'a>(
        &self,
        slot: Slot,
        storage: &AccountStorageEntry,
        accounts_and_meta_to_store: &impl StorableAccounts<'a>,
    ) -> Vec<AccountInfo> {
        let mut infos: Vec<AccountInfo> = Vec::with_capacity(accounts_and_meta_to_store.len());
        while infos.len() < accounts_and_meta_to_store.len() {
            let stored_accounts_info = storage
                .accounts
                .write_accounts(accounts_and_meta_to_store, infos.len());
            let Some(stored_accounts_info) = stored_accounts_info else {
                // See if an account overflows the storage in the slot.
                let data_len = accounts_and_meta_to_store.data_len(infos.len());
                let data_len = (data_len + STORE_META_OVERHEAD) as u64;
                if data_len > storage.accounts.remaining_bytes() {
                    info!(
                        "write_accounts_to_storage, no space: {}, {}, {}, {}, {}",
                        storage.accounts.capacity(),
                        storage.accounts.remaining_bytes(),
                        data_len,
                        infos.len(),
                        accounts_and_meta_to_store.len()
                    );
                    self.create_and_insert_store(slot, data_len * 2, "large create");
                }
                continue;
            };

            let store_id = storage.id();
            for (i, offset) in stored_accounts_info.offsets.iter().enumerate() {
                infos.push(AccountInfo::new(
                    StorageLocation::AppendVec(store_id, *offset),
                    accounts_and_meta_to_store.is_zero_lamport(i),
                ));
            }
            storage.add_accounts(
                stored_accounts_info.offsets.len(),
                stored_accounts_info.size,
            );
        }

        infos
    }

    /// Marks zero lamport single reference accounts in the storage during store_accounts_frozen
    ///
    /// Returns the number of accounts marked.
    fn mark_zero_lamport_single_ref_accounts(
        &self,
        account_infos: &[AccountInfo],
        storage: &AccountStorageEntry,
        reclaim_handling: UpsertReclaim,
    ) -> u64 {
        let mut num_marked = 0;
        // If the reclaim handling is `ReclaimOldSlots`, then all zero lamport accounts are single
        // ref accounts and they need to be inserted into the storages zero lamport single ref
        // accounts list
        // For other values of reclaim handling, there are no zero lamport single ref accounts
        // so nothing needs to be done in this function
        if reclaim_handling == UpsertReclaim::ReclaimOldSlots {
            for account_info in account_infos {
                if account_info.is_zero_lamport() {
                    storage.insert_zero_lamport_single_ref_account_offset(account_info.offset());
                    num_marked += 1;
                }
            }

            // If any zero lamport accounts were marked, the storage may be valid for shrinking
            if num_marked > 0
                && self.is_candidate_for_shrink(storage)
                && AppendVecBackend::is_shrinking_productive(storage)
            {
                self.shrink_candidate_slots
                    .lock()
                    .unwrap()
                    .insert(storage.slot);
            }
        }

        num_marked
    }

    pub fn store(
        &self,
        slot: Slot,
        accounts_to_flush: Vec<(&Pubkey, &AccountSharedData)>,
    ) -> FlushStats {
        // Slot is now in storage — register it in the backend's roots tracker.
        self.storage_roots.write().unwrap().add_root(slot);

        let mut flush_stats = FlushStats::default();

        // Byte sizing is AppendVec-specific.
        for (_, account) in &accounts_to_flush {
            flush_stats.num_bytes_flushed +=
                AppendVec::calculate_stored_size(account.data().len()) as u64;
            flush_stats.num_accounts_flushed += 1;
        }

        if !accounts_to_flush.is_empty() {
            // Write to AppendVec before updating the index so any reader that sees a real
            // index entry can also find the account in storage.
            let flushed_store = self.create_and_insert_store(
                slot,
                flush_stats.num_bytes_flushed.0,
                "flush_slot_cache",
            );

            let (store_accounts_timing_inner, store_accounts_total_inner_us) =
                measure_us!(self._store_accounts_frozen(
                    (slot, &accounts_to_flush[..]),
                    &flushed_store,
                    UpsertReclaim::ReclaimOldSlots,
                    UpdateIndexThreadSelection::PoolWithThreshold,
                ));
            flush_stats.store_accounts_timing = store_accounts_timing_inner;
            flush_stats.store_accounts_total_us = Saturating(store_accounts_total_inner_us);

            assert!(self.storage.get_slot_storage_entry(slot).is_some());
            self.reopen_storage_as_readonly_shrinking_in_progress_ok(slot);
        }

        // Track zero-lamport pubkeys for the cleaner — non-zero displaced entries were
        // already reclaimed inline by the index update above.
        self.uncleaned_pubkeys.entry(slot).or_default().extend(
            accounts_to_flush
                .into_iter()
                .filter(|(_, account)| account.is_zero_lamport())
                .map(|(pubkey, _)| pubkey),
        );

        flush_stats
    }

    fn update_index_stored_accounts<'a>(
        &self,
        infos: Vec<AccountInfo>,
        accounts: &impl StorableAccounts<'a>,
        reclaim: UpsertReclaim,
        update_index_thread_selection: UpdateIndexThreadSelection,
        thread_pool: &ThreadPool,
    ) -> Vec<ReclaimsSlotList<AccountInfo>> {
        let target_slot = accounts.target_slot();
        let len = std::cmp::min(accounts.len(), infos.len());

        let update = |start, end| {
            let mut reclaims = ReclaimsSlotList::with_capacity((end - start) / 2);

            (start..end).for_each(|i| {
                let info: AccountInfo = infos[i];
                debug_assert!(!info.is_cached());
                accounts.account(i, |account| {
                    let displaced = self
                        .accounts_index
                        .insert(target_slot, account.pubkey(), info);
                    if reclaim != UpsertReclaim::IgnoreReclaims {
                        if let Some(entry) = displaced {
                            reclaims.push(entry);
                        }
                    }
                });
            });
            reclaims
        };

        let threshold = 1;
        if matches!(
            update_index_thread_selection,
            UpdateIndexThreadSelection::PoolWithThreshold,
        ) && len > threshold
        {
            let chunk_size = std::cmp::max(1, len / quarter_thread_count());
            let batches = 1 + len / chunk_size;
            thread_pool.install(|| {
                (0..batches)
                    .into_par_iter()
                    .map(|batch| {
                        let start = batch * chunk_size;
                        let end = std::cmp::min(start + chunk_size, len);
                        update(start, end)
                    })
                    .filter(|reclaims| !reclaims.is_empty())
                    .collect()
            })
        } else {
            let reclaims = update(0, len);
            if reclaims.is_empty() {
                vec![]
            } else {
                vec![reclaims]
            }
        }
    }

    /// Stores accounts in the storage and updates the index.
    /// Intended for frozen (rooted) accounts. Does not add the slot to the roots tracker —
    /// the caller is responsible for that.
    pub(super) fn _store_accounts_frozen<'a>(
        &self,
        accounts: impl StorableAccounts<'a>,
        storage: &AccountStorageEntry,
        reclaim_handling: UpsertReclaim,
        update_index_thread_selection: UpdateIndexThreadSelection,
    ) -> StoreAccountsTiming {
        let slot = accounts.target_slot();
        let num_accounts_stored = accounts.len();
        let stats = &self.store_accounts_frozen_stats;

        let flush_read_cache_time = Measure::start("flush_read_cache");
        if self.read_only_accounts_cache.can_slot_be_in_cache(slot) {
            (0..accounts.len()).for_each(|index| {
                self.read_only_accounts_cache
                    .remove_assume_not_present(accounts.pubkey(index));
            });
        }
        let flush_read_cache_us = flush_read_cache_time.end_as_us();

        let write_accounts_time = Measure::start("write_accounts");
        let infos = self.write_accounts_to_storage(slot, storage, &accounts);
        let write_accounts_us = write_accounts_time.end_as_us();

        let mark_zero_lamport_time = Measure::start("mark_zero_lamport");
        let num_zero_lamport_single_ref_accounts_marked =
            self.mark_zero_lamport_single_ref_accounts(&infos, storage, reclaim_handling);
        let mark_zero_lamport_us = mark_zero_lamport_time.end_as_us();

        let update_index_time = Measure::start("update_index");
        let reclaims = self.update_index_stored_accounts(
            infos,
            &accounts,
            reclaim_handling,
            update_index_thread_selection,
            &self.thread_pool_background,
        );
        let update_index_us = update_index_time.end_as_us();

        let handle_reclaims_time = Measure::start("handle_reclaims");
        if !reclaims.is_empty() {
            let reclaims_len = reclaims.iter().map(|r| r.len()).sum::<usize>();
            let purge_stats = PurgeStats::default();
            self.handle_reclaims(
                reclaims.iter().flatten(),
                None,
                &purge_stats,
                MarkAccountsObsolete::Yes(slot),
            );
            stats.num_obsolete_slots_removed.fetch_add(
                purge_stats.num_stored_slots_removed.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            stats.num_obsolete_bytes_removed.fetch_add(
                purge_stats
                    .total_removed_stored_bytes
                    .load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            stats
                .num_reclaims
                .fetch_add(reclaims_len as u64, Ordering::Relaxed);
        }
        let handle_reclaims_us = handle_reclaims_time.end_as_us();

        stats
            .flush_read_cache_us
            .fetch_add(flush_read_cache_us, Ordering::Relaxed);
        stats
            .write_to_storage_us
            .fetch_add(write_accounts_us, Ordering::Relaxed);
        stats
            .update_index_us
            .fetch_add(update_index_us, Ordering::Relaxed);
        stats
            .mark_zero_lamport_single_ref_accounts_us
            .fetch_add(mark_zero_lamport_us, Ordering::Relaxed);
        stats
            .handle_reclaims_us
            .fetch_add(handle_reclaims_us, Ordering::Relaxed);
        stats
            .num_accounts_stored
            .fetch_add(num_accounts_stored as u64, Ordering::Relaxed);
        stats.num_zero_lamport_single_ref_accounts_marked.fetch_add(
            num_zero_lamport_single_ref_accounts_marked,
            Ordering::Relaxed,
        );
        stats.report();

        StoreAccountsTiming {
            store_accounts_elapsed: write_accounts_us,
            update_index_elapsed: update_index_us,
            handle_reclaims_elapsed: handle_reclaims_us,
        }
    }

    pub fn store_accounts_frozen<'a>(
        &self,
        accounts: impl StorableAccounts<'a>,
        storage: &AccountStorageEntry,
        update_index_thread_selection: UpdateIndexThreadSelection,
    ) -> StoreAccountsTiming {
        self._store_accounts_frozen(
            accounts,
            storage,
            UpsertReclaim::IgnoreReclaims,
            update_index_thread_selection,
        )
    }

    pub(super) fn remove_dead_slots_metadata<'a>(
        &'a self,
        dead_slots_iter: impl Iterator<Item = &'a Slot>,
    ) {
        let accounts_index_root_stats = self
            .storage_roots
            .write()
            .unwrap()
            .clean_dead_slots(dead_slots_iter);
        self.clean_accounts_stats
            .latest_storage_roots_stats
            .update(&accounts_index_root_stats);
    }

    pub(super) fn report_storage_stats(&self) {
        datapoint_info!(
            "accounts_db_store_timings",
            (
                "handle_dead_keys_us",
                self.appendvec_stats
                    .handle_dead_keys_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "purge_exact_us",
                self.appendvec_stats
                    .purge_exact_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "purge_exact_count",
                self.appendvec_stats
                    .purge_exact_count
                    .swap(0, Ordering::Relaxed),
                i64
            ),
        );
        datapoint_info!(
            "accounts_db_store_timings2",
            (
                "create_store_count",
                self.appendvec_stats
                    .create_store_count
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "dropped_stores",
                self.appendvec_stats
                    .dropped_stores
                    .swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }

    pub(super) fn report_read_cache_stats(&self) {
        let read_cache_stats = self.read_only_accounts_cache.get_and_reset_stats();
        datapoint_info!(
            "accounts_db_read_cache",
            (
                "read_only_accounts_cache_entries",
                self.read_only_accounts_cache.cache_len(),
                i64
            ),
            (
                "read_only_accounts_cache_data_size",
                self.read_only_accounts_cache.data_size(),
                i64
            ),
            ("read_only_accounts_cache_hits", read_cache_stats.hits, i64),
            (
                "read_only_accounts_cache_misses",
                read_cache_stats.misses,
                i64
            ),
            (
                "read_only_accounts_cache_evicts",
                read_cache_stats.evicts,
                i64
            ),
            (
                "read_only_accounts_cache_load_us",
                read_cache_stats.load_us,
                i64
            ),
            (
                "read_only_accounts_cache_store_us",
                read_cache_stats.store_us,
                i64
            ),
            (
                "read_only_accounts_cache_evict_us",
                read_cache_stats.evict_us,
                i64
            ),
            (
                "read_only_accounts_cache_evictor_wakeup_count_all",
                read_cache_stats.evictor_wakeup_count_all,
                i64
            ),
            (
                "read_only_accounts_cache_evictor_wakeup_count_productive",
                read_cache_stats.evictor_wakeup_count_productive,
                i64
            ),
        );
    }

    fn report_store_stats(&self) {
        let mut total_count = 0;
        let mut newest_slot = 0;
        let mut oldest_slot = u64::MAX;
        let mut total_bytes = 0;
        let mut total_alive_bytes = 0;
        for (slot, store) in self.storage.iter() {
            total_count += 1;
            newest_slot = std::cmp::max(newest_slot, slot);
            oldest_slot = std::cmp::min(oldest_slot, slot);
            total_alive_bytes += store.alive_bytes();
            total_bytes += store.capacity();
        }
        info!(
            "total_stores: {total_count}, newest_slot: {newest_slot}, oldest_slot: {oldest_slot}"
        );

        let total_alive_ratio = if total_bytes > 0 {
            total_alive_bytes as f64 / total_bytes as f64
        } else {
            0.
        };

        datapoint_info!(
            "accounts_db-stores",
            ("total_count", total_count, i64),
            ("total_bytes", total_bytes, i64),
            ("total_alive_bytes", total_alive_bytes, i64),
            ("total_alive_ratio", total_alive_ratio, f64),
        );
    }

    fn handle_reclaims<'a, I>(
        &'a self,
        reclaims: I,
        expected_single_dead_slot: Option<Slot>,
        purge_stats: &PurgeStats,
        mark_accounts_obsolete: MarkAccountsObsolete,
    ) -> ReclaimResult
    where
        I: Iterator<Item = &'a (Slot, AccountInfo)>,
    {
        let mut reclaim_result = ReclaimResult::default();
        let (dead_slots, reclaimed_offsets) =
            self.remove_dead_accounts(reclaims, expected_single_dead_slot, mark_accounts_obsolete);
        reclaim_result.1 = reclaimed_offsets;
        if let Some(expected_single_dead_slot) = expected_single_dead_slot {
            assert!(dead_slots.len() <= 1);
            if dead_slots.len() == 1 {
                assert!(dead_slots.contains(&expected_single_dead_slot));
            }
        }
        let clean_stored_dead_slots =
            !matches!(mark_accounts_obsolete, MarkAccountsObsolete::Yes(_));
        self.process_dead_slots(
            &dead_slots,
            Some(&mut reclaim_result.0),
            purge_stats,
            clean_stored_dead_slots,
        );
        reclaim_result
    }

    // Must be kept private!, does sensitive cleanup that should only be called from
    // supported pipelines in AppendVecBackend
    fn process_dead_slots(
        &self,
        dead_slots: &IntSet<Slot>,
        purged_account_slots: Option<&mut AccountSlots>,
        purge_stats: &PurgeStats,
        clean_stored_dead_slots: bool,
    ) {
        if dead_slots.is_empty() {
            return;
        }
        let mut clean_dead_slots = Measure::start("reclaims::clean_dead_slots");
        if clean_stored_dead_slots {
            self.clean_stored_dead_slots(dead_slots, purged_account_slots);
        }
        self.remove_dead_slots_metadata(dead_slots.iter());
        clean_dead_slots.stop();
        let mut purge_removed_slots = Measure::start("reclaims::purge_removed_slots");
        self.purge_dead_slots_from_storage(dead_slots.iter(), purge_stats);
        purge_removed_slots.stop();
        {
            let mut list = self.shrink_candidate_slots.lock().unwrap();
            for slot in dead_slots {
                list.remove(slot);
            }
        }
        debug!(
            "process_dead_slots({}): {} {} {:?}",
            dead_slots.len(),
            clean_dead_slots,
            purge_removed_slots,
            dead_slots,
        );
    }

    // This impl iterates over all the index bins in parallel, and computes the lt hash
    // sequentially per bin.  Then afterwards reduces to a single lt hash.
    // This implementation is quite fast.  Runtime is about 150 seconds on mnb as of 10/2/2024.
    // The sequential implementation took about 6,275 seconds!
    // A different parallel implementation that iterated over the bins *sequentially* and then
    // hashed the accounts *within* a bin in parallel took about 600 seconds.  That impl uses
    // less memory, as only a single index bin is loaded into mem at a time.
    pub fn calculate_accounts_lt_hash_at_startup_from_index(
        &self,
        ancestors: &Ancestors,
    ) -> AccountsLtHash {
        let lt_hash = self
            .accounts_index
            .par_iter_bins()
            .fold(
                LtHash::identity,
                |mut accumulator_lt_hash, accounts_index_bin| {
                    for pubkey in accounts_index_bin.keys() {
                        let account_lt_hash = self
                            .accounts_index
                            .get(&pubkey)
                            .and_then(|(slot, account_info)| {
                                let valid = ancestors.min_slot().is_none_or(|min| slot < min);
                                let valid = valid || ancestors.contains_key(&slot);
                                valid.then(|| {
                                    (!account_info.is_zero_lamport()).then(|| {
                                        self.get_account_accessor(
                                            slot,
                                            &pubkey,
                                            &account_info.storage_location(),
                                        )
                                        .get_loaded_account(|loaded_account| {
                                            Self::lt_hash_account(&loaded_account, &pubkey)
                                        })
                                        // SAFETY: The index said this pubkey exists, so
                                        // there must be an account to load.
                                        .unwrap()
                                    })
                                })
                            })
                            .flatten();
                        if let Some(account_lt_hash) = account_lt_hash {
                            accumulator_lt_hash.mix_in(&account_lt_hash.0);
                        }
                    }
                    accumulator_lt_hash
                },
            )
            .reduce(LtHash::identity, |mut accum, elem| {
                accum.mix_in(&elem);
                accum
            });

        AccountsLtHash(lt_hash)
    }

    pub fn get_storages(
        &self,
        requested_slots: impl RangeBounds<Slot> + Sync,
    ) -> (Vec<Arc<AccountStorageEntry>>, Vec<Slot>) {
        let start = Instant::now();
        let (slots, storages) = self
            .storage
            .get_if(|slot, storage| requested_slots.contains(slot) && storage.has_accounts())
            .into_vec()
            .into_iter()
            .unzip();
        let duration = start.elapsed();
        debug!("get_snapshot_storages: {duration:?}");
        (storages, slots)
    }

    /// Iterate over all storage entries for archiving into a snapshot tarball.
    ///
    /// Only entries with `slot > base_slot` (if provided) are yielded — incremental
    /// snapshots use a non-None `base_slot`. For each entry the callback receives the
    /// canonical `"<slot>.<id>"` filename, a reader that skips obsolete bytes, and the
    /// reader length. Entries are ordered small-to-large (4:1 interleave ratio) to
    /// balance large and small file writes during archiving.
    /// Emits the entries that make up a snapshot archive for `snapshot_slot`.
    ///
    /// Each call to `callback` represents one entry in the snapshot archive,
    /// identified by `archive_path` (the path the entry should occupy in the
    /// archive), a `Read` for the entry's bytes, and the entry's length.
    ///
    /// If `base_slot` is `Some`, this is an incremental snapshot and only
    /// entries reflecting state past `base_slot` are emitted.
    pub fn write_snapshot_archive_entries(
        &self,
        base_slot: Option<Slot>,
        snapshot_slot: Slot,
        mut callback: impl FnMut(&str, &mut dyn io::Read, u64) -> io::Result<()>,
    ) -> io::Result<()> {
        // Balance large and small files in the archive with a bias towards
        // small (4 small + 1 large), so that during unpacking large writes
        // are mixed with file metadata operations.
        const INTERLEAVE_SMALL_TO_LARGE_RATIO: (usize, usize) = (4, 1);

        let (storages, _slots) = self.get_storages(0..=snapshot_slot);
        let storages: Vec<_> = if let Some(base) = base_slot {
            storages.into_iter().filter(|s| s.slot() > base).collect()
        } else {
            storages
        };
        let orderer = AccountStoragesOrderer::with_small_to_large_ratio(
            &storages,
            INTERLEAVE_SMALL_TO_LARGE_RATIO,
        );
        for storage in orderer.iter() {
            let filename =
                crate::accounts_file::AccountsFile::file_name(storage.slot(), storage.id());
            let mut reader = AccountStorageReader::new(storage, Some(snapshot_slot))?;
            let len = reader.len() as u64;
            callback(&filename, &mut reader, len)?;
        }
        Ok(())
    }

    /// Flush, hard-link, and write the obsolete-accounts sidecar for a fastboot snapshot.
    ///
    /// This is the entire "save accounts to snapshot directory" operation:
    /// 1. Flush every storage file to disk.
    /// 2. Hard-link each file into `snapshot_dir/accounts_hardlinks/<account_path_N>/<slot>/`.
    /// 3. Serialize the obsolete-accounts map to `snapshot_dir/obsolete_accounts`.
    pub fn save_to_snapshot_dir(&self, snapshot_dir: &Path, slot: Slot) -> io::Result<()> {
        let (storages, _slots) = self.get_storages(0..=slot);

        // Flush all storages.
        for storage in &storages {
            storage
                .flush()
                .map_err(|e| io::Error::other(e.to_string()))?;
        }

        // Hard-link storages into snapshot_dir/accounts_hardlinks/.
        let hardlinks_dir = snapshot_dir.join(SNAPSHOT_ACCOUNTS_HARDLINKS);
        std::fs::create_dir_all(&hardlinks_dir)?;
        let mut seen_account_paths: HashSet<PathBuf> = HashSet::new();
        for storage in &storages {
            let storage_path = storage.accounts.path();
            let snapshot_hardlink_dir = Self::get_or_create_snapshot_hardlink_dir(
                storage_path,
                slot,
                &mut seen_account_paths,
                &hardlinks_dir,
            )?;
            let hardlink_filename =
                crate::accounts_file::AccountsFile::file_name(storage.slot(), storage.id());
            std::fs::hard_link(storage_path, snapshot_hardlink_dir.join(hardlink_filename))?;
        }

        // Serialize obsolete accounts sidecar.
        let obsolete_accounts_path = snapshot_dir.join(SNAPSHOT_OBSOLETE_ACCOUNTS_FILENAME);
        let serde_map = SerdeObsoleteAccountsMap::new_from_storages(&storages, slot);
        type Cfg = Configuration<true, { 32 * 1024 * 1024 * 1024 }>;
        let file = io::BufWriter::new(std::fs::File::create(&obsolete_accounts_path)?);
        wincode::config::serialize_into(WriteAdapter::new(file), &serde_map, Cfg::new())
            .map_err(|e| io::Error::other(e.to_string()))?;

        Ok(())
    }

    fn get_or_create_snapshot_hardlink_dir(
        appendvec_path: &Path,
        bank_slot: Slot,
        seen_account_paths: &mut HashSet<PathBuf>,
        hardlinks_dir: &Path,
    ) -> io::Result<PathBuf> {
        let account_path = appendvec_path
            .parent()
            .and_then(|p| p.parent())
            .ok_or_else(|| {
                io::Error::other(format!(
                    "cannot derive account path from {}",
                    appendvec_path.display()
                ))
            })?
            .to_path_buf();

        let snapshot_hardlink_dir = account_path
            .join(ACCOUNTS_SNAPSHOT_DIR)
            .join(bank_slot.to_string());

        if !seen_account_paths.contains(&account_path) {
            let idx = seen_account_paths.len();
            std::fs::create_dir_all(&snapshot_hardlink_dir)?;
            let symlink_path = hardlinks_dir.join(format!("account_path_{idx}"));
            symlink::symlink_dir(&snapshot_hardlink_dir, &symlink_path)?;
            seen_account_paths.insert(account_path);
        }

        Ok(snapshot_hardlink_dir)
    }

    pub fn latest_full_snapshot_slot(&self) -> Option<Slot> {
        self.latest_full_snapshot_slot.read()
    }

    pub fn set_latest_full_snapshot_slot(&self, slot: Slot) {
        *self.latest_full_snapshot_slot.lock_write() = Some(slot);
    }

    pub fn remove_dead_slot_storages_for_snapshot_minimizer(&self, dead_slots: &[Slot]) {
        for slot in dead_slots {
            self.storage.remove(slot, false);
        }
    }

    fn set_storage_count_and_alive_bytes(
        &self,
        stored_sizes_and_counts: StorageSizeAndCountList,
        timings: &mut GenerateIndexTimings,
    ) {
        let mut storage_size_storages_time = Measure::start("storage_size_storages");
        let stored_sizes_and_counts: IntMap<_, _> = stored_sizes_and_counts.into_iter().collect();
        for (_slot, store) in self.storage.iter() {
            let id = store.id();
            // Should be default at this point
            assert_eq!(store.alive_bytes(), 0);
            if let Some(entry) = stored_sizes_and_counts.get(&id) {
                trace!(
                    "id: {} setting count: {} cur: {}",
                    id,
                    entry.count,
                    store.count(),
                );
                {
                    let prev_count = store.count.swap(entry.count, Ordering::Release);
                    assert_eq!(prev_count, 0);
                }
                store
                    .alive_bytes
                    .store(entry.stored_size, Ordering::Release);
            } else {
                trace!("id: {id} clearing count");
                store.count.store(0, Ordering::Release);
            }
        }
        storage_size_storages_time.stop();
        timings.storage_size_storages_us = storage_size_storages_time.as_us();
    }

    pub fn calculate_capitalization_at_startup_from_index(&self, ancestors: &Ancestors) -> u64 {
        self.accounts_index
            .par_iter_bins()
            .map(|accounts_index_bin| {
                accounts_index_bin
                    .keys()
                    .into_iter()
                    .map(|pubkey| {
                        self.accounts_index
                            .get(&pubkey)
                            .and_then(|(slot, account_info)| {
                                let valid = ancestors.min_slot().is_none_or(|min| slot < min);
                                let valid = valid || ancestors.contains_key(&slot);
                                valid.then(|| {
                                    (!account_info.is_zero_lamport()).then(|| {
                                        self.get_account_accessor(
                                            slot,
                                            &pubkey,
                                            &account_info.storage_location(),
                                        )
                                        .get_loaded_account(|loaded_account| {
                                            loaded_account.lamports()
                                        })
                                        // SAFETY: The index said this pubkey exists, so
                                        // there must be an account to load.
                                        .unwrap()
                                    })
                                })
                            })
                            .flatten()
                            .unwrap_or(0)
                    })
                    .try_fold(0, u64::checked_add)
            })
            .try_reduce(|| 0, u64::checked_add)
            .expect("capitalization cannot overflow")
    }

    pub fn purge_keys_exact<C>(
        &self,
        pubkey_to_slot_set: impl IntoIterator<Item = (Pubkey, C)>,
    ) -> (
        ReclaimsSlotList<AccountInfo>,
        PubkeysRemovedFromAccountsIndex,
    )
    where
        C: for<'a> Contains<'a, Slot>,
    {
        let mut reclaims = ReclaimsSlotList::new();
        let mut dead_keys = Vec::new();

        let mut purge_exact_count = 0;
        let (_, purge_exact_us) =
            measure_us!(for (pubkey, slots_set) in pubkey_to_slot_set.into_iter() {
                purge_exact_count += 1;
                let is_empty = self
                    .accounts_index
                    .purge_exact(&pubkey, slots_set, &mut reclaims);
                if is_empty {
                    dead_keys.push(pubkey);
                }
            });

        let (pubkeys_removed_from_accounts_index, handle_dead_keys_us) =
            measure_us!(self.accounts_index.handle_dead_keys(&dead_keys));

        self.appendvec_stats
            .purge_exact_count
            .fetch_add(purge_exact_count, Ordering::Relaxed);
        self.appendvec_stats
            .handle_dead_keys_us
            .fetch_add(handle_dead_keys_us, Ordering::Relaxed);
        self.appendvec_stats
            .purge_exact_us
            .fetch_add(purge_exact_us, Ordering::Relaxed);
        (reclaims, pubkeys_removed_from_accounts_index)
    }

    /// Load an account from the backend — index + read cache + storage.
    /// The write cache is handled by the caller before this is called, so if the index
    /// returns `Cached` that is a bug; we assert against it.
    pub(super) fn get(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        load_hint: LoadHint,
        populate_read_cache: PopulateReadCache,
    ) -> Option<(AccountSharedData, Slot)> {
        let (slot, storage_location, _) =
            self.read_index_for_accessor_or_load_slow(ancestors, pubkey, false)?;

        debug_assert!(
            !storage_location.is_cached(),
            "index returned Cached for {pubkey} after write-cache miss"
        );

        if let Some(account) = self.read_only_accounts_cache.load(*pubkey, slot) {
            return Some((account, slot));
        }

        let (mut accessor, slot) = self.retry_to_get_account_accessor(
            slot,
            storage_location,
            ancestors,
            pubkey,
            load_hint,
        )?;

        let account = accessor.check_and_get_loaded_account_shared_data();

        if populate_read_cache == PopulateReadCache::True {
            self.read_only_accounts_cache
                .store(*pubkey, slot, account.clone());
        }

        Some((account, slot))
    }

    fn retry_to_get_account_accessor<'a>(
        &'a self,
        mut slot: Slot,
        mut storage_location: StorageLocation,
        ancestors: &'a Ancestors,
        pubkey: &'a Pubkey,
        load_hint: LoadHint,
    ) -> Option<(LoadedAccountAccessor<'a>, Slot)> {
        // Happy drawing time! :)
        //
        // Reader                               | Accessed data source for cached/stored
        // -------------------------------------+----------------------------------
        // R1 read_index_for_accessor_or_load_slow()| cached/stored: index
        //          |                           |
        //        <(store_id, offset, ..)>      |
        //          V                           |
        // R2 retry_to_get_account_accessor()/  | cached: map of caches & entry for (slot, pubkey)
        //        get_account_accessor()        | stored: map of stores
        //          |                           |
        //        <Accessor>                    |
        //          V                           |
        // R3 check_and_get_loaded_account()/   | cached: N/A (note: basically noop unwrap)
        //        get_loaded_account()          | stored: store's entry for slot
        //          |                           |
        //        <LoadedAccount>               |
        //          V                           |
        // R4 take_account()                    | cached/stored: entry of cache/storage for (slot, pubkey)
        //          |                           |
        //        <AccountSharedData>           |
        //          V                           |
        //    Account!!                         V
        //
        // Flusher                              | Accessed data source for cached/stored
        // -------------------------------------+----------------------------------
        // F1 flush_slot_cache()                | N/A
        //          |                           |
        //          V                           |
        // F2 store_accounts_frozen()/          | map of stores (creates new entry)
        //        write_accounts_to_storage()   |
        //          |                           |
        //          V                           |
        // F3 store_accounts_frozen()/          | index
        //        update_index_stored_accounts()| (replaces existing store_id, offset in caches)
        //          |                           |
        //          V                           |
        // F4 accounts_cache.remove_slot()      | map of caches (removes old entry)
        //                                      V
        //
        // Remarks for flusher: So, for any reading operations, it's a race condition where F4 happens
        // between R1 and R2. In that case, retrying from R1 is safu because F3 should have
        // been occurred.
        //
        // Shrinker                             | Accessed data source for stored
        // -------------------------------------+----------------------------------
        // S1 do_shrink_slot_store()            | N/A
        //          |                           |
        //          V                           |
        // S2 store_accounts_frozen()/          | map of stores (creates new entry)
        //        write_accounts_to_storage()   |
        //          |                           |
        //          V                           |
        // S3 store_accounts_frozen()/          | index
        //        update_index_stored_accounts()| (replaces existing store_id, offset in stores)
        //          |                           |
        //          V                           |
        // S4 do_shrink_slot_store()/           | map of stores (removes old entry)
        //        dead_storages
        //
        // Remarks for shrinker: So, for any reading operations, it's a race condition
        // where S4 happens between R1 and R2. In that case, retrying from R1 is safu because S3 should have
        // been occurred, and S3 atomically replaced the index accordingly.
        //
        // Cleaner                              | Accessed data source for stored
        // -------------------------------------+----------------------------------
        // C1 clean_accounts()                  | N/A
        //          |                           |
        //          V                           |
        // C2 clean_accounts()/                 | index
        //        purge_keys_exact()            | (removes existing store_id, offset for stores)
        //          |                           |
        //          V                           |
        // C3 clean_accounts()/                 | map of stores (removes old entry)
        //        handle_reclaims()             |
        //
        // Remarks for cleaner: So, for any reading operations, it's a race condition
        // where C3 happens between R1 and R2. In that case, retrying from R1 is safu.
        // In that case, None would be returned while bailing out at R1.
        //
        // Purger                                 | Accessed data source for cached/stored
        // ---------------------------------------+----------------------------------
        // P1 purge_slot()                        | N/A
        //          |                             |
        //          V                             |
        // P2 purge_slots_from_cache_and_store()  | map of caches/stores (removes old entry)
        //          |                             |
        //          V                             |
        // P3 purge_slots_from_cache_and_store()/ | index
        //       purge_slot_cache()/              |
        //          purge_slot_cache_pubkeys()    | (removes existing store_id, offset for cache)
        //       purge_slot_storage()/            |
        //          purge_keys_exact()            | (removes accounts index entries)
        //          handle_reclaims()             | (removes storage entries)
        //      OR                                |
        //    clean_accounts()/                   |
        //        purge_keys_exact()              | (removes existing store_id, offset for stores)
        //                                        V
        //
        // Remarks for purger: So, for any reading operations, it's a race condition
        // where P2 happens between R1 and R2. In that case, retrying from R1 is safu.
        // In that case, we may bail at index read retry when P3 hasn't been run
        //
        // Remover                                | Accessed data source for cached
        // ---------------------------------------+----------------------------------
        // M1 purge_slots_from_cache_and_store()  | index
        //        (via remove_unrooted_slots())   | (removes old entries for slot)
        //          |                             |
        //          V                             |
        // M2 purge_slots_from_cache_and_store()  | map of caches
        //        (via remove_unrooted_slots())   | (removes old slot cache)
        //          |                             |
        //          V                             |
        // M3 store_accounts_cached()             | map of caches (creates new Cached entry)
        //          |                             |
        //          V                             |
        // M4 update index                        | index (writes fresh Cached entry)
        //                                        V
        //
        // Remarks for remover: This scenario arises when remove_unrooted_slots()
        // purges a cached slot (e.g. duplicate-block detection abandoning a fork)
        // and the same slot is subsequently re-stored (e.g. re-processed by banking
        // stage on a fresh fork).
        //
        // M1 removes the index entries before M2 removes the cache (see
        // purge_slots_from_cache_and_store). M3 writes the fresh cache entry
        // before M4 writes the fresh index entry, so any reader who observes M4's
        // index entry is guaranteed to find M3's cache entry too.
        //
        // The observable race is M2 happening between R1 and R2, with M3+M4
        // completing before the subsequent index re-read:
        //   R1  → (slot, Cached)   [old index entry, before M1]
        //   M1/M2 remove old index and cache entries
        //   M3/M4 write fresh cache and index entries
        //   get_account_accessor() → Cached(None)  [old entry removed by M2]
        //   re-read index         → (slot, Cached) [fresh M4 entry, same store_id]
        // In that case, retrying from R1 is safe: the next get_account_accessor()
        // on the fresh (slot, Cached) entry is guaranteed to return Cached(Some(_)).

        #[cfg(test)]
        {
            // Give some time for cache flushing to occur here for unit tests
            thread::sleep(Duration::from_millis(self.load_delay));
        }

        // Failsafe for potential race conditions with other subsystems
        let mut num_acceptable_failed_iterations = 0;
        loop {
            let account_accessor = self.get_account_accessor(slot, pubkey, &storage_location);
            match account_accessor {
                LoadedAccountAccessor::Cached(Some(_)) | LoadedAccountAccessor::Stored(Some(_)) => {
                    // Great! There was no race, just return :) This is the most usual situation
                    return Some((account_accessor, slot));
                }
                LoadedAccountAccessor::Cached(None) => {
                    num_acceptable_failed_iterations += 1;
                    // Cache was flushed in between checking the index and retrieving from the cache,
                    // so retry. This works because in accounts cache flush, an account is written to
                    // storage *before* it is removed from the cache
                    match load_hint {
                        LoadHint::FixedMaxRoot => {
                            // Under FixedMaxRoot, Cached(None) can occur at most once per load.
                            // There are two ways the initial cache miss can happen:
                            //
                            // 1) The write-cache entry for the located slot was being concurrently
                            //    flushed to storage.  After re-reading the index the entry will
                            //    have moved to Stored, so the next get_account_accessor() call
                            //    succeeds via Stored(Some(_)).  With a fixed root the index
                            //    references a single ancestor slot for a given account; that
                            //    slot is flushed exactly once, so this race cannot repeat.
                            //
                            // 2) A parent slot was removed by remove_unrooted_slots() and
                            //    concurrently re-stored (the M1-M4 "Remover" sequence above).
                            //    We read the stale index entry (R1, before M1 ran), found the
                            //    old cache entry already removed (M2), and got Cached(None).
                            //    After re-reading the index we see the fresh M4 entry, and the
                            //    next get_account_accessor() call is guaranteed to find M3's
                            //    cache entry.  For a second Cached(None) to occur here the
                            //    same parent slot would have to complete the full
                            //    remove-replay-confirm-duplicate cycle a second time between
                            //    consecutive loop iterations — a far stricter requirement than
                            //    the first miss, which only needed a single remove + re-store.
                            assert!(num_acceptable_failed_iterations <= 1);
                        }
                        LoadHint::Unspecified => {
                            // Because newer root can be added to the index (= not fixed),
                            // multiple flush race conditions can be observed under very rare
                            // condition, at least theoretically
                        }
                    }
                }
                LoadedAccountAccessor::Stored(None) => {
                    match load_hint {
                        LoadHint::FixedMaxRoot => {
                            // When running replay on the validator, or banking stage on the leader,
                            // it should be very rare that the storage entry doesn't exist if the
                            // entry in the accounts index is the latest version of this account.
                            //
                            // There are only a few places where the storage entry may not exist
                            // after reading the index:
                            // 1) Shrink has removed the old storage entry and rewritten to
                            // a newer storage entry
                            // 2) The `pubkey` asked for in this function is a zero-lamport account,
                            // and the storage entry holding this account qualified for zero-lamport clean.
                            //
                            // In both these cases, it should be safe to retry and recheck the accounts
                            // index indefinitely, without incrementing num_acceptable_failed_iterations.
                            // That's because if the root is fixed, there should be a bounded number
                            // of pending cleans/shrinks (depends how far behind the AccountsBackgroundService
                            // is), termination to the desired condition is guaranteed.
                            //
                            // Also note that in both cases, if we do find the storage entry,
                            // we can guarantee that the storage entry is safe to read from because
                            // we grabbed a reference to the storage entry while it was still in the
                            // storage map. This means even if the storage entry is removed from the storage
                            // map after we grabbed the storage entry, the recycler should not reset the
                            // storage entry until we drop the reference to the storage entry.
                            //
                            // eh, no code in this arm? yes!
                        }
                        LoadHint::Unspecified => {
                            // RPC get_account() may have fetched an old root from the index that was
                            // either:
                            // 1) Cleaned up by clean_accounts(), so the accounts index has been updated
                            // and the storage entries have been removed.
                            // 2) Dropped by purge_slots() because the slot was on a minor fork, which
                            // removes the slots' storage entries but doesn't purge from the accounts index
                            // (account index cleanup is left to clean for stored slots). Note that
                            // this generally is impossible to occur in the wild because the RPC
                            // should hold the slot's bank, preventing it from being purged() to
                            // begin with.
                            num_acceptable_failed_iterations += 1;
                        }
                    }
                }
            }
            #[cfg(not(test))]
            let load_limit = ABSURD_CONSECUTIVE_FAILED_ITERATIONS;

            #[cfg(test)]
            let load_limit = self.load_limit.load(Ordering::Relaxed);

            let fallback_to_slow_path = if num_acceptable_failed_iterations >= load_limit {
                // The latest version of the account existed in the index, but could not be
                // fetched from storage. This means a race occurred between this function and clean
                // accounts/purge_slots
                let message = format!(
                    "do_load() failed to get key: {pubkey} from storage, latest attempt was for \
                     slot: {slot}, storage_location: {storage_location:?}, load_hint: \
                     {load_hint:?}",
                );
                datapoint_warn!("accounts_db-do_load_warn", ("warn", message, String));
                true
            } else {
                false
            };

            // Because reading from the cache/storage failed, retry from the index read
            let (new_slot, new_storage_location, maybe_account_accessor) = self
                .read_index_for_accessor_or_load_slow(ancestors, pubkey, fallback_to_slow_path)?;
            // Notice the subtle `?` at previous line, we bail out pretty early if missing.

            if new_slot == slot && new_storage_location.is_store_id_equal(&storage_location) {
                if !new_storage_location.is_cached() {
                    {
                        let entry = self.accounts_index.get(pubkey);
                        let message = format!(
                            "Bad index entry detected ({pubkey}, {slot}, {storage_location:?}, \
                             {load_hint:?}, {new_storage_location:?}, {entry:?})"
                        );
                        // Considering that we've failed to get accessor above and further that
                        // the index still returned the same (slot, store_id) tuple, offset must be same
                        // too.
                        assert!(
                            new_storage_location.is_offset_equal(&storage_location),
                            "{message}"
                        );

                        // If this is not a cache entry, then this was a minor fork slot
                        // that had its storage entries cleaned up by purge_slots() but hasn't been
                        // cleaned yet. That means this must be rpc access and not replay/banking at the
                        // very least. Note that purge shouldn't occur even for RPC as caller must hold all
                        // of ancestor slots..
                        assert_eq!(load_hint, LoadHint::Unspecified, "{message}");

                        // Everything being assert!()-ed, let's panic!() here as it's an error condition
                        // after all....
                        // That reasoning is based on the fact all of code-path reaching this fn
                        // retry_to_get_account_accessor() must outlive the Arc<Bank> (and its all
                        // ancestors) over this fn invocation, guaranteeing the prevention of being purged,
                        // first of all.
                        // For details, see the comment in ScanGuard::should_use_ancestors(),
                        // which is referring back here.
                        panic!("{message}");
                    }
                } else {
                    // For the Cached variant: remove_unrooted_slots() removes the index entry and
                    // the cache entry, then a subsequent re-store writes a fresh Cached entry for
                    // the same slot.  This produces the observable sequence:
                    //   get_account_accessor()              -> Cached(None)   [old entry gone]
                    //   read_index_for_accessor_or_load_slow() -> (slot, Cached) [new entry]
                    // That is not an index corruption -- the next get_account_accessor() call on
                    // the fresh (slot, Cached) will succeed.  Fall through to retry.
                    //
                    // Also no code in this arm!
                }
            } else if fallback_to_slow_path {
                // the above bad-index-entry check must had been checked first to retain the same
                // behavior
                return Some((
                    maybe_account_accessor.expect("must be some if clone_in_lock=true"),
                    new_slot,
                ));
            }

            slot = new_slot;
            storage_location = new_storage_location;
        }
    }

    fn hash_account_helper(account: &impl ReadableAccount, pubkey: &Pubkey) -> blake3::Hasher {
        let mut hasher = blake3::Hasher::new();

        const META_SIZE: usize = 8 /* lamports */ + 1 /* executable */ + 32 /* owner */ + 32 /* pubkey */;
        const DATA_SIZE: usize = 200;
        const BUFFER_SIZE: usize = META_SIZE + DATA_SIZE;
        let mut buffer = SmallVec::<[u8; BUFFER_SIZE]>::new();

        buffer.extend_from_slice(&account.lamports().to_le_bytes());

        let data = account.data();
        if data.len() > DATA_SIZE {
            hasher.update(&buffer);
            buffer.clear();
            hasher.update(data);
        } else {
            buffer.extend_from_slice(data);
        }

        buffer.push(account.executable().into());
        buffer.extend_from_slice(account.owner().as_ref());
        buffer.extend_from_slice(pubkey.as_ref());
        hasher.update(&buffer);

        hasher
    }

    /// Calculates the `AccountLtHash` of `account`
    pub fn lt_hash_account(account: &impl ReadableAccount, pubkey: &Pubkey) -> AccountLtHash {
        if account.lamports() == 0 {
            return ZERO_LAMPORT_ACCOUNT_LT_HASH;
        }
        let hasher = Self::hash_account_helper(account, pubkey);
        let lt_hash = LtHash::with(&hasher);
        AccountLtHash(lt_hash)
    }

    pub fn generate_index(
        &self,
        limit_load_slot_count_from_snapshot: Option<usize>,
        verify: bool,
    ) -> IndexGenerationInfo {
        let mut total_time = Measure::start("generate_index");
        let mut storages = self.storage.all_storages();
        storages.sort_unstable_by_key(|storage| storage.slot);
        if let Some(limit) = limit_load_slot_count_from_snapshot {
            storages.truncate(limit);
        }
        let storages = storages.as_slice();
        let num_storages = storages.len();

        // Start from a clean per-storage accounting baseline. In production
        // these counters are 0 (fresh from snapshot files) so this is a no-op.
        // It only matters for tests that populate storages via the write-cache
        // flush path — that path increments the counters, and we'd otherwise
        // double-count when `set_storage_count_and_alive_bytes` runs below.
        for storage in storages {
            storage.alive_bytes.store(0, Ordering::Release);
            storage.count.store(0, Ordering::Release);
        }

        let mut total_accum = IndexGenerationAccumulator::with_slots_capacity(num_storages);
        let storages_orderer =
            AccountStoragesOrderer::with_random_order(storages).into_concurrent_consumer();
        let exit_logger = AtomicBool::new(false);
        let num_processed = AtomicU64::new(0);
        let num_threads = num_cpus::get();
        let mut index_time = Measure::start("index");
        thread::scope(|s| {
            let thread_handles = (0..num_threads)
                .map(|i| {
                    thread::Builder::new()
                        .name(format!("solGenIndex{i:02}"))
                        .spawn_scoped(s, || {
                            let mut thread_accum = IndexGenerationAccumulator::with_slots_capacity(
                                num_storages.div_ceil(num_threads),
                            );
                            let mut reader = append_vec::new_scan_accounts_reader();
                            for next_item in storages_orderer.iter() {
                                let storage = next_item.storage;
                                self.generate_index_for_slot(
                                    &mut reader,
                                    &mut thread_accum,
                                    next_item.original_index,
                                    storage,
                                );
                                num_processed.fetch_add(1, Ordering::Relaxed);
                            }
                            thread_accum
                        })
                })
                .collect::<Result<Vec<_>, _>>()
                .expect("spawn threads");
            let logger_thread_handle = thread::Builder::new()
                .name("solGenIndexLog".to_string())
                .spawn_scoped(s, || {
                    let mut last_update = Instant::now();
                    loop {
                        if exit_logger.load(Ordering::Relaxed) {
                            break;
                        }
                        let num_processed = num_processed.load(Ordering::Relaxed);
                        if num_processed == num_storages as u64 {
                            info!("generating index: processed all slots");
                            break;
                        }
                        let now = Instant::now();
                        if now - last_update > Duration::from_secs(2) {
                            info!(
                                "generating index: processed {num_processed}/{num_storages} \
                                 slots..."
                            );
                            last_update = now;
                        }
                        thread::sleep(Duration::from_millis(500))
                    }
                })
                .expect("spawn thread");
            for thread_handle in thread_handles {
                let Ok(thread_accum) = thread_handle.join() else {
                    exit_logger.store(true, Ordering::Relaxed);
                    panic!("index generation failed");
                };
                total_accum.accumulate(thread_accum);
            }
            // Make sure to join the logger thread *after* the main threads.
            // This way, if a main thread errors, we won't spin indefinitely
            // waiting for the logger thread to finish (it never will).
            logger_thread_handle.join().expect("join thread");
        });
        index_time.stop();

        {
            let index_stats = self.accounts_index.stats();
            index_stats.inc_insert_count(total_accum.num_accounts);
            index_stats.add_mem_count(total_accum.num_accounts as usize);
        }

        if verify {
            info!("Verifying index...");
            let start = Instant::now();
            storages.par_iter().for_each(|storage| {
                let store_id = storage.id();
                let slot = storage.slot();
                storage
                    .accounts
                    .scan_accounts_without_data(|offset, account| {
                        let key = account.pubkey();
                        if let Some((slot2, account_info2)) = self.accounts_index.get(key) {
                            if slot2 == slot {
                                let ai = AccountInfo::new(
                                    StorageLocation::AppendVec(store_id, offset),
                                    account.is_zero_lamport(),
                                );
                                assert_eq!(&ai, &account_info2);
                            }
                        }
                    })
                    .expect("must scan accounts storage");
            });
            info!("Verifying index... Done in {:?}", start.elapsed());
        }

        let mut timings = GenerateIndexTimings {
            index_time: index_time.as_us(),
            insertion_time_us: total_accum.insert_time_us,
            total_including_duplicates: total_accum.num_accounts,
            total_slots: num_storages as u64,
            all_accounts_are_zero_lamports_slots: total_accum.all_accounts_are_zero_lamports_slots,
            num_obsolete_accounts_skipped: total_accum.num_obsolete_accounts_skipped,
            ..GenerateIndexTimings::default()
        };

        let mut visit_duplicate_accounts_timer = Measure::start("visit displaced accounts");
        let displaced_reclaims = total_accum.displaced_reclaims;
        let (
            accounts_data_len_from_displaced,
            num_displaced_accounts,
            displaced_lt_hash,
            capitalization_from_displaced,
        ) = displaced_reclaims
            .par_iter()
            .fold(
                || (0u64, 0u64, LtHash::identity(), 0u128),
                |mut acc, (old_slot, old_info, pubkey)| {
                    let maybe_storage = self
                        .storage
                        .get_account_storage_entry(*old_slot, old_info.store_id());
                    let mut accessor = LoadedAccountAccessor::Stored(
                        maybe_storage.map(|entry| (entry, old_info.offset())),
                    );
                    accessor.check_and_get_loaded_account(|loaded_account| {
                        let lamports = loaded_account.lamports();
                        let data_len = loaded_account.data_len() as u64;
                        if lamports > 0 {
                            acc.0 += data_len;
                        }
                        acc.1 += 1;
                        let account_lt_hash = Self::lt_hash_account(&loaded_account, pubkey);
                        acc.2.mix_in(&account_lt_hash.0);
                        acc.3 = acc
                            .3
                            .checked_add(u128::from(lamports))
                            .expect("capitalization cannot overflow");
                    });
                    acc
                },
            )
            .reduce(
                || (0u64, 0u64, LtHash::identity(), 0u128),
                |mut a, elem| {
                    a.0 += elem.0;
                    a.1 += elem.1;
                    a.2.mix_in(&elem.2);
                    a.3 =
                        a.3.checked_add(elem.3)
                            .expect("capitalization cannot overflow");
                    a
                },
            );
        visit_duplicate_accounts_timer.stop();
        timings.visit_duplicate_accounts_time_us = visit_duplicate_accounts_timer.as_us();
        timings.num_duplicate_accounts = num_displaced_accounts;

        total_accum.lt_hash.mix_out(&displaced_lt_hash);
        total_accum.capitalization = total_accum
            .capitalization
            .checked_sub(capitalization_from_displaced)
            .expect("capitalization cannot underflow");
        total_accum.accounts_data_len -= accounts_data_len_from_displaced;
        info!("accounts data len: {}", total_accum.accounts_data_len);

        info!(
            "insert all zero slots to clean at startup {}",
            total_accum.slots_with_only_zero_lamport_accounts.len()
        );
        for (slot, storage_index) in total_accum.slots_with_only_zero_lamport_accounts {
            self.dirty_stores
                .insert(slot, storages[storage_index].clone());
        }

        for storage in storages {
            self.storage_roots.write().unwrap().add_root(storage.slot());
        }

        self.set_storage_count_and_alive_bytes(total_accum.storage_info, &mut timings);

        let mut mark_obsolete_accounts_time = Measure::start("mark_obsolete_accounts_time");
        let slot_marked_obsolete = storages.last().unwrap().slot();
        let displaced_for_reclaims: Vec<(Slot, AccountInfo)> = displaced_reclaims
            .iter()
            .map(|(s, i, _)| (*s, *i))
            .collect();
        let purge_stats = PurgeStats::default();
        self.handle_reclaims(
            displaced_for_reclaims.iter(),
            None,
            &purge_stats,
            MarkAccountsObsolete::Yes(slot_marked_obsolete),
        );
        mark_obsolete_accounts_time.stop();
        timings.mark_obsolete_accounts_us = mark_obsolete_accounts_time.as_us();
        timings.num_obsolete_accounts_marked = displaced_for_reclaims.len() as u64;
        timings.num_slots_removed_as_obsolete = purge_stats
            .total_removed_storage_entries
            .load(Ordering::Relaxed) as u64;
        total_time.stop();
        timings.total_time_us = total_time.as_us();
        timings.report();

        let (index_len, index_capacity) = self
            .accounts_index
            .iter_bins()
            .map(|bin| bin.len_and_cap_for_startup())
            .fold((0, 0), |mut accum, (len, cap)| {
                accum.0 += len;
                accum.1 += cap;
                accum
            });
        self.accounts_index
            .stats()
            .count_in_mem
            .store(index_len, Ordering::Relaxed);
        self.accounts_index
            .stats()
            .capacity_in_mem
            .store(index_capacity, Ordering::Relaxed);

        let Ok(calculated_capitalization) = u64::try_from(total_accum.capitalization) else {
            panic!(
                "calculated capitalization overflowed a u64, which is invalid! calculated \
                 capitalization: {}",
                total_accum.capitalization,
            );
        };
        IndexGenerationInfo {
            accounts_data_len: total_accum.accounts_data_len,
            calculated_accounts_lt_hash: AccountsLtHash(total_accum.lt_hash),
            calculated_capitalization,
        }
    }

    pub(crate) fn generate_index_for_slot<'a>(
        &self,
        reader: &mut impl RequiredLenBufFileRead<'a>,
        accum: &mut IndexGenerationAccumulator,
        storage_index: usize,
        storage: &'a AccountStorageEntry,
    ) {
        let slot = storage.slot();
        let store_id = storage.id();

        let mut capitalization = 0_u64;
        let mut accounts_data_len = 0;
        let mut stored_size_alive = 0;
        let mut winner_count = 0usize;
        let mut all_accounts_are_zero_lamports = true;
        accum.slot_arena.ensure_empty();
        let zero_lamport_offsets = &mut accum.slot_arena.zero_lamport_offsets;
        let displaced_reclaims = &mut accum.displaced_reclaims;

        let obsolete_accounts: IntSet<_> = storage
            .obsolete_accounts_read_lock()
            .filter_obsolete_accounts(None)
            .map(|(offset, _)| offset)
            .collect();
        let mut num_obsolete_accounts_skipped = 0;

        let insert_time_us;
        ((), insert_time_us) = measure_us!({
            storage
                .accounts
                .scan_accounts(reader, |offset, account| {
                    if obsolete_accounts.contains(&offset) {
                        num_obsolete_accounts_skipped += 1;
                        return;
                    }

                    let pubkey = *account.pubkey;
                    let is_account_zero_lamport = account.is_zero_lamport();
                    let account_info = AccountInfo::new(
                        StorageLocation::AppendVec(store_id, offset),
                        is_account_zero_lamport,
                    );

                    let result = self
                        .accounts_index
                        .insert_if_newer(slot, &pubkey, account_info);

                    match result {
                        InsertIfNewerResult::Inserted | InsertIfNewerResult::Replaced { .. } => {
                            let data_len = account.data.len();
                            stored_size_alive += storage.accounts.calculate_stored_size(data_len);
                            winner_count += 1;
                            if !is_account_zero_lamport {
                                accounts_data_len += data_len as u64;
                                all_accounts_are_zero_lamports = false;
                            } else {
                                zero_lamport_offsets.push(offset);
                            }
                            let account_lt_hash = Self::lt_hash_account(&account, account.pubkey());
                            accum.lt_hash.mix_in(&account_lt_hash.0);
                            capitalization = capitalization
                                .checked_add(account.lamports())
                                .expect("capitalization cannot overflow");

                            if let InsertIfNewerResult::Replaced { old_slot, old_info } = result {
                                displaced_reclaims.push((old_slot, old_info, pubkey));
                            }
                        }
                        InsertIfNewerResult::ExistingIsNewer => {}
                    }
                })
                .expect("must scan accounts storage");
        });

        accum.capitalization = accum
            .capitalization
            .checked_add(u128::from(capitalization))
            .expect("capitalization cannot overflow");

        if winner_count > 0 {
            let info = StorageSizeAndCount {
                stored_size: stored_size_alive,
                count: winner_count,
            };
            assert!(
                info.stored_size <= u64_align!(storage.accounts.len()),
                "Stored size ({}) is larger than the size of the accounts file ({}) for store_id: \
                 {}",
                info.stored_size,
                storage.accounts.len(),
                store_id
            );
            accum.storage_info.push((store_id, info));
        }

        storage.batch_insert_zero_lamport_single_ref_account_offsets(zero_lamport_offsets);

        accum.num_accounts += winner_count as u64;
        accum.insert_time_us += insert_time_us;
        accum.accounts_data_len += accounts_data_len;
        accum.num_obsolete_accounts_skipped += num_obsolete_accounts_skipped;
        if all_accounts_are_zero_lamports {
            accum.all_accounts_are_zero_lamports_slots += 1;
            accum
                .slots_with_only_zero_lamport_accounts
                .push((slot, storage_index));
        }
    }
}

/// Specify whether obsolete accounts should be marked or not during reclaims
/// They should only be marked if they are also getting unreffed in the index
/// Temporarily allow dead code until the feature is implemented
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum MarkAccountsObsolete {
    Yes(Slot),
    No,
}

pub enum UpdateIndexThreadSelection {
    /// Use current thread only
    Inline,
    /// Use a thread-pool if the number of updates exceeds a threshold
    PoolWithThreshold,
}

#[cfg(test)]
impl AccountStorageEntry {
    fn accounts_count(&self) -> usize {
        let mut count = 0;
        self.accounts
            .scan_pubkeys(|_| {
                count += 1;
            })
            .expect("must scan accounts storage");
        count
    }
}
