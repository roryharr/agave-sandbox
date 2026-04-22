mod account_map_entry;
mod accounts_index_storage;
mod bucket_map_holder;
pub(crate) mod in_mem_accounts_index;
mod iter;
pub(crate) mod secondary;
mod stats;
use {
    crate::{
        contains::Contains,
        is_zero_lamport::IsZeroLamport,
        pubkey_bins::{PubkeyBinCalculator, PubkeyBinCalculatorBuilder},
    },
    account_map_entry::{PreAllocatedAccountMapEntry, SlotListWriteGuard},
    accounts_index_storage::AccountsIndexStorage,
    bucket_map_holder::Age,
    in_mem_accounts_index::{InMemAccountsIndex, InsertIfNewerResult},
    iter::AccountsIndexPubkeyIterator,
    log::*,
    smallvec::SmallVec,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    stats::Stats,
    std::{
        collections::HashSet,
        fmt::Debug,
        num::NonZeroUsize,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU32},
        },
    },
};
pub use {
    bucket_map_holder::{DEFAULT_NUM_ENTRIES_OVERHEAD, DEFAULT_NUM_ENTRIES_TO_EVICT},
    secondary::{
        AccountIndex, AccountSecondaryIndexes, AccountSecondaryIndexesIncludeExclude, IndexKey,
    },
};

pub const BINS_DEFAULT: usize = 8192;
pub const BINS_FOR_TESTING: usize = 2; // we want > 1, but each bin is a few disk files with a disk based index, so fewer is better
pub const BINS_FOR_BENCHMARKS: usize = 8192;
// The unsafe is safe because we're using a fixed, known non-zero value
pub const FLUSH_THREADS_TESTING: NonZeroUsize = NonZeroUsize::new(1).unwrap();
pub const ACCOUNTS_INDEX_CONFIG_FOR_TESTING: AccountsIndexConfig = AccountsIndexConfig {
    bins: Some(BINS_FOR_TESTING),
    num_flush_threads: Some(FLUSH_THREADS_TESTING),
    drives: None,
    index_limit: IndexLimit::InMemOnly,
    ages_to_stay_in_cache: None,
    num_initial_accounts: None,
    disable_eviction: false,
};
pub const ACCOUNTS_INDEX_CONFIG_FOR_BENCHMARKS: AccountsIndexConfig = AccountsIndexConfig {
    bins: Some(BINS_FOR_BENCHMARKS),
    num_flush_threads: Some(FLUSH_THREADS_TESTING),
    drives: None,
    index_limit: IndexLimit::InMemOnly,
    ages_to_stay_in_cache: None,
    num_initial_accounts: None,
    disable_eviction: false,
};
pub type SlotList<T> = SmallVec<[SlotListItem<T>; 1]>;
pub type ReclaimsSlotList<T> = Vec<SlotListItem<T>>;
pub type SlotListItem<T> = (Slot, T);

// The ref count cannot be higher than the total number of storages, and we should never have more
// than 1 million storages. A 32-bit ref count should be *significantly* more than enough.
// (We already effectively limit the number of storages to 2^32 since the storage ID type is a u32.)
// The majority of accounts should only exist in one storage, so the most common ref count is '1'.
// Heavily updated accounts should still have a ref count that is < 100.
pub type RefCount = u32;
pub type AtomicRefCount = AtomicU32;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
/// which accounts `scan` should load from disk
pub enum ScanFilter {
    /// Scan both in-memory and on-disk index
    #[default]
    All,
    /// abnormal = ref_count != 1 or slot list.len() != 1
    /// Scan only in-memory index and skip on-disk index
    OnlyAbnormal,
    /// Similar to `OnlyAbnormal but also check on-disk index to verify the
    /// entry on-disk is indeed normal.
    OnlyAbnormalWithVerify,
    /// Similar to `OnlyAbnormal but mark entries in memory as not found
    /// if they are normal
    /// This removes the possibility of any race conditions with index
    /// flushing and simulates the system running an uncached disk index
    /// where nothing 'normal' is ever held in the in memory index as far as
    /// callers are concerned. This could also be a  correct/ideal future api
    /// to similarly provide consistency and remove race condition behavior.
    OnlyAbnormalTest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// how accounts index 'upsert' should handle reclaims
pub enum UpsertReclaim {
    /// previous entry for this slot in the index is expected to be cached, so irrelevant to reclaims
    PreviousSlotEntryWasCached,
    /// previous entry for this slot in the index may need to be reclaimed, so return it.
    /// reclaims is the only output of upsert, requiring a synchronous execution
    PopulateReclaims,
    /// overwrite existing data in the same slot and do not return in 'reclaims'
    IgnoreReclaims,
    // Reclaim all older versions of the account from the index and return
    // in the 'reclaims'
    ReclaimOldSlots,
}

pub trait IsCached {
    fn is_cached(&self) -> bool;
}

pub trait IndexValue: 'static + IsCached + IsZeroLamport + DiskIndexValue {}

pub trait DiskIndexValue:
    'static + Clone + Debug + PartialEq + Copy + Default + Sync + Send
{
}

/// specification of how much memory the in-mem portion of account index can hold
#[derive(Debug, Clone)]
pub enum IndexLimit {
    /// use disk index while keeping a minimal amount in-mem
    /// deprecated in v4.1.0
    Minimal,
    /// in-mem-only was specified, no disk index
    InMemOnly,
    /// evict from in-mem when usage exceeds threshold in bytes
    Threshold(IndexLimitThreshold),
}

/// Configuration for threshold-based accounts index limit
#[derive(Debug, Clone)]
pub struct IndexLimitThreshold {
    /// The memory limit, in bytes, for the entire accounts index.
    pub num_bytes: u64,
    /// Number of entries below an in-mem index bin's usable capacity at which to begin evicting.
    pub num_entries_overhead: usize,
    /// Number of entries to evict, once we've hit the high watermark.
    pub num_entries_to_evict: usize,
}

#[derive(Debug, Clone)]
pub struct AccountsIndexConfig {
    pub bins: Option<usize>,
    pub num_flush_threads: Option<NonZeroUsize>,
    pub drives: Option<Vec<PathBuf>>,
    pub index_limit: IndexLimit,
    pub ages_to_stay_in_cache: Option<Age>,
    /// Initial number of accounts, used to pre-allocate HashMap capacity at startup.
    pub num_initial_accounts: Option<usize>,
    /// When true, the flush/eviction loop is a no-op. Used when the bin store manages
    /// its own memory (e.g. RocksDB), so in-memory eviction is unnecessary.
    pub disable_eviction: bool,
}

impl Default for AccountsIndexConfig {
    fn default() -> Self {
        Self {
            bins: None,
            num_flush_threads: None,
            drives: None,
            index_limit: IndexLimit::InMemOnly,
            ages_to_stay_in_cache: None,
            num_initial_accounts: None,
            disable_eviction: false,
        }
    }
}

pub fn default_num_flush_threads() -> NonZeroUsize {
    NonZeroUsize::new(std::cmp::max(2, num_cpus::get() / 4)).expect("non-zero system threads")
}

#[derive(Copy, Clone)]
pub enum AccountsIndexScanResult {
    /// if the entry is not in the in-memory index, do not add it unless the entry becomes dirty
    OnlyKeepInMemoryIfDirty,
    /// keep the entry in the in-memory index
    KeepInMemory,
    /// reduce refcount by 1
    Unref,
    /// reduce refcount by 1 and assert that ref_count = 0 after unref
    UnrefAssert0,
    /// reduce refcount by 1 and log if ref_count != 0 after unref
    UnrefLog0,
}

#[derive(Debug)]
/// T: account info type to interact in in-memory items
/// U: account info type to be persisted to disk
pub struct AccountsIndex<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> {
    account_maps: Box<[Arc<InMemAccountsIndex<T, U>>]>,
    bin_calculator: PubkeyBinCalculator,
    storage: AccountsIndexStorage<T, U>,
}

impl<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> AccountsIndex<T, U> {
    pub fn default_for_tests() -> Self {
        Self::new(&ACCOUNTS_INDEX_CONFIG_FOR_TESTING, Arc::default())
    }

    pub fn new(config: &AccountsIndexConfig, exit: Arc<AtomicBool>) -> Self {
        let (account_maps, bin_calculator, storage) = Self::allocate_accounts_index(config, exit);
        info!("AccountsIndex bin calculator: {bin_calculator:?}");
        Self {
            account_maps,
            bin_calculator,
            storage,
        }
    }

    #[allow(clippy::type_complexity)]
    fn allocate_accounts_index(
        config: &AccountsIndexConfig,
        exit: Arc<AtomicBool>,
    ) -> (
        Box<[Arc<InMemAccountsIndex<T, U>>]>,
        PubkeyBinCalculator,
        AccountsIndexStorage<T, U>,
    ) {
        let bins = config.bins.unwrap_or(BINS_DEFAULT);
        // create bin_calculator early to verify # bins is reasonable
        let bin_calculator = PubkeyBinCalculatorBuilder::with_bins(
            NonZeroUsize::new(bins).expect("bins is non-zero"),
        );
        let storage = AccountsIndexStorage::new(bins, config, exit);

        let account_maps: Box<_> = (0..bins)
            .map(|bin| Arc::clone(&storage.in_mem[bin]))
            .collect();
        (account_maps, bin_calculator, storage)
    }

    pub fn iter<'a>(&'a self) -> AccountsIndexPubkeyIterator<'a, T, U> {
        AccountsIndexPubkeyIterator::new(self)
    }

    /// is the accounts index using disk as a backing store
    pub fn is_disk_index_enabled(&self) -> bool {
        self.storage.storage.is_disk_index_enabled()
    }

    /// Returns the `(slot, info)` pair for `pubkey`, or `None` if not indexed.
    ///
    /// On this branch the slot list always has exactly one entry, so the first
    /// element is also the only element.
    pub fn get(&self, pubkey: &Pubkey) -> Option<(Slot, T)> {
        self.get_bin(pubkey).get_internal_inner(pubkey, |entry| {
            (
                false,
                entry.and_then(|e| {
                    e.slot_list_read_lock()
                        .first()
                        .map(|(slot, info)| (*slot, *info))
                }),
            )
        })
    }

    fn slot_list_mut<RT>(
        &self,
        pubkey: &Pubkey,
        user_fn: impl FnOnce(SlotListWriteGuard<T>) -> RT,
    ) -> Option<RT> {
        let read_lock = self.get_bin(pubkey);
        read_lock.slot_list_mut(pubkey, user_fn)
    }

    /// Remove keys from the account index if the key's slot list is empty.
    /// Returns the keys that were removed from the index. These keys should not be accessed again in the current code path.
    #[must_use]
    pub fn handle_dead_keys(&self, dead_keys: &[Pubkey]) -> HashSet<Pubkey> {
        let mut pubkeys_removed_from_accounts_index = HashSet::default();
        if !dead_keys.is_empty() {
            for key in dead_keys.iter() {
                let w_index = self.get_bin(key);
                if w_index.remove_if_slot_list_empty(*key) {
                    pubkeys_removed_from_accounts_index.insert(*key);
                }
            }
        }
        pubkeys_removed_from_accounts_index
    }

    pub fn get_rooted_entries(
        &self,
        slot_list: &[SlotListItem<T>],
        max_inclusive: Option<Slot>,
    ) -> SlotList<T> {
        let max_inclusive = max_inclusive.unwrap_or(Slot::MAX);
        // On this branch all index entries are rooted, so no alive_roots check needed.
        slot_list
            .iter()
            .filter(|(slot, _)| *slot <= max_inclusive)
            .cloned()
            .collect()
    }

    /// returns true if, after this fn call:
    /// accounts index entry for `pubkey` has an empty slot list
    /// or `pubkey` does not exist in accounts index
    pub(crate) fn purge_exact(
        &self,
        pubkey: &Pubkey,
        slots_to_purge: impl for<'a> Contains<'a, Slot>,
        reclaims: &mut ReclaimsSlotList<T>,
    ) -> bool {
        self.slot_list_mut(pubkey, |mut slot_list| {
            slot_list.retain_and_count(|(slot, item)| {
                let should_purge = slots_to_purge.contains(slot);
                if should_purge {
                    reclaims.push((*slot, *item));
                    false
                } else {
                    true
                }
            }) == 0
        })
        .unwrap_or(true)
    }

    pub(crate) fn stats(&self) -> &Stats {
        &self.storage.storage.stats
    }

    pub(crate) fn get_bin(&self, pubkey: &Pubkey) -> &InMemAccountsIndex<T, U> {
        &self.account_maps[self.bin_calculator.bin_from_pubkey(pubkey)]
    }

    #[allow(dead_code)]
    pub(crate) fn bins(&self) -> usize {
        self.account_maps.len()
    }

    pub(crate) fn par_iter_bins(
        &self,
    ) -> impl rayon::iter::ParallelIterator<Item = &Arc<InMemAccountsIndex<T, U>>> {
        use rayon::iter::IntoParallelRefIterator as _;
        self.account_maps.par_iter()
    }

    pub(crate) fn iter_bins(&self) -> impl Iterator<Item = &Arc<InMemAccountsIndex<T, U>>> {
        self.account_maps.iter()
    }

    /// Updates the given pubkey at the given slot with the new account information.
    /// on return, the index's previous account info may be returned in 'reclaims' depending on 'previous_slot_entry_was_cached'
    pub fn insert(&self, new_slot: Slot, pubkey: &Pubkey, account_info: T) -> Option<(Slot, T)> {
        let store_raw = true;
        let new_item = PreAllocatedAccountMapEntry::new(
            new_slot,
            account_info,
            &self.storage.storage,
            store_raw,
        );
        // Read the existing slot so the internal upsert knows which entry to replace.
        // Safe: read lock released before write lock is acquired below.
        let old_slot = self.get(pubkey).map(|(s, _)| s);
        let map = self.get_bin(pubkey);
        let mut reclaims = ReclaimsSlotList::new();
        map.upsert(
            pubkey,
            new_item,
            old_slot,
            &mut reclaims,
            UpsertReclaim::PopulateReclaims,
        );
        reclaims.into_iter().next()
    }

    /// Insert `(new_slot, new_info)` for `pubkey` only if `new_slot` is strictly greater than any
    /// existing slot for that pubkey.  Used during startup index generation so that the highest-slot
    /// entry wins regardless of the order in which storages are processed.
    pub(crate) fn insert_if_newer(
        &self,
        new_slot: Slot,
        pubkey: &Pubkey,
        new_info: T,
    ) -> InsertIfNewerResult<T> {
        let map = self.get_bin(pubkey);
        map.insert_if_newer(new_slot, pubkey, new_info)
    }

    #[cfg(test)]
    pub fn ref_count_from_storage(&self, pubkey: &Pubkey) -> RefCount {
        let map = self.get_bin(pubkey);
        map.get_internal_inner(pubkey, |entry| {
            (false, entry.map(|e| e.ref_count()).unwrap_or_default())
        })
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use {
        super::{AccountIndex, secondary::AccountSecondaryIndexes},
        std::collections::HashSet,
    };
    pub fn spl_token_mint_index_enabled() -> AccountSecondaryIndexes {
        let mut account_indexes = HashSet::new();
        account_indexes.insert(AccountIndex::SplTokenMint);
        AccountSecondaryIndexes {
            indexes: account_indexes,
            keys: None,
        }
    }
    #[allow(dead_code)]
    pub fn spl_token_owner_index_enabled() -> AccountSecondaryIndexes {
        let mut account_indexes = HashSet::new();
        account_indexes.insert(AccountIndex::SplTokenOwner);
        AccountSecondaryIndexes {
            indexes: account_indexes,
            keys: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{bucket_map_holder::BucketMapHolder, *};

    #[test]
    fn test_get_empty() {
        let storage_roots = crate::storage_roots::StorageRoots::default();
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool, bool>::default_for_tests();
        let key = &key;
        assert!(index.get(key).is_none());

        let mut num = 0;
        for pubkeys in index.iter() {
            for pubkey in pubkeys {
                if let Some((slot, _)) = index.get(&pubkey) {
                    if slot <= storage_roots.max_root_inclusive() {
                        num += 1;
                    }
                }
            }
        }
        assert_eq!(num, 0);
    }

    #[test]
    fn test_secondary_index_include_exclude() {
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();
        let mut index = AccountSecondaryIndexes::default();

        assert!(!index.contains(&AccountIndex::ProgramId));
        index.indexes.insert(AccountIndex::ProgramId);
        assert!(index.contains(&AccountIndex::ProgramId));
        assert!(index.include_key(&pk1));
        assert!(index.include_key(&pk2));

        let exclude = false;
        index.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [pk1].iter().cloned().collect::<HashSet<_>>(),
            exclude,
        });
        assert!(index.include_key(&pk1));
        assert!(!index.include_key(&pk2));

        let exclude = true;
        index.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [pk1].iter().cloned().collect::<HashSet<_>>(),
            exclude,
        });
        assert!(!index.include_key(&pk1));
        assert!(index.include_key(&pk2));

        let exclude = true;
        index.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [pk1, pk2].iter().cloned().collect::<HashSet<_>>(),
            exclude,
        });
        assert!(!index.include_key(&pk1));
        assert!(!index.include_key(&pk2));

        let exclude = false;
        index.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [pk1, pk2].iter().cloned().collect::<HashSet<_>>(),
            exclude,
        });
        assert!(index.include_key(&pk1));
        assert!(index.include_key(&pk2));
    }

    type AccountInfoTest = f64;

    impl IndexValue for AccountInfoTest {}
    impl DiskIndexValue for AccountInfoTest {}
    impl IsCached for AccountInfoTest {
        fn is_cached(&self) -> bool {
            true
        }
    }

    impl IsZeroLamport for AccountInfoTest {
        fn is_zero_lamport(&self) -> bool {
            true
        }
    }

    fn get_pre_allocated<T: IndexValue>(
        slot: Slot,
        account_info: T,
        storage: &Arc<BucketMapHolder<T, T>>,
        store_raw: bool,
        to_raw_first: bool,
    ) -> PreAllocatedAccountMapEntry<T> {
        let entry = PreAllocatedAccountMapEntry::new(slot, account_info, storage, store_raw);

        if to_raw_first {
            // convert to raw
            let (slot2, account_info2) = entry.into();
            // recreate using extracted raw
            PreAllocatedAccountMapEntry::new(slot2, account_info2, storage, store_raw)
        } else {
            entry
        }
    }

    #[test]
    fn test_new_entry() {
        for store_raw in [false, true] {
            for to_raw_first in [false, true] {
                let slot = 0;
                // account_info type that IS cached
                let account_info = AccountInfoTest::default();
                let index = AccountsIndex::default_for_tests();

                let new_entry = get_pre_allocated(
                    slot,
                    account_info,
                    &index.storage.storage,
                    store_raw,
                    to_raw_first,
                )
                .into_account_map_entry(&index.storage.storage);
                assert_eq!(new_entry.ref_count(), 0);
                assert_eq!(new_entry.slot_list_lock_read_len(), 1);
                assert_eq!(
                    new_entry.slot_list_read_lock().to_vec(),
                    vec![(slot, account_info)]
                );

                // account_info type that is NOT cached
                let account_info = true;
                let index = AccountsIndex::default_for_tests();

                let new_entry = get_pre_allocated(
                    slot,
                    account_info,
                    &index.storage.storage,
                    store_raw,
                    to_raw_first,
                )
                .into_account_map_entry(&index.storage.storage);
                assert_eq!(new_entry.ref_count(), 1);
                assert_eq!(new_entry.slot_list_lock_read_len(), 1);
                assert_eq!(
                    new_entry.slot_list_read_lock().to_vec(),
                    vec![(slot, account_info)]
                );
            }
        }
    }

    #[test]
    fn test_insert_ignore_reclaims() {
        {
            // non-cached: insert always returns the displaced entry; callers may ignore it
            let key = solana_pubkey::new_rand();
            let index = AccountsIndex::<u64, u64>::default_for_tests();
            let slot = 0;
            let value = 1u64;
            assert!(!value.is_cached());
            assert!(index.insert(slot, &key, value).is_none()); // first insert, no displaced entry
            assert!(index.insert(slot, &key, value).is_some()); // replaces non-cached entry
            let _ = index.insert(slot, &key, value); // caller ignores displaced entry
        }
        {
            // cached: cached entries are never returned as displaced
            let key = solana_pubkey::new_rand();
            let index = AccountsIndex::<AccountInfoTest, AccountInfoTest>::default_for_tests();
            let slot = 0;
            let value = 1.0f64;
            assert!(value.is_cached());
            assert!(index.insert(slot, &key, value).is_none()); // first insert
            assert!(index.insert(slot, &key, value).is_none()); // replaces cached → no displaced
            let _ = index.insert(slot, &key, value); // caller ignores
        }
    }

    #[test]
    fn test_insert_with_ancestors() {
        let storage_roots = crate::storage_roots::StorageRoots::default();
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool, bool>::default_for_tests();
        let _ = index.insert(0, &key, true);

        {
            let (slot, account_info) = index.get(&key).unwrap();
            assert_eq!(slot, 0);
            assert!(account_info);
        }

        let mut num = 0;
        let mut found_key = false;
        for pubkeys in index.iter() {
            for pubkey in pubkeys {
                if let Some((slot, _)) = index.get(&pubkey) {
                    if slot <= storage_roots.max_root_inclusive() {
                        if pubkey == key {
                            found_key = true;
                        }
                        num += 1;
                    }
                }
            }
        }

        assert_eq!(num, 1);
        assert!(found_key);
    }

    fn setup_accounts_index_keys(
        num_pubkeys: usize,
    ) -> (
        AccountsIndex<bool, bool>,
        Vec<Pubkey>,
        crate::storage_roots::StorageRoots,
    ) {
        let index = AccountsIndex::<bool, bool>::default_for_tests();
        let root_slot = 0;

        let mut pubkeys: Vec<Pubkey> = std::iter::repeat_with(|| {
            let new_pubkey = solana_pubkey::new_rand();
            let _ = index.insert(root_slot, &new_pubkey, true);
            new_pubkey
        })
        .take(num_pubkeys.saturating_sub(1))
        .collect();

        if num_pubkeys != 0 {
            pubkeys.push(Pubkey::default());
            let _ = index.insert(root_slot, &Pubkey::default(), true);
        }

        let mut storage_roots = crate::storage_roots::StorageRoots::default();
        storage_roots.add_root(root_slot);

        (index, pubkeys, storage_roots)
    }

    fn run_test_scan_accounts(num_pubkeys: usize) {
        let (index, _, storage_roots) = setup_accounts_index_keys(num_pubkeys);

        let mut scanned_keys = HashSet::new();
        for pubkeys in index.iter() {
            for pubkey in pubkeys {
                if let Some((slot, _)) = index.get(&pubkey) {
                    if slot <= storage_roots.max_root_inclusive() {
                        scanned_keys.insert(pubkey);
                    }
                }
            }
        }
        assert_eq!(scanned_keys.len(), num_pubkeys);
    }

    #[test]
    fn test_scan_accounts() {
        run_test_scan_accounts(0);
        run_test_scan_accounts(1);
        run_test_scan_accounts(9_999);
        run_test_scan_accounts(10_000);
        run_test_scan_accounts(10_001)
    }

    #[test]
    fn test_insert_with_root() {
        let mut storage_roots = crate::storage_roots::StorageRoots::default();
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool, bool>::default_for_tests();
        let _ = index.insert(0, &key, true);

        storage_roots.add_root(0);
        let (slot, account_info) = index.get(&key).unwrap();
        assert_eq!(slot, 0);
        assert!(account_info);
    }

    #[test]
    fn test_update_last_wins() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<u64, u64>::default_for_tests();
        assert!(index.insert(0, &key, 1u64).is_none());
        let (slot, account_info) = index.get(&key).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(account_info, 1);

        assert_eq!(index.insert(0, &key, 0u64), Some((0, 1u64)));
        let (slot, account_info) = index.get(&key).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(account_info, 0);
    }

    #[test]
    fn test_upsert_reclaims() {
        let key = solana_pubkey::new_rand();
        let index =
            AccountsIndex::<CacheableIndexValueTest, CacheableIndexValueTest>::default_for_tests();

        // No displaced entry on the first insert
        assert!(
            index
                .insert(0, &key, CacheableIndexValueTest(true))
                .is_none()
        );

        // Replacing a cached entry does not displace it (cached entries aren't in storage)
        assert!(
            index
                .insert(0, &key, CacheableIndexValueTest(false))
                .is_none()
        );

        // Slot list should only have a single entry
        assert!(index.get(&key).is_some());

        // Replacing an uncached entry returns the displaced entry
        assert!(
            index
                .insert(0, &key, CacheableIndexValueTest(false))
                .is_some()
        );

        // Slot list should only have a single entry
        assert!(index.get(&key).is_some());
    }
    impl IndexValue for bool {}
    impl IndexValue for u64 {}
    impl DiskIndexValue for bool {}
    impl DiskIndexValue for u64 {}
    impl IsCached for bool {
        fn is_cached(&self) -> bool {
            false
        }
    }
    impl IsCached for u64 {
        fn is_cached(&self) -> bool {
            false
        }
    }
    impl IsZeroLamport for bool {
        fn is_zero_lamport(&self) -> bool {
            false
        }
    }

    impl IsZeroLamport for u64 {
        fn is_zero_lamport(&self) -> bool {
            false
        }
    }

    /// Type that supports caching for tests. Used to test upsert behaviour
    /// when the slot list has mixed cached and uncached items.
    #[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
    struct CacheableIndexValueTest(bool);
    impl IndexValue for CacheableIndexValueTest {}
    impl DiskIndexValue for CacheableIndexValueTest {}
    impl IsCached for CacheableIndexValueTest {
        fn is_cached(&self) -> bool {
            // Return self value as whether the item is cached or not
            self.0
        }
    }
    impl IsZeroLamport for CacheableIndexValueTest {
        fn is_zero_lamport(&self) -> bool {
            false
        }
    }

    impl<T: IndexValue> AccountsIndex<T, T> {
        #[allow(dead_code)]
        fn upsert_simple_test(&self, key: &Pubkey, slot: Slot, value: T) {
            let _ = self.insert(slot, key, value);
        }
    }

    #[test]
    fn test_handle_dead_keys_return() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool, bool>::default_for_tests();

        assert_eq!(
            index.handle_dead_keys(&[key]),
            vec![key].into_iter().collect::<HashSet<_>>()
        );
    }
}
