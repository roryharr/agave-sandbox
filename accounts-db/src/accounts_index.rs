mod account_map_entry;
mod secondary;
mod stats;
pub use secondary::{
    AccountIndex, AccountSecondaryIndexes, AccountSecondaryIndexesIncludeExclude, IndexKey,
};
use {
    crate::{
        ancestors::Ancestors,
        contains::Contains,
        is_zero_lamport::IsZeroLamport,
        pubkey_bins::{PubkeyBinCalculator, PubkeyBinCalculatorBuilder},
    },
    account_map_entry::AccountMapEntry,
    dashmap::{DashMap, mapref::entry::Entry},
    log::*,
    secondary::{RwLockSecondaryIndexEntry, SecondaryIndex, SecondaryIndexEntry},
    solana_account::ReadableAccount,
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    stats::Stats,
    std::{
        fmt::Debug,
        num::NonZeroUsize,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    },
};

/// Partition count for clean's candidate sets. The index map itself shards internally.
pub const BINS_DEFAULT: usize = 8192;
pub const BINS_FOR_TESTING: usize = 2; // we want > 1
pub const BINS_FOR_BENCHMARKS: usize = 8192;
// The unsafe is safe because we're using a fixed, known non-zero value
pub const FLUSH_THREADS_TESTING: NonZeroUsize = NonZeroUsize::new(1).unwrap();

/// The number of entries below an in-mem index bin's usable capacity at which to begin evicting.
/// Ignored: the index holds every entry and never evicts. Kept so callers can still build an
/// `IndexLimitThreshold`.
pub const DEFAULT_NUM_ENTRIES_OVERHEAD: usize = 5_000;

/// The number of entries to evict, once we've hit the high watermark.
/// Ignored: the index holds every entry and never evicts. Kept so callers can still build an
/// `IndexLimitThreshold`.
pub const DEFAULT_NUM_ENTRIES_TO_EVICT: usize = 10_000;

/// Byte threshold used when the deprecated `minimal` index limit is specified.
/// Ignored: the index holds every entry.
pub const MINIMAL_THRESHOLD_NUM_BYTES: u64 = 25_000_000_000;

pub const ACCOUNTS_INDEX_CONFIG_FOR_TESTING: AccountsIndexConfig = AccountsIndexConfig {
    bins: Some(BINS_FOR_TESTING),
    num_flush_threads: Some(FLUSH_THREADS_TESTING),
    drives: None,
    index_limit: IndexLimit::InMemOnly,
    ages_to_stay_in_cache: None,
    num_initial_accounts: None,
};
pub const ACCOUNTS_INDEX_CONFIG_FOR_BENCHMARKS: AccountsIndexConfig = AccountsIndexConfig {
    bins: Some(BINS_FOR_BENCHMARKS),
    num_flush_threads: Some(FLUSH_THREADS_TESTING),
    drives: None,
    index_limit: IndexLimit::InMemOnly,
    ages_to_stay_in_cache: None,
    num_initial_accounts: None,
};
pub type SlotList<T> = [SlotListItem<T>; 1];
pub type ReclaimsSlotList<T> = Vec<SlotListItem<T>>;
/// Reclaimed slot-list items, each with the slot of the newest surviving entry for that account
pub type ReclaimsWithNewestSlot<T> = Vec<(SlotListItem<T>, Slot)>;
pub type SlotListItem<T> = (Slot, T);

/// values returned from `insert_new_if_missing_into_primary_index()`
#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct InsertNewIfMissingIntoPrimaryIndexInfo {
    /// number of accounts inserted in the index
    pub count: usize,
    /// Number of accounts added to the index that didn't already exist in the index
    pub num_did_not_exist: u64,
    /// Number of accounts added to the index that already existed
    pub num_existed: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
/// which accounts `scan` should load from disk
///
/// The index holds every entry in memory, so every variant scans the same way. The variants
/// are kept so callers can still parse and pass a filter.
pub enum ScanFilter {
    /// Scan the index
    #[default]
    All,

    /// abnormal = slot list.len() != 1
    /// The index holds a single entry per pubkey, so no entry is abnormal.
    OnlyAbnormal,

    /// Similar to `OnlyAbnormal but also verify the entry.
    OnlyAbnormalWithVerify,

    /// Similar to `OnlyAbnormal but mark entries in memory as not found
    /// if they are normal
    OnlyAbnormalTest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// how accounts index 'upsert' should handle reclaims
pub enum UpsertReclaim {
    /// overwrite existing data in the same slot and do not return in 'reclaims'
    IgnoreReclaims,
    /// Reclaim all older versions of the account from the index and return
    /// in the 'reclaims'
    ReclaimOldSlots,
}

pub trait IndexValue:
    'static + IsZeroLamport + Clone + Debug + PartialEq + Copy + Default + Sync + Send
{
    /// pack into the low 64 bits of an index entry
    fn to_bits(self) -> u64;
    fn from_bits(bits: u64) -> Self;
}

/// specification of how much memory the in-mem portion of account index can hold
///
/// Ignored: the index holds every entry. Kept so callers can still build and pass a limit.
#[derive(Debug, Clone)]
pub enum IndexLimit {
    /// in-mem-only was specified, no disk index
    InMemOnly,
    /// evict from in-mem when usage exceeds threshold in bytes
    Threshold(IndexLimitThreshold),
}

/// Configuration for threshold-based accounts index limit
///
/// Ignored: the index holds every entry.
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
    /// Partition count for clean's candidate sets. The index map itself shards internally.
    pub bins: Option<usize>,
    /// Ignored: there are no flush threads. Kept for callers that still set it.
    pub num_flush_threads: Option<NonZeroUsize>,
    /// Ignored: there is no disk index. Kept for callers that still set it.
    pub drives: Option<Vec<PathBuf>>,
    /// Ignored: the index holds every entry. Kept for callers that still set it.
    pub index_limit: IndexLimit,
    /// Ignored: entries are never evicted. Kept for callers that still set it.
    pub ages_to_stay_in_cache: Option<u8>,
    /// Initial number of accounts, used to pre-allocate the map's capacity at startup.
    pub num_initial_accounts: Option<usize>,
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
        }
    }
}

pub fn default_num_flush_threads() -> NonZeroUsize {
    NonZeroUsize::new(std::cmp::max(2, num_cpus::get() / 4)).expect("non-zero system threads")
}

#[derive(Debug)]
/// T: account info type to interact in in-memory items
pub struct AccountsIndex<T: IndexValue> {
    /// Every index entry, keyed by pubkey. Entry updates are a compare-and-exchange on the
    /// entry itself; the map's shard locks are only held for lookup, insert, and remove.
    pub(crate) map: DashMap<Pubkey, AccountMapEntry<T>, PubkeyHasherBuilder>,
    /// Partitions clean's candidate sets. The index map itself shards internally.
    pub bin_calculator: PubkeyBinCalculator,
    /// Partition count for `bin_calculator`
    bins: usize,
    program_id_index: SecondaryIndex<RwLockSecondaryIndexEntry>,
    spl_token_mint_index: SecondaryIndex<RwLockSecondaryIndexEntry>,
    spl_token_owner_index: SecondaryIndex<RwLockSecondaryIndexEntry>,

    stats: Stats,
    /// true while generate_index is populating the index
    startup: AtomicBool,
    /// older versions of pubkeys that were found in more than one slot during startup.
    /// The index holds the version from the newest slot; every other version is here.
    startup_duplicates: Mutex<Vec<(Slot, Pubkey, T)>>,
}

impl<T: IndexValue> AccountsIndex<T> {
    pub fn default_for_tests() -> Self {
        Self::new(&ACCOUNTS_INDEX_CONFIG_FOR_TESTING, Arc::default())
    }

    pub fn new(config: &AccountsIndexConfig, _exit: Arc<AtomicBool>) -> Self {
        let bins = config.bins.unwrap_or(BINS_DEFAULT);
        // create bin_calculator early to verify # bins is reasonable
        let bin_calculator = PubkeyBinCalculatorBuilder::with_bins(
            NonZeroUsize::new(bins).expect("bins is non-zero"),
        );
        info!("AccountsIndex bin calculator: {bin_calculator:?}");

        let map = DashMap::with_capacity_and_hasher(
            config.num_initial_accounts.unwrap_or(0),
            PubkeyHasherBuilder::default(),
        );

        Self {
            map,
            bin_calculator,
            bins,
            program_id_index: SecondaryIndex::<RwLockSecondaryIndexEntry>::new(
                "program_id_index_stats",
            ),
            spl_token_mint_index: SecondaryIndex::<RwLockSecondaryIndexEntry>::new(
                "spl_token_mint_index_stats",
            ),
            spl_token_owner_index: SecondaryIndex::<RwLockSecondaryIndexEntry>::new(
                "spl_token_owner_index_stats",
            ),
            stats: Stats::default(),
            startup: AtomicBool::default(),
            startup_duplicates: Mutex::default(),
        }
    }

    /// Gets the index's entry for `pubkey` and applies `callback` to it
    pub fn get_and_then<R>(
        &self,
        pubkey: &Pubkey,
        callback: impl FnOnce(Option<&AccountMapEntry<T>>) -> R,
    ) -> R {
        let mut m = Measure::start("get");
        let entry = self.map.get(pubkey);
        m.stop();
        let found = entry.is_some();
        let result = callback(entry.as_deref());
        drop(entry);

        let (count, time) = if found {
            (&self.stats.gets_from_mem, &self.stats.get_mem_us)
        } else {
            (&self.stats.gets_missing, &self.stats.get_missing_us)
        };
        Self::update_stat(time, m.as_us());
        Self::update_stat(count, 1);

        result
    }

    /// Gets the index's entry for `pubkey`, with `ancestors`,
    /// and applies `callback` to it
    pub(crate) fn get_with_and_then<R>(
        &self,
        pubkey: &Pubkey,
        ancestors: &Ancestors,
        callback: impl FnOnce(SlotListItem<T>) -> R,
    ) -> Option<R> {
        let max_root = ancestors.min_slot();
        self.get_and_then(pubkey, |entry| {
            entry.and_then(|entry| {
                self.get_account_info_with_and_then(entry, Some(ancestors), max_root, callback)
            })
        })
    }

    /// Gets the account info (and slot) in `entry`, with `ancestors` and `max_root`,
    /// and applies `callback` to it
    pub(crate) fn get_account_info_with_and_then<R>(
        &self,
        entry: &AccountMapEntry<T>,
        ancestors: Option<&Ancestors>,
        max_root: Option<Slot>,
        callback: impl FnOnce(SlotListItem<T>) -> R,
    ) -> Option<R> {
        let slot_list = entry.slot_list();
        self.latest_slot(ancestors, &slot_list, max_root)
            .map(|found_index| callback(slot_list[found_index]))
    }

    /// Is `pubkey` in the index?
    pub(crate) fn contains(&self, pubkey: &Pubkey) -> bool {
        self.get_and_then(pubkey, |entry| entry.is_some())
    }

    /// Is `pubkey`, with `ancestors`, in the index?
    #[cfg(test)]
    fn contains_with(&self, pubkey: &Pubkey, ancestors: &Ancestors) -> bool {
        self.get_with_and_then(pubkey, ancestors, |_| ()).is_some()
    }

    /// Remove keys from the account index if the key's slot list is empty.
    /// Returns the keys that were removed from the index.
    ///
    /// The index holds a single `(slot, account_info)` per pubkey, so an indexed pubkey never
    /// has an empty slot list; only a missing pubkey has one.
    ///
    /// When secondary indexes are enabled, callers must pass the returned keys to
    /// `AccountsDb::purge_secondary_indexes_for_dead_keys`, otherwise their secondary index
    /// entries leak.
    #[must_use]
    pub fn handle_dead_keys(&self, dead_keys: &[Pubkey]) -> Vec<Pubkey> {
        let mut pubkeys_removed_from_accounts_index = Vec::default();
        if !dead_keys.is_empty() {
            for key in dead_keys.iter() {
                if !self.map.contains_key(key) {
                    pubkeys_removed_from_accounts_index.push(*key);
                }
            }
        }
        pubkeys_removed_from_accounts_index
    }

    /// call func with every pubkey and entry visible from a given set of ancestors.
    /// `should_abort` is checked after each entry; the scan stops once it returns true.
    pub(crate) fn scan_accounts<F>(
        &self,
        ancestors: &Ancestors,
        max_root: Slot,
        mut func: F,
        should_abort: impl Fn() -> bool,
    ) where
        F: FnMut(&Pubkey, SlotListItem<T>),
    {
        for shard in self.map.shards() {
            // snapshot the shard so `func` runs without the shard lock held
            let entries: Vec<_> = shard
                .read()
                .iter()
                .map(|(pubkey, value)| (*pubkey, value.get().entry()))
                .collect();
            for (pubkey, item) in entries {
                if self
                    .latest_slot(Some(ancestors), std::slice::from_ref(&item), Some(max_root))
                    .is_some()
                {
                    func(&pubkey, item);
                }
                if should_abort() {
                    return;
                }
            }
        }
    }

    /// Returns the list of pubkeys from the secondary index for the given key.
    pub(crate) fn get_index_key_pubkeys(&self, index_key: &IndexKey) -> Vec<Pubkey> {
        match index_key {
            IndexKey::ProgramId(key) => self.program_id_index.get(key),
            IndexKey::SplTokenMint(key) => self.spl_token_mint_index.get(key),
            IndexKey::SplTokenOwner(key) => self.spl_token_owner_index.get(key),
        }
    }

    /// Removes `slots_to_purge` from the slot list of `pubkey`, pushing removed entries into
    /// `reclaims`.
    ///
    /// returns true if, after this fn call:
    /// accounts index entry for `pubkey` has an empty slot list
    /// or `pubkey` does not exist in accounts index
    pub(crate) fn purge_exact(
        &self,
        pubkey: &Pubkey,
        slots_to_purge: impl for<'a> Contains<'a, Slot>,
        reclaims: &mut ReclaimsSlotList<T>,
    ) -> bool {
        self.remove_entry_if(pubkey, |(slot, item)| {
            let should_purge = slots_to_purge.contains(slot);
            if should_purge {
                reclaims.push((*slot, *item));
            }
            should_purge
        })
        // `None` means the pubkey is not in the index
        .unwrap_or(true)
    }

    /// Is an entry at `slot` visible from `ancestors`?
    ///
    /// This is `latest_slot` for the single entry the index holds per pubkey, with the same
    /// `max_root` bound that `get_with_and_then` applies.
    pub(crate) fn is_slot_visible(&self, slot: Slot, ancestors: &Ancestors) -> bool {
        ancestors.contains_key(&slot)
            || ancestors.min_slot().is_none_or(|max_root| slot <= max_root)
    }

    // Given a SlotList `L`, a list of ancestors and a maximum slot, find the latest element
    // in `L`, where the slot `S` is an ancestor or root, and if `S` is a root, then `S <= max_root`
    pub(crate) fn latest_slot(
        &self,
        ancestors: Option<&Ancestors>,
        slot_list: &[SlotListItem<T>],
        max_root_inclusive: Option<Slot>,
    ) -> Option<usize> {
        let mut current_max = 0;
        let mut rv = None;
        if let Some(ancestors) = ancestors
            && !ancestors.is_empty()
        {
            for (i, (slot, _t)) in slot_list.iter().rev().enumerate() {
                if (rv.is_none() || *slot > current_max) && ancestors.contains_key(slot) {
                    rv = Some(i);
                    current_max = *slot;
                }
            }
        }

        // If we found an ancestor, then we can return early without checking the roots
        // If there is a root that is newer than the newest ancestor but not an ancestor
        // then the root is from a different fork and should not be returned
        if let Some(rv) = rv {
            return Some(slot_list.len() - 1 - rv);
        }

        let max_root_inclusive = max_root_inclusive.unwrap_or(Slot::MAX);

        slot_list
            .iter()
            .enumerate()
            .filter(|(_, (slot, _t))| *slot <= max_root_inclusive)
            .max_by_key(|(_, (slot, _t))| *slot)
            .map(|(index, _)| index)
    }

    pub(crate) fn stats(&self) -> &Stats {
        &self.stats
    }

    /// The number of entries in the index, and the map capacity backing them
    pub(crate) fn len_and_capacity(&self) -> (usize, usize) {
        let capacity = self
            .map
            .shards()
            .iter()
            .map(|shard| shard.read().capacity())
            .sum();
        (self.map.len(), capacity)
    }

    /// report index stats, rate-limited to the stats interval
    pub fn report_stats(&self) {
        let (count, capacity) = self.len_and_capacity();
        // map memory is based on capacity and the footprint of a KV-pair
        // (we ignore other map details, such as load factor)
        let estimate_mem_bytes = capacity * (size_of::<Pubkey>() + size_of::<AccountMapEntry<T>>());
        self.stats.report_stats(
            self.startup.load(Ordering::Relaxed),
            count,
            capacity,
            estimate_mem_bytes,
        );
    }

    pub(crate) fn set_startup(&self, value: Startup) {
        self.startup
            .store(value != Startup::Normal, Ordering::Relaxed);
    }

    /// Scan AccountsIndex for a given iterator of Pubkeys.
    ///
    /// This fn takes 3 arguments.
    ///  - an iterator of pubkeys to scan
    ///  - callback fn to run for each pubkey in the accounts index
    ///  - a ScanFilter. The index holds every entry in memory, so every filter scans the
    ///    same way.
    ///
    /// The `callback` fn takes in 2 arguments:
    ///   - the first an immutable ref of the pubkey,
    ///   - the second an option of the SlotList
    pub(crate) fn scan<'a, F, I>(&self, pubkeys: I, mut callback: F, _filter: ScanFilter)
    where
        F: FnMut(&'a Pubkey, Option<&[SlotListItem<T>]>),
        I: Iterator<Item = &'a Pubkey>,
    {
        pubkeys.into_iter().for_each(|pubkey| {
            self.get_and_then(pubkey, |entry| {
                if let Some(locked_entry) = entry {
                    let slot_list = locked_entry.slot_list();
                    callback(pubkey, Some(slot_list.as_ref()));
                } else {
                    callback(pubkey, None);
                }
            });
        });
    }

    fn update_spl_token_secondary_indexes<G: spl_generic_token::token::GenericTokenAccount>(
        &self,
        token_id: &Pubkey,
        pubkey: &Pubkey,
        account_owner: &Pubkey,
        account_data: &[u8],
        account_indexes: &AccountSecondaryIndexes,
    ) {
        if *account_owner == *token_id {
            if account_indexes.contains(&AccountIndex::SplTokenOwner)
                && let Some(owner_key) = G::unpack_account_owner(account_data)
                && account_indexes.include_key(owner_key)
            {
                self.spl_token_owner_index.insert(owner_key, pubkey);
            }

            if account_indexes.contains(&AccountIndex::SplTokenMint)
                && let Some(mint_key) = G::unpack_account_mint(account_data)
                && account_indexes.include_key(mint_key)
            {
                self.spl_token_mint_index.insert(mint_key, pubkey);
            }
        }
    }

    pub fn get_index_key_size(&self, index: &AccountIndex, index_key: &Pubkey) -> Option<usize> {
        match index {
            AccountIndex::ProgramId => self.program_id_index.index.get(index_key).map(|x| x.len()),
            AccountIndex::SplTokenOwner => self
                .spl_token_owner_index
                .index
                .get(index_key)
                .map(|x| x.len()),
            AccountIndex::SplTokenMint => self
                .spl_token_mint_index
                .index
                .get(index_key)
                .map(|x| x.len()),
        }
    }

    /// log any secondary index counts, if non-zero
    pub(crate) fn log_secondary_indexes(&self) {
        if !self.program_id_index.index.is_empty() {
            info!("secondary index: {:?}", AccountIndex::ProgramId);
            self.program_id_index.log_contents();
        }
        if !self.spl_token_mint_index.index.is_empty() {
            info!("secondary index: {:?}", AccountIndex::SplTokenMint);
            self.spl_token_mint_index.log_contents();
        }
        if !self.spl_token_owner_index.index.is_empty() {
            info!("secondary index: {:?}", AccountIndex::SplTokenOwner);
            self.spl_token_owner_index.log_contents();
        }
    }

    pub(crate) fn update_secondary_indexes(
        &self,
        pubkey: &Pubkey,
        account: &impl ReadableAccount,
        account_indexes: &AccountSecondaryIndexes,
    ) {
        if account_indexes.is_empty() {
            return;
        }

        let account_owner = account.owner();
        let account_data = account.data();

        if account_indexes.contains(&AccountIndex::ProgramId)
            && account_indexes.include_key(account_owner)
        {
            self.program_id_index.insert(account_owner, pubkey);
        }
        // Note because of the below check below on the account data length, when an
        // account hits zero lamports and is reset to AccountSharedData::Default, then we skip
        // the below updates to the secondary indexes.
        //
        // Skipping means not updating secondary index to mark the account as missing.
        // This doesn't introduce false positives during a scan because the caller to scan
        // provides the ancestors to check. So even if a zero-lamport account is not yet
        // removed from the secondary index, the scan function will:
        // 1) consult the primary index via `get(&pubkey, Some(ancestors), max_root)`
        // and find the zero-lamport version
        // 2) When the fetch from storage occurs, it will return AccountSharedData::Default
        // (as persisted tombstone for snapshots). This will then ultimately be
        // filtered out by post-scan filters, like in `get_filtered_spl_token_accounts_by_owner()`.

        self.update_spl_token_secondary_indexes::<spl_generic_token::token::Account>(
            &spl_generic_token::token::id(),
            pubkey,
            account_owner,
            account_data,
            account_indexes,
        );
        self.update_spl_token_secondary_indexes::<spl_generic_token::token_2022::Account>(
            &spl_generic_token::token_2022::id(),
            pubkey,
            account_owner,
            account_data,
            account_indexes,
        );
    }

    /// Partition count for clean's candidate sets
    pub fn bins(&self) -> usize {
        self.bins
    }

    /// Same functionally to upsert, but operates on a batch of items in a reusable Vec,
    /// draining all elements.
    ///
    /// Can save time when inserting lots of new keys.
    /// But, does NOT update secondary index
    /// This is designed to be called at startup time.
    pub(crate) fn insert_new_if_missing_into_primary_index(
        &self,
        slot: Slot,
        items: &mut Vec<(Pubkey, T)>,
    ) -> InsertNewIfMissingIntoPrimaryIndexInfo {
        let count = items.len();
        let mut num_did_not_exist = 0;
        let mut num_existed = 0;
        let mut duplicates = Vec::new();

        for (pubkey, account_info) in items.drain(..) {
            match self.map.entry(pubkey) {
                Entry::Occupied(occupied) => {
                    // only this thread inserts entries for `slot`, so an existing entry at
                    // `slot` is the same pubkey stored twice in one storage
                    let (existing_slot, _existing_account_info) = occupied.get().entry();
                    assert_ne!(
                        existing_slot, slot,
                        "Accounts may only be stored once per slot: {slot}"
                    );
                    // keep whichever version is from the newer slot
                    let older_version = occupied.get().replace_if_newer((slot, account_info), None);
                    duplicates.push((older_version.0, pubkey, older_version.1));
                    num_existed += 1;
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(AccountMapEntry::new([(slot, account_info)]));
                    num_did_not_exist += 1;
                }
            }
        }

        if !duplicates.is_empty() {
            self.startup_update_duplicates(duplicates);
        }

        InsertNewIfMissingIntoPrimaryIndexInfo {
            count,
            num_did_not_exist,
            num_existed,
        }
    }

    fn startup_update_duplicates(&self, items: Vec<(Slot, Pubkey, T)>) {
        assert!(self.startup.load(Ordering::Relaxed));

        let mut duplicates = self.startup_duplicates.lock().unwrap();
        duplicates.extend(items);
    }

    /// pull out all the older versions of duplicate pubkeys found during startup.
    /// The index holds the version from the newest slot; these are all the rest.
    pub(crate) fn take_startup_duplicates(&self, f: impl Fn(Vec<(Slot, Pubkey, T)>) + Sync + Send) {
        f(std::mem::take(&mut self.startup_duplicates.lock().unwrap()));
    }

    /// Updates the primary index for `pubkey` at `new_slot` with `account_info`.
    ///
    /// Does NOT update the secondary indexes — callers that need that must update separately.
    /// The primary and secondary indexes are not updated atomically, and a brief inconsistency is
    /// acceptable: the secondary index is only consulted for `scan`, which is only supported on
    /// frozen banks, and is never used as a source of truth for gets/stores.
    ///
    /// On return, the previous account info may be returned in `reclaims` depending on `reclaim`.
    pub fn upsert(
        &self,
        new_slot: Slot,
        old_slot: Slot,
        pubkey: &Pubkey,
        account_info: T,
        reclaims: &mut ReclaimsSlotList<T>,
        reclaim: UpsertReclaim,
    ) {
        let new_value = (new_slot, account_info);
        self.get_or_create_index_entry_for_pubkey(pubkey, new_value, |entry| {
            Self::lock_and_update_slot_list(entry, new_value, Some(old_slot), reclaims, reclaim);
        });
    }

    /// Gets the entry for `pubkey` and calls `callback` with it.
    /// If `pubkey` is not in the index, a new entry holding `new_item` is created and `callback`
    /// is not called, since there is no prior entry to update.
    pub fn get_or_create_index_entry_for_pubkey(
        &self,
        pubkey: &Pubkey,
        new_item: SlotListItem<T>,
        callback: impl FnOnce(&AccountMapEntry<T>),
    ) {
        // try to get it with the shard read lock first
        {
            let entry = self.map.get(pubkey);
            if let Some(entry) = entry {
                callback(&entry);
                drop(entry);
                Self::update_stat(&self.stats.updates_in_mem, 1);
                return;
            }
        }

        let mut m = Measure::start("entry");
        let entry = self.map.entry(*pubkey);
        m.stop();
        let found = matches!(entry, Entry::Occupied(_));
        match entry {
            Entry::Occupied(occupied) => {
                callback(occupied.get());
                Self::update_stat(&self.stats.updates_in_mem, 1);
            }
            Entry::Vacant(vacant) => {
                // not in the index, so insert the new item. There is no prior entry to
                // update, so `callback` is not called
                self.stats.inc_insert();
                vacant.insert(AccountMapEntry::new([new_item]));
            }
        };
        self.update_entry_stats(m, found);
    }

    fn update_entry_stats(&self, stopped_measure: Measure, found: bool) {
        let (count, time) = if found {
            (&self.stats.entries_from_mem, &self.stats.entry_mem_us)
        } else {
            (&self.stats.entries_missing, &self.stats.entry_missing_us)
        };
        Self::update_stat(time, stopped_measure.as_us());
        Self::update_stat(count, 1);
    }

    /// Try to update an item in the slot list the given `slot` If an item for the slot
    /// already exists in the list, remove the older item, add it to `reclaims`, and insert
    /// the new item.
    /// if 'other_slot' is some, then remove any entries in the slot list at 'other_slot' instead
    fn lock_and_update_slot_list(
        current: &AccountMapEntry<T>,
        new_value: SlotListItem<T>,
        other_slot: Option<Slot>,
        reclaims: &mut ReclaimsSlotList<T>,
        _reclaim: UpsertReclaim,
    ) {
        // The index holds one entry per pubkey, so it cannot carry the older version for a
        // later clean to reclaim. Reclaim it here, whatever the caller asked for, otherwise
        // its record is never marked obsolete and its storage never dies.
        reclaims.push(current.replace_if_newer(new_value, other_slot));
    }

    /// Call `user_fn` with the entry for `pubkey`, writing the result back into the index
    pub(crate) fn update_entry<RT>(
        &self,
        pubkey: &Pubkey,
        user_fn: impl FnOnce(SlotListItem<T>) -> (SlotListItem<T>, RT),
    ) -> Option<RT> {
        self.get_and_then(pubkey, |entry| {
            entry.map(|entry| {
                let (new_item, result) = user_fn(entry.entry());
                entry.replace(new_item);
                result
            })
        })
    }

    /// Replaces the slot list entry at `old_slot` with `(new_slot, account_info)` for `pubkey`.
    ///
    /// Used by the shrink path: the account already exists in the index at `old_slot`, and
    /// shrink is rewriting it into a new storage at `new_slot`. The previous entry is discarded
    /// (no reclaims are returned — the caller manages the source storage's alive-bytes accounting).
    ///
    /// Panics if `old_slot` is not present in the slot list.
    pub fn replace(&self, new_slot: Slot, old_slot: Slot, pubkey: &Pubkey, account_info: T) {
        self.update_entry(pubkey, |current| {
            let (current_slot, _current_account_info) = current;
            // The entry may have moved on to a newer slot since the caller read the account it
            // is relocating, which makes the account it holds dead. Leave the newer entry.
            if current_slot == old_slot {
                ((new_slot, account_info), ())
            } else {
                assert!(
                    current_slot > old_slot,
                    "index holds an entry from an older slot: {current_slot} vs {old_slot}"
                );
                (current, ())
            }
        })
        .expect("Expected entry to exist in accounts index");
    }

    /// Removes the pubkey from the index
    /// Populate reclaims with any entries previously in the slot list
    pub fn delete(&self, pubkey: &Pubkey, reclaims: &mut ReclaimsSlotList<T>) {
        if let Some((_pubkey, entry)) = self.map.remove(pubkey) {
            reclaims.extend(entry.slot_list().iter().copied());
            self.stats.inc_delete();
        }
    }

    /// Remove `pubkey`'s entry from the index if `should_remove` returns true for its entry.
    ///
    /// Returns `None` if `pubkey` is not in the index, otherwise whether it was removed.
    pub(crate) fn remove_entry_if(
        &self,
        pubkey: &Pubkey,
        should_remove: impl FnOnce(&SlotListItem<T>) -> bool,
    ) -> Option<bool> {
        match self.map.entry(*pubkey) {
            Entry::Occupied(occupied) => {
                let remove = should_remove(&occupied.get().entry());
                if remove {
                    self.stats.inc_delete();
                    occupied.remove();
                }
                Some(remove)
            }
            Entry::Vacant(_vacant) => None,
        }
    }

    /// Length of `pubkey`'s slot list, 0 if the pubkey is not in the index
    #[cfg(feature = "dev-context-only-utils")]
    pub fn slot_list_len(&self, pubkey: &Pubkey) -> usize {
        self.get_and_then(pubkey, |entry| {
            entry
                .map(|entry| entry.slot_list().len())
                .unwrap_or_default()
        })
    }

    /// Purges `inner_key` from each enabled secondary index
    pub(crate) fn purge_secondary_indexes_by_inner_key_if(
        &self,
        inner_key: &Pubkey,
        account_indexes: &AccountSecondaryIndexes,
        should_remove: impl Fn() -> bool,
    ) {
        if account_indexes.contains(&AccountIndex::ProgramId) {
            self.program_id_index
                .remove_by_inner_key_if(inner_key, &should_remove);
        }

        if account_indexes.contains(&AccountIndex::SplTokenOwner) {
            self.spl_token_owner_index
                .remove_by_inner_key_if(inner_key, &should_remove);
        }

        if account_indexes.contains(&AccountIndex::SplTokenMint) {
            self.spl_token_mint_index
                .remove_by_inner_key_if(inner_key, &should_remove);
        }
    }

    /// Remove `pubkey`'s entry from the accounts index if it is a zero lamport account, pushing
    /// the removed entry into `reclaims`.
    /// Return true if this call removed the pubkey's entry from the accounts index.
    ///
    /// The index holds a single entry per pubkey, so there are never older rooted entries to
    /// reclaim here. `max_clean_root_inclusive` only ever selected which of those older entries
    /// to purge, so it is unused.
    ///
    /// When secondary indexes are enabled and this returns true, callers must pass `pubkey` to
    /// `AccountsDb::purge_secondary_indexes_for_dead_keys`, otherwise its secondary index
    /// entries leak.
    #[must_use]
    pub fn clean_rooted_entries(
        &self,
        pubkey: &Pubkey,
        reclaims: &mut ReclaimsWithNewestSlot<T>,
        _max_clean_root_inclusive: Option<Slot>,
    ) -> bool {
        self.remove_entry_if(pubkey, |(slot, account_info)| {
            // If a zero lamport account is the only version left, reclaim it. It will be
            // converted into a tombstone.
            let is_zero_lamport = account_info.is_zero_lamport();
            if is_zero_lamport {
                reclaims.push(((*slot, *account_info), *slot));
            }
            is_zero_lamport
        })
        // `None` means the pubkey is not in the index; nothing was removed.
        .unwrap_or(false)
    }

    fn update_stat(stat: &AtomicU64, value: u64) {
        if value != 0 {
            stat.fetch_add(value, Ordering::Relaxed);
        }
    }
}

/// modes the system can be in
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum Startup {
    /// not startup, but steady state execution
    Normal,
    /// startup (not steady state execution)
    Startup,
}

#[cfg(test)]
pub(crate) mod test_utils {
    use {
        super::{AccountIndex, secondary::AccountSecondaryIndexes},
        std::collections::HashSet,
    };
    pub fn program_id_index_enabled() -> AccountSecondaryIndexes {
        let mut account_indexes = HashSet::new();
        account_indexes.insert(AccountIndex::ProgramId);
        AccountSecondaryIndexes {
            indexes: account_indexes,
            keys: None,
        }
    }
    pub fn spl_token_mint_index_enabled() -> AccountSecondaryIndexes {
        let mut account_indexes = HashSet::new();
        account_indexes.insert(AccountIndex::SplTokenMint);
        AccountSecondaryIndexes {
            indexes: account_indexes,
            keys: None,
        }
    }
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
    use {
        super::*,
        solana_account::AccountSharedData,
        solana_pubkey::PUBKEY_BYTES,
        spl_generic_token::{spl_token_ids, token::SPL_TOKEN_ACCOUNT_OWNER_OFFSET},
        std::collections::HashSet,
        test_case::test_case,
    };

    enum SecondaryIndexTypes<'a> {
        // We don't access the inner value, but we do use it for type checking during cmopilation.
        #[allow(dead_code)]
        RwLock(&'a SecondaryIndex<RwLockSecondaryIndexEntry>),
    }

    fn create_spl_token_mint_secondary_index_state() -> (usize, usize, AccountSecondaryIndexes) {
        {
            // Check that we're actually testing the correct variant
            let index = AccountsIndex::<bool>::default_for_tests();
            let _type_check = SecondaryIndexTypes::RwLock(&index.spl_token_mint_index);
        }

        (0, PUBKEY_BYTES, test_utils::spl_token_mint_index_enabled())
    }

    fn create_spl_token_owner_secondary_index_state() -> (usize, usize, AccountSecondaryIndexes) {
        {
            // Check that we're actually testing the correct variant
            let index = AccountsIndex::<bool>::default_for_tests();
            let _type_check = SecondaryIndexTypes::RwLock(&index.spl_token_owner_index);
        }

        (
            SPL_TOKEN_ACCOUNT_OWNER_OFFSET,
            SPL_TOKEN_ACCOUNT_OWNER_OFFSET + PUBKEY_BYTES,
            test_utils::spl_token_owner_index_enabled(),
        )
    }

    #[test]
    fn test_get_empty() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool>::default_for_tests();
        let ancestors = Ancestors::default();
        let key = &key;
        assert!(!index.contains_with(key, &ancestors));
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

    const UPSERT_RECLAIM_TEST_DEFAULT: UpsertReclaim = UpsertReclaim::ReclaimOldSlots;

    #[test]
    fn test_insert_no_ancestors() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool>::default_for_tests();
        let mut gc = ReclaimsSlotList::new();
        index.upsert(0, 0, &key, true, &mut gc, UPSERT_RECLAIM_TEST_DEFAULT);
        assert!(gc.is_empty());

        let ancestors = Ancestors::default();
        assert!(index.contains_with(&key, &ancestors));
    }

    type AccountInfoTest = f64;

    impl IndexValue for AccountInfoTest {
        fn to_bits(self) -> u64 {
            self.to_bits()
        }
        fn from_bits(bits: u64) -> Self {
            Self::from_bits(bits)
        }
    }

    impl IsZeroLamport for AccountInfoTest {
        fn is_zero_lamport(&self) -> bool {
            true
        }
    }

    #[test]
    #[should_panic(expected = "Accounts may only be stored once per slot:")]
    fn test_insert_duplicates() {
        let key = solana_pubkey::new_rand();
        let pubkey = &key;
        let slot = 0;
        let mut ancestors = Ancestors::default();
        ancestors.insert(slot);

        let account_info = true;
        let index = AccountsIndex::<bool>::default_for_tests();
        let account_info2: bool = !account_info;
        let mut items = vec![(*pubkey, account_info), (*pubkey, account_info2)];
        index.set_startup(Startup::Startup);
        index.insert_new_if_missing_into_primary_index(slot, &mut items);
    }

    /// Startup storages are visited in random order, so a duplicate's older version can
    /// arrive after the newer one; it is recorded as a duplicate, not a same-slot store.
    #[test]
    fn test_insert_older_version_after_newer() {
        let key = solana_pubkey::new_rand();
        let newer_slot = 5;
        let older_slot = 3;

        let index = AccountsIndex::<bool>::default_for_tests();
        index.set_startup(Startup::Startup);

        let mut items = vec![(key, true)];
        index.insert_new_if_missing_into_primary_index(newer_slot, &mut items);

        let mut items = vec![(key, false)];
        let result = index.insert_new_if_missing_into_primary_index(older_slot, &mut items);
        assert_eq!(result.num_existed, 1);

        // the index keeps the newer version and the older version is a duplicate
        index.get_and_then(&key, |entry| {
            assert_eq!(entry.unwrap().slot_list(), [(newer_slot, true)]);
        });
        index.take_startup_duplicates(|duplicates| {
            assert_eq!(duplicates, vec![(older_slot, key, false)]);
        });
        index.set_startup(Startup::Normal);
    }

    #[test]
    fn test_insert_new_with_lock_no_ancestors() {
        let key = solana_pubkey::new_rand();
        let pubkey = &key;
        let slot = 0;

        let index = AccountsIndex::<bool>::default_for_tests();
        let account_info = true;
        let mut items = vec![(*pubkey, account_info)];
        index.set_startup(Startup::Startup);
        let expected_len = items.len();
        let result = index.insert_new_if_missing_into_primary_index(slot, &mut items);
        assert_eq!(result.count, expected_len);
        index.set_startup(Startup::Normal);

        let ancestors = Ancestors::default();
        assert!(index.contains_with(pubkey, &ancestors));
        assert_eq!(index.slot_list_len(pubkey), 1);

        // not zero lamports
        let index = AccountsIndex::<bool>::default_for_tests();
        let account_info = false;
        let mut items = vec![(*pubkey, account_info)];
        index.set_startup(Startup::Startup);
        let expected_len = items.len();
        let result = index.insert_new_if_missing_into_primary_index(slot, &mut items);
        assert_eq!(result.count, expected_len);
        index.set_startup(Startup::Normal);

        let ancestors = Ancestors::default();
        assert!(index.contains_with(pubkey, &ancestors));
        assert_eq!(index.slot_list_len(pubkey), 1);
    }

    #[test]
    fn test_batch_insert() {
        let slot0 = 0;
        let key0 = solana_pubkey::new_rand();
        let key1 = solana_pubkey::new_rand();

        let index = AccountsIndex::<bool>::default_for_tests();
        let account_infos = [true, false];

        index.set_startup(Startup::Startup);
        let mut items = vec![(key0, account_infos[0]), (key1, account_infos[1])];
        let expected_len = items.len();
        let result = index.insert_new_if_missing_into_primary_index(slot0, &mut items);
        assert_eq!(result.count, expected_len);
        index.set_startup(Startup::Normal);

        for (i, key) in [key0, key1].iter().enumerate() {
            index.get_and_then(key, |entry| {
                assert_eq!(
                    entry.unwrap().slot_list().as_ref(),
                    &[(slot0, account_infos[i])],
                );
            });
        }
    }

    /// insert a new pubkey, then update it at a newer slot; the older version is reclaimed
    #[test]
    fn test_new_entry_and_update_code_paths() {
        let slot0 = 0;
        let slot1 = 1;
        let key = solana_pubkey::new_rand();

        let index = AccountsIndex::<u64>::new(&ACCOUNTS_INDEX_CONFIG_FOR_TESTING, Arc::default());
        let mut gc = ReclaimsSlotList::new();

        // insert first entry for pubkey
        index.upsert(
            slot0,
            slot0,
            &key,
            1,
            &mut gc,
            UpsertReclaim::ReclaimOldSlots,
        );
        assert!(gc.is_empty());

        index.get_and_then(&key, |entry| {
            assert_eq!(entry.unwrap().slot_list().as_ref(), &[(slot0, 1)]);
        });

        // insert second entry for pubkey; the first is reclaimed
        index.upsert(
            slot1,
            slot1,
            &key,
            2,
            &mut gc,
            UpsertReclaim::ReclaimOldSlots,
        );
        assert_eq!(gc, ReclaimsSlotList::from([(slot0, 1)]));

        index.get_and_then(&key, |entry| {
            assert_eq!(entry.unwrap().slot_list().as_ref(), &[(slot1, 2)]);
        });
    }

    #[test]
    fn test_get_or_create_index_entry_for_pubkey_insert_new() {
        let index = AccountsIndex::<u64>::default_for_tests();
        let pubkey = solana_pubkey::new_rand();
        let slot = 0;

        let mut callback_called = false;
        index.get_or_create_index_entry_for_pubkey(&pubkey, (slot, 0), |_entry| {
            callback_called = true;
        });

        // there was no prior entry to update, so the new entry is created holding the new item
        assert!(!callback_called);
        index.get_and_then(&pubkey, |entry| {
            let entry = entry.expect("entry should be in the index");
            assert_eq!(entry.slot_list(), [(slot, 0)]);
        });
    }

    #[test]
    fn test_get_or_create_index_entry_for_pubkey_existing() {
        let index = AccountsIndex::<u64>::default_for_tests();
        let pubkey = solana_pubkey::new_rand();

        // Insert an entry manually
        index
            .map
            .insert(pubkey, AccountMapEntry::new(SlotList::from([(0, 42)])));

        let mut callback_called = false;
        index.get_or_create_index_entry_for_pubkey(&pubkey, (1, 43), |entry| {
            assert_eq!(entry.slot_list(), [(0, 42)]);
            callback_called = true;
        });

        assert!(callback_called);
    }

    /// The index holds the entry from the newest slot, so an update at an older slot keeps the
    /// entry it has and reclaims the older version instead
    #[test_case(None; "no other slot")]
    #[test_case(Some(3); "other slot is not the one in the index")]
    fn test_update_slot_list_older_slot(other_slot: Option<Slot>) {
        let entry = AccountMapEntry::<u64>::new([(2, 20)]);

        let older_version = entry.replace_if_newer((1, 10), other_slot);

        assert_eq!(entry.slot_list(), [(2, 20)]);
        assert_eq!(older_version, (1, 10));
    }

    #[test]
    fn test_lock_and_update_slot_list() {
        let test = AccountMapEntry::<u64>::new([(1, 65)]);
        let info = 66;
        let mut reclaims = ReclaimsSlotList::new();

        // update at the same slot replaces the entry
        AccountsIndex::<u64>::lock_and_update_slot_list(
            &test,
            (1, info),
            None,
            &mut reclaims,
            UpsertReclaim::ReclaimOldSlots,
        );
        assert_eq!(test.slot_list(), [(1, info)]);
        assert_eq!(reclaims, ReclaimsSlotList::from([(1, 65)]));

        // update at a newer slot replaces the entry, reclaiming the older one
        reclaims.clear();
        AccountsIndex::<u64>::lock_and_update_slot_list(
            &test,
            (2, info),
            None,
            &mut reclaims,
            UpsertReclaim::ReclaimOldSlots,
        );
        assert_eq!(test.slot_list(), [(2, info)]);
        assert_eq!(reclaims, ReclaimsSlotList::from([(1, info)]));
    }

    /// `delete` on an entry drains every slot-list item into reclaims and removes the
    /// entry from the index.
    #[test]
    fn test_delete_entry() {
        let index = AccountsIndex::<u64>::default_for_tests();
        let pubkey = Pubkey::new_unique();
        let mut reclaims = ReclaimsSlotList::new();

        index.upsert(
            2,
            2,
            &pubkey,
            20,
            &mut ReclaimsSlotList::new(),
            UpsertReclaim::IgnoreReclaims,
        );

        index.delete(&pubkey, &mut reclaims);

        assert_eq!(reclaims, vec![(2, 20)]);
        assert!(!index.contains(&pubkey));
    }

    /// `delete` on a pubkey that was never indexed reclaims nothing and does not create an
    /// index entry.
    #[test]
    fn test_delete_missing_pubkey() {
        let index = AccountsIndex::<u64>::default_for_tests();
        let pubkey = Pubkey::new_unique();
        let mut reclaims = ReclaimsSlotList::new();

        index.delete(&pubkey, &mut reclaims);

        assert!(reclaims.is_empty());
        assert!(!index.contains(&pubkey));
    }

    /// An indexed pubkey always has an entry, so only a missing pubkey is reported removed.
    #[test]
    fn test_handle_dead_keys_only_reports_missing() {
        let key = solana_pubkey::new_rand();
        let unknown_key = solana_pubkey::new_rand();
        let index = AccountsIndex::<u64>::default_for_tests();

        index.upsert(
            1,
            1,
            &key,
            1,
            &mut ReclaimsSlotList::new(),
            UpsertReclaim::IgnoreReclaims,
        );

        assert_eq!(
            index.handle_dead_keys(&[key, unknown_key]),
            vec![unknown_key]
        );
        // the indexed pubkey is still in the index
        assert!(index.contains(&key));
    }

    #[test]
    fn test_new_with_num_initial_accounts() {
        let config = AccountsIndexConfig {
            num_initial_accounts: Some(10_000),
            ..AccountsIndexConfig::default()
        };
        let index = AccountsIndex::<u64>::new(&config, Arc::default());
        let (_len, capacity) = index.len_and_capacity();
        assert!(capacity >= 10_000);
    }

    #[test]
    fn test_insert_with_ancestors() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool>::default_for_tests();
        let mut gc = ReclaimsSlotList::new();
        index.upsert(0, 0, &key, true, &mut gc, UPSERT_RECLAIM_TEST_DEFAULT);
        assert!(gc.is_empty());

        let ancestors = Ancestors::from(vec![0]);
        index
            .get_with_and_then(&key, &ancestors, |(slot, account_info)| {
                assert_eq!(slot, 0);
                assert!(account_info);
            })
            .unwrap();
    }

    #[test]
    fn test_insert_with_root() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool>::default_for_tests();
        let mut gc = ReclaimsSlotList::new();
        index.upsert(0, 0, &key, true, &mut gc, UPSERT_RECLAIM_TEST_DEFAULT);
        assert!(gc.is_empty());

        let ancestors = Ancestors::from(vec![0]);
        index
            .get_with_and_then(&key, &ancestors, |(slot, account_info)| {
                assert_eq!(slot, 0);
                assert!(account_info);
            })
            .unwrap();
    }

    #[test]
    fn test_update_last_wins() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<u64>::default_for_tests();
        let ancestors = Ancestors::from(vec![0]);
        let mut gc = ReclaimsSlotList::new();
        index.upsert(0, 0, &key, 1, &mut gc, UPSERT_RECLAIM_TEST_DEFAULT);
        assert!(gc.is_empty());
        index
            .get_with_and_then(&key, &ancestors, |(slot, account_info)| {
                assert_eq!(slot, 0);
                assert_eq!(account_info, 1);
            })
            .unwrap();

        let mut gc = ReclaimsSlotList::new();
        index.upsert(0, 0, &key, 0, &mut gc, UPSERT_RECLAIM_TEST_DEFAULT);
        assert_eq!(gc, ReclaimsSlotList::from([(0, 1)]));
        index
            .get_with_and_then(&key, &ancestors, |(slot, account_info)| {
                assert_eq!(slot, 0);
                assert_eq!(account_info, 0);
            })
            .unwrap();
    }

    #[test]
    fn test_upsert_reclaims() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<u64>::default_for_tests();
        let mut reclaims = ReclaimsSlotList::new();

        index.upsert(0, 0, &key, 0, &mut reclaims, UPSERT_RECLAIM_TEST_DEFAULT);
        // Cached item should not be reclaimed
        assert!(reclaims.is_empty());

        // Slot list should only have a single entry
        assert_eq!(index.slot_list_len(&key), 1);

        index.upsert(0, 0, &key, 0, &mut reclaims, UPSERT_RECLAIM_TEST_DEFAULT);

        // Uncached item should be returned as reclaim
        assert!(!reclaims.is_empty());

        // Slot list should only have a single entry
        assert_eq!(index.slot_list_len(&key), 1);
    }

    #[test]
    fn test_replace_same_slot() {
        // When new_slot == old_slot, replace acts as an in-place update of the account_info.
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<u64>::default_for_tests();
        let mut gc = ReclaimsSlotList::new();

        let slot = 5;
        index.upsert(
            slot,
            slot,
            &key,
            100,
            &mut gc,
            UpsertReclaim::IgnoreReclaims,
        );
        assert_eq!(index.slot_list_len(&key), 1);

        let account_info = 200;

        index.replace(slot, slot, &key, account_info);

        // Slot list now holds the new account_info at the same slot.
        let slot_list = index.get_and_then(&key, |entry| entry.unwrap().slot_list());
        assert_eq!(slot_list, SlotList::from([(slot, account_info)]));
        // Replace doesn't change the slot list length.
        assert_eq!(index.slot_list_len(&key), 1);
    }

    #[test]
    fn test_replace_moves_entry_to_new_slot() {
        // Replace finds the entry at old_slot, swaps it out for one at new_slot.
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<u64>::default_for_tests();
        let mut gc = ReclaimsSlotList::new();

        let old_slot = 5;
        let new_slot = 10;
        let account_info = 200;
        index.upsert(
            old_slot,
            old_slot,
            &key,
            100,
            &mut gc,
            UpsertReclaim::IgnoreReclaims,
        );
        assert_eq!(index.slot_list_len(&key), 1);

        index.replace(new_slot, old_slot, &key, account_info);

        let slot_list = index.get_and_then(&key, |entry| entry.unwrap().slot_list());
        assert_eq!(slot_list, SlotList::from([(new_slot, account_info)]));
        // Moving an entry between slots must not change the slot list length.
        assert_eq!(index.slot_list_len(&key), 1);
    }

    #[test]
    #[should_panic(expected = "index holds an entry from an older slot")]
    fn test_replace_missing_old_slot_panics() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<u64>::default_for_tests();
        let mut gc = ReclaimsSlotList::new();

        index.upsert(5, 5, &key, 100, &mut gc, UpsertReclaim::IgnoreReclaims);
        // No entry at slot 99 — replace must panic rather than silently appending.
        index.replace(10, 99, &key, 200);
    }

    #[test]
    fn test_latest_slot() {
        let slot_slice = vec![(0, true), (5, true), (3, true), (7, true)];
        let index = AccountsIndex::<bool>::default_for_tests();

        // No ancestors: every slot is a root, so return the newest slot (7)
        assert_eq!(index.latest_slot(None, &slot_slice, None).unwrap(), 3);

        // Given a max_root, should return the newest slot <= max_root (5)
        assert_eq!(index.latest_slot(None, &slot_slice, Some(5)).unwrap(), 1);

        // Given a max_root between slots, should return the newest slot <= max_root (3)
        assert_eq!(index.latest_slot(None, &slot_slice, Some(4)).unwrap(), 2);

        // Given a max_root, should filter out roots < max_root, but specified
        // ancestors should not be affected
        let ancestors = Ancestors::from(vec![3, 7]);
        assert_eq!(
            index
                .latest_slot(Some(&ancestors), &slot_slice, Some(4))
                .unwrap(),
            3
        );
        assert_eq!(
            index
                .latest_slot(Some(&ancestors), &slot_slice, Some(7))
                .unwrap(),
            3
        );

        // Given no max_root, should just return the greatest ancestor or root
        assert_eq!(
            index
                .latest_slot(Some(&ancestors), &slot_slice, None)
                .unwrap(),
            3
        );

        // Given ancestors that are *older* than the newest root, should still return ancestors
        let ancestors = Ancestors::from(vec![3]);
        assert_eq!(
            index
                .latest_slot(Some(&ancestors), &slot_slice, None)
                .unwrap(),
            2
        );
    }

    fn make_empty_token_account_data() -> Vec<u8> {
        const SPL_TOKEN_INITIALIZED_OFFSET: usize = 108;
        let mut data = vec![0; spl_generic_token::token::Account::get_packed_len()];
        data[SPL_TOKEN_INITIALIZED_OFFSET] = 1;
        data
    }

    fn run_test_purge_exact_secondary_index<
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    >(
        index: &AccountsIndex<bool>,
        secondary_index: &SecondaryIndex<SecondaryIndexEntryType>,
        key_start: usize,
        key_end: usize,
        secondary_indexes: &AccountSecondaryIndexes,
    ) {
        // No roots, should be no reclaims
        let slots = vec![1, 2, 5, 9];
        let index_key = Pubkey::new_unique();
        let account_key = Pubkey::new_unique();

        let mut account_data = make_empty_token_account_data();
        account_data[key_start..key_end].clone_from_slice(&(index_key.to_bytes()));

        // Insert slots into secondary index
        for slot in &slots {
            index.upsert(
                *slot,
                *slot,
                &account_key,
                true,
                &mut ReclaimsSlotList::new(),
                UPSERT_RECLAIM_TEST_DEFAULT,
            );
            // Make sure these accounts are added to secondary index
            index.update_secondary_indexes(
                &account_key,
                &AccountSharedData::create_from_existing_shared_data(
                    0,
                    Arc::new(account_data.to_vec()),
                    spl_generic_token::token::id(),
                    false,
                    0,
                ),
                secondary_indexes,
            );
        }

        // Only one top level index entry exists
        assert_eq!(secondary_index.index.get(&index_key).unwrap().len(), 1);

        // In the reverse index, one account maps across multiple slots
        // to the same top level key
        assert_eq!(
            secondary_index
                .reverse_index
                .get(&account_key)
                .unwrap()
                .value()
                .read()
                .unwrap()
                .len(),
            1
        );

        // the index holds the entry from the newest slot only
        index.purge_exact(
            &account_key,
            slots.into_iter().collect::<HashSet<Slot>>(),
            &mut ReclaimsSlotList::new(),
        );

        let pubkeys = index.handle_dead_keys(&[account_key]);
        for pubkey in pubkeys {
            index.purge_secondary_indexes_by_inner_key_if(&pubkey, secondary_indexes, || true);
        }
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());
    }

    #[test]
    fn test_purge_exact_spl_token_mint_secondary_index() {
        let (key_start, key_end, secondary_indexes) = create_spl_token_mint_secondary_index_state();
        let index = AccountsIndex::<bool>::default_for_tests();
        run_test_purge_exact_secondary_index(
            &index,
            &index.spl_token_mint_index,
            key_start,
            key_end,
            &secondary_indexes,
        );
    }

    #[test]
    fn test_purge_exact_spl_token_owner_secondary_index() {
        let (key_start, key_end, secondary_indexes) =
            create_spl_token_owner_secondary_index_state();
        let index = AccountsIndex::<bool>::default_for_tests();
        run_test_purge_exact_secondary_index(
            &index,
            &index.spl_token_owner_index,
            key_start,
            key_end,
            &secondary_indexes,
        );
    }

    fn check_secondary_index_mapping_correct<SecondaryIndexEntryType>(
        secondary_index: &SecondaryIndex<SecondaryIndexEntryType>,
        secondary_index_keys: &[Pubkey],
        account_key: &Pubkey,
    ) where
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    {
        // Check secondary index has unique mapping from secondary index key
        // to the account key and slot
        for secondary_index_key in secondary_index_keys {
            assert_eq!(secondary_index.index.len(), secondary_index_keys.len());
            let account_key_map = secondary_index.get(secondary_index_key);
            assert_eq!(account_key_map.len(), 1);
            assert_eq!(account_key_map, vec![*account_key]);
        }
        // Check reverse index contains all of the `secondary_index_keys`
        let secondary_index_key_map = secondary_index.reverse_index.get(account_key).unwrap();
        assert_eq!(
            &*secondary_index_key_map.value().read().unwrap(),
            secondary_index_keys
        );
    }

    fn run_test_spl_token_secondary_indexes<
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    >(
        token_id: &Pubkey,
        index: &AccountsIndex<bool>,
        secondary_index: &SecondaryIndex<SecondaryIndexEntryType>,
        key_start: usize,
        key_end: usize,
        secondary_indexes: &AccountSecondaryIndexes,
    ) {
        let mut secondary_indexes = secondary_indexes.clone();
        let account_key = Pubkey::new_unique();
        let index_key = Pubkey::new_unique();
        let mut account_data = make_empty_token_account_data();
        account_data[key_start..key_end].clone_from_slice(&(index_key.to_bytes()));

        // Wrong program id
        index.upsert(
            0,
            0,
            &account_key,
            true,
            &mut ReclaimsSlotList::new(),
            UPSERT_RECLAIM_TEST_DEFAULT,
        );
        index.update_secondary_indexes(
            &account_key,
            &AccountSharedData::create_from_existing_shared_data(
                0,
                Arc::new(account_data.to_vec()),
                Pubkey::default(),
                false,
                0,
            ),
            &secondary_indexes,
        );
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());

        // Wrong account data size
        index.upsert(
            0,
            0,
            &account_key,
            true,
            &mut ReclaimsSlotList::new(),
            UPSERT_RECLAIM_TEST_DEFAULT,
        );
        index.update_secondary_indexes(
            &account_key,
            &AccountSharedData::create_from_existing_shared_data(
                0,
                Arc::new(account_data[1..].to_vec()),
                *token_id,
                false,
                0,
            ),
            &secondary_indexes,
        );
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());

        secondary_indexes.keys = None;

        // Just right. Inserting the same index multiple times should be ok
        for _ in 0..2 {
            index.update_secondary_indexes(
                &account_key,
                &AccountSharedData::create_from_existing_shared_data(
                    0,
                    Arc::new(account_data.to_vec()),
                    *token_id,
                    false,
                    0,
                ),
                &secondary_indexes,
            );
            check_secondary_index_mapping_correct(secondary_index, &[index_key], &account_key);
        }

        // included
        assert!(!secondary_index.index.is_empty());
        assert!(!secondary_index.reverse_index.is_empty());

        secondary_indexes.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [index_key].iter().cloned().collect::<HashSet<_>>(),
            exclude: false,
        });
        secondary_index.index.clear();
        secondary_index.reverse_index.clear();
        index.update_secondary_indexes(
            &account_key,
            &AccountSharedData::create_from_existing_shared_data(
                0,
                Arc::new(account_data.to_vec()),
                *token_id,
                false,
                0,
            ),
            &secondary_indexes,
        );
        assert!(!secondary_index.index.is_empty());
        assert!(!secondary_index.reverse_index.is_empty());
        check_secondary_index_mapping_correct(secondary_index, &[index_key], &account_key);

        // not-excluded
        secondary_indexes.keys = Some(AccountSecondaryIndexesIncludeExclude {
            keys: [].iter().cloned().collect::<HashSet<_>>(),
            exclude: true,
        });
        secondary_index.index.clear();
        secondary_index.reverse_index.clear();
        index.update_secondary_indexes(
            &account_key,
            &AccountSharedData::create_from_existing_shared_data(
                0,
                Arc::new(account_data.to_vec()),
                *token_id,
                false,
                0,
            ),
            &secondary_indexes,
        );
        assert!(!secondary_index.index.is_empty());
        assert!(!secondary_index.reverse_index.is_empty());
        check_secondary_index_mapping_correct(secondary_index, &[index_key], &account_key);

        secondary_indexes.keys = None;

        // remove the account from the index so that it is a dead key
        index.remove_entry_if(&account_key, |_entry| true);

        // Everything should be deleted
        let pubkeys = index.handle_dead_keys(&[account_key]);
        for pubkey in pubkeys {
            index.purge_secondary_indexes_by_inner_key_if(&pubkey, &secondary_indexes, || true);
        }
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());
    }

    #[test]
    fn test_spl_token_mint_secondary_index() {
        let (key_start, key_end, secondary_indexes) = create_spl_token_mint_secondary_index_state();
        let index = AccountsIndex::<bool>::default_for_tests();
        for token_id in &spl_token_ids() {
            run_test_spl_token_secondary_indexes(
                token_id,
                &index,
                &index.spl_token_mint_index,
                key_start,
                key_end,
                &secondary_indexes,
            );
        }
    }

    #[test]
    fn test_spl_token_owner_secondary_index() {
        let (key_start, key_end, secondary_indexes) =
            create_spl_token_owner_secondary_index_state();
        let index = AccountsIndex::<bool>::default_for_tests();
        for token_id in &spl_token_ids() {
            run_test_spl_token_secondary_indexes(
                token_id,
                &index,
                &index.spl_token_owner_index,
                key_start,
                key_end,
                &secondary_indexes,
            );
        }
    }

    fn run_test_secondary_indexes_same_slot_and_forks<
        SecondaryIndexEntryType: SecondaryIndexEntry + Default + Sync + Send,
    >(
        token_id: &Pubkey,
        index: &AccountsIndex<bool>,
        secondary_index: &SecondaryIndex<SecondaryIndexEntryType>,
        index_key_start: usize,
        index_key_end: usize,
        secondary_indexes: &AccountSecondaryIndexes,
    ) {
        let account_key = Pubkey::new_unique();
        let secondary_key1 = Pubkey::new_unique();
        let secondary_key2 = Pubkey::new_unique();
        let slot = 1;
        let mut account_data1 = make_empty_token_account_data();
        account_data1[index_key_start..index_key_end]
            .clone_from_slice(&(secondary_key1.to_bytes()));
        let mut account_data2 = make_empty_token_account_data();
        account_data2[index_key_start..index_key_end]
            .clone_from_slice(&(secondary_key2.to_bytes()));

        // First write one mint index
        index.upsert(
            slot,
            slot,
            &account_key,
            true,
            &mut ReclaimsSlotList::new(),
            UPSERT_RECLAIM_TEST_DEFAULT,
        );
        index.update_secondary_indexes(
            &account_key,
            &AccountSharedData::create_from_existing_shared_data(
                0,
                Arc::new(account_data1.to_vec()),
                *token_id,
                false,
                0,
            ),
            secondary_indexes,
        );

        // Now write a different mint index for the same account
        index.upsert(
            slot,
            slot,
            &account_key,
            true,
            &mut ReclaimsSlotList::new(),
            UPSERT_RECLAIM_TEST_DEFAULT,
        );
        index.update_secondary_indexes(
            &account_key,
            &AccountSharedData::create_from_existing_shared_data(
                0,
                Arc::new(account_data2.to_vec()),
                *token_id,
                false,
                0,
            ),
            secondary_indexes,
        );

        // Both pubkeys will now be present in the index
        check_secondary_index_mapping_correct(
            secondary_index,
            &[secondary_key1, secondary_key2],
            &account_key,
        );

        // If a later slot also introduces secondary_key1, then it should still exist in the index
        let later_slot = slot + 1;
        index.upsert(
            later_slot,
            later_slot,
            &account_key,
            true,
            &mut ReclaimsSlotList::new(),
            UPSERT_RECLAIM_TEST_DEFAULT,
        );
        index.update_secondary_indexes(
            &account_key,
            &AccountSharedData::create_from_existing_shared_data(
                0,
                Arc::new(account_data1.to_vec()),
                *token_id,
                false,
                0,
            ),
            secondary_indexes,
        );
        assert_eq!(secondary_index.get(&secondary_key1), vec![account_key]);

        // If we set a root at `later_slot`, and clean, then even though the account with secondary_key1
        // was outdated by the update in the later slot, the primary account key is still alive,
        // so both secondary keys will still be kept alive.
        let _ = index.clean_rooted_entries(&account_key, &mut ReclaimsWithNewestSlot::new(), None);

        check_secondary_index_mapping_correct(
            secondary_index,
            &[secondary_key1, secondary_key2],
            &account_key,
        );

        // Removing the remaining entry for this pubkey in the index should mark the
        // pubkey as dead and finally remove all the secondary indexes
        let mut reclaims = ReclaimsSlotList::new();
        index.purge_exact(&account_key, later_slot, &mut reclaims);
        let pubkeys = index.handle_dead_keys(&[account_key]);
        for pubkey in pubkeys {
            index.purge_secondary_indexes_by_inner_key_if(&pubkey, secondary_indexes, || true);
        }
        assert!(secondary_index.index.is_empty());
        assert!(secondary_index.reverse_index.is_empty());
    }

    #[test]
    fn test_spl_token_mint_secondary_index_same_slot_and_forks() {
        let (key_start, key_end, account_index) = create_spl_token_mint_secondary_index_state();
        let index = AccountsIndex::<bool>::default_for_tests();
        for token_id in &spl_token_ids() {
            run_test_secondary_indexes_same_slot_and_forks(
                token_id,
                &index,
                &index.spl_token_mint_index,
                key_start,
                key_end,
                &account_index,
            );
        }
    }

    #[test]
    fn test_rwlock_secondary_index_same_slot_and_forks() {
        let (key_start, key_end, account_index) = create_spl_token_owner_secondary_index_state();
        let index = AccountsIndex::<bool>::default_for_tests();
        for token_id in &spl_token_ids() {
            run_test_secondary_indexes_same_slot_and_forks(
                token_id,
                &index,
                &index.spl_token_owner_index,
                key_start,
                key_end,
                &account_index,
            );
        }
    }

    impl IndexValue for bool {
        fn to_bits(self) -> u64 {
            self as u64
        }
        fn from_bits(bits: u64) -> Self {
            bits != 0
        }
    }
    impl IndexValue for u64 {
        fn to_bits(self) -> u64 {
            self
        }
        fn from_bits(bits: u64) -> Self {
            bits
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

    #[test]
    fn test_handle_dead_keys_return() {
        let key = solana_pubkey::new_rand();
        let index = AccountsIndex::<bool>::default_for_tests();

        assert_eq!(index.handle_dead_keys(&[key]), vec![key]);
    }
}
