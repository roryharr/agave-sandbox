//! ReadOnlyAccountsCache used to store accounts, such as executable accounts,
//! which can be large, loaded many times, and rarely change.
//!
//! The cache's pubkey map doubles as the write cache's index: each entry carries the highest
//! slot the pubkey may be stored at in the write cache and the number of slot caches holding
//! it, alongside the optional read-cached account. A load probes this one map to learn about
//! both caches.
//!
//! The map is lock free for readers: a probe writes no shared line, where a sharded-lock map
//! pays an atomic read-lock round trip per probe. Structural updates replace the entry via
//! compare-and-swap; only the eviction stamp is written in place.
//!
//! There is no eviction in this proof of concept — the configured size limit was never
//! reached in practice, so the cache simply grows with the set of accounts loaded from
//! storage. `data_size` still reports what it holds.
#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::{field_qualifiers, qualifiers};
use {
    crate::accounts_cache::CachedAccount,
    ahash::random_state::RandomState as AHashRandomState,
    papaya::{Compute, HashMap, Operation},
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::Slot,
    solana_measure::{measure::Measure, measure_us},
    solana_pubkey::Pubkey,
    std::{
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::Instant,
    },
};

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
const CACHE_ENTRY_SIZE: usize = size_of::<CacheEntry>() + size_of::<ReadOnlyCacheKey>();

/// A hit only refreshes an entry's stamp once this interval has passed. Re-stamping on every
/// hit would dirty the entry's cacheline, costing every other reader of a hot account a
/// coherence miss on their next probe.
const LRU_STAMP_INTERVAL_NS: u64 = 10_000_000;

type ReadOnlyCacheKey = Pubkey;

/// One entry per pubkey present in either cache.
#[derive(Debug, Default)]
struct CacheEntry {
    /// the newest rooted version, flushed to storage (the read cache)
    read: Option<ReadOnlyAccountCacheEntry>,
    /// the write-cache version at `max_slot`, when that slot cache still holds it. A load
    /// whose ancestors contain this slot is answered from here without searching the slot
    /// caches. `None` after the version's slot is flushed while others still hold the pubkey.
    latest_write: Option<(Slot, Arc<CachedAccount>)>,
    /// the highest slot at which the pubkey has been written into the write cache. May be
    /// stale after a removal; `load_latest` handles a miss at this slot by scanning all
    /// slots in the write cache.
    max_slot: Slot,
    /// the number of slot caches currently holding the pubkey. Zero means the pubkey is not
    /// in the write cache.
    ref_count: u32,
}

impl CacheEntry {
    /// A copy of this entry for a compare-and-swap replacement. Written by hand because the
    /// eviction stamp is an atomic; its value is carried over.
    fn duplicate(&self) -> Self {
        Self {
            read: self.read.as_ref().map(|read| ReadOnlyAccountCacheEntry {
                account: read.account.clone(),
                slot: read.slot,
                last_update_time: AtomicU64::new(read.last_update_time.load(Ordering::Relaxed)),
            }),
            latest_write: self.latest_write.clone(),
            max_slot: self.max_slot,
            ref_count: self.ref_count,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
#[cfg_attr(
    feature = "dev-context-only-utils",
    field_qualifiers(account(pub), slot(pub), last_update_time(pub))
)]
struct ReadOnlyAccountCacheEntry {
    account: AccountSharedData,
    /// 'slot' tracks when the 'account' is stored. This important for
    /// correctness. When 'loading' from the cache by pubkey+slot, we need to
    /// make sure that both pubkey and slot matches in the cache. Otherwise, we
    /// may return the wrong account.
    slot: Slot,
    /// Timestamp when the entry was updated, in ns
    last_update_time: AtomicU64,
}

/// what one probe of the cache's pubkey map found, covering both caches
#[derive(Debug)]
pub(crate) enum Probe {
    /// the pubkey is in neither cache
    Absent,
    /// not in the write cache; the read cache holds this visible version
    Read(AccountSharedData, Slot),
    /// in the write cache: `latest_write` is the version at the highest cached slot, and
    /// answers the load directly when that slot is an ancestor; otherwise search the slot
    /// caches, bounded by `max_slot`. The account is cloned here, under the map guard,
    /// rather than handing out the `Arc<CachedAccount>`: reaching through that Arc and
    /// dropping it costs a second contended refcount round trip on the hot account's line.
    Write {
        max_slot: Slot,
        latest_write: Option<(Slot, AccountSharedData)>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct ReadOnlyCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub load_us: u64,
    pub store_us: u64,
}

/// A counter striped across cachelines, so bumps from many threads do not all contend on one
/// line. Totals are summed at report time.
#[derive(Debug)]
struct StripedCounter([PaddedAtomicU64; STRIPES]);

const STRIPES: usize = 64;

#[derive(Debug, Default)]
#[repr(align(128))]
struct PaddedAtomicU64(AtomicU64);

impl Default for StripedCounter {
    fn default() -> Self {
        Self(std::array::from_fn(|_index| PaddedAtomicU64::default()))
    }
}

impl StripedCounter {
    fn add_one(&self) {
        self.0[stripe_index()].0.fetch_add(1, Ordering::Relaxed);
    }

    fn swap_total(&self) -> u64 {
        self.0
            .iter()
            .map(|stripe| stripe.0.swap(0, Ordering::Relaxed))
            .sum()
    }
}

/// this thread's stripe, assigned round-robin the first time the thread bumps a counter
fn stripe_index() -> usize {
    static NEXT_STRIPE_INDEX: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static STRIPE_INDEX: usize =
            NEXT_STRIPE_INDEX.fetch_add(1, Ordering::Relaxed) % STRIPES;
    }
    STRIPE_INDEX.with(|stripe_index| *stripe_index)
}

#[derive(Default, Debug)]
struct AtomicReadOnlyCacheStats {
    hits: StripedCounter,
    misses: StripedCounter,
    load_us: AtomicU64,
    store_us: AtomicU64,
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
#[derive(Debug)]
pub(crate) struct ReadOnlyAccountsCache {
    cache: HashMap<ReadOnlyCacheKey, CacheEntry, AHashRandomState>,
    data_size: AtomicUsize,
    cache_len: AtomicUsize,
    /// The number of pubkeys currently in the write cache, for reporting purposes. This is to
    /// avoid having to lock each shard of the map to count them on demand
    num_write_pubkeys: AtomicU64,

    // Performance statistics
    stats: AtomicReadOnlyCacheStats,
    highest_slot_stored: AtomicU64,

    /// Timer for generating timestamps for entries.
    timer: Instant,
}

impl ReadOnlyAccountsCache {
    /// The size limit and eviction parameters are accepted and ignored: this proof of concept
    /// does not evict.
    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    pub(crate) fn new(
        _max_data_size_lo: usize,
        _max_data_size_hi: usize,
        _evict_sample_size: usize,
        _num_shards: usize,
    ) -> Self {
        let cache = HashMap::builder()
            .hasher(AHashRandomState::default())
            .build();
        Self {
            highest_slot_stored: AtomicU64::default(),
            cache,
            data_size: AtomicUsize::default(),
            cache_len: AtomicUsize::default(),
            num_write_pubkeys: AtomicU64::default(),
            stats: AtomicReadOnlyCacheStats::default(),
            timer: Instant::now(),
        }
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    pub(crate) fn load(&self, pubkey: Pubkey, slot: Slot) -> Option<AccountSharedData> {
        self.load_visible(&pubkey, |cached_slot| cached_slot == slot)
            .map(|(account, _slot)| account)
    }

    /// Load `pubkey`'s cached account, and the slot it was cached at, if `is_visible` accepts
    /// that slot.
    ///
    /// The cache holds one version per pubkey, and flushing a newer version to storage drops
    /// that pubkey's entry, so a hit is the newest version in storage.
    pub(crate) fn load_visible(
        &self,
        pubkey: &Pubkey,
        is_visible: impl FnOnce(Slot) -> bool,
    ) -> Option<(AccountSharedData, Slot)> {
        let (found, load_us) = measure_us!({
            let mut found = None;
            let guard = self.cache.pin();
            if let Some(entry) = guard.get(pubkey)
                && let Some(read) = entry.read.as_ref()
                && is_visible(read.slot)
            {
                read.refresh_last_update_time(self.timestamp());
                found = Some((read.account.clone(), read.slot));
            }
            drop(guard);

            if found.is_some() {
                self.stats.hits.add_one();
            } else {
                self.stats.misses.add_one();
            }
            found
        });
        self.stats.load_us.fetch_add(load_us, Ordering::Relaxed);
        found
    }

    /// One probe of the pubkey map, covering both caches: reports the write cache's presence,
    /// or the visible read-cached version when the pubkey is not in the write cache.
    ///
    /// The read half is deliberately not returned when the pubkey is in the write cache: the
    /// caller searches the slot caches first and, on a miss there, calls `load_visible`, so a
    /// version flushed during that search is never shadowed by a stale read-cache snapshot.
    pub(crate) fn probe(
        &self,
        pubkey: &Pubkey,
        is_read_visible: impl FnOnce(Slot) -> bool,
    ) -> Probe {
        let guard = self.cache.pin();
        let Some(entry) = guard.get(pubkey) else {
            drop(guard);
            self.stats.misses.add_one();
            return Probe::Absent;
        };
        if entry.ref_count > 0 {
            return Probe::Write {
                max_slot: entry.max_slot,
                latest_write: entry
                    .latest_write
                    .as_ref()
                    .map(|(slot, cached_account)| (*slot, cached_account.account.clone())),
            };
        }
        // not in the write cache: an entry only exists for its read half
        if let Some(read) = entry.read.as_ref()
            && is_read_visible(read.slot)
        {
            read.refresh_last_update_time(self.timestamp());
            let account_and_slot = (read.account.clone(), read.slot);
            drop(guard);
            self.stats.hits.add_one();
            return Probe::Read(account_and_slot.0, account_and_slot.1);
        }
        drop(guard);
        self.stats.misses.add_one();
        Probe::Absent
    }

    fn account_size(account: &AccountSharedData) -> usize {
        CACHE_ENTRY_SIZE + account.data().len()
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    pub(crate) fn store(&self, pubkey: Pubkey, slot: Slot, account: AccountSharedData) {
        self.store_with_timestamp(pubkey, slot, account, self.timestamp())
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    fn store_with_timestamp(
        &self,
        pubkey: Pubkey,
        slot: Slot,
        account: AccountSharedData,
        timestamp: u64,
    ) {
        let measure_store = Measure::start("");
        self.highest_slot_stored.fetch_max(slot, Ordering::Release);
        let new_account_size = Self::account_size(&account);
        let guard = self.cache.pin();
        let compute = guard.compute(pubkey, |current| {
            let mut new_entry = match current {
                Some((_pubkey, entry)) => entry.duplicate(),
                None => CacheEntry::default(),
            };
            new_entry.read = Some(ReadOnlyAccountCacheEntry::new(
                account.clone(),
                slot,
                timestamp,
            ));
            Operation::Insert::<_, ()>(new_entry)
        });
        let old_account_size = match compute {
            Compute::Inserted(_pubkey, _entry) => {
                self.cache_len.fetch_add(1, Ordering::Relaxed);
                0
            }
            Compute::Updated {
                old: (_pubkey, old_entry),
                ..
            } => match old_entry.read.as_ref() {
                Some(old_read) => Self::account_size(&old_read.account),
                None => {
                    self.cache_len.fetch_add(1, Ordering::Relaxed);
                    0
                }
            },
            Compute::Removed(..) | Compute::Aborted(..) => unreachable!("store always inserts"),
        };
        drop(guard);
        update_stat(&self.data_size, old_account_size, new_account_size);
        let store_us = measure_store.end_as_us();
        self.stats.store_us.fetch_add(store_us, Ordering::Relaxed);
    }

    /// true if any pubkeys could have ever been stored into the cache at `slot`
    pub(crate) fn can_slot_be_in_cache(&self, slot: Slot) -> bool {
        self.highest_slot_stored.load(Ordering::Acquire) >= slot
    }

    /// remove entry if it exists.
    /// Assume the entry does not exist for performance.
    pub(crate) fn remove_assume_not_present(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        // read first to see if a read-cached account exists
        self.cache
            .pin()
            .get(pubkey)
            .is_some_and(|entry| entry.read.is_some())
            .then(|| self.remove(pubkey))
            .flatten()
    }

    /// Removes `pubkey`'s read-cached account, if present, and returns it. The entry itself is
    /// removed once neither cache holds the pubkey.
    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    pub(crate) fn remove(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        let guard = self.cache.pin();
        let compute = guard.compute(*pubkey, |current| {
            let Some((_pubkey, entry)) = current else {
                return Operation::Abort(());
            };
            if entry.read.is_none() {
                Operation::Abort(())
            } else if entry.ref_count == 0 {
                Operation::Remove
            } else {
                let mut new_entry = entry.duplicate();
                new_entry.read = None;
                Operation::Insert(new_entry)
            }
        });
        let removed_read = match compute {
            Compute::Removed(_pubkey, entry) => entry.read.as_ref().map(|read| &read.account),
            Compute::Updated {
                old: (_pubkey, old_entry),
                ..
            } => old_entry.read.as_ref().map(|read| &read.account),
            Compute::Aborted(()) => None,
            Compute::Inserted(..) => unreachable!("remove never creates an entry"),
        };
        let removed_read = removed_read.cloned();
        drop(guard);
        if let Some(account) = removed_read.as_ref() {
            self.data_size
                .fetch_sub(Self::account_size(account), Ordering::Relaxed);
            self.cache_len.fetch_sub(1, Ordering::Relaxed);
        }
        removed_read
    }

    /// Record that `pubkey` was written into the write cache at `slot`. `is_new_key` is
    /// whether this was the pubkey's first store into that slot's cache; overwrites within a
    /// slot only refresh `latest_write`. Stores for one (pubkey, slot) are serialized by the
    /// account locks upstream, so `cached_account` cannot be stale for its slot.
    pub(crate) fn insert_write(
        &self,
        pubkey: &Pubkey,
        slot: Slot,
        cached_account: &Arc<CachedAccount>,
        is_new_key: bool,
    ) {
        let guard = self.cache.pin();
        let compute = guard.compute(*pubkey, |current| {
            let mut entry = match current {
                Some((_pubkey, entry)) => entry.duplicate(),
                None => CacheEntry::default(),
            };
            if is_new_key {
                entry.ref_count += 1;
            }
            if slot >= entry.max_slot {
                entry.max_slot = slot;
                entry.latest_write = Some((slot, Arc::clone(cached_account)));
            }
            Operation::Insert::<_, ()>(entry)
        });
        let entered_write_cache = match compute {
            Compute::Inserted(..) => is_new_key,
            Compute::Updated {
                old: (_pubkey, old_entry),
                ..
            } => is_new_key && old_entry.ref_count == 0,
            Compute::Removed(..) | Compute::Aborted(..) => {
                unreachable!("insert_write always inserts")
            }
        };
        drop(guard);
        if entered_write_cache {
            self.num_write_pubkeys.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record that each pubkey in `pubkeys` left `slot`'s slot cache. Returns the pubkeys that
    /// are no longer in the write cache at all. `max_slot` is not updated; it will become stale
    /// if the removed slot is the highest slot.
    pub(crate) fn remove_write(
        &self,
        slot: Slot,
        pubkeys: impl IntoIterator<Item = Pubkey>,
    ) -> Vec<Pubkey> {
        let mut removed_pubkeys = Vec::new();
        let guard = self.cache.pin();
        for pubkey in pubkeys {
            let compute = guard.compute(pubkey, |current| {
                let Some((_pubkey, entry)) = current else {
                    // If this has happened the write cache's index is corrupted
                    panic!("pubkey {pubkey} not found in cache index during remove");
                };
                let new_ref_count = entry
                    .ref_count
                    .checked_sub(1)
                    .expect("pubkey is in the write cache");
                if new_ref_count == 0 && entry.read.is_none() {
                    return Operation::<_, ()>::Remove;
                }
                let mut new_entry = entry.duplicate();
                new_entry.ref_count = new_ref_count;
                if new_ref_count == 0
                    || new_entry
                        .latest_write
                        .as_ref()
                        .is_some_and(|(latest_slot, _)| *latest_slot == slot)
                {
                    new_entry.latest_write = None;
                }
                Operation::Insert(new_entry)
            });
            let left_write_cache = match compute {
                Compute::Removed(..) => true,
                Compute::Updated {
                    new: (_pubkey, new_entry),
                    ..
                } => new_entry.ref_count == 0,
                Compute::Inserted(..) | Compute::Aborted(..) => {
                    unreachable!("remove_write never creates an entry")
                }
            };
            if left_write_cache {
                self.num_write_pubkeys.fetch_sub(1, Ordering::Relaxed);
                removed_pubkeys.push(pubkey);
            }
        }
        removed_pubkeys
    }

    /// Returns the recorded max slot for `pubkey`, or `None` if the pubkey is not present in the
    /// write cache. Note: the account is not necessarily in this slot if it was removed during
    /// flush. This is just the maximum slot that it could be found in during search
    pub(crate) fn write_max_slot(&self, pubkey: &Pubkey) -> Option<Slot> {
        self.cache
            .pin()
            .get(pubkey)
            .and_then(|entry| (entry.ref_count > 0).then_some(entry.max_slot))
    }

    /// Is `pubkey` in the write cache?
    pub(crate) fn contains_write(&self, pubkey: &Pubkey) -> bool {
        self.cache
            .pin()
            .get(pubkey)
            .is_some_and(|entry| entry.ref_count > 0)
    }

    /// Returns a vector of all pubkeys currently in the write cache.
    pub(crate) fn write_pubkeys(&self) -> Vec<Pubkey> {
        self.cache
            .pin()
            .iter()
            .filter(|(_pubkey, entry)| entry.ref_count > 0)
            .map(|(pubkey, _entry)| *pubkey)
            .collect()
    }

    pub(crate) fn num_write_pubkeys(&self) -> u64 {
        self.num_write_pubkeys.load(Ordering::Relaxed)
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    pub(crate) fn cache_len(&self) -> usize {
        self.cache_len.load(Ordering::Relaxed)
    }

    pub(crate) fn data_size(&self) -> usize {
        self.data_size.load(Ordering::Relaxed)
    }

    pub(crate) fn get_and_reset_stats(&self) -> ReadOnlyCacheStats {
        ReadOnlyCacheStats {
            hits: self.stats.hits.swap_total(),
            misses: self.stats.misses.swap_total(),
            load_us: self.stats.load_us.swap(0, Ordering::Relaxed),
            store_us: self.stats.store_us.swap(0, Ordering::Relaxed),
        }
    }

    /// Return the elapsed time of the cache.
    fn timestamp(&self) -> u64 {
        self.timer.elapsed().as_nanos() as u64
    }
}

impl ReadOnlyAccountCacheEntry {
    fn new(account: AccountSharedData, slot: Slot, timestamp: u64) -> Self {
        Self {
            account,
            slot,
            last_update_time: AtomicU64::new(timestamp),
        }
    }

    /// Refresh the eviction stamp, only writing it once per `LRU_STAMP_INTERVAL_NS`
    fn refresh_last_update_time(&self, now: u64) {
        let last_update_time = self.last_update_time.load(Ordering::Relaxed);
        if now.wrapping_sub(last_update_time) > LRU_STAMP_INTERVAL_NS {
            self.last_update_time.store(now, Ordering::Relaxed);
        }
    }
}

/// Updates atomic `stat` with the delta of `old` and `new`
#[inline]
fn update_stat(stat: &AtomicUsize, old: usize, new: usize) {
    if new != old {
        stat.fetch_add(new.wrapping_sub(old), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::{Rng, SeedableRng, seq::IndexedRandom as _},
        rand_chacha::ChaChaRng,
        solana_account::Account,
        std::{collections::HashMap, iter::repeat_with},
        test_case::test_case,
    };

    impl ReadOnlyAccountsCache {
        /// reset the read only accounts cache
        ///
        /// Entries held by the write cache keep their write half, so the write cache's index
        /// stays intact.
        #[cfg(feature = "dev-context-only-utils")]
        pub fn reset_for_tests(&self) {
            let guard = self.cache.pin();
            let pubkeys: Vec<_> = guard.iter().map(|(pubkey, _entry)| *pubkey).collect();
            for pubkey in pubkeys {
                guard.compute(pubkey, |current| {
                    let Some((_pubkey, entry)) = current else {
                        return Operation::Abort(());
                    };
                    if entry.ref_count == 0 {
                        Operation::Remove
                    } else {
                        let mut new_entry = entry.duplicate();
                        new_entry.read = None;
                        Operation::Insert(new_entry)
                    }
                });
            }
            self.data_size.store(0, Ordering::Relaxed);
            self.cache_len.store(0, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_accountsdb_sizeof() {
        // size_of(arc(x)) does not return the size of x
        assert!(std::mem::size_of::<Arc<u64>>() == std::mem::size_of::<Arc<u8>>());
        assert!(std::mem::size_of::<Arc<u64>>() == std::mem::size_of::<Arc<[u8; 32]>>());
    }

    /// Checks the integrity of data stored in the cache after a sequence of loads and stores.
    #[test]
    fn test_read_only_accounts_cache_random() {
        const SEED: [u8; 32] = [0xdb; 32];
        const DATA_SIZE: usize = 19;
        let mut rng = ChaChaRng::from_seed(SEED);
        let cache = ReadOnlyAccountsCache::new(usize::MAX, usize::MAX, 8, 8);
        let slots: Vec<Slot> = repeat_with(|| rng.random_range(0..1000)).take(5).collect();
        let pubkeys: Vec<Pubkey> = repeat_with(|| {
            let mut arr = [0u8; 32];
            rng.fill(&mut arr[..]);
            Pubkey::new_from_array(arr)
        })
        .take(35)
        .collect();
        let mut hash_map = HashMap::<ReadOnlyCacheKey, (AccountSharedData, Slot, usize)>::new();
        for ix in 0..1000 {
            if rng.random_bool(0.1) && !hash_map.is_empty() {
                let (pubkey, (_account, slot, _)) = {
                    let keys: Vec<_> = hash_map.keys().copied().collect();
                    let pubkey = *keys.choose(&mut rng).unwrap();
                    (pubkey, hash_map.get(&pubkey).unwrap().clone())
                };
                let account = cache.load(pubkey, slot).unwrap();
                let (other, other_slot, index) = hash_map.get_mut(&pubkey).unwrap();
                assert_eq!(account, *other);
                assert_eq!(slot, *other_slot);
                *index = ix;
            } else {
                let mut data = vec![0u8; DATA_SIZE];
                rng.fill(&mut data[..]);
                let account = AccountSharedData::from(Account {
                    lamports: rng.random(),
                    data,
                    executable: rng.random(),
                    rent_epoch: rng.random(),
                    owner: Pubkey::default(),
                });
                let slot = *slots.choose(&mut rng).unwrap();
                let pubkey = *pubkeys.choose(&mut rng).unwrap();
                hash_map.insert(pubkey, (account.clone(), slot, ix));
                cache.store(pubkey, slot, account);
            }
        }
        assert_eq!(cache.cache_len(), hash_map.len());
        // Every cache entry must hold what the local hash map last stored for it.
        let guard = cache.cache.pin();
        for (pubkey, entry) in guard.iter() {
            let read = entry.read.as_ref().unwrap();
            let (local_account, local_slot, _) = hash_map
                .get(pubkey)
                .expect("account to be present in the map");
            assert_eq!(&read.account, local_account);
            assert_eq!(read.slot, *local_slot);
        }
    }

    #[test]
    fn test_cache_len_sequential_add_remove() {
        const ACCOUNT_DATA_SIZE: usize = 16;
        const NUM_ACCOUNTS: usize = 1_000;
        let cache = ReadOnlyAccountsCache::new(usize::MAX, usize::MAX, 1, 8);

        let pubkeys: Vec<_> = (0..NUM_ACCOUNTS).map(|_| Pubkey::new_unique()).collect();

        for (i, pubkey) in pubkeys.iter().enumerate() {
            let slot = i as Slot;
            let account = AccountSharedData::new(i as u64, ACCOUNT_DATA_SIZE, &Pubkey::default());
            cache.store(*pubkey, slot, account);
        }

        // Updating an existing entry should not change the tracked length.
        for (i, pubkey) in pubkeys.iter().enumerate() {
            let slot = i.saturating_add(1) as Slot;
            let account = AccountSharedData::new(
                i.saturating_add(1) as u64,
                ACCOUNT_DATA_SIZE,
                &Pubkey::default(),
            );
            cache.store(*pubkey, slot, account);
            assert_eq!(cache.cache_len(), NUM_ACCOUNTS);
        }

        for (index, pubkey) in pubkeys.iter().enumerate() {
            let removed = cache
                .remove(pubkey)
                .unwrap_or_else(|| panic!("missing account #{index}"));
            assert_eq!(removed.data().len(), ACCOUNT_DATA_SIZE);
            assert_eq!(cache.cache_len(), NUM_ACCOUNTS - index - 1);
        }

        assert_eq!(cache.cache_len(), 0);
        assert!(cache.remove(&Pubkey::new_unique()).is_none());
    }

    /// An entry holding both halves loses only its read half to removal, and the entry itself
    /// stays for the write cache's index.
    #[test]
    fn test_write_half_pins_entry() {
        let cache = ReadOnlyAccountsCache::new(usize::MAX, usize::MAX, 1, 8);
        let pubkey = Pubkey::new_unique();
        let account = AccountSharedData::new(1, 16, &Pubkey::default());

        let cached_account = Arc::new(CachedAccount::new(account.clone(), pubkey));
        cache.insert_write(&pubkey, 7, &cached_account, true);
        cache.store(pubkey, 5, account.clone());
        assert_eq!(cache.cache_len(), 1);
        assert_eq!(cache.write_max_slot(&pubkey), Some(7));

        // removing the read half keeps the write half
        assert_eq!(cache.remove(&pubkey), Some(account.clone()));
        assert_eq!(cache.cache_len(), 0);
        assert_eq!(cache.write_max_slot(&pubkey), Some(7));
        assert!(cache.contains_write(&pubkey));

        // and the write half leaving removes the entry entirely
        cache.store(pubkey, 5, account);
        assert_eq!(cache.remove_write(7, [pubkey]), vec![pubkey]);
        assert!(!cache.contains_write(&pubkey));
        // the read half is still there
        assert_eq!(cache.cache_len(), 1);
        assert!(cache.load(pubkey, 5).is_some());
        cache.remove(&pubkey);
        assert!(!cache.cache.pin().contains_key(&pubkey));
    }

    #[test_case(11, 11; "equal")]
    #[test_case(22, 27; "greater")]
    #[test_case(33, 30; "less")]
    fn test_update_stat(old: usize, new: usize) {
        let val = old + new;
        let stat = val.into();
        update_stat(&stat, old, new);
        assert_eq!(stat.into_inner(), val - old + new);
    }
}
