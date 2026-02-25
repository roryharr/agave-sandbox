use {
    crate::ancestors::Ancestors,
    dashmap::DashMap,
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::Slot,
    solana_nohash_hasher::BuildNoHashHasher,
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    std::{
        collections::BTreeSet,
        ops::Deref,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        },
    },
};

#[derive(Debug)]
pub struct SlotCache {
    cache: DashMap<Pubkey, Arc<CachedAccount>, PubkeyHasherBuilder>,
    same_account_writes: AtomicU64,
    same_account_writes_size: AtomicU64,
    unique_account_writes_size: AtomicU64,
    /// The size of account data stored in `cache` (just this slot), in bytes
    size: AtomicU64,
    /// The size of account data stored in the whole AccountsCache, in bytes
    total_size: Arc<AtomicU64>,
    is_frozen: AtomicBool,
    /// The number of accounts stored in `cache` (just this slot)
    accounts_count: AtomicU64,
    /// The number of accounts stored in the whole AccountsCache
    total_accounts_count: Arc<AtomicU64>,
}

impl Drop for SlotCache {
    fn drop(&mut self) {
        // broader cache no longer holds our size/counts in memory
        self.total_size
            .fetch_sub(*self.size.get_mut(), Ordering::Relaxed);
        self.total_accounts_count
            .fetch_sub(*self.accounts_count.get_mut(), Ordering::Relaxed);
    }
}

impl SlotCache {
    pub fn report_slot_store_metrics(&self) {
        datapoint_info!(
            "slot_repeated_writes",
            (
                "same_account_writes",
                self.same_account_writes.load(Ordering::Relaxed),
                i64
            ),
            (
                "same_account_writes_size",
                self.same_account_writes_size.load(Ordering::Relaxed),
                i64
            ),
            (
                "unique_account_writes_size",
                self.unique_account_writes_size.load(Ordering::Relaxed),
                i64
            ),
            ("size", self.size.load(Ordering::Relaxed), i64),
            (
                "accounts_count",
                self.accounts_count.load(Ordering::Relaxed),
                i64
            )
        );
    }

    /// Insert an account into this slot's cache.
    /// Returns the cached account and whether this was a new unique key for this slot.
    fn insert(&self, pubkey: &Pubkey, account: AccountSharedData) -> (Arc<CachedAccount>, bool) {
        let data_len = account.data().len() as u64;
        let item = Arc::new(CachedAccount {
            account,
            pubkey: *pubkey,
        });
        let new_key = if let Some(old) = self.cache.insert(*pubkey, item.clone()) {
            self.same_account_writes.fetch_add(1, Ordering::Relaxed);
            self.same_account_writes_size
                .fetch_add(data_len, Ordering::Relaxed);

            let old_len = old.account.data().len() as u64;
            let grow = data_len.saturating_sub(old_len);
            if grow > 0 {
                self.size.fetch_add(grow, Ordering::Relaxed);
                self.total_size.fetch_add(grow, Ordering::Relaxed);
            } else {
                let shrink = old_len.saturating_sub(data_len);
                if shrink > 0 {
                    self.size.fetch_sub(shrink, Ordering::Relaxed);
                    self.total_size.fetch_sub(shrink, Ordering::Relaxed);
                }
            }
            false
        } else {
            self.size.fetch_add(data_len, Ordering::Relaxed);
            self.total_size.fetch_add(data_len, Ordering::Relaxed);
            self.unique_account_writes_size
                .fetch_add(data_len, Ordering::Relaxed);
            self.accounts_count.fetch_add(1, Ordering::Relaxed);
            self.total_accounts_count.fetch_add(1, Ordering::Relaxed);
            true
        };
        (item, new_key)
    }

    fn get_cloned(&self, pubkey: &Pubkey) -> Option<Arc<CachedAccount>> {
        self.cache
            .get(pubkey)
            // 1) Maybe can eventually use a Cow to avoid a clone on every read
            // 2) Popping is only safe if it's guaranteed that only
            //    replay/banking threads are reading from the AccountsDb
            .map(|account_ref| account_ref.value().clone())
    }

    pub fn mark_slot_frozen(&self) {
        self.is_frozen.store(true, Ordering::Release);
    }

    fn is_frozen(&self) -> bool {
        self.is_frozen.load(Ordering::Acquire)
    }

    pub fn total_bytes(&self) -> u64 {
        self.unique_account_writes_size.load(Ordering::Relaxed)
            + self.same_account_writes_size.load(Ordering::Relaxed)
    }
}

impl Deref for SlotCache {
    type Target = DashMap<Pubkey, Arc<CachedAccount>, PubkeyHasherBuilder>;
    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

#[derive(Debug)]
pub struct CachedAccount {
    pub account: AccountSharedData,
    pubkey: Pubkey,
}

impl CachedAccount {
    pub fn pubkey(&self) -> &Pubkey {
        &self.pubkey
    }
}

#[derive(Debug, Default)]
pub struct AccountsCacheIndex {
    // Maps each pubkey to (max_slot, count) where max_slot is the highest slot at
    // which the pubkey has been written into the cache, and count is the number of
    // slot-cache entries that currently hold the pubkey.  max_slot may be
    // transiently stale after a removal; callers must handle a cache miss on
    // max_slot by falling back to a full scan (see load_pubkey).
    cached_account_maps: DashMap<Pubkey, (Slot, u64), PubkeyHasherBuilder>,
    num_accounts_in_cache: AtomicU64,
}

impl AccountsCacheIndex {
    /// Decrement the reference count for each pubkey in `pubkeys`.
    /// Removes the entry entirely when the count reaches zero.
    /// `max_slot` is left as-is when count > 0; it may become stale if the
    /// removed slot happened to be the highest one.
    pub fn remove(&self, pubkeys: impl IntoIterator<Item = Pubkey>) {
        for pubkey in pubkeys {
            if let dashmap::mapref::entry::Entry::Occupied(mut occupied_entry) =
                self.cached_account_maps.entry(pubkey)
            {
                let (_, count) = occupied_entry.get_mut();
                *count -= 1;
                if *count == 0 {
                    occupied_entry.remove_entry();
                    self.num_accounts_in_cache.fetch_sub(1, Ordering::Relaxed);
                }
            } else {
                panic!("pubkey not found in cache index during remove");
            }
        }
    }

    /// Returns the recorded max slot for `pubkey`, or `None` if the pubkey is
    /// not present in the cache.  The returned value may be stale if the slot
    /// at which the pubkey was last written has since been removed.
    fn max_slot_for_pubkey(&self, pubkey: &Pubkey) -> Option<Slot> {
        self.cached_account_maps
            .get(pubkey)
            .map(|account| account.0)
    }

    pub fn insert(&self, pubkey: &Pubkey, slot: Slot) {
        self.cached_account_maps
            .entry(*pubkey)
            .and_modify(|(stored_slot, count)| {
                *stored_slot = slot.max(*stored_slot);
                *count += 1;
            })
            .or_insert_with(|| {
                self.num_accounts_in_cache.fetch_add(1, Ordering::Relaxed);
                (slot, 1)
            });
    }
}

#[derive(Debug)]
pub struct AccountsCache {
    cache: DashMap<Slot, Arc<SlotCache>, BuildNoHashHasher<Slot>>,
    // Map of pubkey to (slot, count) for accounts in the cache
    index: AccountsCacheIndex,
    // Queue of potentially unflushed roots. Random eviction + cache too large
    // could have triggered a flush of this slot already
    maybe_unflushed_roots: RwLock<BTreeSet<Slot>>,
    max_flushed_root: AtomicU64,
    // The maximum rooted slot that has been cleared from the cache.
    // -1 means no root has been removed yet.
    max_root_cleared: AtomicI64,
    /// The size of account data stored in the whole AccountsCache, in bytes
    total_size: Arc<AtomicU64>,
    /// The number of accounts stored in the whole AccountsCache
    total_accounts_counts: Arc<AtomicU64>,
    /// The minimum slot currently in the cache
    min_slot: AtomicU64,
    /// The maximum slot currently in the cache
    max_slot: AtomicU64,
}

impl Default for AccountsCache {
    fn default() -> Self {
        Self {
            cache: DashMap::default(),
            index: AccountsCacheIndex::default(),
            maybe_unflushed_roots: RwLock::default(),
            max_flushed_root: AtomicU64::default(),
            max_root_cleared: AtomicI64::new(-1),
            total_size: Arc::default(),
            total_accounts_counts: Arc::default(),
            min_slot: AtomicU64::new(u64::MAX),
            max_slot: AtomicU64::default(),
        }
    }
}

impl AccountsCache {
    pub fn new_inner(&self) -> Arc<SlotCache> {
        Arc::new(SlotCache {
            cache: DashMap::default(),
            same_account_writes: AtomicU64::default(),
            same_account_writes_size: AtomicU64::default(),
            unique_account_writes_size: AtomicU64::default(),
            size: AtomicU64::default(),
            total_size: Arc::clone(&self.total_size),
            is_frozen: AtomicBool::default(),
            accounts_count: AtomicU64::default(),
            total_accounts_count: Arc::clone(&self.total_accounts_counts),
        })
    }
    pub fn size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }
    pub fn report_size(&self) {
        datapoint_info!(
            "accounts_cache_size",
            (
                "num_roots",
                self.maybe_unflushed_roots.read().unwrap().len(),
                i64
            ),
            ("num_slots", self.cache.len(), i64),
            ("total_size", self.size(), i64),
            (
                "total_accounts_count",
                self.total_accounts_counts.load(Ordering::Relaxed),
                i64
            ),
            (
                "num_unique_accounts_in_cache",
                self.index
                    .num_accounts_in_cache
                    .load(Ordering::Relaxed),
                i64
            ),
            (
                "slot_range",
                self.fetch_max_slot().saturating_sub(self.fetch_min_slot()),
                i64
            )
        );
    }

    pub fn store(
        &self,
        slot: Slot,
        pubkey: &Pubkey,
        account: AccountSharedData,
    ) -> Arc<CachedAccount> {
        let slot_cache = self.slot_cache(slot).unwrap_or_else(|| {
            // DashMap entry.or_insert() returns a RefMut, essentially a write lock,
            // which is dropped after this block ends, minimizing time held by the lock.
            // However, we still want to persist the reference to the `SlotStores` behind
            // the lock, hence we clone it out, (`SlotStores` is an Arc so is cheap to clone).
            // Update min_slot if this is the first slot or if slot is smaller
            self.min_slot.fetch_min(slot, Ordering::Release);
            // Update max_slot if slot is larger
            self.max_slot.fetch_max(slot, Ordering::Release);
            self.cache
                .entry(slot)
                .or_insert_with(|| self.new_inner())
                .clone()
        });

        let (item, is_new) = slot_cache.insert(pubkey, account);
        if is_new {
            self.index.insert(pubkey, slot);
        }
        item
    }

    pub fn load(&self, slot: Slot, pubkey: &Pubkey) -> Option<Arc<CachedAccount>> {
        self.slot_cache(slot)
            .and_then(|slot_cache| slot_cache.get_cloned(pubkey))
    }

    pub fn remove_slot(&self, slot: Slot) -> Option<Arc<SlotCache>> {
        let result = self.cache.remove(&slot).map(|(_, slot_cache)| slot_cache);

        // If we removed the min slot, recalculate it
        if slot == self.min_slot.load(Ordering::Acquire) {
            // Search upwards from the removed slot to find the next min.
            // If nothing is found (cache is now empty) reset to the sentinel value.
            let max_slot = self.max_slot.load(Ordering::Acquire);
            let new_min = (slot + 1..=max_slot)
                .find(|s| self.cache.contains_key(s))
                .unwrap_or(u64::MAX);
            self.min_slot.store(new_min, Ordering::Release);
        }
        if let Some(ref slot_cache) = result {
            let pubkeys = slot_cache
                .iter()
                .map(|item| *item.key())
                .collect::<Vec<_>>();
            self.index.remove(pubkeys);
        }
        result
    }

    /// Finds the newest write-cache entry for `pubkey` if it exists in the index
    pub fn load_pubkey(
        &self,
        pubkey: &Pubkey,
        ancestors: &Ancestors,
    ) -> Option<(Arc<CachedAccount>, Slot)> {
        // If pubkey is not in the cache at all, exit early without doing any more work
        let max_slot = self.index.max_slot_for_pubkey(pubkey)?;

        let min_slot = self.fetch_min_slot();
        for slot in (min_slot..=max_slot).rev() {
            if let Some(account) = self.load(slot, pubkey) {
                if self.matching_slot(ancestors, slot) {
                    return Some((account, slot));
                }
            }
        }
        None
    }

    /// Returns true if the slot would be considered a valid entry to return with the passed in
    /// `ancestors`. This means it is in the ancestor list or a rooted slot.
    pub(crate) fn matching_slot(&self, ancestors: &Ancestors, slot: Slot) -> bool {
        if ancestors.contains_key(&slot) {
            return true;
        }

        // If the slot is less than the max_root cleared then it must be a root, even if it's not
        // in the maybe_unflushed_roots set, as it is currently being flushed
        let max_root_cleared = self.max_root_cleared.load(Ordering::Acquire);
        if max_root_cleared >= 0 && slot <= max_root_cleared as u64 {
            return true;
        }
        self.maybe_unflushed_roots.read().unwrap().contains(&slot)
    }

    pub fn slot_cache(&self, slot: Slot) -> Option<Arc<SlotCache>> {
        self.cache.get(&slot).map(|result| result.value().clone())
    }

    pub fn add_root(&self, root: Slot) {
        self.maybe_unflushed_roots.write().unwrap().insert(root);
    }

    pub fn clear_roots(&self, max_root: Option<Slot>) -> BTreeSet<Slot> {
        let mut w_maybe_unflushed_roots = self.maybe_unflushed_roots.write().unwrap();
        if let Some(max_root) = max_root {
            self.max_root_cleared
                .fetch_max(max_root as i64, Ordering::Release);
            // `greater_than_max_root` contains all slots >= `max_root + 1`, or alternatively,
            // all slots > `max_root`. Meanwhile, `w_maybe_unflushed_roots` is left with all slots
            // <= `max_root`.
            let greater_than_max_root = w_maybe_unflushed_roots.split_off(&(max_root + 1));
            // After the replace, `w_maybe_unflushed_roots` contains slots > `max_root`, and
            // we return all slots <= `max_root`
            std::mem::replace(&mut w_maybe_unflushed_roots, greater_than_max_root)
        } else {
            if let Some(&max_root) = w_maybe_unflushed_roots.last() {
                self.max_root_cleared
                    .fetch_max(max_root as i64, Ordering::Release);
            }
            std::mem::take(&mut *w_maybe_unflushed_roots)
        }
    }

    pub fn cached_frozen_slots(&self) -> Vec<Slot> {
        self.cache
            .iter()
            .filter_map(|item| {
                let (slot, slot_cache) = item.pair();
                slot_cache.is_frozen().then_some(*slot)
            })
            .collect()
    }

    pub fn contains(&self, slot: Slot) -> bool {
        self.cache.contains_key(&slot)
    }

    pub fn num_slots(&self) -> usize {
        self.cache.len()
    }

    pub fn fetch_max_flush_root(&self) -> Slot {
        self.max_flushed_root.load(Ordering::Acquire)
    }

    pub fn set_max_flush_root(&self, root: Slot) {
        self.max_flushed_root.fetch_max(root, Ordering::Release);
    }

    pub fn fetch_max_slot(&self) -> Slot {
        self.max_slot.load(Ordering::Acquire)
    }

    pub fn fetch_min_slot(&self) -> Slot {
        self.min_slot.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl AccountsCache {
        // Removes slots less than or equal to `max_root`. Only safe to pass in a rooted slot,
        // otherwise the slot removed could still be undergoing replay!
        pub fn remove_slots_le(&self, max_root: Slot) -> Vec<(Slot, Arc<SlotCache>)> {
            let mut removed_slots = vec![];
            self.cache.retain(|slot, slot_cache| {
                let should_remove = *slot <= max_root;
                if should_remove {
                    removed_slots.push((*slot, slot_cache.clone()))
                }
                !should_remove
            });
            removed_slots
        }
    }

    #[test]
    fn test_remove_slots_le() {
        let cache = AccountsCache::default();
        // Cache is empty, should return nothing
        assert!(cache.remove_slots_le(1).is_empty());
        let inserted_slot = 0;
        cache.store(
            inserted_slot,
            &Pubkey::new_unique(),
            AccountSharedData::new(1, 0, &Pubkey::default()),
        );
        // If the cache is told the size limit is 0, it should return the one slot
        let removed = cache.remove_slots_le(0);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, inserted_slot);
    }

    #[test]
    fn test_cached_frozen_slots() {
        let cache = AccountsCache::default();
        // Cache is empty, should return nothing
        assert!(cache.cached_frozen_slots().is_empty());
        let inserted_slot = 0;
        cache.store(
            inserted_slot,
            &Pubkey::new_unique(),
            AccountSharedData::new(1, 0, &Pubkey::default()),
        );

        // If the cache is told the size limit is 0, it should return nothing, because there's no
        // frozen slots
        assert!(cache.cached_frozen_slots().is_empty());
        cache.slot_cache(inserted_slot).unwrap().mark_slot_frozen();
        // If the cache is told the size limit is 0, it should return the one frozen slot
        assert_eq!(cache.cached_frozen_slots(), vec![inserted_slot]);
    }

    #[test]
    fn test_cache_index_insert_and_max_slot() {
        let index = AccountsCacheIndex::default();
        let pubkey = Pubkey::new_unique();

        // Initially empty
        assert!(index.max_slot_for_pubkey(&pubkey).is_none());

        // Insert at slot 5
        index.insert(&pubkey, 5);
        assert_eq!(index.max_slot_for_pubkey(&pubkey), Some(5));

        // Insert same pubkey at a higher slot updates max_slot
        index.insert(&pubkey, 10);
        assert_eq!(index.max_slot_for_pubkey(&pubkey), Some(10));

        // Insert same pubkey at a lower slot does not decrease max_slot
        index.insert(&pubkey, 3);
        assert_eq!(index.max_slot_for_pubkey(&pubkey), Some(10));
    }

    #[test]
    fn test_cache_index_remove_decrements_count() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        // Store pubkey into 3 different slots
        cache.store(1, &pk, AccountSharedData::new(1, 0, &Pubkey::default()));
        cache.store(5, &pk, AccountSharedData::new(5, 0, &Pubkey::default()));
        cache.store(3, &pk, AccountSharedData::new(3, 0, &Pubkey::default()));

        // Remove and drop slot 1 — entry should still exist (count goes from 3 to 2)
        let removed = cache.remove_slot(1);
        assert!(removed.is_some());
        drop(removed);
        assert_eq!(cache.index.max_slot_for_pubkey(&pk), Some(5));

        // Remove and drop slot 5 — entry should still exist (count goes from 2 to 1).
        // max_slot stays stale at 5 because the index doesn't scan for a new max on removal.
        let removed = cache.remove_slot(5);
        assert!(removed.is_some());
        drop(removed);
        assert!(cache.index.max_slot_for_pubkey(&pk).is_some());

        // Remove and drop slot 3 — last reference gone, entry removed
        let removed = cache.remove_slot(3);
        assert!(removed.is_some());
        drop(removed);
        assert!(cache.index.max_slot_for_pubkey(&pk).is_none());
    }

    #[test]
    fn test_min_max_slot_tracking() {
        let cache = AccountsCache::default();

        // Empty cache: min_slot sentinel is 0, max_slot is 0
        assert_eq!(cache.fetch_min_slot(), u64::MAX);
        assert_eq!(cache.fetch_max_slot(), 0);

        // Store into slot 5
        let pk = Pubkey::new_unique();
        cache.store(5, &pk, AccountSharedData::new(1, 0, &Pubkey::default()));
        assert_eq!(cache.fetch_min_slot(), 5);
        assert_eq!(cache.fetch_max_slot(), 5);

        // Store into slot 10
        cache.store(10, &pk, AccountSharedData::new(1, 0, &Pubkey::default()));
        assert_eq!(cache.fetch_min_slot(), 5);
        assert_eq!(cache.fetch_max_slot(), 10);

        cache.remove_slot(5);
        assert_eq!(cache.fetch_min_slot(), 10);
        assert_eq!(cache.fetch_max_slot(), 10);

        // Store into slot 2 — min should update
        cache.store(2, &pk, AccountSharedData::new(1, 0, &Pubkey::default()));
        assert_eq!(cache.fetch_min_slot(), 2);
        assert_eq!(cache.fetch_max_slot(), 10);
    }

    #[test]
    fn test_remove_slot_cleans_up_index() {
        let cache = AccountsCache::default();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();

        // pk1 in slots 1 and 3; pk2 only in slot 1
        cache.store(1, &pk1, AccountSharedData::new(1, 0, &Pubkey::default()));
        cache.store(1, &pk2, AccountSharedData::new(1, 0, &Pubkey::default()));
        cache.store(3, &pk1, AccountSharedData::new(1, 0, &Pubkey::default()));

        // Before removal: both pubkeys are in the index
        assert!(
            cache
                .index
                .max_slot_for_pubkey(&pk1)
                .is_some()
        );
        assert!(
            cache
                .index
                .max_slot_for_pubkey(&pk2)
                .is_some()
        );

        // Remove slot 1 — pk2 should disappear, pk1 still present (in slot 3)
        cache.remove_slot(1);
        assert!(
            cache
                .index
                .max_slot_for_pubkey(&pk1)
                .is_some()
        );
        assert!(
            cache
                .index
                .max_slot_for_pubkey(&pk2)
                .is_none()
        );

        // Remove slot 3 — pk1 should also disappear
        cache.remove_slot(3);
        assert!(
            cache
                .index
                .max_slot_for_pubkey(&pk1)
                .is_none()
        );
    }

    #[test]
    fn test_load_pubkey_visibility() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        cache.store(5, &pk, AccountSharedData::new(100, 0, &Pubkey::default()));
        cache.store(10, &pk, AccountSharedData::new(200, 0, &Pubkey::default()));
        cache.store(15, &pk, AccountSharedData::new(300, 0, &Pubkey::default()));

        // matching_slot: slot in ancestors matches
        let ancestors = Ancestors::from(vec![(5, 1), (10, 1)]);
        assert!(cache.matching_slot(&ancestors, 5));
        assert!(cache.matching_slot(&ancestors, 10));

        // matching_slot: slot not in ancestors and not a root does not match
        assert!(!cache.matching_slot(&ancestors, 15));

        // load_pubkey returns highest visible slot, skipping slot 15
        let (account, slot) = cache.load_pubkey(&pk, &ancestors).unwrap();
        assert_eq!(slot, 10);
        assert_eq!(account.account.lamports(), 200);

        // Ancestors that don't cover any stored slot returns None
        let disjoint_ancestors = Ancestors::from(vec![(3, 1)]);
        assert!(cache.load_pubkey(&pk, &disjoint_ancestors).is_none());

        // Empty ancestors with no roots — nothing visible
        let empty_ancestors = Ancestors::default();
        assert!(cache.load_pubkey(&pk, &empty_ancestors).is_none());

        // Add slot 10 as a root — now visible even with empty ancestors
        cache.add_root(10);
        assert!(cache.matching_slot(&empty_ancestors, 10));
        let (account, slot) = cache.load_pubkey(&pk, &empty_ancestors).unwrap();
        assert_eq!(slot, 10);
        assert_eq!(account.account.lamports(), 200);
    }
}
