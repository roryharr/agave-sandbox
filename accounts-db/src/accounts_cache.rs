use {
    crate::{accounts_db::AccountsDb, accounts_hash::AccountHash},
    ahash::RandomState as AHashRandomState,
    dashmap::DashMap,
    seqlock::SeqLock,
    solana_pubkey::Pubkey,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
    },
    std::{
        collections::BTreeSet,
        ops::Deref,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, RwLock,
        },
    },
};

pub type SlotCache = Arc<SlotCacheInner>;

// Set to 8192 to reduce shard collision rate
// Must be power of 2.
const ACCOUNTS_CACHE_INNER_NUM_SHARDS: usize = 8192;
const NUM_WRITE_CACHE_SLOTS: u64 = 256;

#[derive(Debug)]
pub struct SlotCacheInner {
    cache: DashMap<Pubkey, CachedAccount, AHashRandomState>,
    same_account_writes: AtomicU64,
    same_account_writes_size: AtomicU64,
    unique_account_writes_size: AtomicU64,
    size: AtomicU64,
    is_frozen: AtomicBool,
    slot: AtomicU64,
}

impl SlotCacheInner {
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
            ("size", self.size.load(Ordering::Relaxed), i64)
        );
    }

    pub fn insert(&self, pubkey: &Pubkey, account: AccountSharedData) -> (CachedAccount, i64) {
        let data_len = account.data().len() as u64;
        let item = Arc::new(CachedAccountInner {
            account,
            hash: SeqLock::new(None),
            pubkey: *pubkey,
        });
        let size_change: i64 = if let Some(old) = self.cache.insert(*pubkey, item.clone()) {
            self.same_account_writes.fetch_add(1, Ordering::Relaxed);
            self.same_account_writes_size
                .fetch_add(data_len, Ordering::Relaxed);

            let old_len = old.account.data().len() as u64;
            let grow = data_len.saturating_sub(old_len);
            if grow > 0 {
                self.size.fetch_add(grow, Ordering::Relaxed);
                grow as i64
            } else {
                let shrink = old_len.saturating_sub(data_len);
                if shrink > 0 {
                    self.size.fetch_sub(shrink, Ordering::Relaxed);
                }
                -(shrink as i64)
            }
        } else {
            self.size.fetch_add(data_len, Ordering::Relaxed);
            self.unique_account_writes_size
                .fetch_add(data_len, Ordering::Relaxed);
            data_len as i64
        };
        (item, size_change)
    }

    pub fn get_cloned(&self, pubkey: &Pubkey) -> Option<CachedAccount> {
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

    pub fn is_frozen(&self) -> bool {
        self.is_frozen.load(Ordering::Acquire)
    }

    pub fn total_bytes(&self) -> u64 {
        self.unique_account_writes_size.load(Ordering::Relaxed)
            + self.same_account_writes_size.load(Ordering::Relaxed)
    }
}

impl Deref for SlotCacheInner {
    type Target = DashMap<Pubkey, CachedAccount, AHashRandomState>;
    fn deref(&self) -> &Self::Target {
        &self.cache
    }
}

pub type CachedAccount = Arc<CachedAccountInner>;

#[derive(Debug)]
pub struct CachedAccountInner {
    pub account: AccountSharedData,
    hash: SeqLock<Option<AccountHash>>,
    pubkey: Pubkey,
}

impl CachedAccountInner {
    pub fn hash(&self) -> AccountHash {
        let hash = self.hash.read();
        match hash {
            Some(hash) => hash,
            None => {
                let hash = AccountsDb::hash_account(&self.account, &self.pubkey);
                *self.hash.lock_write() = Some(hash);
                hash
            }
        }
    }
    pub fn pubkey(&self) -> &Pubkey {
        &self.pubkey
    }
}

#[derive(Debug)]
pub struct AccountsCache {
    cache: [SlotCache; NUM_WRITE_CACHE_SLOTS as usize],
    // Queue of potentially unflushed roots. Random eviction + cache too large
    // could have triggered a flush of this slot already
    maybe_unflushed_roots: RwLock<BTreeSet<Slot>>,
    max_flushed_root: AtomicU64,
    total_size: AtomicU64,
    num_slots: AtomicU64,
}

impl Default for AccountsCache {
    fn default() -> AccountsCache {
        AccountsCache {
            maybe_unflushed_roots: RwLock::default(),
            max_flushed_root: AtomicU64::default(),
            num_slots: AtomicU64::default(),
            total_size: AtomicU64::default(),
            cache: std::array::from_fn(|_| {
                Arc::new(SlotCacheInner {
                    cache: DashMap::with_hasher_and_shard_amount(
                        AHashRandomState::default(),
                        ACCOUNTS_CACHE_INNER_NUM_SHARDS,
                    ),
                    same_account_writes: AtomicU64::default(),
                    same_account_writes_size: AtomicU64::default(),
                    unique_account_writes_size: AtomicU64::default(),
                    size: AtomicU64::default(),
                    is_frozen: AtomicBool::default(),
                    slot: AtomicU64::new(u64::MAX),
                })
            }),
        }
    }
}

impl AccountsCache {
    fn unique_account_writes_size(&self) -> u64 {
        self.cache
            .iter()
            .map(|item| item.unique_account_writes_size.load(Ordering::Relaxed))
            .sum()
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
            ("num_slots", self.num_slots.load(Ordering::Relaxed), i64),
            (
                "total_unique_writes_size",
                self.unique_account_writes_size(),
                i64
            ),
            ("total_size", self.size(), i64),
        );
    }

    pub fn store(&self, slot: Slot, pubkey: &Pubkey, account: AccountSharedData) -> CachedAccount {
        let slot_cache = self.slot_cache_create(slot);
        let (ret, size) = slot_cache.unwrap().insert(pubkey, account);
        if size >= 0 {
            self.total_size.fetch_add(size as u64, Ordering::Relaxed);
        } else {
            self.total_size.fetch_sub((-size) as u64, Ordering::Relaxed);
        }
        ret
    }

    pub fn load(&self, slot: Slot, pubkey: &Pubkey) -> Option<CachedAccount> {
        self.slot_cache(slot)
            .and_then(|slot_cache| slot_cache.get_cloned(pubkey))
    }

    pub fn remove_slot(&self, slot: Slot) {
        if let Some(cache_slot) = self.slot_cache(slot)
        {            
            self.total_size.fetch_sub(
                cache_slot.size.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            cache_slot.cache.clear();
            cache_slot.slot.store(u64::MAX, Ordering::Relaxed);
            self.num_slots.fetch_sub(1, Ordering::Relaxed);            
        }
    }

    pub fn slot_cache_create(&self, slot: Slot) -> Option<SlotCache> {
        
        let cache_slot = self.cache[(slot % NUM_WRITE_CACHE_SLOTS) as usize].clone();
        if cache_slot.slot.load(Ordering::Relaxed) != slot
        {
            cache_slot.slot.store(slot, Ordering::Relaxed);
            self.num_slots.fetch_add(1, Ordering::Relaxed);
        }
        Some(cache_slot)
    }

    pub fn slot_cache(&self, slot: Slot) -> Option<SlotCache> {
        if self.cache[(slot % NUM_WRITE_CACHE_SLOTS) as usize]
            .slot.load(Ordering::Relaxed) == slot            
        {
            Some(self.cache[(slot % NUM_WRITE_CACHE_SLOTS) as usize].clone())
        } else {
            None
        }
    }

    pub fn add_root(&self, root: Slot) {
        self.maybe_unflushed_roots.write().unwrap().insert(root);
    }

    pub fn clear_roots(&self, max_root: Option<Slot>) -> BTreeSet<Slot> {
        let mut w_maybe_unflushed_roots = self.maybe_unflushed_roots.write().unwrap();
        if let Some(max_root) = max_root {
            // `greater_than_max_root` contains all slots >= `max_root + 1`, or alternatively,
            // all slots > `max_root`. Meanwhile, `w_maybe_unflushed_roots` is left with all slots
            // <= `max_root`.
            let greater_than_max_root = w_maybe_unflushed_roots.split_off(&(max_root + 1));
            // After the replace, `w_maybe_unflushed_roots` contains slots > `max_root`, and
            // we return all slots <= `max_root`
            std::mem::replace(&mut w_maybe_unflushed_roots, greater_than_max_root)
        } else {
            std::mem::take(&mut *w_maybe_unflushed_roots)
        }
    }

    pub fn contains_any_slots(&self, max_slot_inclusive: Slot) -> bool {
        self.cache
            .iter()
            .any(|e| e.slot.load(Ordering::Relaxed) <= max_slot_inclusive)
    }

    pub fn cached_frozen_slots(&self) -> Vec<Slot> {
        self.cache
            .iter()
            .enumerate()
            .filter_map(|(index, item)| item.is_frozen().then_some(index as u64))
            .collect()
    }

    pub fn contains(&self, slot: Slot) -> bool {
        self.slot_cache(slot).is_some()                
    }

    pub fn num_slots(&self) -> usize {
        self.num_slots.load(Ordering::Relaxed) as usize
    }

    pub fn fetch_max_flush_root(&self) -> Slot {
        self.max_flushed_root.load(Ordering::Acquire)
    }

    pub fn set_max_flush_root(&self, root: Slot) {
        self.max_flushed_root.fetch_max(root, Ordering::Release);
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

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
}
