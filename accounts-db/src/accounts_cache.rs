use {
    crate::{
        ancestors::Ancestors,
        bank_id::BankId,
        read_only_accounts_cache::{Probe, ReadOnlyAccountsCache},
    },
    dashmap::DashMap,
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::Slot,
    solana_nohash_hasher::{BuildNoHashHasher, IntMap},
    solana_pubkey::Pubkey,
    std::{
        collections::{BTreeMap, BTreeSet},
        ops::Deref,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    },
};

/// Tracks the maximum flushed root slot.
///
/// Internally stores `slot + 1` so that the default value of `0` naturally
/// represents "no flushed roots".
#[derive(Debug, Default)]
struct MaxFlushedRoot(AtomicU64);

impl MaxFlushedRoot {
    /// Returns the current max flushed root, or `None` if no root has been flushed.
    fn get(&self) -> Option<Slot> {
        self.0.load(Ordering::Acquire).checked_sub(1)
    }

    /// Atomically update to the maximum of the current value and `slot`.
    /// Stores `slot + 1` internally so 0 can represent no slots flushed
    /// Note: Will overflow at u64::MAX, but realistically should never get that high
    fn fetch_max(&self, slot: Slot) {
        self.0.fetch_max(slot + 1, Ordering::Release);
    }
}

#[derive(Debug)]
pub struct SlotCache {
    slot: Slot,
    cache: DashMap<Pubkey, Arc<CachedAccount>, ahash::RandomState>,
    same_account_writes: AtomicU64,
    same_account_writes_size: AtomicU64,
    unique_account_writes_size: AtomicU64,
    /// The size of account data stored in `cache` (just this slot), in bytes
    size: AtomicU64,
    /// The size of account data stored in the whole AccountsCache, in bytes
    total_size: Arc<AtomicU64>,
    is_frozen: AtomicBool,
    /// The number of snapshots holding this cache to archive it as it is
    num_snapshot_holds: AtomicU64,
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
    pub fn len(&self) -> usize {
        self.accounts_count.load(Ordering::Acquire) as usize
    }

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

    /// Insert an account into this slot's cache
    ///
    /// Returns the cached account and whether this was a new unique key for this slot
    fn insert(&self, pubkey: &Pubkey, account: AccountSharedData) -> (Arc<CachedAccount>, bool) {
        let data_len = account.data().len() as u64;
        let item = Arc::new(CachedAccount::new(*pubkey, account));
        let is_new_key = if let Some(old) = self.cache.insert(*pubkey, item.clone()) {
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
            self.accounts_count.fetch_add(1, Ordering::Release);
            self.total_accounts_count.fetch_add(1, Ordering::Relaxed);
            true
        };
        (item, is_new_key)
    }

    /// Removes an account from this slot's cache
    ///
    /// Returns the removed account, or `None` if `pubkey` is not in this slot
    fn remove(&self, pubkey: &Pubkey) -> Option<Arc<CachedAccount>> {
        let (_, item) = self.cache.remove(pubkey)?;
        let data_len = item.account.data().len() as u64;
        self.size.fetch_sub(data_len, Ordering::Relaxed);
        self.total_size.fetch_sub(data_len, Ordering::Relaxed);
        self.accounts_count.fetch_sub(1, Ordering::Release);
        self.total_accounts_count.fetch_sub(1, Ordering::Relaxed);
        Some(item)
    }

    fn get_cloned(&self, pubkey: &Pubkey) -> Option<Arc<CachedAccount>> {
        self.cache
            .get(pubkey)
            // 1) Maybe can eventually use a Cow to avoid a clone on every read
            // 2) Popping is only safe if it's guaranteed that only
            //    replay/banking threads are reading from the AccountsDb
            .map(|account_ref| account_ref.value().clone())
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn mark_slot_frozen(&self) {
        self.is_frozen.store(true, Ordering::Release);
    }

    pub fn is_frozen(&self) -> bool {
        self.is_frozen.load(Ordering::Acquire)
    }

    /// A held cache is left alone by the dedupe until every snapshot holding it releases it
    pub(crate) fn hold_for_snapshot(&self) {
        self.num_snapshot_holds.fetch_add(1, Ordering::Relaxed);
    }

    /// Released after the snapshot is done reading this cache
    pub(crate) fn release_for_snapshot(&self) {
        self.num_snapshot_holds.fetch_sub(1, Ordering::Release);
    }

    pub(crate) fn is_held_by_snapshot(&self) -> bool {
        self.num_snapshot_holds.load(Ordering::Acquire) > 0
    }

    pub fn total_bytes(&self) -> u64 {
        self.unique_account_writes_size.load(Ordering::Relaxed)
            + self.same_account_writes_size.load(Ordering::Relaxed)
    }
}

impl Deref for SlotCache {
    type Target = DashMap<Pubkey, Arc<CachedAccount>, ahash::RandomState>;
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
    pub(crate) fn new(pubkey: Pubkey, account: AccountSharedData) -> Self {
        Self { account, pubkey }
    }

    pub fn pubkey(&self) -> &Pubkey {
        &self.pubkey
    }
}

/// A hit from `AccountsCache::load_cached`
#[derive(Debug)]
pub enum CachedLoad {
    /// from the write cache: the freshest version visible on this fork
    WriteCache(Arc<CachedAccount>, Slot),
    /// from the read cache: the newest version in storage
    ReadCache(AccountSharedData, Slot),
}

/// What a dedupe of the write cache's roots removed
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DedupeStats {
    pub num_accounts_removed: usize,
    /// The size of the removed accounts' data, in bytes
    pub num_bytes_removed: u64,
}

#[derive(Debug)]
pub struct AccountsCache {
    cache: DashMap<BankId, Arc<SlotCache>, BuildNoHashHasher<BankId>>,
    /// Maps a slot to the id of the bank that stored accounts at it
    bank_ids: RwLock<IntMap<Slot, BankId>>,
    // The read cache's pubkey map, which doubles as the index recording which pubkeys are in
    // this write cache, so one probe covers both caches (see `load_cached`).
    index: Arc<ReadOnlyAccountsCache>,
    // Rooted banks that still have accounts in the write cache, and the slot each stored at. A
    // bank enters via `add_root` if it has a slot cache, and is dropped by `remove_slot` when its
    // slot cache is flushed. Roots form one chain, so bank id order is slot order.
    unflushed_roots: RwLock<BTreeMap<BankId, Slot>>,
    max_flushed_root: MaxFlushedRoot,
    /// The size of account data stored in the whole AccountsCache, in bytes
    total_size: Arc<AtomicU64>,
    /// The number of accounts stored in the whole AccountsCache
    total_accounts_count: Arc<AtomicU64>,
}

impl Default for AccountsCache {
    fn default() -> Self {
        // an unlimited read cache; its evictor never triggers
        Self::new(Arc::new(ReadOnlyAccountsCache::new(
            usize::MAX,
            usize::MAX,
            8,
            8,
        )))
    }
}

impl AccountsCache {
    pub(crate) fn new(read_only_accounts_cache: Arc<ReadOnlyAccountsCache>) -> Self {
        Self {
            cache: DashMap::default(),
            bank_ids: RwLock::default(),
            index: read_only_accounts_cache,
            unflushed_roots: RwLock::default(),
            max_flushed_root: MaxFlushedRoot::default(),
            total_size: Arc::default(),
            total_accounts_count: Arc::default(),
        }
    }

    pub fn new_inner(&self, slot: Slot) -> Arc<SlotCache> {
        Arc::new(SlotCache {
            slot,
            cache: DashMap::default(),
            same_account_writes: AtomicU64::default(),
            same_account_writes_size: AtomicU64::default(),
            unique_account_writes_size: AtomicU64::default(),
            size: AtomicU64::default(),
            total_size: Arc::clone(&self.total_size),
            is_frozen: AtomicBool::default(),
            num_snapshot_holds: AtomicU64::default(),
            accounts_count: AtomicU64::new(0),
            total_accounts_count: Arc::clone(&self.total_accounts_count),
        })
    }
    pub fn size(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }
    pub fn report_size(&self) {
        datapoint_info!(
            "accounts_cache_size",
            ("num_roots", self.unflushed_roots.read().unwrap().len(), i64),
            ("num_slots", self.cache.len(), i64),
            ("total_size", self.size(), i64),
            (
                "total_accounts_count",
                self.total_accounts_count.load(Ordering::Relaxed),
                i64
            ),
            ("num_unique_pubkeys", self.index.num_write_pubkeys(), i64),
        );
    }

    pub fn store(
        &self,
        slot: Slot,
        bank_id: BankId,
        pubkey: &Pubkey,
        account: AccountSharedData,
    ) -> Arc<CachedAccount> {
        let slot_cache = self.bank_cache(bank_id).unwrap_or_else(|| {
            let slot_bank_id = *self
                .bank_ids
                .write()
                .unwrap()
                .entry(slot)
                .or_insert(bank_id);
            debug_assert_eq!(
                slot_bank_id, bank_id,
                "slot {slot} already holds accounts stored by another bank"
            );
            // DashMap entry.or_insert() returns a RefMut, essentially a write lock,
            // which is dropped after this block ends, minimizing time held by the lock.
            // However, we still want to persist the reference to the `SlotStores` behind
            // the lock, hence we clone it out, (`SlotStores` is an Arc so is cheap to clone).
            self.cache
                .entry(bank_id)
                .or_insert_with(|| self.new_inner(slot))
                .clone()
        });
        debug_assert_eq!(slot_cache.slot, slot, "{bank_id:?} stores at another slot");

        let (item, is_new_key) = slot_cache.insert(pubkey, account);
        // Always update the index: even an overwrite within the same slot (is_new_key = false)
        // replaces the `Arc` the index points at. `is_new_key` tells the index whether this also
        // added a slot cache to the ref count.
        self.index
            .insert_write(pubkey, bank_id, slot, &item, is_new_key);
        item
    }

    /// Returns `pubkey`'s account from `bank_id`'s cache, and the slot it was stored at
    pub fn load(&self, bank_id: BankId, pubkey: &Pubkey) -> Option<(Arc<CachedAccount>, Slot)> {
        let slot_cache = self.bank_cache(bank_id)?;
        let account = slot_cache.get_cloned(pubkey)?;
        Some((account, slot_cache.slot))
    }

    /// Removes a slot from the accounts cache and returns the set of pubkeys removed from the index.
    #[must_use]
    pub fn remove_slot(&self, slot: Slot) -> Option<Vec<Pubkey>> {
        let bank_id = {
            // Held across the bank id removal so a concurrent `add_root` can't re-track this bank
            let mut unflushed_roots = self.unflushed_roots.write().unwrap();
            let bank_id = self.bank_ids.write().unwrap().remove(&slot)?;
            // If this bank was a root, it has now left the cache, so stop tracking it as unflushed.
            unflushed_roots.remove(&bank_id);
            bank_id
        };
        let result = self
            .cache
            .remove(&bank_id)
            .map(|(_, slot_cache)| slot_cache);

        result.as_ref().map(|slot_cache| {
            self.index
                .remove_write(bank_id, slot_cache.iter().map(|item| *item.key()))
        })
    }

    /// One lookup covering the write cache and the read cache: returns the newest cached
    /// version of `pubkey` visible from `ancestors`, preferring the write cache.
    ///
    /// A read-cache hit is the newest version in storage: the read cache holds one version per
    /// pubkey and flushing a newer version drops that pubkey's entry, and anything newer than
    /// storage is in the write cache, which wins. So the slot a read-cached account was cached
    /// at only has to be an ancestor.
    pub fn load_cached(&self, pubkey: &Pubkey, ancestors: &Ancestors) -> Option<CachedLoad> {
        match self.index.probe(pubkey, |slot| ancestors.is_ancestor(slot)) {
            Probe::Absent => None,
            Probe::Read(account, slot) => Some(CachedLoad::ReadCache(account, slot)),
            Probe::Write {
                newest,
                max_bank_id,
            } => {
                // The probe carried the version at `max_bank_id`, and no cached version sits
                // above it, so if it is visible here it is the answer and no slot cache is touched.
                if let Some((cached_account, slot)) = newest
                    && self.is_write_visible(max_bank_id, slot, ancestors)
                {
                    return Some(CachedLoad::WriteCache(cached_account, slot));
                }
                if let Some((cached_account, slot)) =
                    self.load_latest_bounded(pubkey, ancestors, max_bank_id)
                {
                    return Some(CachedLoad::WriteCache(cached_account, slot));
                }
                // The write-cache version is not visible on this fork (or was flushed during
                // the search); fall back to the read half.
                self.index
                    .load(pubkey, |slot| ancestors.is_ancestor(slot))
                    .map(|(account, slot)| CachedLoad::ReadCache(account, slot))
            }
        }
    }

    /// Finds the newest write-cache entry for `pubkey` visible from `ancestors`. Searches
    /// ancestors first (highest to lowest), then roots (highest to lowest). Ancestors are
    /// checked exhaustively before roots, so a lower-slot ancestor wins over a higher-slot
    /// root. Returns the account and its slot, or `None` if not found.
    pub fn load_latest(
        &self,
        pubkey: &Pubkey,
        ancestors: &Ancestors,
    ) -> Option<(Arc<CachedAccount>, Slot)> {
        // Exit early if the pubkey isn't in the cache
        let (newest, index_max_bank_id) = self.index.write_newest(pubkey)?;
        // The index carried the version at `index_max_bank_id`, and no cached version sits above
        // it, so if it is visible here it is the answer and no slot cache is touched.
        if let Some((cached_account, slot)) = newest
            && self.is_write_visible(index_max_bank_id, slot, ancestors)
        {
            return Some((cached_account, slot));
        }
        self.load_latest_bounded(pubkey, ancestors, index_max_bank_id)
    }

    /// Is a write-cache version stored by `bank_id` at `slot` visible from `ancestors`? This is
    /// the fast-path equivalent of what `load_latest_bounded` decides by searching: `bank_id` is
    /// either an unrooted ancestor, or a root at or below the lowest ancestor of this fork.
    ///
    /// Note this is deliberately stricter than [`Ancestors::is_ancestor`], which the read half
    /// uses. The read cache only ever holds versions that came from storage, so they are rooted
    /// by construction and a slot below `ancestors.min_slot()` is necessarily visible. The write
    /// cache can still hold a dead fork's slot that has not been purged yet, and such a slot can
    /// sit below `min_slot()` without being visible to this bank, so the root case has to be
    /// confirmed against `unflushed_roots`.
    fn is_write_visible(&self, bank_id: BankId, slot: Slot, ancestors: &Ancestors) -> bool {
        if ancestors.contains_bank(slot, bank_id) {
            return true;
        }
        // With no ancestors there is no bound on which roots are visible, matching the
        // `map_or` that `load_latest_bounded` applies to its root search.
        ancestors
            .iter_bank_ids()
            .next()
            .is_none_or(|min_bank_id| bank_id <= min_bank_id)
            && self.unflushed_roots.read().unwrap().contains_key(&bank_id)
    }

    /// `load_latest`, with the index's max-bank-id probe already done by the caller.
    fn load_latest_bounded(
        &self,
        pubkey: &Pubkey,
        ancestors: &Ancestors,
        index_max_bank_id: BankId,
    ) -> Option<(Arc<CachedAccount>, Slot)> {
        // Ancestors take priority over roots regardless of slot. Walk them in descending order,
        // skipping those above the newest cached version, and return the first (highest) that has it.
        for bank_id in ancestors
            .iter_bank_ids()
            .rev()
            .skip_while(|bank_id| *bank_id > index_max_bank_id)
        {
            if let Some(found) = self.load(bank_id, pubkey) {
                return Some(found);
            }
        }

        // If the slot is not found in the ancestors fall back to searching roots.
        // Bound the search to the oldest ancestor so that roots beyond the querying
        // bank's ancestor chain are not visible. Using the oldest is more correct than
        // the newest because a root between them that is not an ancestor belongs to a
        // different fork and should not be returned.
        let max_root_bank_id = ancestors
            .iter_bank_ids()
            .next()
            .map_or(index_max_bank_id, |bank_id| bank_id.min(index_max_bank_id));

        let r_unflushed_roots = self.unflushed_roots.read().unwrap();
        for &bank_id in r_unflushed_roots
            .range(..=max_root_bank_id)
            .rev()
            .map(|(bank_id, _slot)| bank_id)
        {
            if let Some(found) = self.load(bank_id, pubkey) {
                return Some(found);
            }
        }
        drop(r_unflushed_roots);

        // Found nothing, the version of the account in the cache must be on a different fork
        None
    }

    /// Removes older versions of accounts from `roots`, keeping each account's newest version
    /// between consecutive `boundaries`. A version is never removed because of a newer version
    /// above a boundary at or above it, so anything that only sees slots up to a boundary still
    /// finds the same version. Roots held by a snapshot are left as they are.
    pub fn dedupe_roots(&self, roots: &BTreeSet<Slot>, boundaries: &BTreeSet<Slot>) -> DedupeStats {
        let mut stats = DedupeStats::default();
        // The accounts with a version in a newer root between the same boundaries
        let mut newer_pubkeys = ahash::HashSet::default();
        let mut current_boundary = None;

        // Iterate from newest root to oldest
        for &root in roots.iter().rev() {
            // The lowest boundary at or above a root separates it from newer roots
            let boundary = boundaries.range(root..).next().copied();
            if boundary != current_boundary {
                newer_pubkeys.clear();
                current_boundary = boundary;
            }

            let Some(bank_id) = self.bank_id(root) else {
                continue;
            };
            let Some(slot_cache) = self.bank_cache(bank_id) else {
                continue;
            };
            if slot_cache.is_held_by_snapshot() {
                // Its versions still supersede older roots' versions
                newer_pubkeys.extend(slot_cache.iter().map(|entry| *entry.key()));
                continue;
            }
            // Collected first, since removing while iterating the dashmap would deadlock
            let older_pubkeys: Vec<_> = slot_cache
                .iter()
                .map(|entry| *entry.key())
                .filter(|pubkey| !newer_pubkeys.insert(*pubkey))
                .collect();
            for pubkey in &older_pubkeys {
                let removed = slot_cache
                    .remove(pubkey)
                    .expect("only the dedupe removes accounts from a root");
                stats.num_accounts_removed += 1;
                stats.num_bytes_removed += removed.account.data().len() as u64;
            }
            let removed_from_index = self.index.remove_write(bank_id, older_pubkeys);
            debug_assert!(
                removed_from_index.is_empty(),
                "the newest version of each account stays in the cache",
            );
        }
        stats
    }

    /// Returns the cache of the bank that stored accounts at `slot`
    pub fn slot_cache(&self, slot: Slot) -> Option<Arc<SlotCache>> {
        let bank_id = self.bank_id(slot)?;
        self.bank_cache(bank_id)
    }

    /// Returns `bank_id`'s cache
    pub fn bank_cache(&self, bank_id: BankId) -> Option<Arc<SlotCache>> {
        self.cache
            .get(&bank_id)
            .map(|result| result.value().clone())
    }

    /// Returns the id of the bank that stored accounts at `slot`
    pub fn bank_id(&self, slot: Slot) -> Option<BankId> {
        self.bank_ids.read().unwrap().get(&slot).copied()
    }

    /// A root without a slot cache is not tracked; nothing stores into a slot once it is rooted
    pub fn add_root(&self, root: Slot) {
        // Held across the lookup so a concurrent `remove_slot` can't leave this root stranded
        let mut unflushed_roots = self.unflushed_roots.write().unwrap();
        if let Some(bank_id) = self.bank_id(root) {
            unflushed_roots.insert(bank_id, root);
        }
    }

    pub fn num_unflushed_roots(&self) -> usize {
        self.unflushed_roots.read().unwrap().len()
    }

    /// Returns whether `slot` is a root that has been added but not yet flushed to storage.
    pub fn contains_unflushed_root(&self, slot: Slot) -> bool {
        self.bank_id(slot)
            .is_some_and(|bank_id| self.unflushed_roots.read().unwrap().contains_key(&bank_id))
    }

    /// Returns the unflushed roots up to and including `max_root` (or all roots if `None`). The
    /// returned roots remain tracked as unflushed until `remove_slot` drops each one when its cache
    /// is flushed.
    pub fn roots_to_flush(&self, max_root: Option<Slot>) -> BTreeSet<Slot> {
        // `None` means no upper bound, i.e. every root.
        let max_root = max_root.unwrap_or(Slot::MAX);
        self.unflushed_roots
            .read()
            .unwrap()
            .values()
            .take_while(|slot| **slot <= max_root)
            .copied()
            .collect()
    }

    pub fn cached_frozen_slots(&self) -> Vec<Slot> {
        self.cache
            .iter()
            .filter_map(|item| {
                let slot_cache = item.value();
                slot_cache.is_frozen().then_some(slot_cache.slot)
            })
            .collect()
    }

    pub fn contains(&self, slot: Slot) -> bool {
        self.bank_ids.read().unwrap().contains_key(&slot)
    }

    pub fn contains_pubkey(&self, pubkey: &Pubkey) -> bool {
        self.index.contains_write(pubkey)
    }

    /// Returns a vector of all pubkeys currently in the cache index.
    /// In iterator is not returned as the dashmap shards would be readlocked for the duration
    /// of the iterator
    pub(crate) fn cached_pubkeys(&self) -> Vec<Pubkey> {
        self.index.write_pubkeys()
    }

    pub fn num_slots(&self) -> usize {
        self.cache.len()
    }

    pub fn fetch_max_flush_root(&self) -> Option<Slot> {
        self.max_flushed_root.get()
    }

    pub fn set_max_flush_root(&self, root: Slot) {
        self.max_flushed_root.fetch_max(root);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, test_case::test_case};

    impl AccountsCache {
        // Removes slots less than or equal to `max_root`. Only safe to pass in a rooted slot,
        // otherwise the slot removed could still be undergoing replay!
        pub fn remove_slots_le(&self, max_root: Slot) -> Vec<(Slot, Arc<SlotCache>)> {
            let mut removed_slots = vec![];
            self.cache.retain(|_bank_id, slot_cache| {
                let should_remove = slot_cache.slot <= max_root;
                if should_remove {
                    removed_slots.push((slot_cache.slot, slot_cache.clone()))
                }
                !should_remove
            });
            self.bank_ids
                .write()
                .unwrap()
                .retain(|slot, _bank_id| *slot > max_root);
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
            BankId::new(inserted_slot),
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
            BankId::new(inserted_slot),
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
    fn test_slot_cache_len_tracks_unique_accounts() {
        let cache = AccountsCache::default();
        let slot = 0;
        let pubkey = Pubkey::new_unique();
        let other_pubkey = Pubkey::new_unique();

        cache.store(
            slot,
            BankId::new(slot),
            &pubkey,
            AccountSharedData::new(1, 0, &Pubkey::default()),
        );
        let slot_cache = cache.slot_cache(slot).unwrap();
        assert_eq!(slot_cache.len(), 1);

        cache.store(
            slot,
            BankId::new(slot),
            &pubkey,
            AccountSharedData::new(2, 0, &Pubkey::default()),
        );
        assert_eq!(slot_cache.len(), 1);

        cache.store(
            slot,
            BankId::new(slot),
            &other_pubkey,
            AccountSharedData::new(3, 0, &Pubkey::default()),
        );
        assert_eq!(slot_cache.len(), 2);
    }

    #[test]
    fn test_max_flushed_root_fetch_max() {
        let root = MaxFlushedRoot::default();
        assert_eq!(root.get(), None);

        root.fetch_max(0);
        assert_eq!(root.get(), Some(0));

        // first real slot replaces sentinel
        root.fetch_max(10);
        assert_eq!(root.get(), Some(10));

        // larger slot wins
        root.fetch_max(20);
        assert_eq!(root.get(), Some(20));

        // smaller and equal slots are ignored
        root.fetch_max(5);
        assert_eq!(root.get(), Some(20));
        root.fetch_max(20);
        assert_eq!(root.get(), Some(20));
    }

    #[test]
    fn test_cache_index_insert_and_max_bank_id() {
        let cache = AccountsCache::default();
        let pubkey = Pubkey::new_unique();
        let account = |slot| AccountSharedData::new(slot, 0, &Pubkey::default());
        // the version the index recorded for max_bank_id, by lamports, and its slot
        let newest = |pubkey: &Pubkey| {
            cache
                .index
                .write_newest(pubkey)
                .map(|(newest, max_bank_id)| {
                    (
                        newest.map(|(account, slot)| (account.account.lamports(), slot)),
                        max_bank_id,
                    )
                })
        };

        // Initially empty
        assert!(newest(&pubkey).is_none());

        // Insert from bank 5
        cache.store(5, BankId::new(5), &pubkey, account(5));
        assert_eq!(newest(&pubkey), Some((Some((5, 5)), BankId::new(5))));

        // Insert same pubkey from a higher bank id updates max_bank_id and the recorded version
        cache.store(10, BankId::new(10), &pubkey, account(10));
        assert_eq!(newest(&pubkey), Some((Some((10, 10)), BankId::new(10))));

        // Insert same pubkey from a lower bank id does not decrease max_bank_id or replace the
        // version
        cache.store(3, BankId::new(3), &pubkey, account(3));
        assert_eq!(newest(&pubkey), Some((Some((10, 10)), BankId::new(10))));

        // An overwrite within max_bank_id does replace the version, since the old `Arc` is no
        // longer what that bank's cache holds
        cache.store(10, BankId::new(10), &pubkey, account(11));
        assert_eq!(newest(&pubkey), Some((Some((11, 10)), BankId::new(10))));

        // Dropping max_bank_id's cache leaves max_bank_id behind as a bound, with no version
        let _ = cache.remove_slot(10);
        assert_eq!(newest(&pubkey), Some((None, BankId::new(10))));
    }

    #[test]
    fn test_cache_index_remove_decrements_count() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        assert_eq!(cache.index.num_write_pubkeys(), 0);

        // Store pubkey into 3 different slots
        cache.store(
            1,
            BankId::new(1),
            &pk,
            AccountSharedData::new(1, 0, &Pubkey::default()),
        );
        assert_eq!(cache.index.num_write_pubkeys(), 1);
        cache.store(
            5,
            BankId::new(5),
            &pk,
            AccountSharedData::new(5, 0, &Pubkey::default()),
        );
        cache.store(
            3,
            BankId::new(3),
            &pk,
            AccountSharedData::new(3, 0, &Pubkey::default()),
        );
        // Same pubkey across 3 slots — still only 1 unique pubkey
        assert_eq!(cache.index.num_write_pubkeys(), 1);

        // Remove and drop slot 1 — entry should still exist (count goes from 3 to 2)
        let removed = cache.remove_slot(1);
        assert!(removed.is_some());
        drop(removed);
        assert_eq!(cache.index.write_max_bank_id(&pk), Some(BankId::new(5)));
        assert_eq!(cache.index.num_write_pubkeys(), 1);

        // Remove and drop slot 5 — entry should still exist (count goes from 2 to 1)
        // max_bank_id stays stale at 5 because the index doesn't scan for a new max on removal
        let removed = cache.remove_slot(5);
        assert!(removed.is_some());
        drop(removed);
        assert!(cache.index.write_max_bank_id(&pk).is_some());
        assert_eq!(cache.index.num_write_pubkeys(), 1);

        // Remove and drop slot 3 — last reference gone, entry removed
        let removed = cache.remove_slot(3);
        assert!(removed.is_some());
        drop(removed);
        assert!(cache.index.write_max_bank_id(&pk).is_none());
        assert_eq!(cache.index.num_write_pubkeys(), 0);
    }

    #[test]
    fn test_remove_slot_cleans_up_index() {
        let cache = AccountsCache::default();
        let pk1 = Pubkey::new_unique();
        let pk2 = Pubkey::new_unique();

        // pk1 in slots 1 and 3; pk2 only in slot 1
        cache.store(
            1,
            BankId::new(1),
            &pk1,
            AccountSharedData::new(1, 0, &Pubkey::default()),
        );
        cache.store(
            1,
            BankId::new(1),
            &pk2,
            AccountSharedData::new(1, 0, &Pubkey::default()),
        );
        cache.store(
            3,
            BankId::new(3),
            &pk1,
            AccountSharedData::new(1, 0, &Pubkey::default()),
        );

        // Before removal: both pubkeys are in the index
        assert!(cache.index.write_max_bank_id(&pk1).is_some());
        assert!(cache.index.write_max_bank_id(&pk2).is_some());

        // Remove slot 1 — pk2 should disappear, pk1 still present (in slot 3)
        let pubkeys_removed = cache.remove_slot(1);
        assert_eq!(pubkeys_removed, Some(vec![pk2]));
        assert!(cache.index.write_max_bank_id(&pk1).is_some());
        assert!(cache.index.write_max_bank_id(&pk2).is_none());

        // Remove slot 3 — pk1 should also disappear
        let pubkeys_removed = cache.remove_slot(3);
        assert_eq!(pubkeys_removed, Some(vec![pk1]));
        assert!(cache.index.write_max_bank_id(&pk1).is_none());
    }

    /// Tests that `load_latest` returns the correct slot and account value
    /// given various combinations of ancestor slots and root slots.
    ///
    /// Ancestors always take priority over roots regardless of slot
    // None case
    // `uncached_ancestors` are slots added to the Ancestors set but with no account
    // data stored in the cache. This lets us test root bounding by min_slot
    // without the ancestor path short-circuiting the lookup.
    #[test_case(&[], &[], &[], None; "not ancestor not root")]
    #[test_case(&[10], &[], &[], Some(10); "ancestor only")]
    #[test_case(&[5, 10, 15], &[], &[], Some(15); "highest ancestor returned")]
    #[test_case(&[], &[10, 20], &[], Some(20); "rooted, with no ancestors")]
    #[test_case(&[5], &[20], &[], Some(5); "ancestor wins over higher root")]
    // Root beyond ancestors.min_slot() is excluded; older root still found
    #[test_case(&[], &[5, 11], &[10], Some(5); "root beyond min ancestor excluded")]
    // Root within min_slot bound is still returned
    #[test_case(&[], &[10], &[15], Some(10); "root below min ancestor returned")]
    fn test_load_latest_slot_priority(
        ancestor_slots: &[Slot],
        root_slots: &[Slot],
        uncached_ancestors: &[Slot],
        expected: Option<Slot>,
    ) {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        for &slot in ancestor_slots {
            cache.store(
                slot,
                BankId::new(slot),
                &pk,
                AccountSharedData::new(slot, 0, &Pubkey::default()),
            );
        }
        for &slot in root_slots {
            cache.store(
                slot,
                BankId::new(slot),
                &pk,
                AccountSharedData::new(slot, 0, &Pubkey::default()),
            );
            cache.add_root(slot);
        }

        let mut all_ancestors: Vec<Slot> = ancestor_slots.to_vec();
        all_ancestors.extend_from_slice(uncached_ancestors);
        let ancestors = Ancestors::from(all_ancestors);
        let result = cache.load_latest(&pk, &ancestors).map(|(account, slot)| {
            assert_eq!(account.account.lamports(), slot);
            slot
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_load_latest_ignores_non_ancestor_non_root_slot() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        // Store an account at slot 10, but don't add it as an ancestor or root.
        cache.store(
            10,
            BankId::new(10),
            &pk,
            AccountSharedData::new(10, 0, &Pubkey::default()),
        );

        let ancestors = Ancestors::from(vec![5, 15]);
        let result = cache.load_latest(&pk, &ancestors);
        assert!(result.is_none());
    }

    /// A write-cache slot below the fork's lowest ancestor is visible only if it really is a
    /// root. `Ancestors::is_ancestor` accepts it on the slot bound alone, which is safe for the
    /// read cache — everything in it came from storage, so it is rooted — but not for the write
    /// cache, which can still hold a dead fork's slot that has not been purged yet.
    #[test]
    fn test_load_ignores_unrooted_slot_below_min_ancestor() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        // slot 10 is on a dead fork: written, never rooted, not yet purged
        cache.store(
            10,
            BankId::new(10),
            &pk,
            AccountSharedData::new(10, 0, &Pubkey::default()),
        );

        let ancestors = Ancestors::from(vec![15, 20]);
        // the read half's predicate would accept slot 10 on the bound alone
        assert!(ancestors.is_ancestor(10));
        assert!(!cache.is_write_visible(BankId::new(10), 10, &ancestors));
        assert!(cache.load_latest(&pk, &ancestors).is_none());
        assert!(cache.load_cached(&pk, &ancestors).is_none());

        // rooting the slot is what makes it visible
        cache.add_root(10);
        assert_eq!(
            cache
                .load_latest(&pk, &ancestors)
                .map(|(_account, slot)| slot),
            Some(10)
        );
    }

    /// Flushing `max_bank_id` must stop the index offering that version as the newest, even
    /// while the pubkey is still cached by a lower bank.
    #[test]
    fn test_newest_dropped_when_max_bank_id_leaves_the_cache() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        for slot in [10, 20] {
            cache.store(
                slot,
                BankId::new(slot),
                &pk,
                AccountSharedData::new(slot, 0, &Pubkey::default()),
            );
            cache.add_root(slot);
        }

        // slot 20 flushes to storage and leaves the cache, so only slot 10 is still cached
        let _ = cache.remove_slot(20);

        let ancestors = Ancestors::from(vec![25]);
        let (account, slot) = cache.load_latest(&pk, &ancestors).unwrap();
        assert_eq!(slot, 10);
        assert_eq!(account.account.lamports(), 10);
    }

    /// `load_cached` covers both caches: the write cache wins when its version is visible,
    /// the read cache answers when it is not.
    #[test]
    fn test_load_cached_prefers_write_cache() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        // nothing cached at all
        assert!(cache.load_cached(&pk, &Ancestors::from(vec![10])).is_none());

        // a rooted version in the read cache
        let rooted_account = AccountSharedData::new(1, 0, &Pubkey::default());
        cache.index.store(pk, 5, rooted_account.clone());
        match cache.load_cached(&pk, &Ancestors::from(vec![10])) {
            Some(CachedLoad::ReadCache(account, slot)) => {
                assert_eq!(account, rooted_account);
                assert_eq!(slot, 5);
            }
            other => panic!("expected a read cache hit, got {other:?}"),
        }

        // a newer version in the write cache wins
        let unrooted_account = AccountSharedData::new(2, 0, &Pubkey::default());
        cache.store(10, BankId::new(10), &pk, unrooted_account.clone());
        match cache.load_cached(&pk, &Ancestors::from(vec![10])) {
            Some(CachedLoad::WriteCache(cached_account, slot)) => {
                assert_eq!(cached_account.account, unrooted_account);
                assert_eq!(slot, 10);
            }
            other => panic!("expected a write cache hit, got {other:?}"),
        }

        // the write-cache version is on a different fork: the read cache answers
        match cache.load_cached(&pk, &Ancestors::from(vec![11])) {
            Some(CachedLoad::ReadCache(account, slot)) => {
                assert_eq!(account, rooted_account);
                assert_eq!(slot, 5);
            }
            other => panic!("expected a read cache hit, got {other:?}"),
        }
    }

    #[test]
    fn test_visibility_after_flush() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();

        cache.store(
            10,
            BankId::new(10),
            &pk,
            AccountSharedData::new(100, 0, &Pubkey::default()),
        );
        cache.add_root(10);
        // A flush finishes a slot with `remove_slot`, which drops both its cache and its
        // unflushed-root tracking; call it directly here to stand in for that flush.
        let _ = cache.remove_slot(10);

        // With the slot gone from the cache and untracked as a root, it is not visible.
        let empty = Ancestors::default();
        assert!(cache.load_latest(&pk, &empty).is_none());
        assert!(cache.unflushed_roots.read().unwrap().is_empty());
    }

    #[test]
    fn test_roots_to_flush_with_max_root() {
        let cache = AccountsCache::default();
        for slot in [1, 3, 5, 7] {
            cache.store(
                slot,
                BankId::new(slot),
                &Pubkey::new_unique(),
                AccountSharedData::new(1, 0, &Pubkey::default()),
            );
            cache.add_root(slot);
        }

        // roots_to_flush(Some(5)) returns {1, 3, 5}, excluding 7.
        let to_flush = cache.roots_to_flush(Some(5));
        assert_eq!(to_flush, BTreeSet::from([1, 3, 5]));

        // roots_to_flush only reads: the tracked roots are unchanged.
        assert_eq!(cache.roots_to_flush(None), BTreeSet::from([1, 3, 5, 7]));
    }

    #[test]
    fn test_roots_to_flush_none_takes_all() {
        let cache = AccountsCache::default();
        for slot in [2, 4, 6] {
            cache.store(
                slot,
                BankId::new(slot),
                &Pubkey::new_unique(),
                AccountSharedData::new(1, 0, &Pubkey::default()),
            );
            cache.add_root(slot);
        }

        let to_flush = cache.roots_to_flush(None);
        assert_eq!(to_flush, BTreeSet::from([2, 4, 6]));

        // roots_to_flush only reads: the tracked roots are unchanged.
        assert_eq!(cache.roots_to_flush(None), BTreeSet::from([2, 4, 6]));
    }

    #[test]
    fn test_remove_slot_drops_unflushed_root() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();
        cache.store(
            1,
            BankId::new(1),
            &pk,
            AccountSharedData::new(1, 0, &Pubkey::default()),
        );
        cache.add_root(1);
        cache.store(
            2,
            BankId::new(2),
            &pk,
            AccountSharedData::new(2, 0, &Pubkey::default()),
        );
        cache.add_root(2);

        // remove_slot drops slot 1 from both the cache and the tracked roots, leaving slot 2.
        let _ = cache.remove_slot(1);
        assert_eq!(cache.roots_to_flush(None), BTreeSet::from([2]));
    }

    /// Stores `pubkey` with `data_len` bytes of data in each of `roots`, and roots them
    fn store_in_roots(cache: &AccountsCache, pubkey: &Pubkey, roots: &[Slot], data_len: usize) {
        for &root in roots {
            cache.store(
                root,
                BankId::new(root),
                pubkey,
                AccountSharedData::new(root, data_len, &Pubkey::default()),
            );
            cache.add_root(root);
        }
    }

    #[test]
    fn test_dedupe_roots_keeps_newest_version() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();
        let other_pk = Pubkey::new_unique();
        store_in_roots(&cache, &pk, &[1, 2, 3], 10);
        store_in_roots(&cache, &other_pk, &[2], 10);

        let stats = cache.dedupe_roots(&cache.roots_to_flush(None), &BTreeSet::new());
        assert_eq!(
            stats,
            DedupeStats {
                num_accounts_removed: 2,
                num_bytes_removed: 20,
            }
        );
        assert!(cache.load(BankId::new(1), &pk).is_none());
        assert!(cache.load(BankId::new(2), &pk).is_none());
        assert_eq!(cache.load(BankId::new(3), &pk).unwrap().0.account.lamports(), 3);
        assert!(cache.load(BankId::new(2), &other_pk).is_some());
        assert_eq!(cache.size(), 20);
        assert_eq!(cache.slot_cache(2).unwrap().len(), 1);
        // only slot 3 still holds pk, so dropping it removes pk from the cache
        assert_eq!(cache.remove_slot(3), Some(vec![pk]));
    }

    #[test]
    fn test_dedupe_roots_keeps_versions_across_boundaries() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();
        store_in_roots(&cache, &pk, &[1, 2, 3, 4], 0);

        // A boundary at 2 keeps the newest version at or below 2, which is what a scan that
        // sees up to 2 finds
        let stats = cache.dedupe_roots(&cache.roots_to_flush(None), &BTreeSet::from([2]));
        assert_eq!(stats.num_accounts_removed, 2);
        assert!(cache.load(BankId::new(1), &pk).is_none());
        assert!(cache.load(BankId::new(2), &pk).is_some());
        assert!(cache.load(BankId::new(3), &pk).is_none());
        assert!(cache.load(BankId::new(4), &pk).is_some());
    }

    #[test]
    fn test_dedupe_roots_only_touches_given_roots() {
        let cache = AccountsCache::default();
        let pk = Pubkey::new_unique();
        store_in_roots(&cache, &pk, &[1, 2, 3], 0);

        let stats = cache.dedupe_roots(&BTreeSet::from([2, 3]), &BTreeSet::new());
        assert_eq!(stats.num_accounts_removed, 1);
        assert!(cache.load(BankId::new(1), &pk).is_some());
        assert!(cache.load(BankId::new(2), &pk).is_none());
        assert!(cache.load(BankId::new(3), &pk).is_some());
    }
}
