use {
    super::{
        DiskIndexValue, IndexValue, ReclaimsSlotList, SlotList, SlotListItem, UpsertReclaim,
        account_map_entry::{AccountMapEntry, AccountMapEntryMeta, PreAllocatedAccountMapEntry},
        bucket_map_holder::{AGE_MASK, Age, AtomicAge, BucketMapHolder, age_distance},
        stats::Stats,
        tag::{Tag, TagCalculator, TagHasherBuilder},
    },
    rand::{Rng, rng},
    solana_bucket_map::bucket_api::BucketApi,
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet, hash_map::Entry},
        fmt::Debug,
        mem,
        num::NonZeroUsize,
        sync::{
            Arc, Mutex, RwLock,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    },
};

#[derive(Debug, Default)]
pub struct StartupStats {
    pub copy_data_us: AtomicU64,
}

// one instance of this represents one bin of the accounts index.
pub struct InMemAccountsIndex<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> {
    last_age_flushed: AtomicAge,

    // backing store
    map_internal: RwLock<HashMap<Tag, AccountMapEntry<T>, TagHasherBuilder>>,
    /// computes the `Tag` this bin's map is keyed by
    tag_calculator: TagCalculator,
    storage: Arc<BucketMapHolder<T, U>>,
    _bin: usize,

    bucket: Option<Arc<BucketApi<(Slot, U)>>>,

    // set to true while this bin is being actively flushed
    flushing_active: AtomicBool,

    /// info to streamline initial index generation
    startup_info: StartupInfo<T, U>,

    /// how many more ages to skip before this bucket is scanned.
    /// When this reaches 0, this bucket is scanned.
    ages_to_skip_before_scan: AtomicAge,

    /// an individual bucket will scan for evictions every 1/num_ages_to_distribute_scans ages
    /// Higher numbers mean we scan less buckets/s
    /// Lower numbers mean we scan more buckets/s
    num_ages_to_distribute_scans: Age,

    /// stats related to starting up
    pub(crate) startup_stats: Arc<StartupStats>,

    /// If true, flush dirty entries to disk once `slot_list.len() == 1`, making it evictable
    should_write_through: bool,
}

impl<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> Debug for InMemAccountsIndex<T, U> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

/// An entry was inserted into the index; did it already exist in the index?
#[derive(Debug)]
pub enum InsertNewEntryResults<T> {
    DidNotExist,
    Existed {
        /// the version that lost, ie. the one from the older slot
        older_version: (Slot, T),
        location: ExistedLocation,
    },
}

/// An entry was inserted into the index that previously existed; where did it previously exist?
#[derive(Debug)]
pub enum ExistedLocation {
    InMem,
    OnDisk,
}

#[derive(Default, Debug)]
struct StartupInfoDuplicates<T: IndexValue> {
    /// older versions of pubkeys that were found in more than one slot.
    /// The index holds the version from the newest slot; every other version is here.
    duplicates: Vec<(Slot, Pubkey, T)>,
}

#[derive(Default, Debug)]
struct StartupInfo<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> {
    /// entries to add next time we are flushing to disk
    insert: Mutex<Vec<(Pubkey, (Slot, U))>>,
    /// pubkeys with more than 1 entry
    duplicates: Mutex<StartupInfoDuplicates<T>>,
}

impl<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> InMemAccountsIndex<T, U> {
    pub fn new(
        storage: &Arc<BucketMapHolder<T, U>>,
        bin: usize,
        num_initial_accounts: Option<usize>,
    ) -> Self {
        let num_ages_to_distribute_scans = AGE_MASK - storage.ages_to_stay_in_cache;

        let map_internal = if let Some(num_initial_accounts) = num_initial_accounts {
            let capacity_per_bin = num_initial_accounts / storage.bins;
            RwLock::new(HashMap::with_capacity_and_hasher(
                capacity_per_bin,
                TagHasherBuilder,
            ))
        } else {
            RwLock::default()
        };

        Self {
            map_internal,
            tag_calculator: TagCalculator::default(),
            storage: Arc::clone(storage),
            _bin: bin,
            bucket: storage
                .disk
                .as_ref()
                .map(|disk| disk.get_bucket_from_index(bin))
                .cloned(),
            flushing_active: AtomicBool::default(),
            // initialize this to max, to make it clear we have not flushed at age 0, the starting age
            last_age_flushed: AtomicAge::new(Age::MAX),
            startup_info: StartupInfo::default(),
            // Spread out the scanning across all ages within the window.
            // This causes us to scan 1/N of the bins each 'Age'
            ages_to_skip_before_scan: AtomicAge::new(
                rng().random_range(0..num_ages_to_distribute_scans),
            ),
            num_ages_to_distribute_scans,
            startup_stats: Arc::clone(&storage.startup_stats),
            should_write_through: storage.should_write_through(),
        }
    }

    /// true if this bucket needs to call flush for the current age
    /// we need to scan each bucket once per value of age
    fn get_should_age(&self, age: Age) -> bool {
        let last_age_flushed = self.last_age_flushed();
        last_age_flushed != age
    }

    /// called after flush scans this bucket at the current age
    fn set_has_aged(&self, age: Age, can_advance_age: bool) {
        self.last_age_flushed.store(age, Ordering::Release);
        self.storage.bucket_flushed_at_current_age(can_advance_age);
    }

    fn last_age_flushed(&self) -> Age {
        self.last_age_flushed.load(Ordering::Acquire)
    }

    /// the `Tag` this bin's map is keyed by
    #[inline]
    fn tag(&self, pubkey: &Pubkey) -> Tag {
        self.tag_calculator.tag_from_pubkey(pubkey)
    }

    /// return all keys in this bin
    ///
    /// The in-mem map is keyed by `Tag`, so the pubkeys come from the disk index, which holds
    /// every pubkey in the bin: write-through writes an entry to disk as soon as it is modified,
    /// and index generation writes every entry to disk before the in-mem index is populated.
    ///
    /// Panics without a disk index. `in_mem_entries` enumerates the bin in that case, and the
    /// pubkey of each entry is recoverable from the account record it points at.
    pub fn keys(&self) -> Vec<Pubkey> {
        Self::update_stat(&self.stats().keys, 1);

        let disk = self
            .bucket
            .as_ref()
            .expect("keys() requires a disk index to recover pubkeys from");
        disk.keys()
    }

    /// return the entry of every pubkey held in memory in this bin
    pub fn in_mem_entries(&self) -> Vec<SlotListItem<T>> {
        self.map_internal
            .read()
            .unwrap()
            .values()
            .map(|entry| entry.entry())
            .collect()
    }

    fn load_from_disk(&self, pubkey: &Pubkey) -> Option<SlotList<U>> {
        self.bucket.as_ref().and_then(|disk| {
            let m = Measure::start("load_disk_found_count");
            let entry_disk = disk.read_value(pubkey);
            match &entry_disk {
                Some(_) => {
                    Self::update_time_stat(&self.stats().load_disk_found_us, m);
                    Self::update_stat(&self.stats().load_disk_found_count, 1);
                }
                None => {
                    Self::update_time_stat(&self.stats().load_disk_missing_us, m);
                    Self::update_stat(&self.stats().load_disk_missing_count, 1);
                }
            }
            entry_disk.map(|(slot_list, _ref_count): (Vec<_>, _)| {
                assert_eq!(
                    slot_list.len(),
                    1,
                    "the disk index holds a single entry per pubkey"
                );
                [slot_list[0]]
            })
        })
    }

    /// lookup 'pubkey' in disk map.
    /// If it is found, convert it to a cache entry and return the cache entry.
    /// Cache entries from this function will always not be dirty.
    fn load_account_entry_from_disk(&self, pubkey: &Pubkey) -> Option<AccountMapEntry<T>> {
        let entry_disk = self.load_from_disk(pubkey)?; // returns None if not on disk
        let entry_cache = self.disk_to_cache_entry(entry_disk);
        debug_assert!(!entry_cache.dirty());
        Some(entry_cache)
    }

    /// lookup 'pubkey' by only looking in memory. Does not look on disk.
    /// callback is called whether pubkey is found or not
    pub(super) fn get_only_in_mem<RT>(
        &self,
        pubkey: &Pubkey,
        update_age: bool,
        callback: impl for<'a> FnOnce(Option<&'a AccountMapEntry<T>>) -> RT,
    ) -> RT {
        let mut found = true;
        let tag = self.tag(pubkey);
        let mut m = Measure::start("get");
        let result = {
            let map = self.map_internal.read().unwrap();
            let result = map.get(&tag);
            m.stop();

            callback(if let Some(entry) = result {
                if update_age {
                    self.set_age_to_future(entry, false);
                }
                Some(entry)
            } else {
                drop(map);
                found = false;
                None
            })
        };

        let stats = self.stats();
        let (count, time) = if found {
            (&stats.gets_from_mem, &stats.get_mem_us)
        } else {
            (&stats.gets_missing, &stats.get_missing_us)
        };
        Self::update_stat(time, m.as_us());
        Self::update_stat(count, 1);

        result
    }

    /// set age of 'entry' to the future
    /// if 'is_cached', age will be set farther
    fn set_age_to_future(&self, entry: &AccountMapEntry<T>, is_cached: bool) {
        entry.set_age(self.storage.future_age_to_flush(is_cached));
    }

    /// lookup 'pubkey' in index (in_mem or disk).
    /// call 'callback' whether found or not
    pub(super) fn get_internal_inner<RT>(
        &self,
        pubkey: &Pubkey,
        // return true if item should be added to in_mem cache
        callback: impl FnOnce(Option<&AccountMapEntry<T>>) -> (bool, RT),
    ) -> RT {
        self.get_only_in_mem(pubkey, true, |entry| {
            if let Some(entry) = entry {
                callback(Some(entry)).1
            } else {
                // not in cache, look on disk
                let stats = self.stats();
                let disk_entry = self.load_account_entry_from_disk(pubkey);
                if disk_entry.is_none() {
                    return callback(None).1;
                }
                let disk_entry = disk_entry.unwrap();
                let mut map = self.map_internal.write().unwrap();
                let capacity_pre = map.capacity();
                let entry = map.entry(self.tag(pubkey));
                let retval = match entry {
                    Entry::Occupied(occupied) => callback(Some(occupied.get())).1,
                    Entry::Vacant(vacant) => {
                        debug_assert!(!disk_entry.dirty());
                        let (add_to_cache, rt) = callback(Some(&disk_entry));
                        // We are holding a write lock to the in-memory map.
                        // This pubkey is not in the in-memory map.
                        // If the entry is now dirty, then it must be put in the cache or the modifications will be lost.
                        if add_to_cache || disk_entry.dirty() {
                            stats.inc_mem_count();
                            vacant.insert(disk_entry);
                        }
                        rt
                    }
                };
                let capacity_post = map.capacity();
                drop(map);
                stats.update_in_mem_capacity(capacity_pre, capacity_post);
                retval
            }
        })
    }

    fn remove_if_slot_list_empty_value(&self, is_empty: bool) -> bool {
        if is_empty {
            self.stats().inc_delete();
            true
        } else {
            false
        }
    }

    fn delete_disk_key(&self, pubkey: &Pubkey) {
        if let Some(disk) = self.bucket.as_ref() {
            disk.delete_key(pubkey)
        }
    }

    /// return false if the entry is in the index (disk or memory) and has a slot list len > 0
    /// return true in all other cases, including if the entry is NOT in the index at all
    fn remove_if_slot_list_empty_entry(
        &self,
        pubkey: &Pubkey,
        entry: Entry<Tag, AccountMapEntry<T>>,
    ) -> bool {
        match entry {
            Entry::Occupied(occupied) => {
                let result =
                    self.remove_if_slot_list_empty_value(occupied.get().slot_list().len() == 0);
                if result {
                    // note there is a potential race here that has existed.
                    // if someone else holds the arc,
                    //  then they think the item is still in the index and can make modifications.
                    // We have to have a write lock to the map here, which means nobody else can get
                    //  the arc, but someone may already have retrieved a clone of it.
                    // account index in_mem flushing is one such possibility
                    self.delete_disk_key(pubkey);
                    self.stats().dec_mem_count();
                    occupied.remove();
                }
                result
            }
            Entry::Vacant(_vacant) => {
                // not in cache, look on disk
                let entry_disk = self.load_from_disk(pubkey);
                match entry_disk {
                    Some(entry_disk) => {
                        // on disk
                        if self.remove_if_slot_list_empty_value(entry_disk.is_empty()) {
                            // not in cache, but on disk, so just delete from disk
                            self.delete_disk_key(pubkey);
                            true
                        } else {
                            // could insert into cache here, but not required for correctness and value is unclear
                            false
                        }
                    }
                    None => true, // not in cache or on disk, but slot list is 'empty' and entry is not in index, so return true
                }
            }
        }
    }

    /// Drain the slot list for `pubkey` into `reclaims` and remove the pubkey from the index.
    /// Does nothing if `pubkey` was never indexed.
    pub fn delete(&self, pubkey: &Pubkey, reclaims: &mut ReclaimsSlotList<T>) {
        let mut map = self.map_internal.write().unwrap();
        match map.entry(self.tag(pubkey)) {
            Entry::Occupied(occupied) => {
                reclaims.extend(occupied.get().slot_list().iter().copied());
                self.delete_disk_key(pubkey);
                self.stats().dec_mem_count();
                self.stats().inc_delete();
                occupied.remove();
            }
            Entry::Vacant(_vacant) => {
                // Disk-only entry: load the entry from disk and drain slot list into reclaims
                // then delete the entry from disk.
                if let Some(slot_list) = self.load_from_disk(pubkey) {
                    reclaims.extend(
                        slot_list
                            .into_iter()
                            .map(|(slot, account_info)| (slot, account_info.into())),
                    );
                    self.delete_disk_key(pubkey);
                    self.stats().inc_delete();
                }
            }
        }
    }

    // If the slot list for pubkey exists in the index and is empty, remove the index entry for pubkey and return true.
    // Return false otherwise.
    pub fn remove_if_slot_list_empty(&self, pubkey: Pubkey) -> bool {
        let mut m = Measure::start("entry");
        let mut map = self.map_internal.write().unwrap();
        let capacity_pre = map.capacity();
        let entry = map.entry(self.tag(&pubkey));
        m.stop();
        let found = matches!(entry, Entry::Occupied(_));
        let result = self.remove_if_slot_list_empty_entry(&pubkey, entry);
        let capacity_post = map.capacity();
        drop(map);
        self.stats()
            .update_in_mem_capacity(capacity_pre, capacity_post);
        self.update_entry_stats(m, found);
        result
    }

    /// Call `user_fn` with a write lock of the slot list.
    /// The entry is always marked dirty after `user_fn` returns, regardless of whether
    /// `user_fn` modifies the slot list — callers should ideally know they will modify it.
    /// When write-through is active and the resulting slot list has exactly one entry, the entry
    /// is additionally flushed to disk immediately and the dirty flag may be cleared.
    /// Call `user_fn` with the entry for `pubkey`, writing the result back into the index
    pub(crate) fn update_entry<RT>(
        &self,
        pubkey: &Pubkey,
        user_fn: impl FnOnce(SlotListItem<T>) -> (SlotListItem<T>, RT),
    ) -> Option<RT> {
        let mut write_through_args: Option<(Slot, T)> = None;
        let result = self.get_internal_inner(pubkey, |entry| {
            (
                true,
                entry.map(|entry| {
                    let (new_item, result) = user_fn(entry.entry());
                    entry.replace(new_item);
                    if self.should_write_through {
                        write_through_args = Some(new_item);
                    }
                    result
                }),
            )
        });
        if let Some((slot, account_info)) = write_through_args {
            self.write_through(pubkey, slot, account_info);
        }
        result
    }

    /// Writes `disk_entry` for `pubkey` to `disk`, retrying after a grow if needed.
    /// Returns the total time spent waiting for disk grows, in microseconds.
    fn write_to_disk(
        disk: &BucketApi<(Slot, U)>,
        pubkey: &Pubkey,
        disk_entry: &[(Slot, U)],
    ) -> u64 {
        let mut grow_us = 0u64;
        loop {
            match disk.try_write(pubkey, (disk_entry, 1)) {
                Ok(_) => break,
                Err(err) => {
                    let m = Measure::start("flush_grow");
                    disk.grow(err);
                    grow_us += m.end_as_us();
                }
            }
        }
        grow_us
    }

    /// Write `(slot, account_info)` to the disk index, then under the slot list read lock
    /// verify the in-mem entry still matches; if so, clear the dirty flag so the entry
    /// is eligible for eviction without waiting for the background flush.
    ///
    /// We hold the slot list read lock during the equality check to prevent concurrent
    /// modifications from invalidating our check between the disk write and the dirty-clear.
    /// Any concurrent upsert that modifies the slot list must hold the write lock, so it
    /// cannot proceed until we release. If it ran before us the check will fail and we leave
    /// the entry dirty for the next write to clean up; if it runs after, it will re-dirty
    /// the now-clean entry and call write_through itself.
    fn write_through(&self, pubkey: &Pubkey, slot: Slot, account_info: T) {
        let disk = self.bucket.as_ref().unwrap();
        let disk_entry = [(slot, account_info.into())];
        let grow_us = Self::write_to_disk(disk, pubkey, &disk_entry);
        Self::update_stat(&self.stats().flush_entries_updated_on_disk_immediate, 1);
        Self::update_stat(&self.stats().flush_grow_us, grow_us);
        self.get_only_in_mem(pubkey, false, |entry| {
            if let Some(entry) = entry {
                let slot_list = entry.slot_list();
                if slot_list.len() == 1 && slot_list[0] == (slot, account_info) {
                    entry.clear_dirty();
                }
            }
        });
    }

    /// If the in-mem entry for pubkey is `slot_list.len() == 1` and currently dirty, write it
    /// through to disk
    pub fn try_write_through(&self, pubkey: &Pubkey) {
        let to_write = self.get_only_in_mem(pubkey, false, |entry| {
            entry.and_then(|entry| {
                if !entry.dirty() {
                    return None;
                }

                let slot_list = entry.slot_list();
                match &slot_list[..] {
                    [info] => Some(*info),
                    _ => None,
                }
            })
        });
        if let Some((slot, info)) = to_write {
            self.write_through(pubkey, slot, info);
        }
    }

    /// Remove `pubkey`'s entry from the index, and from the disk index, if `should_remove`
    /// returns true for its entry.
    ///
    /// Returns `None` if `pubkey` is not in the index, otherwise whether it was removed.
    pub(crate) fn remove_entry_if(
        &self,
        pubkey: &Pubkey,
        should_remove: impl FnOnce(&SlotListItem<T>) -> bool,
    ) -> Option<bool> {
        // faults the entry in from disk, if necessary, so that its entry can be inspected
        let should_remove = self.get_internal_inner(pubkey, |entry| {
            (
                true,
                entry.map(|entry| should_remove(&entry.slot_list()[0])),
            )
        })?;

        if should_remove {
            let mut map = self.map_internal.write().unwrap();
            if let Entry::Occupied(occupied) = map.entry(self.tag(pubkey)) {
                self.delete_disk_key(pubkey);
                self.stats().dec_mem_count();
                self.stats().inc_delete();
                occupied.remove();
            }
        }
        Some(should_remove)
    }

    pub fn upsert(
        &self,
        pubkey: &Pubkey,
        new_value: PreAllocatedAccountMapEntry<T>,
        other_slot: Option<Slot>,
        reclaims: &mut ReclaimsSlotList<T>,
        reclaim: UpsertReclaim,
    ) {
        let (slot, account_info) = new_value.into();

        self.get_or_create_index_entry_for_pubkey(pubkey, (slot, account_info), |entry| {
            Self::lock_and_update_slot_list(
                entry,
                (slot, account_info),
                other_slot,
                reclaims,
                reclaim,
            );
            // the index holds a single entry per pubkey, so it is never in more than one slot
            self.set_age_to_future(entry, false);
        });
    }

    /// Replaces the slot list entry at `old_slot` with `new_item`.
    ///
    /// Panics if `old_slot` is not present in the slot list, or if more than one entry at
    /// `old_slot` is found (which would indicate prior corruption).
    pub fn replace(&self, pubkey: &Pubkey, new_item: SlotListItem<T>, old_slot: Slot) {
        self.update_entry(pubkey, |current| {
            let (current_slot, _current_account_info) = current;
            // The entry may have moved on to a newer slot since the caller read the account it
            // is relocating, which makes the account it holds dead. Leave the newer entry.
            if current_slot == old_slot {
                (new_item, ())
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

    /// Gets the entry for `pubkey` and calls `callback` with it.
    /// If `pubkey` is not in the index, a new entry holding `new_item` is created and `callback`
    /// is not called, since there is no prior entry to update.
    pub fn get_or_create_index_entry_for_pubkey(
        &self,
        pubkey: &Pubkey,
        new_item: SlotListItem<T>,
        callback: impl FnOnce(&AccountMapEntry<T>),
    ) {
        let mut updated_in_mem = true;
        // try to get it just from memory first using only a read lock
        self.get_only_in_mem(pubkey, false, |entry| {
            if let Some(entry) = entry {
                callback(entry);
            } else {
                let stats = self.stats();
                let tag = self.tag(pubkey);
                let mut m = Measure::start("entry");
                let mut map = self.map_internal.write().unwrap();
                let capacity_pre = map.capacity();

                // Inline eviction: if at capacity and this pubkey is not already in the map,
                // evict one clean entry to make room before inserting.
                // Only enable when should_write_through is true, as finding a candidate for eviction
                // is expensive when the dirty entries are not being written through
                // This is a rare case; background eviction clears the excess over time.
                if self.should_write_through
                    && self.storage.should_evict_based_on_count(map.len())
                    && !map.contains_key(&tag)
                {
                    let evict_tag = map.iter().find(|(_, v)| !v.dirty()).map(|(k, _)| *k);
                    if let Some(evict_tag) = evict_tag {
                        map.remove(&evict_tag);
                        stats.sub_mem_count(1);
                        Self::update_stat(&stats.flush_entries_evicted_from_mem_immediate, 1);
                    }
                }

                let entry = map.entry(tag);
                m.stop();
                let found = matches!(entry, Entry::Occupied(_));
                match entry {
                    Entry::Occupied(mut occupied) => {
                        let current = occupied.get_mut();
                        callback(current);
                    }
                    Entry::Vacant(vacant) => {
                        // not in cache, look on disk
                        updated_in_mem = false;

                        // go to in-mem cache first
                        let disk_entry = self.load_account_entry_from_disk(pubkey);
                        let new_value = if let Some(disk_entry) = disk_entry {
                            // on disk, so update what was on disk
                            callback(&disk_entry);
                            disk_entry
                        } else {
                            // not on disk, so insert new thing. There is no prior entry to
                            // update, so `callback` is not called
                            self.stats().inc_insert();
                            AccountMapEntry::new(
                                [new_item],
                                AccountMapEntryMeta::new_dirty(&self.storage, true),
                            )
                        };
                        assert!(new_value.dirty());
                        vacant.insert(new_value);
                        stats.inc_mem_count();
                    }
                };
                let capacity_post = map.capacity();
                drop(map);
                stats.update_in_mem_capacity(capacity_pre, capacity_post);
                self.update_entry_stats(m, found);
            };
        });
        if updated_in_mem {
            Self::update_stat(&self.stats().updates_in_mem, 1);
        }
    }

    fn update_entry_stats(&self, stopped_measure: Measure, found: bool) {
        let stats = self.stats();
        let (count, time) = if found {
            (&stats.entries_from_mem, &stats.entry_mem_us)
        } else {
            (&stats.entries_missing, &stats.entry_missing_us)
        };
        Self::update_stat(time, stopped_measure.as_us());
        Self::update_stat(count, 1);
    }

    /// Try to update an item in the slot list the given `slot` If an item for the slot
    /// already exists in the list, remove the older item, add it to `reclaims`, and insert
    /// the new item.
    /// if 'other_slot' is some, then remove any entries in the slot list at 'other_slot' instead
    /// if UpsertReclaim is RemoveOldSlots, remove all uncached slots older than 'slot'
    /// and add them to reclaims
    /// Note:: This function only supports uncached types `T`.
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

    // convert from raw data on disk to AccountMapEntry, set to age in future
    fn disk_to_cache_entry(&self, slot_list: SlotList<U>) -> AccountMapEntry<T> {
        let (slot, account_info) = slot_list[0];
        AccountMapEntry::new(
            [(slot, account_info.into())],
            AccountMapEntryMeta::new_clean(&self.storage),
        )
    }

    /// Queue up these insertions for when the flush thread is dealing with this bin.
    /// This is very fast and requires no lookups or disk access.
    pub fn startup_insert_only(
        &self,
        slot: Slot,
        items: impl ExactSizeIterator<Item = (Pubkey, T)>,
    ) {
        assert!(self.storage.get_startup());
        assert!(self.bucket.is_some());

        let mut insert = self.startup_info.insert.lock().unwrap();
        let m = Measure::start("copy");
        insert.extend(items.map(|(k, v)| (k, (slot, v.into()))));
        self.startup_stats
            .copy_data_us
            .fetch_add(m.end_as_us(), Ordering::Relaxed);
    }

    pub fn startup_update_duplicates(&self, items: Vec<(Slot, Pubkey, T)>) {
        assert!(self.storage.get_startup());

        let mut duplicates = self.startup_info.duplicates.lock().unwrap();
        duplicates.duplicates.extend(items);
    }

    /// Upsert `new_entry` for `pubkey` into the primary index
    ///
    /// Returns info about existing entries for `pubkey`.
    ///
    /// This fn is only called at startup. The return information is used by the callers to
    /// batch-update accounts index stats.
    pub fn insert_new_entry_if_missing_with_lock(
        &self,
        pubkey: Pubkey,
        new_entry: PreAllocatedAccountMapEntry<T>,
    ) -> InsertNewEntryResults<T> {
        let mut map = self.map_internal.write().unwrap();
        let entry = map.entry(self.tag(&pubkey));
        let mut older_version = None;
        let (found_in_mem, already_existed) = match entry {
            Entry::Occupied(occupied) => {
                // in cache, so keep whichever version is from the newer slot
                let (slot, account_info) = new_entry.into();

                older_version = Some(occupied.get().replace_if_newer((slot, account_info), None));

                (
                    true, /* found in mem */
                    true, /* already existed */
                )
            }
            Entry::Vacant(vacant) => {
                // not in cache, look on disk
                let disk_entry = self.load_account_entry_from_disk(&pubkey);
                if let Some(disk_entry) = disk_entry {
                    let (slot, account_info) = new_entry.into();
                    older_version = Some(disk_entry.replace_if_newer((slot, account_info), None));
                    vacant.insert(disk_entry);
                    (
                        false, /* found in mem */
                        true,  /* already existed */
                    )
                } else {
                    // not on disk, so insert new thing and we're done
                    let new_entry = new_entry.into_account_map_entry(&self.storage);
                    assert!(new_entry.dirty());
                    vacant.insert(new_entry);
                    (
                        false, /* found in mem */
                        false, /* already existed */
                    )
                }
            }
        };
        drop(map);

        if already_existed {
            let location = if found_in_mem {
                ExistedLocation::InMem
            } else {
                ExistedLocation::OnDisk
            };
            InsertNewEntryResults::Existed {
                older_version: older_version.expect("an existing entry has an older version"),
                location,
            }
        } else {
            InsertNewEntryResults::DidNotExist
        }
    }

    pub fn flush(&self, can_advance_age: bool) {
        if let Some(flush_guard) = FlushGuard::lock(&self.flushing_active) {
            self.flush_internal(&flush_guard, can_advance_age)
        }
    }

    /// The footprint of a single element in the in-mem hashmap
    pub const fn size_of_uninitialized() -> usize {
        size_of::<Tag>()
    }

    /// The size of an index value, with only a single entry in the slot list
    pub const fn size_of_single_entry() -> usize {
        size_of::<AccountMapEntry<T>>()
    }

    fn should_evict_based_on_age(
        current_age: Age,
        entry: &AccountMapEntry<T>,
        ages_to_scan: Age,
    ) -> bool {
        age_distance(current_age, entry.age()) <= ages_to_scan
    }

    /// Collect candidates to evict from `iter` by checking age
    fn gather_possible_evict_candidates<'a>(
        iter: impl Iterator<Item = (&'a Tag, &'a AccountMapEntry<T>)>,
        current_age: Age,
        ages_to_scan: Age,
        max_evictions: NonZeroUsize,
    ) -> CandidatesToEvict {
        let mut rng = rng();
        // use reservoir sampling to select a bounded, roughly uniform subset
        let mut sampling_state = ReservoirState {
            samples: Vec::with_capacity(max_evictions.get()),
            seen: 0,
            max_samples: max_evictions,
        };
        for (k, v) in iter {
            if !Self::should_evict_based_on_age(current_age, v, ages_to_scan) {
                // not planning to evict this item from memory within 'ages_to_scan' ages
                continue;
            }

            if !v.dirty() {
                sampling_state.select(*k, &mut rng);
            }
        }
        CandidatesToEvict(mem::take(&mut sampling_state.samples))
    }

    /// scan loop
    /// holds read lock
    /// Returns candidates to evict now, pending further checks.
    fn evict_scan(
        &self,
        current_age: Age,
        _flush_guard: &FlushGuard,
        ages_to_scan: Age,
    ) -> CandidatesToEvict {
        let (possible_evictions, m) = {
            let map = self.map_internal.read().unwrap();
            let m = Measure::start("evict_scan"); // we don't care about lock time in this metric - bg threads can wait
            let max_evictions = self.storage.max_evictions_for_threshold(map.len());
            let possible_evictions = Self::gather_possible_evict_candidates(
                map.iter(),
                current_age,
                ages_to_scan,
                max_evictions,
            );
            (possible_evictions, m)
        };
        Self::update_time_stat(&self.stats().evict_scan_us, m);

        possible_evictions
    }

    /// Takes self's `startup_info` and writes it to disk and in-mem.
    /// When in Threshold mode limit the insertions to the low water mark of the threshold.
    fn write_startup_info(&self) {
        let insert = std::mem::take(&mut *self.startup_info.insert.lock().unwrap());
        if insert.is_empty() {
            // nothing to insert for this bin
            return;
        }

        // this fn should only be called from a single thread, so holding the lock is fine
        let mut duplicates = self.startup_info.duplicates.lock().unwrap();

        // merge all items into the disk index now
        let disk = self.bucket.as_ref().unwrap();
        let duplicate_entries_and_indices = disk.batch_insert_non_duplicates(&insert);
        let duplicate_addresses: HashSet<_> = duplicate_entries_and_indices
            .iter()
            .map(|(index, _entry)| &insert[*index].0)
            .collect();
        let mut count = insert.len() as u64;
        for (i, (resident_slot, resident_value)) in duplicate_entries_and_indices {
            let (pubkey, (slot, value)) = &insert[i];
            assert_ne!(
                resident_slot, *slot,
                "Accounts may only be stored once per slot: {slot}"
            );
            // only the version from the newest slot belongs in the index. The disk index holds
            // whichever version was inserted first, so replace it when this one is newer.
            let older_version = if *slot > resident_slot {
                disk.update(pubkey, |_current| Some((vec![(*slot, *value)], 1)));
                (resident_slot, resident_value.into())
            } else {
                (*slot, (*value).into())
            };
            duplicates
                .duplicates
                .push((older_version.0, *pubkey, older_version.1));
            count -= 1;
        }

        if let Some(threshold_entries_per_bin) = self.storage.threshold_entries_per_bin.as_ref() {
            // If a memory threshold is set, then insert into the in-mem index here,
            // up to that limit.  This way we pre-populate the in-mem index, and can
            // avoid having to load some entries from disk on first access.
            let mut map = self.map_internal.write().unwrap();
            // Insert up to the low water mark.  Purposely do not insert all the way up  to the
            // high water mark, as that then causes the flush loop condition to immediately trigger
            // and evict down to the low water mark anyway.
            let num_available = threshold_entries_per_bin
                .low_water_mark
                .saturating_sub(map.len());
            for (address, (slot, disk_index_value)) in insert
                .iter()
                .filter(|(address, _entry)| !duplicate_addresses.contains(address)) // <- skip known duplicates
                .take(num_available)
            {
                match map.entry(self.tag(address)) {
                    Entry::Vacant(vacant) => {
                        let index_value = (*disk_index_value).into();
                        let slot_list = SlotList::from([(*slot, index_value)]);
                        let meta = AccountMapEntryMeta::new_clean(&self.storage);
                        let account_map_entry = AccountMapEntry::new(slot_list, meta);
                        vacant.insert(account_map_entry);
                    }
                    Entry::Occupied(_occupied) => {
                        // If the account already has an entry in the in-mem index, then that means
                        // it is a duplicate.  We could merge them here, however duplicates
                        // handling happens later during startup/index generation, in
                        // populate_and_retrieve_duplicate_keys_from_startup(), which will insert
                        // them back into the in-mem index.  Thus we should *not* insert any
                        // accounts with duplicate entries here.
                        // Additionally, once marking obsolete accounts is always on, we then
                        // should no longer have any duplicates to worry about.
                    }
                }
            }

            // Related to the comment in the Entry::Occupied match arm above, if inserting
            // into disk (batch_insert_non_duplicates()) returned duplicates, we need to check
            // and make sure they are not in the in-mem index.  (Since the first time we encounter
            // a duplicate we do not know it is a duplicate, so it will have been inserted
            // in mem.)  We must remove them here.
            for duplicate_address in duplicate_addresses {
                map.remove(&self.tag(duplicate_address));
            }
            drop(map);
        } else {
            // Else, we should not have anything in the in-mem index at all.
            let map_internal = self.map_internal.read().unwrap();
            assert!(
                map_internal.is_empty(),
                "len: {}, first: {:?}",
                map_internal.len(),
                map_internal.iter().take(1).collect::<Vec<_>>()
            );
            drop(map_internal);
        }

        self.stats().inc_insert_count(count);
    }

    /// pull out all the older versions of duplicate pubkeys found for this bin during startup.
    /// The index holds the version from the newest slot; these are all the rest.
    pub fn take_startup_duplicates(&self) -> Vec<(Slot, Pubkey, T)> {
        // in order to return accurate and complete duplicates, we must have nothing left remaining to insert
        assert!(self.startup_info.insert.lock().unwrap().is_empty());

        // index generation keeps only the version from the newest slot for each pubkey
        debug_assert!(
            self.map_internal
                .read()
                .unwrap()
                .values()
                .all(|entry| entry.slot_list().len() == 1),
            "index generation must leave a single entry per pubkey"
        );

        let mut duplicate_items = self.startup_info.duplicates.lock().unwrap();
        std::mem::take(&mut duplicate_items.duplicates)
    }

    /// Returns true when the bin's occupancy has crossed its configured thresholds and
    /// the caller should reduce it.
    ///
    /// Fires on either of two conditions:
    /// - Free-entry headroom is below the configured overhead. Tombstones left by prior evictions
    ///   reduce capacity without being included in len, so a rehash can be imminent before the
    ///   count crosses the high-water mark. This is the primary trigger in steady state.
    /// - Entry count exceeds the high-water mark. This is a backstop for the case where the
    ///   hashmap has already doubled in size, leaving plenty of headroom so the first condition
    ///   would not fire on its own.
    ///
    /// Returns false for bins still in initial growth (capacity below `high_water_mark`).
    fn exceeds_thresholds(&self) -> bool {
        let (entries_in_bin, capacity) = {
            let map = self.map_internal.read().unwrap();
            (map.len(), map.capacity())
        };

        // Skip during initial growth: below HWM, low free entries reflect a not-yet-grown
        // table, not tombstones. If tombstones do force a doubling before len crosses HWM,
        // the count check catches it later once len grows past HWM.
        if let Some(thresholds) = &self.storage.threshold_entries_per_bin
            && capacity < thresholds.high_water_mark
        {
            return false;
        }

        let high_count_triggered = self.storage.should_evict_based_on_count(entries_in_bin);
        let low_free_entries_triggered = self
            .storage
            .should_evict_based_on_free_entries(capacity.saturating_sub(entries_in_bin));
        if !high_count_triggered && !low_free_entries_triggered {
            return false;
        }
        if low_free_entries_triggered {
            // Primary case: low free-entry headroom (typically from tombstones).
            Self::update_stat(&self.stats().evict_triggered_by_low_free_entries, 1);
        } else {
            // Backstop: bin is past the high-water mark while free-entry headroom
            // still has slack — typically because the hashmap doubled in size.
            Self::update_stat(&self.stats().evict_triggered_by_high_count, 1);
        }
        true
    }

    /// synchronize the in-mem index with the disk index
    fn flush_internal(&self, flush_guard: &FlushGuard, can_advance_age: bool) {
        let current_age = self.storage.current_age();
        let iterate_for_age = self.get_should_age(current_age);
        let startup = self.storage.get_startup();

        if startup {
            // At startup we do not insert index entries into the normal in-mem index.
            // Instead, they are written to a startup-only struct.  Thus, at startup
            // we only need to flush that startup struct and then can return early.
            self.write_startup_info();

            if iterate_for_age {
                // Note we still have to iterate ages too, since it is checked when
                // transitioning from startup back to normal/steady state.
                assert_eq!(current_age, self.storage.current_age());
                self.set_has_aged(current_age, can_advance_age);
            }
            return;
        }

        // from this point forward, we know startup == false
        debug_assert!(!startup);

        if !iterate_for_age {
            // no need to age, so no need to scan this bucket
            return;
        }

        // from this point forward, we know iterate_for_age == true
        debug_assert!(iterate_for_age);

        if !self.exceeds_thresholds() {
            // Still mark as aged to avoid infinite scanning
            assert_eq!(current_age, self.storage.current_age());
            self.set_has_aged(current_age, can_advance_age);
            return;
        }

        let ages_to_scan = {
            let old_value = self.ages_to_skip_before_scan.fetch_sub(1, Ordering::AcqRel);
            if old_value == 0 {
                self.ages_to_skip_before_scan
                    .store(self.num_ages_to_distribute_scans, Ordering::Release);
            } else {
                // skipping iteration of the buckets at the current age, but mark the bucket as having aged
                assert_eq!(current_age, self.storage.current_age());
                self.set_has_aged(current_age, can_advance_age);
                return;
            }
            self.num_ages_to_distribute_scans
        };

        Self::update_stat(&self.stats().buckets_scanned, 1);

        // scan in-mem map for candidates to evict
        let candidates_to_evict = self.evict_scan(current_age, flush_guard, ages_to_scan);

        let m = Measure::start("evict");
        self.evict_from_cache(&candidates_to_evict.0, current_age, ages_to_scan);
        Self::update_time_stat(&self.stats().evict_us, m);

        if iterate_for_age {
            // completed iteration of the buckets at the current age
            assert_eq!(current_age, self.storage.current_age());
            self.set_has_aged(current_age, can_advance_age);
        }
    }

    /// Rebuild the bin's HashMap into a fresh allocation to clear tombstones left
    /// behind by evictions. hashbrown counts tombstones against `capacity`, so
    /// without this the bin's effective capacity drifts down over time and triggers
    /// the hashmap to double in capacity.
    ///
    /// Only called in Threshold mode, where `capacity >= target_entries` is guaranteed
    /// by the time eviction runs (`exceeds_thresholds` gates on `high_water_mark`).
    fn reallocate_to_clear_tombstones(&self) {
        let stats = self.stats();
        let m = Measure::start("reallocate_hashmap");

        let target_entries = self
            .storage
            .threshold_entries_per_bin
            .as_ref()
            .expect("reallocate_to_clear_tombstones only runs in Threshold mode")
            .target_entries;

        let mut map = self.map_internal.write().unwrap();
        let capacity_pre = map.capacity();

        // Drain the old map into a fresh allocation sized to `target_entries` so the
        // backing storage stays stable across eviction cycles. Building a brand-new
        // map (rather than `shrink_to_fit`) guarantees a full rehash, which is what
        // actually clears the tombstones.
        let mut new_map = HashMap::with_capacity_and_hasher(target_entries, map.hasher().clone());
        new_map.extend(map.drain());
        *map = new_map;
        let capacity_post = map.capacity();
        drop(map);

        stats.update_in_mem_capacity(capacity_pre, capacity_post);
        Self::update_stat(&stats.num_hashmap_reallocates, 1);
        Self::update_time_stat(&stats.hashmap_reallocate_us, m);
    }

    // evict keys in 'evictions' from in-mem cache, likely due to age
    fn evict_from_cache(&self, evictions: &[Tag], current_age: Age, ages_to_scan: Age) {
        if evictions.is_empty() {
            return;
        }

        let stats = self.stats();
        let mut failed = 0;
        let mut evicted = 0;
        // chunk these so we don't hold the write lock too long
        for evictions in evictions.chunks(50) {
            let mut map = self.map_internal.write().unwrap();
            let capacity_pre = map.capacity();
            for k in evictions {
                if let Entry::Occupied(occupied) = map.entry(*k) {
                    let v = occupied.get();

                    if v.dirty() || !Self::should_evict_based_on_age(current_age, v, ages_to_scan) {
                        // marked dirty or bumped in age after we looked above
                        // these evictions will be handled in later passes (at later ages)
                        failed += 1;
                        continue;
                    }

                    // A clean entry always has a single-slot slot list identical to its disk
                    // copy: every slot-list mutation marks the entry dirty, and dirty is only
                    // cleared after a verified single-slot disk write. This keeps multi-slot
                    // entries in-mem, which ScanFilter::OnlyAbnormal relies on.
                    assert_eq!(v.slot_list().len(), 1, "{k}");

                    // all conditions for eviction succeeded, so really evict item from in-mem cache
                    evicted += 1;
                    occupied.remove();
                }
            }
            let capacity_post = map.capacity();
            drop(map);
            stats.update_in_mem_capacity(capacity_pre, capacity_post);
        }

        // Only Threshold mode cares about tombstone-driven capacity doublings
        if evicted > 0 && self.storage.threshold_entries_per_bin.is_some() {
            self.reallocate_to_clear_tombstones();
        }

        stats.sub_mem_count(evicted);
        Self::update_stat(
            &stats.flush_entries_evicted_from_mem_background,
            evicted as u64,
        );
        Self::update_stat(&stats.failed_to_evict, failed as u64);
    }

    pub fn stats(&self) -> &Stats {
        &self.storage.stats
    }

    fn update_stat(stat: &AtomicU64, value: u64) {
        if value != 0 {
            stat.fetch_add(value, Ordering::Relaxed);
        }
    }

    pub fn update_time_stat(stat: &AtomicU64, mut m: Measure) {
        m.stop();
        let value = m.as_us();
        Self::update_stat(stat, value);
    }

    /// Returns the length and capacity of this bin's map
    ///
    /// Only intended to be called at startup, since it grabs the map's read lock.
    pub(crate) fn len_and_cap_for_startup(&self) -> (usize, usize) {
        let map = self.map_internal.read().unwrap();
        (map.len(), map.capacity())
    }

    /// Returns the number of entries currently held in memory for this bin.
    pub(crate) fn len(&self) -> usize {
        self.map_internal.read().unwrap().len()
    }
}

/// State of reservoir sampling algorithm for eviction candidates.
#[derive(Debug)]
struct ReservoirState {
    samples: Vec<Tag>,
    seen: usize,
    max_samples: NonZeroUsize,
}

impl ReservoirState {
    /// Select a candidate, keeping a bounded roughly uniform sample set.
    fn select(&mut self, candidate: Tag, rng: &mut impl Rng) {
        self.seen += 1;
        if self.samples.len() < self.max_samples.get() {
            self.samples.push(candidate);
            return;
        }

        let idx = rng.random_range(0..self.seen);
        if idx < self.max_samples.get() {
            self.samples[idx] = candidate;
        }
    }
}

/// An RAII implementation of a scoped lock for the `flushing_active` atomic flag in
/// `InMemAccountsIndex`.  When this structure is dropped (falls out of scope), the flag will be
/// cleared (set to false).
///
/// After successfully locking (calling `FlushGuard::lock()`), pass a reference to the `FlashGuard`
/// instance to any function/code that requires the `flushing_active` flag has been set (to true).
#[derive(Debug)]
struct FlushGuard<'a> {
    flushing: &'a AtomicBool,
}

impl<'a> FlushGuard<'a> {
    /// Set the `flushing` atomic flag to true.  If the flag was already true, then return `None`
    /// (so as to not clear the flag erroneously).  Otherwise return `Some(FlushGuard)`.
    #[must_use = "if unused, the `flushing` flag will immediately clear"]
    fn lock(flushing: &'a AtomicBool) -> Option<Self> {
        let already_flushing = flushing.swap(true, Ordering::AcqRel);
        // Eager evaluation here would result in dropping Self and clearing flushing flag
        #[allow(clippy::unnecessary_lazy_evaluations)]
        (!already_flushing).then(|| Self { flushing })
    }
}

impl Drop for FlushGuard<'_> {
    fn drop(&mut self) {
        self.flushing.store(false, Ordering::Release);
    }
}

/// Candidates in the in-mem index that may be evicted, pending further checks.
///
/// Note, entries must be 'clean' to be a candidate for eviction.
#[derive(Debug)]
struct CandidatesToEvict(Vec<Tag>);

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::accounts_index::{
            ACCOUNTS_INDEX_CONFIG_FOR_TESTING, AccountsIndexConfig, BINS_FOR_TESTING,
            INDEX_LIMIT_THRESHOLD_FOR_TESTING, IndexLimit, IndexLimitThreshold,
            bucket_map_holder::ThresholdEntriesPerBin,
        },
        assert_matches::assert_matches,
        std::iter,
        test_case::test_case,
    };

    fn new_for_test<T: IndexValue>() -> InMemAccountsIndex<T, T> {
        let holder = Arc::new(BucketMapHolder::new(
            BINS_FOR_TESTING,
            &AccountsIndexConfig::default(),
            1,
        ));
        let bin = 0;
        InMemAccountsIndex::new(&holder, bin, None)
    }

    fn new_disk_buckets_for_test<T: IndexValue>() -> InMemAccountsIndex<T, T> {
        let config = AccountsIndexConfig {
            index_limit: INDEX_LIMIT_THRESHOLD_FOR_TESTING,
            ..Default::default()
        };
        let holder = Arc::new(BucketMapHolder::new(BINS_FOR_TESTING, &config, 1));
        let bin = 0;
        let bucket = InMemAccountsIndex::new(&holder, bin, None);
        assert!(bucket.storage.is_disk_index_enabled());
        bucket
    }

    /// Creates an index with `should_write_through = true` and a live disk bucket.
    ///
    /// Pass `Some((high, low))` to override the computed per-bin threshold with explicit water-marks.
    /// Pass `None` to keep the byte-based default, which is effectively unlimited.
    fn new_should_write_through_for_test(
        threshold: Option<(usize, usize)>,
    ) -> InMemAccountsIndex<u64, u64> {
        let config = AccountsIndexConfig {
            index_limit: IndexLimit::Threshold(IndexLimitThreshold {
                num_bytes: 25_000_000_000,
                num_entries_overhead: 1,
                num_entries_to_evict: 1,
            }),
            ..ACCOUNTS_INDEX_CONFIG_FOR_TESTING
        };
        let mut holder = BucketMapHolder::new(BINS_FOR_TESTING, &config, 1);
        if let Some((high_water_mark, low_water_mark)) = threshold {
            holder.threshold_entries_per_bin = Some(ThresholdEntriesPerBin {
                target_entries: high_water_mark + 1,
                high_water_mark,
                low_water_mark,
            });
        }
        let holder = Arc::new(holder);
        let index = InMemAccountsIndex::<u64, u64>::new(&holder, 0, None);
        assert!(index.should_write_through);
        assert!(index.bucket.is_some());
        index
    }

    #[test]
    fn test_get_or_create_index_entry_for_pubkey_insert_new() {
        let accounts_index = new_for_test::<u64>();
        let pubkey = solana_pubkey::new_rand();
        let slot = 0;

        let mut callback_called = false;
        accounts_index.get_or_create_index_entry_for_pubkey(&pubkey, (slot, 0), |_entry| {
            callback_called = true;
        });

        // there was no prior entry to update, so the new entry is created holding the new item
        assert!(!callback_called);
        accounts_index.get_only_in_mem(&pubkey, false, |entry| {
            let entry = entry.expect("entry should be in memory");
            assert_eq!(entry.slot_list(), [(slot, 0)]);
            assert!(entry.dirty());
        });

        // Ensure the entry is now in memory
        let mut found = false;
        accounts_index.get_only_in_mem(&pubkey, false, |entry| {
            found = entry.is_some();
        });
        assert!(found);
    }

    #[test]
    fn test_get_or_create_index_entry_for_pubkey_existing_in_mem() {
        let accounts_index = new_for_test::<u64>();
        let pubkey = solana_pubkey::new_rand();

        // Insert an entry manually
        let entry = AccountMapEntry::new(
            SlotList::from([(0, 42)]),
            AccountMapEntryMeta::new_dirty(&accounts_index.storage, true),
        );
        accounts_index
            .map_internal
            .write()
            .unwrap()
            .insert(accounts_index.tag(&pubkey), entry);

        let mut callback_called = false;
        accounts_index.get_or_create_index_entry_for_pubkey(&pubkey, (1, 43), |entry| {
            assert_eq!(entry.slot_list(), [(0, 42)]);
            assert!(entry.dirty());
            callback_called = true;
        });

        assert!(callback_called);
    }

    #[test]
    fn test_get_or_create_index_entry_for_pubkey_existing_on_disk() {
        let accounts_index = new_disk_buckets_for_test::<u64>();
        let pubkey = solana_pubkey::new_rand();
        let slot = 0;

        // Simulate an entry on disk
        let disk_entry: (&[(u64, u64)], u64) = (&[(0u64, 42u64)], 1u64);
        accounts_index
            .bucket
            .as_ref()
            .unwrap()
            .try_write(&pubkey, disk_entry)
            .unwrap();

        // Ensure the entry is not found in memory
        let mut found = false;
        accounts_index.get_only_in_mem(&pubkey, false, |entry| {
            found = entry.is_some();
        });
        assert!(!found);

        let mut callback_called = false;
        accounts_index.get_or_create_index_entry_for_pubkey(&pubkey, (slot, 0), |entry| {
            assert_eq!(entry.slot_list().len(), 1);
            assert!(!entry.dirty()); // Entry loaded from disk should not be dirty
            InMemAccountsIndex::<u64, u64>::lock_and_update_slot_list(
                entry,
                (slot, 0),
                None,
                &mut ReclaimsSlotList::new(),
                UpsertReclaim::IgnoreReclaims,
            );
            callback_called = true;
        });

        assert!(callback_called);

        // Ensure the entry is now in memory
        let mut found = false;
        accounts_index.get_only_in_mem(&pubkey, false, |entry| {
            found = entry.is_some();
        });
        assert!(found);
    }

    /// Populates `index` with four entries covering the age/dirty matrix, triggers a
    /// background flush, then asserts:
    ///   - clean new: stays in memory, not on disk
    ///   - clean old: evicted from memory, not on disk
    ///   - dirty new: stays in memory, not on disk
    ///
    /// Returns `(pubkey_dirty_old, slot + 3, info + 3)` so the caller can assert the
    /// outcome for the old dirty entry.
    fn flush_age_mixed_entries(
        accounts_index: &InMemAccountsIndex<u64, u64>,
    ) -> (Pubkey, Slot, u64) {
        let pubkey_clean_new = solana_pubkey::new_rand();
        let pubkey_clean_old = solana_pubkey::new_rand();
        let pubkey_dirty_new = solana_pubkey::new_rand();
        let pubkey_dirty_old = solana_pubkey::new_rand();
        let slot = 123;
        let info = 42;

        assert!(accounts_index.load_from_disk(&pubkey_clean_new).is_none());
        assert!(accounts_index.load_from_disk(&pubkey_clean_old).is_none());
        assert!(accounts_index.load_from_disk(&pubkey_dirty_new).is_none());
        assert!(accounts_index.load_from_disk(&pubkey_dirty_old).is_none());

        // A clean entry that is *not* in the eviction window.
        // This entry should *not* be eligible for eviction.
        let entry_clean_new = AccountMapEntry::new(
            SlotList::from([(slot, info)]),
            AccountMapEntryMeta::new_clean(&accounts_index.storage),
        );
        assert!(!entry_clean_new.dirty());

        // A clean entry that *is* in the eviction window.
        // This entry *should* be eligible for eviction.
        let entry_clean_old = AccountMapEntry::new(
            SlotList::from([(slot + 1, info + 1)]),
            AccountMapEntryMeta::new_clean(&accounts_index.storage),
        );
        entry_clean_old.set_age(accounts_index.storage.current_age());
        assert!(!entry_clean_old.dirty());

        // A dirty entry that is *not* in the eviction window.
        // This entry should *not* be eligible for eviction.
        let entry_dirty_new = AccountMapEntry::new(
            SlotList::from([(slot + 2, info + 2)]),
            AccountMapEntryMeta::new_dirty(&accounts_index.storage, false),
        );
        assert!(entry_dirty_new.dirty());

        // A dirty entry that *is* in the eviction window.
        // The caller asserts the outcome for this entry.
        let entry_dirty_old = AccountMapEntry::new(
            SlotList::from([(slot + 3, info + 3)]),
            AccountMapEntryMeta::new_dirty(&accounts_index.storage, false),
        );
        entry_dirty_old.set_age(accounts_index.storage.current_age());
        assert!(entry_dirty_old.dirty());

        accounts_index.map_internal.write().unwrap().extend([
            (accounts_index.tag(&pubkey_clean_new), entry_clean_new),
            (accounts_index.tag(&pubkey_clean_old), entry_clean_old),
            (accounts_index.tag(&pubkey_dirty_new), entry_dirty_new),
            (accounts_index.tag(&pubkey_dirty_old), entry_dirty_old),
        ]);

        accounts_index
            .ages_to_skip_before_scan
            .store(0, Ordering::Release);

        accounts_index.flush(false);

        // clean new entry should not be flushed/evicted
        let mut found_in_mem = None;
        accounts_index.get_only_in_mem(&pubkey_clean_new, false, |entry| {
            found_in_mem = Some(entry.is_some());
            let entry = entry.expect("entry should remain in memory");
            assert!(!entry.dirty());
        });
        assert_eq!(found_in_mem, Some(true));
        assert!(accounts_index.load_from_disk(&pubkey_clean_new).is_none());

        // clean old entry should be evicted, and not flushed
        let mut found_in_mem = None;
        accounts_index.get_only_in_mem(&pubkey_clean_old, false, |entry| {
            found_in_mem = Some(entry.is_some());
        });
        assert_eq!(found_in_mem, Some(false));
        assert!(accounts_index.load_from_disk(&pubkey_clean_old).is_none());

        // dirty new entry should not be flushed/evicted
        let mut found_in_mem = None;
        accounts_index.get_only_in_mem(&pubkey_dirty_new, false, |entry| {
            found_in_mem = Some(entry.is_some());
            let entry = entry.expect("entry should remain in memory");
            assert!(entry.dirty());
        });
        assert_eq!(found_in_mem, Some(true));
        assert!(accounts_index.load_from_disk(&pubkey_dirty_new).is_none());

        (pubkey_dirty_old, slot + 3, info + 3)
    }

    /// With `should_write_through=true`, the background flush should evict old clean entries but
    /// must NOT flush dirty entries to disk — dirty entries are written through inline on upsert.
    #[test]
    fn test_flush_internal_evicts_in_should_write_through_mode() {
        // high_water_mark=2: 4 entries puts us above threshold, triggering background eviction.
        let accounts_index = new_should_write_through_for_test(Some((2, 1)));
        let (pubkey_dirty_old, _, _) = flush_age_mixed_entries(&accounts_index);

        // old dirty entry should not be flushed, and not evicted
        let mut found_in_mem = None;
        accounts_index.get_only_in_mem(&pubkey_dirty_old, false, |entry| {
            found_in_mem = Some(entry.is_some());
            let entry = entry.expect("entry should remain in memory");
            assert!(entry.dirty()); // should_write_through: dirty entries are skipped by background flush
        });
        assert_eq!(found_in_mem, Some(true));
        assert!(accounts_index.load_from_disk(&pubkey_dirty_old).is_none());
    }

    /// A multi-slot entry can never be evicted: every slot-list mutation marks the entry dirty,
    /// and dirty is only cleared after a verified single-slot disk write, so the dirty check in
    /// `evict_from_cache` pins multi-slot entries in-mem. `ScanFilter::OnlyAbnormal` relies on
    /// this to find every multi-slot entry without reading the disk index.
    #[test]
    fn test_gather_possible_evict_candidates_with_max_evictions() {
        let current_age = 100;
        let ages_to_scan = 0;
        let total_entries = 256;
        let max_evictions = NonZeroUsize::new(5).unwrap();

        // Create a map with 256 entries
        let map: HashMap<Tag, _> = (0..total_entries)
            .map(|i| {
                let one_element_slot_list = SlotList::from([(0, 0)]);
                let one_element_slot_list_entry =
                    AccountMapEntry::new(one_element_slot_list, AccountMapEntryMeta::default());
                if i % 2 == 0 {
                    one_element_slot_list_entry.mark_dirty();
                }
                one_element_slot_list_entry.set_age(current_age);
                (i as Tag, one_element_slot_list_entry)
            })
            .collect();

        let to_evict = InMemAccountsIndex::<u64, u64>::gather_possible_evict_candidates(
            map.iter(),
            current_age,
            ages_to_scan,
            max_evictions,
        );

        assert_eq!(to_evict.0.len(), max_evictions.get());

        for key in to_evict.0.iter() {
            let entry = map.get(key).unwrap();
            assert!(InMemAccountsIndex::<u64, u64>::should_evict_based_on_age(
                current_age,
                entry,
                ages_to_scan,
            ));
        }
        for key in &to_evict.0 {
            assert!(!map.get(key).unwrap().dirty());
        }
    }

    #[test]
    fn test_gather_possible_evict_candidates_skips_dirty() {
        let accounts_index = new_disk_buckets_for_test::<u64>();
        let current_age = accounts_index.storage.current_age();
        let ages_to_scan = accounts_index.num_ages_to_distribute_scans;
        let slot = 1;

        // Clean entry in the eviction window.
        let pubkey_clean = solana_pubkey::new_rand();
        let entry_clean = AccountMapEntry::new(
            SlotList::from([(slot, 1)]),
            AccountMapEntryMeta::new_clean(&accounts_index.storage),
        );
        entry_clean.set_age(current_age);

        // Dirty entry in the eviction window.
        let pubkey_dirty = solana_pubkey::new_rand();
        let entry_dirty = AccountMapEntry::new(
            SlotList::from([(slot + 1, 2)]),
            AccountMapEntryMeta::new_dirty(&accounts_index.storage, false),
        );
        entry_dirty.set_age(current_age);

        let tag_clean = accounts_index.tag(&pubkey_clean);
        let tag_dirty = accounts_index.tag(&pubkey_dirty);
        let map: HashMap<Tag, AccountMapEntry<u64>> =
            HashMap::from([(tag_clean, entry_clean), (tag_dirty, entry_dirty)]);

        let max_evictions = NonZeroUsize::new(map.len()).unwrap();
        let to_evict = InMemAccountsIndex::<u64, u64>::gather_possible_evict_candidates(
            map.iter(),
            current_age,
            ages_to_scan,
            max_evictions,
        );

        assert!(!to_evict.0.contains(&tag_dirty));
        assert!(to_evict.0.contains(&tag_clean));
    }

    #[test]
    fn test_age() {
        let test = new_for_test::<u64>();
        assert!(test.get_should_age(test.storage.current_age()));
        assert_eq!(test.storage.count_buckets_flushed(), 0);
        test.set_has_aged(0, true);
        assert!(!test.get_should_age(test.storage.current_age()));
        assert_eq!(test.storage.count_buckets_flushed(), 1);
        // simulate rest of buckets aging
        for _ in 1..BINS_FOR_TESTING {
            assert!(!test.storage.all_buckets_flushed_at_current_age());
            test.storage.bucket_flushed_at_current_age(true);
        }
        assert!(test.storage.all_buckets_flushed_at_current_age());
        // advance age
        test.storage.increment_age();
        assert_eq!(test.storage.current_age(), 1);
        assert!(!test.storage.all_buckets_flushed_at_current_age());
        assert!(test.get_should_age(test.storage.current_age()));
        assert_eq!(test.storage.count_buckets_flushed(), 0);
    }

    /// The index holds the entry from the newest slot, so an update at an older slot keeps the
    /// entry it has and reclaims the older version instead
    #[test_case(None; "no other slot")]
    #[test_case(Some(3); "other slot is not the one in the index")]
    fn test_update_slot_list_older_slot(other_slot: Option<Slot>) {
        let entry = AccountMapEntry::<u64>::new([(2, 20)], AccountMapEntryMeta::default());

        let older_version = entry.replace_if_newer((1, 10), other_slot);

        assert_eq!(entry.slot_list(), [(2, 20)]);
        assert_eq!(older_version, (1, 10));
    }

    #[test]
    fn test_flush_guard() {
        let flushing_active = AtomicBool::new(false);

        {
            let flush_guard = FlushGuard::lock(&flushing_active);
            assert!(flush_guard.is_some());
            assert!(flushing_active.load(Ordering::Acquire));

            {
                // Trying to lock the FlushGuard again will not succeed.
                let flush_guard2 = FlushGuard::lock(&flushing_active);
                assert!(flush_guard2.is_none());
            }

            // The `flushing_active` flag will remain true, even after `flush_guard2` goes out of
            // scope (and is dropped).  This ensures `lock()` and `drop()` work harmoniously.
            assert!(flushing_active.load(Ordering::Acquire));
        }

        // After the FlushGuard is dropped, the flag will be cleared.
        assert!(!flushing_active.load(Ordering::Acquire));
    }

    #[test]
    fn test_remove_if_slot_list_empty_entry() {
        let key = solana_pubkey::new_rand();
        let unknown_key = solana_pubkey::new_rand();

        let test = new_for_test::<u64>();

        let unknown_tag = test.tag(&unknown_key);
        let tag = test.tag(&key);
        let mut map = test.map_internal.write().unwrap();

        {
            // item is NOT in index at all, still return true from remove_if_slot_list_empty_entry
            // make sure not initially in index
            let entry = map.entry(unknown_tag);
            assert_matches!(entry, Entry::Vacant(_));
            let entry = map.entry(unknown_tag);
            assert!(test.remove_if_slot_list_empty_entry(&unknown_key, entry));
            // make sure still not in index
            let entry = map.entry(unknown_tag);
            assert_matches!(entry, Entry::Vacant(_));
        }

        {
            // an indexed pubkey always has an entry, so it is never removed here
            let val = AccountMapEntry::<u64>::new([(1, 1)], AccountMapEntryMeta::default());
            map.insert(tag, val);
            let entry = map.entry(tag);
            assert!(!test.remove_if_slot_list_empty_entry(&key, entry));
            let entry = map.entry(tag);
            assert_matches!(entry, Entry::Occupied(_));
        }
    }

    /// `delete` on an in-mem entry drains every slot-list item into reclaims and removes the
    /// entry from the index.
    #[test]
    fn test_delete_in_mem_entry() {
        let index = new_for_test::<u64>();
        let pubkey = Pubkey::new_unique();
        let mut reclaims = ReclaimsSlotList::new();

        let new_value = PreAllocatedAccountMapEntry::new(2, 20, &index.storage, true);
        index.upsert(
            &pubkey,
            new_value,
            None,
            &mut ReclaimsSlotList::new(),
            UpsertReclaim::IgnoreReclaims,
        );

        index.delete(&pubkey, &mut reclaims);

        assert_eq!(reclaims, vec![(2, 20)]);
        assert!(
            !index
                .map_internal
                .read()
                .unwrap()
                .contains_key(&index.tag(&pubkey))
        );
    }

    /// `delete` on a pubkey that was never indexed reclaims nothing and does not create an
    /// index entry.
    #[test]
    fn test_delete_missing_pubkey() {
        let index = new_for_test::<u64>();
        let pubkey = Pubkey::new_unique();
        let mut reclaims = ReclaimsSlotList::new();

        index.delete(&pubkey, &mut reclaims);

        assert!(reclaims.is_empty());
        assert!(
            !index
                .map_internal
                .read()
                .unwrap()
                .contains_key(&index.tag(&pubkey))
        );
    }

    /// `delete` on a disk-only entry (present in the disk bucket, evicted from memory) drains
    /// the disk slot list into reclaims and deletes the disk key, without re-inserting the
    /// entry into the in-mem map.
    #[test]
    fn test_delete_disk_only_entry() {
        let index = new_should_write_through_for_test(None);
        let pubkey = Pubkey::new_unique();
        let slot = 1;
        let info = 10;
        let mut reclaims = ReclaimsSlotList::new();

        let new_value = PreAllocatedAccountMapEntry::new(slot, info, &index.storage, true);
        index.upsert(
            &pubkey,
            new_value,
            None,
            &mut ReclaimsSlotList::new(),
            UpsertReclaim::IgnoreReclaims,
        );
        index.try_write_through(&pubkey);
        assert!(index.load_from_disk(&pubkey).is_some());

        // Evict the entry from memory so only the disk copy remains
        index
            .map_internal
            .write()
            .unwrap()
            .remove(&index.tag(&pubkey));

        index.delete(&pubkey, &mut reclaims);

        assert_eq!(reclaims, vec![(slot, info)]);
        assert!(index.load_from_disk(&pubkey).is_none());
        assert!(
            !index
                .map_internal
                .read()
                .unwrap()
                .contains_key(&index.tag(&pubkey))
        );
    }

    #[test]
    fn test_lock_and_update_slot_list() {
        let test = AccountMapEntry::<u64>::new([(1, 65)], AccountMapEntryMeta::default());
        let info = 66;
        let mut reclaims = ReclaimsSlotList::new();

        // update at the same slot replaces the entry
        InMemAccountsIndex::<u64, u64>::lock_and_update_slot_list(
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
        InMemAccountsIndex::<u64, u64>::lock_and_update_slot_list(
            &test,
            (2, info),
            None,
            &mut reclaims,
            UpsertReclaim::ReclaimOldSlots,
        );
        assert_eq!(test.slot_list(), [(2, info)]);
        assert_eq!(reclaims, ReclaimsSlotList::from([(1, info)]));
    }

    #[test_case(Some(10000);  "with pre-allocation 10000")]
    #[test_case(Some(20000);  "with pre-allocation 20000")]
    #[test_case(Some(30000);  "with pre-allocation 30000")]
    #[test_case(None; "without pre-allocation")]
    fn test_new_with_num_initial_accounts(num_initial_accounts: Option<usize>) {
        let config = AccountsIndexConfig::default();

        let bin_counts = [2, 4, 8];

        for bin_count in bin_counts {
            let holder = Arc::new(BucketMapHolder::new(bin_count, &config, 1));
            let mut total_capacity = 0;

            for bin in 0..bin_count {
                let accounts_index =
                    InMemAccountsIndex::<u64, u64>::new(&holder, bin, num_initial_accounts);
                total_capacity += accounts_index.map_internal.read().unwrap().capacity();
            }

            if let Some(num_initial_accounts) = num_initial_accounts {
                assert!(total_capacity > num_initial_accounts);
            } else {
                assert_eq!(total_capacity, 0);
            }
        }
    }

    /// Ensure `write_startup_info()` populates the in-mem index,
    /// while also respecting the configured memory threshold.
    #[test]
    fn test_write_startup_info() {
        let num_bins = 1;
        let num_entries_overhead = 300;
        let num_entries_to_evict = 200;
        let config = AccountsIndexConfig {
            bins: Some(num_bins),
            index_limit: {
                // Ensure we use an IndexLimit that (1) enables the disk index,
                // and (2) is a valid threshold, as per the logic in BucketMapHolder::new().
                // We will override the threshold afterwards, so the actual value doesn't matter.
                IndexLimit::Threshold(IndexLimitThreshold {
                    num_bytes: 25_000_000_000,
                    num_entries_overhead,
                    num_entries_to_evict,
                })
            },
            ..ACCOUNTS_INDEX_CONFIG_FOR_TESTING
        };
        let mut holder = BucketMapHolder::new(num_bins, &config, 1);

        // Override the threshold values to make testing faster.
        let low_water_mark = 100;
        let high_water_mark = low_water_mark + num_entries_to_evict;
        holder.threshold_entries_per_bin = Some(ThresholdEntriesPerBin {
            target_entries: high_water_mark + num_entries_overhead,
            high_water_mark,
            low_water_mark,
        });
        let holder = Arc::new(holder);
        let index = InMemAccountsIndex::<u64, u64>::new(&holder, num_bins - 1, None);

        // Emulate index generation where we push startup values into the `startup_info`
        // side-band struct when disk index is enabled.  Ensure we push more than
        // `low_water_mark` number of values.
        let to_insert = iter::repeat_with(|| {
            // the addresses need to be unique, but the actual values do not matter
            (Pubkey::new_unique(), (/*slot*/ 11, /*T*/ 42))
        })
        .take(high_water_mark);
        index.startup_info.insert.lock().unwrap().extend(to_insert);

        // Also push some duplicates, to ensure we do not put those in-mem
        let duplicate_pubkey = Pubkey::new_unique();
        {
            let mut startup_info_insert = index.startup_info.insert.lock().unwrap();
            // Yes, we want three duplicates.  Two is the minimum (by definition), but we want
            // three to ensure we don't see the first two, remove 'em, then see a third and think
            // "oh, this is a new non-duplicate!" and erroneously insert it in-mem.
            startup_info_insert.push((duplicate_pubkey, (/*slot*/ 13, /*T*/ 43)));
            startup_info_insert.push((duplicate_pubkey, (/*slot*/ 14, /*T*/ 44)));
            startup_info_insert.push((duplicate_pubkey, (/*slot*/ 15, /*T*/ 45)));
            // Reverse the vec to ensure the duplicates end up at the front.
            // Otherwise they would not be selected to be put in-mem.
            startup_info_insert.reverse();
        }
        assert!(index.map_internal.read().unwrap().is_empty());

        // Index generation calls `write_startup_info()`, which is responsible for writing the
        // values to disk, and also populating the in-mem index. So call `write_startup_info()`
        // here, and ensure:
        // - we end up with the expected number of items in the in-mem index
        // - duplicates do not end up in-mem
        index.write_startup_info();
        assert_eq!(index.map_internal.read().unwrap().len(), low_water_mark);
        assert!(
            !index
                .map_internal
                .read()
                .unwrap()
                .contains_key(&index.tag(&duplicate_pubkey))
        );
    }

    /// `update_entry` writes the entry through to disk
    #[test_case([(1, 0)], true ; "writes_through")]
    fn test_update_entry_write_through(slot_list: SlotList<u64>, expect_write_through: bool) {
        let index = new_should_write_through_for_test(None);
        let pubkey = solana_pubkey::new_rand();
        let entry = AccountMapEntry::new(
            slot_list,
            AccountMapEntryMeta::new_dirty(&index.storage, false),
        );
        index
            .map_internal
            .write()
            .unwrap()
            .insert(index.tag(&pubkey), entry);
        index.update_entry(&pubkey, |(slot, _account_info)| ((slot, 2), ()));

        index.get_only_in_mem(&pubkey, false, |entry| {
            let entry = entry.expect("entry should be in memory");
            assert_eq!(!entry.dirty(), expect_write_through);
        });

        // Verify whether entry was flushed to disk or not
        assert_eq!(
            index.load_from_disk(&pubkey).is_some(),
            expect_write_through
        );
    }

    /// `upsert` then `try_write_through` clears the dirty flag.
    #[test]
    fn test_try_write_through_clears_dirty() {
        let index = new_should_write_through_for_test(None);
        let pubkey = solana_pubkey::new_rand();
        let slot = 1;
        let info = 10;

        assert!(index.load_from_disk(&pubkey).is_none(), "not on disk yet");

        let new_value = PreAllocatedAccountMapEntry::new(slot, info, &index.storage, true);
        index.upsert(
            &pubkey,
            new_value,
            None,
            &mut ReclaimsSlotList::new(),
            UpsertReclaim::IgnoreReclaims,
        );
        index.try_write_through(&pubkey);

        index.get_only_in_mem(&pubkey, false, |entry| {
            let entry = entry.expect("entry should be in memory");
            assert!(!entry.dirty()); // write-through clears dirty
        });

        let slot_list = index
            .load_from_disk(&pubkey)
            .expect("upsert should have written entry to disk");
        assert_eq!(slot_list, SlotList::from([(slot, info)]));
    }

    /// `try_write_through` must leave a multi-slot entry alone: if the pubkey still has more
    /// than one slot-list entry, persisting just one of them to disk would let a later eviction
    /// drop the fresher in-mem entry in favor of an incomplete disk entry. The entry must stay
    /// dirty and nothing must be written to disk.
    /// When the bin exceeds the threshold and a new pubkey is inserted in `should_write_through`
    /// mode, one clean entry should be evicted inline to make room.
    #[test]
    fn test_inline_eviction_when_bin_exceeds_threshold() {
        // high_water_mark=2: after 3 insertions we are above threshold.
        let index = new_should_write_through_for_test(Some((2, 1)));
        let slot = 1;
        let info = 2;

        // Insert 3 entries via upsert — write-through will clean all of them.
        let initial_pubkeys: Vec<_> = (0..3).map(|_| solana_pubkey::new_rand()).collect();
        for pubkey in &initial_pubkeys {
            let new_value = PreAllocatedAccountMapEntry::new(slot, info, &index.storage, true);
            index.upsert(
                pubkey,
                new_value,
                None,
                &mut ReclaimsSlotList::new(),
                UpsertReclaim::IgnoreReclaims,
            );
            index.try_write_through(pubkey);
        }
        assert_eq!(index.map_internal.read().unwrap().len(), 3);

        // Confirm all entries are clean (write-through fired) and present on disk.
        for pubkey in &initial_pubkeys {
            index.get_only_in_mem(pubkey, false, |entry| {
                let entry = entry.expect("entry should be in memory");
                assert!(!entry.dirty());
            });
            assert!(
                index.load_from_disk(pubkey).is_some(),
                "entry should be on disk after write-through upsert"
            );
        }

        // Insert a 4th new pubkey, this should lead to eviction
        let new_pubkey = solana_pubkey::new_rand();
        let new_value = PreAllocatedAccountMapEntry::new(slot, info + 1, &index.storage, true);
        index.upsert(
            &new_pubkey,
            new_value,
            None,
            &mut ReclaimsSlotList::new(),
            UpsertReclaim::IgnoreReclaims,
        );

        // Inline eviction removes one entry before inserting the new one, leaving the bin count at 3
        assert_eq!(index.map_internal.read().unwrap().len(), 3);

        // The new pubkey must be present in memory.
        let mut found = None;
        index.get_only_in_mem(&new_pubkey, false, |entry| found = Some(entry.is_some()));
        assert_eq!(
            found,
            Some(true),
            "newly inserted entry should be in memory"
        );

        // Exactly one of the original entries was evicted from memory (but remains on disk).
        let evicted_count = initial_pubkeys
            .iter()
            .filter(|pubkey| {
                let mut in_mem = false;
                index.get_only_in_mem(pubkey, false, |entry| in_mem = entry.is_some());
                !in_mem
            })
            .count();
        assert_eq!(
            evicted_count, 1,
            "exactly one original entry should have been evicted"
        );

        // The evicted entry should still be on disk (it was written through before eviction).
        let evicted_pubkey = initial_pubkeys
            .iter()
            .find(|pubkey| {
                let mut in_mem = false;
                index.get_only_in_mem(pubkey, false, |entry| in_mem = entry.is_some());
                !in_mem
            })
            .unwrap();
        assert!(
            index.load_from_disk(evicted_pubkey).is_some(),
            "evicted entry should still be on disk"
        );
    }

    /// While the bin's hashmap is still in initial growth (capacity below `high_water_mark`),
    /// the growth gate short-circuits exceeds_thresholds to false even when the
    /// low-free-entries check would otherwise fire.
    #[test]
    fn test_exceeds_thresholds_below_hwm_gate() {
        // 56 entries fill hashbrown's raw=64 table exactly: capacity=56 (below HWM=100)
        // and free_entries=0 (below overhead=1, so low_free_entries would fire).
        let hwm = 100;
        let lwm = 50;
        let index = new_should_write_through_for_test(Some((hwm, lwm)));
        for _ in 0..56 {
            let pubkey = solana_pubkey::new_rand();
            let entry = AccountMapEntry::new(
                SlotList::from([(0, 0)]),
                AccountMapEntryMeta::new_dirty(&index.storage, true),
            );
            index
                .map_internal
                .write()
                .unwrap()
                .insert(index.tag(&pubkey), entry);
        }

        let map = index.map_internal.read().unwrap();
        let len = map.len();
        let capacity = map.capacity();
        let free_entries = capacity.saturating_sub(len);
        drop(map);

        // Confirm that without the gate that low free entries would fire
        assert!(
            index
                .storage
                .should_evict_based_on_free_entries(free_entries)
        );

        // But with the gate, exceeds_thresholds returns false
        assert!(!index.exceeds_thresholds());
    }

    /// Once capacity has cleared the low-water mark, exceeds_thresholds must still return
    /// false when the entry count is below the high-water mark and free-entry headroom exceeds
    /// the configured overhead.
    #[test]
    fn test_exceeds_thresholds_below_thresholds() {
        // 60 entries push capacity to 112 (above LWM=50), len stays below HWM=100,
        // and free_entries (52) far exceeds overhead (1) — both conditions report false.
        let hwm = 100;
        let lwm = 50;
        let index = new_should_write_through_for_test(Some((hwm, lwm)));
        for _ in 0..60 {
            let pubkey = solana_pubkey::new_rand();
            let entry = AccountMapEntry::new(
                SlotList::from([(0, 0)]),
                AccountMapEntryMeta::new_dirty(&index.storage, true),
            );
            index
                .map_internal
                .write()
                .unwrap()
                .insert(index.tag(&pubkey), entry);
        }
        assert!(index.map_internal.read().unwrap().capacity() > lwm);

        assert!(!index.exceeds_thresholds());
    }

    /// When the entry count crosses the high-water mark, exceeds_thresholds returns true
    /// and the count-based trigger stat is incremented.
    #[test]
    fn test_exceeds_thresholds_high_count() {
        let hwm = 2;
        let lwm = 1;
        // high_water_mark=2: inserting 4 entries puts the bin past the count threshold.
        let index = new_should_write_through_for_test(Some((hwm, lwm)));
        for _ in 0..4 {
            let pubkey = solana_pubkey::new_rand();
            let entry = AccountMapEntry::new(
                SlotList::from([(0, 0)]),
                AccountMapEntryMeta::new_dirty(&index.storage, true),
            );
            index
                .map_internal
                .write()
                .unwrap()
                .insert(index.tag(&pubkey), entry);
        }

        assert!(index.exceeds_thresholds());
    }

    /// reallocate_to_clear_tombstones must rebuild the bin's hashmap so that all
    /// remaining entries survive and `capacity()` recovers from the drop caused by
    /// tombstones consuming `growth_left`.
    #[test]
    fn test_reallocate_to_clear_tombstones_preserves_entries() {
        // Reallocate only runs in Threshold mode. For this test HWM must be less
        // than the number of inserts to ensure the calculated bucket size is
        // the same for hwm and num_inserts
        let hwm = 199;
        let lwm = 140;
        let index = new_should_write_through_for_test(Some((hwm, lwm)));

        // Fill the bin's hashmap exactly to hashbrown's max_load (7/8 of 256 buckets).
        // At this size at least one remove is all but guaranteed (odds ~1e-10) to create a tombstone
        let num_inserts = 224;
        // Then remove enough entries to drop down to the low water mark
        let num_removes = 84;
        let pubkeys: Vec<_> = (0..num_inserts)
            .map(|_| solana_pubkey::new_rand())
            .collect();
        {
            let mut map = index.map_internal.write().unwrap();
            for pubkey in &pubkeys {
                let entry = AccountMapEntry::new(
                    SlotList::from([(0, 42)]),
                    AccountMapEntryMeta::new_dirty(&index.storage, true),
                );
                map.insert(index.tag(pubkey), entry);
            }
        }
        let capacity_after_inserts = index.map_internal.read().unwrap().capacity();

        // Remove a portion of the entries to create tombstones. Hashbrown reduces capacity
        // for each tombstone created, so we should see a capacity drop here.
        let mut map = index.map_internal.write().unwrap();
        for pubkey in &pubkeys[..num_removes] {
            map.remove(&index.tag(pubkey));
        }
        drop(map);

        let capacity_after_removes = index.map_internal.read().unwrap().capacity();

        // Verify that capacity dropped due to added tombstones
        assert!(capacity_after_removes < capacity_after_inserts);

        index.reallocate_to_clear_tombstones();

        let map = index.map_internal.read().unwrap();

        // All remaining entries should survive the realloc.
        assert_eq!(map.len(), num_inserts - num_removes);
        for pubkey in &pubkeys[num_removes..] {
            assert!(map.contains_key(&index.tag(pubkey)));
        }

        // Tombstones cleared: the new map sized for `len()` lands on the same raw
        // bucket count as before the removes, so capacity is back to its post-insert
        // value.
        assert_eq!(map.capacity(), capacity_after_inserts);
        drop(map);

        assert_eq!(
            index
                .stats()
                .num_hashmap_reallocates
                .load(Ordering::Relaxed),
            1
        );
    }
}
