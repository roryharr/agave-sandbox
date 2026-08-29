use {
    super::{
        IndexValue, ReclaimsSlotList, SlotListItem, UpsertReclaim,
        account_map_entry::AccountMapEntry,
        stats::Stats,
        tag::{Tag, TagCalculator, TagHasherBuilder},
    },
    solana_clock::Slot,
    solana_measure::measure::Measure,
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, hash_map::Entry},
        fmt::Debug,
        sync::{
            Arc, Mutex, RwLock,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    },
};

// one instance of this represents one bin of the accounts index.
pub struct InMemAccountsIndex<T: IndexValue> {
    // backing store
    map_internal: RwLock<HashMap<Tag, AccountMapEntry<T>, TagHasherBuilder>>,
    /// computes the `Tag` this bin's map is keyed by
    tag_calculator: TagCalculator,
    stats: Arc<Stats>,
    /// true while generate_index is populating the index
    startup: Arc<AtomicBool>,
    /// older versions of pubkeys that were found in more than one slot during startup.
    /// The index holds the version from the newest slot; every other version is here.
    startup_duplicates: Mutex<Vec<(Slot, Pubkey, T)>>,
    _bin: usize,
}

impl<T: IndexValue> Debug for InMemAccountsIndex<T> {
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
    },
}

impl<T: IndexValue> InMemAccountsIndex<T> {
    pub fn new(
        stats: &Arc<Stats>,
        startup: &Arc<AtomicBool>,
        bin: usize,
        capacity: Option<usize>,
    ) -> Self {
        let map_internal = if let Some(capacity) = capacity {
            RwLock::new(HashMap::with_capacity_and_hasher(
                capacity,
                TagHasherBuilder,
            ))
        } else {
            RwLock::default()
        };

        Self {
            map_internal,
            tag_calculator: TagCalculator::default(),
            stats: Arc::clone(stats),
            startup: Arc::clone(startup),
            startup_duplicates: Mutex::default(),
            _bin: bin,
        }
    }

    /// the `Tag` this bin's map is keyed by
    #[inline]
    fn tag(&self, pubkey: &Pubkey) -> Tag {
        self.tag_calculator.tag_from_pubkey(pubkey)
    }

    /// return the entry of every pubkey in this bin
    ///
    /// The map is keyed by `Tag`, so the pubkey of an entry is not recoverable from the index.
    /// Callers that need it read it from the account record the entry points at.
    pub fn entries(&self) -> Vec<SlotListItem<T>> {
        self.map_internal
            .read()
            .unwrap()
            .values()
            .map(|entry| entry.entry())
            .collect()
    }

    /// lookup 'pubkey' in the index.
    /// callback is called whether pubkey is found or not
    pub(super) fn get_internal_inner<RT>(
        &self,
        pubkey: &Pubkey,
        callback: impl for<'a> FnOnce(Option<&'a AccountMapEntry<T>>) -> RT,
    ) -> RT {
        let tag = self.tag(pubkey);
        let mut m = Measure::start("get");
        let map = self.map_internal.read().unwrap();
        let entry = map.get(&tag);
        m.stop();
        let found = entry.is_some();
        let result = callback(entry);
        drop(map);

        let stats = &self.stats;
        let (count, time) = if found {
            (&stats.gets_from_mem, &stats.get_mem_us)
        } else {
            (&stats.gets_missing, &stats.get_missing_us)
        };
        Self::update_stat(time, m.as_us());
        Self::update_stat(count, 1);

        result
    }

    /// Drain the slot list for `pubkey` into `reclaims` and remove the pubkey from the index.
    /// Does nothing if `pubkey` was never indexed.
    pub fn delete(&self, pubkey: &Pubkey, reclaims: &mut ReclaimsSlotList<T>) {
        let mut map = self.map_internal.write().unwrap();
        if let Entry::Occupied(occupied) = map.entry(self.tag(pubkey)) {
            reclaims.extend(occupied.get().slot_list().iter().copied());
            self.stats.dec_mem_count();
            self.stats.inc_delete();
            occupied.remove();
        }
    }

    /// Return true if `pubkey` is not in the index.
    ///
    /// The index holds a single `(slot, account_info)` per pubkey, so an indexed pubkey never
    /// has an empty slot list; only a missing pubkey does.
    pub fn remove_if_slot_list_empty(&self, pubkey: Pubkey) -> bool {
        !self
            .map_internal
            .read()
            .unwrap()
            .contains_key(&self.tag(&pubkey))
    }

    /// Call `user_fn` with the entry for `pubkey`, writing the result back into the index
    pub(crate) fn update_entry<RT>(
        &self,
        pubkey: &Pubkey,
        user_fn: impl FnOnce(SlotListItem<T>) -> (SlotListItem<T>, RT),
    ) -> Option<RT> {
        self.get_internal_inner(pubkey, |entry| {
            entry.map(|entry| {
                let (new_item, result) = user_fn(entry.entry());
                entry.replace(new_item);
                result
            })
        })
    }

    /// Remove `pubkey`'s entry from the index if `should_remove` returns true for its entry.
    ///
    /// Returns `None` if `pubkey` is not in the index, otherwise whether it was removed.
    pub(crate) fn remove_entry_if(
        &self,
        pubkey: &Pubkey,
        should_remove: impl FnOnce(&SlotListItem<T>) -> bool,
    ) -> Option<bool> {
        let mut map = self.map_internal.write().unwrap();
        match map.entry(self.tag(pubkey)) {
            Entry::Occupied(occupied) => {
                let remove = should_remove(&occupied.get().entry());
                if remove {
                    self.stats.dec_mem_count();
                    self.stats.inc_delete();
                    occupied.remove();
                }
                Some(remove)
            }
            Entry::Vacant(_vacant) => None,
        }
    }

    pub fn upsert(
        &self,
        pubkey: &Pubkey,
        new_value: SlotListItem<T>,
        other_slot: Option<Slot>,
        reclaims: &mut ReclaimsSlotList<T>,
        reclaim: UpsertReclaim,
    ) {
        self.get_or_create_index_entry_for_pubkey(pubkey, new_value, |entry| {
            Self::lock_and_update_slot_list(entry, new_value, other_slot, reclaims, reclaim);
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
        let tag = self.tag(pubkey);

        // try to get it just using a read lock first
        {
            let map = self.map_internal.read().unwrap();
            if let Some(entry) = map.get(&tag) {
                callback(entry);
                drop(map);
                Self::update_stat(&self.stats.updates_in_mem, 1);
                return;
            }
        }

        let stats = &self.stats;
        let mut m = Measure::start("entry");
        let mut map = self.map_internal.write().unwrap();
        let capacity_pre = map.capacity();
        let entry = map.entry(tag);
        m.stop();
        let found = matches!(entry, Entry::Occupied(_));
        match entry {
            Entry::Occupied(occupied) => {
                callback(occupied.get());
                Self::update_stat(&stats.updates_in_mem, 1);
            }
            Entry::Vacant(vacant) => {
                // not in the index, so insert the new item. There is no prior entry to
                // update, so `callback` is not called
                stats.inc_insert();
                vacant.insert(AccountMapEntry::new([new_item]));
                stats.inc_mem_count();
            }
        };
        let capacity_post = map.capacity();
        drop(map);
        stats.update_in_mem_capacity(capacity_pre, capacity_post);
        self.update_entry_stats(m, found);
    }

    fn update_entry_stats(&self, stopped_measure: Measure, found: bool) {
        let stats = &self.stats;
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

    pub fn startup_update_duplicates(&self, items: Vec<(Slot, Pubkey, T)>) {
        assert!(self.startup.load(Ordering::Relaxed));

        let mut duplicates = self.startup_duplicates.lock().unwrap();
        duplicates.extend(items);
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
        new_entry: SlotListItem<T>,
    ) -> InsertNewEntryResults<T> {
        let mut map = self.map_internal.write().unwrap();
        match map.entry(self.tag(&pubkey)) {
            Entry::Occupied(occupied) => {
                // keep whichever version is from the newer slot
                let older_version = occupied.get().replace_if_newer(new_entry, None);
                InsertNewEntryResults::Existed { older_version }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(AccountMapEntry::new([new_entry]));
                InsertNewEntryResults::DidNotExist
            }
        }
    }

    /// pull out all the older versions of duplicate pubkeys found for this bin during startup.
    /// The index holds the version from the newest slot; these are all the rest.
    pub fn take_startup_duplicates(&self) -> Vec<(Slot, Pubkey, T)> {
        let mut duplicate_items = self.startup_duplicates.lock().unwrap();
        std::mem::take(&mut duplicate_items)
    }

    /// The footprint of a single element in the in-mem hashmap
    pub const fn size_of_uninitialized() -> usize {
        size_of::<Tag>()
    }

    /// The size of an index value, with only a single entry in the slot list
    pub const fn size_of_single_entry() -> usize {
        size_of::<AccountMapEntry<T>>()
    }

    fn update_stat(stat: &AtomicU64, value: u64) {
        if value != 0 {
            stat.fetch_add(value, Ordering::Relaxed);
        }
    }

    /// Returns the length and capacity of this bin's map
    ///
    /// Only intended to be called at startup, since it grabs the map's read lock.
    pub(crate) fn len_and_cap_for_startup(&self) -> (usize, usize) {
        let map = self.map_internal.read().unwrap();
        (map.len(), map.capacity())
    }

    /// Returns the number of entries currently held in this bin.
    pub(crate) fn len(&self) -> usize {
        self.map_internal.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::accounts_index::SlotList, solana_pubkey::Pubkey, test_case::test_case};

    fn new_for_test<T: IndexValue>() -> InMemAccountsIndex<T> {
        let stats = Arc::new(Stats::new(1));
        let startup = Arc::new(AtomicBool::new(false));
        let bin = 0;
        InMemAccountsIndex::new(&stats, &startup, bin, None)
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
        accounts_index.get_internal_inner(&pubkey, |entry| {
            let entry = entry.expect("entry should be in the index");
            assert_eq!(entry.slot_list(), [(slot, 0)]);
        });
    }

    #[test]
    fn test_get_or_create_index_entry_for_pubkey_existing() {
        let accounts_index = new_for_test::<u64>();
        let pubkey = solana_pubkey::new_rand();

        // Insert an entry manually
        let entry = AccountMapEntry::new(SlotList::from([(0, 42)]));
        accounts_index
            .map_internal
            .write()
            .unwrap()
            .insert(accounts_index.tag(&pubkey), entry);

        let mut callback_called = false;
        accounts_index.get_or_create_index_entry_for_pubkey(&pubkey, (1, 43), |entry| {
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

    /// An indexed pubkey always has an entry, so only a missing pubkey reports true.
    #[test]
    fn test_remove_if_slot_list_empty() {
        let key = solana_pubkey::new_rand();
        let unknown_key = solana_pubkey::new_rand();

        let test = new_for_test::<u64>();

        test.map_internal
            .write()
            .unwrap()
            .insert(test.tag(&key), AccountMapEntry::<u64>::new([(1, 1)]));

        assert!(test.remove_if_slot_list_empty(unknown_key));
        assert!(!test.remove_if_slot_list_empty(key));
        // the indexed pubkey is still in the index
        assert!(
            test.map_internal
                .read()
                .unwrap()
                .contains_key(&test.tag(&key))
        );
    }

    /// `delete` on an entry drains every slot-list item into reclaims and removes the
    /// entry from the index.
    #[test]
    fn test_delete_entry() {
        let index = new_for_test::<u64>();
        let pubkey = Pubkey::new_unique();
        let mut reclaims = ReclaimsSlotList::new();

        index.upsert(
            &pubkey,
            (2, 20),
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

    #[test]
    fn test_lock_and_update_slot_list() {
        let test = AccountMapEntry::<u64>::new([(1, 65)]);
        let info = 66;
        let mut reclaims = ReclaimsSlotList::new();

        // update at the same slot replaces the entry
        InMemAccountsIndex::<u64>::lock_and_update_slot_list(
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
        InMemAccountsIndex::<u64>::lock_and_update_slot_list(
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
    #[test_case(None; "without pre-allocation")]
    fn test_new_with_capacity(capacity: Option<usize>) {
        let stats = Arc::new(Stats::new(1));
        let startup = Arc::new(AtomicBool::new(false));
        let accounts_index = InMemAccountsIndex::<u64>::new(&stats, &startup, 0, capacity);
        let map_capacity = accounts_index.map_internal.read().unwrap().capacity();
        if let Some(capacity) = capacity {
            assert!(map_capacity >= capacity);
        } else {
            assert_eq!(map_capacity, 0);
        }
    }
}
