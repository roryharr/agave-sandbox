#[cfg(feature = "dev-context-only-utils")]
use crate::bucket_item::BucketItem;
use {
    crate::{
        MaxSearch,
        bucket_map::BucketMapError,
        bucket_stats::BucketMapStats,
        bucket_storage::{
            BucketCapacity, BucketOccupied, BucketStorage, Capacity, DEFAULT_CAPACITY_POW2,
            IncludeHeader,
        },
        index_entry::{
            DataBucket, IndexBucket, IndexEntry, IndexEntryPlaceInBucket, MultipleSlots,
            OccupiedEnum,
        },
        restart::RestartableBucket,
    },
    rand::{Rng, rng},
    solana_measure::measure::Measure,
    solana_pubkey::Pubkey,
    std::{
        fs,
        num::NonZeroU64,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
    },
};

pub struct ReallocatedItems<I: BucketOccupied, D: BucketOccupied> {
    // Some if the index was reallocated
    pub index: Option<BucketStorage<I>>,
    // Some for a data bucket reallocation
    // u64 is data bucket index
    pub data: Option<(u64, BucketStorage<D>)>,
}

impl<I: BucketOccupied, D: BucketOccupied> Default for ReallocatedItems<I, D> {
    fn default() -> Self {
        Self {
            index: None,
            data: None,
        }
    }
}

pub struct Reallocated<I: BucketOccupied, D: BucketOccupied> {
    /// > 0 if reallocations are encoded
    pub active_reallocations: AtomicUsize,

    /// actual reallocated bucket
    /// mutex because bucket grow code runs with a read lock
    pub items: Mutex<ReallocatedItems<I, D>>,
}

impl<I: BucketOccupied, D: BucketOccupied> Default for Reallocated<I, D> {
    fn default() -> Self {
        Self {
            active_reallocations: AtomicUsize::default(),
            items: Mutex::default(),
        }
    }
}

impl<I: BucketOccupied, D: BucketOccupied> Reallocated<I, D> {
    /// Return true if this caller reserved the next reallocation.
    pub fn try_reserve_reallocation(&self) -> bool {
        self.active_reallocations
            .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }
    /// Return true IFF a reallocation has occurred.
    /// Calling this takes conceptual ownership of the reallocation encoded in the struct.
    pub fn get_reallocated(&self) -> bool {
        self.active_reallocations
            .compare_exchange(1, 0, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }
}

/// when updating the index, this keeps track of the previous data entry which will need to be freed
struct DataFileEntryToFree {
    bucket_ix: usize,
    location: u64,
}

// >= 2 instances of BucketStorage per 'bucket' in the bucket map. 1 for index, >= 1 for data
pub struct Bucket<T: Copy + PartialEq + 'static> {
    drives: Arc<Vec<PathBuf>>,
    /// index
    pub index: BucketStorage<IndexBucket<T>>,
    /// random offset for the index
    random: u64,
    /// storage buckets to store SlotSlice up to a power of 2 in len
    pub data: Vec<BucketStorage<DataBucket>>,
    stats: Arc<BucketMapStats>,

    /// # entries caller expects the map to need to contain.
    /// Used as a hint for the next time we need to grow.
    anticipated_size: u64,

    pub reallocated: Reallocated<IndexBucket<T>, DataBucket>,

    /// set to true once any entries have been deleted from the index.
    at_least_one_entry_deleted: bool,

    /// keep track of which index file this bucket is using so on restart we can try to reuse it
    restartable_bucket: RestartableBucket,
}

/// Where a key is, or would go, in the index.
///
/// The index uses Robin Hood placement: each run of occupied entries is kept in order of the
/// entries' home slots, so no entry is farther from its home than an entry that hashes after it.
/// A probe for a key can then stop at the first free entry, or at the first entry that is closer to
/// its home than the probe is to the key's.
#[derive(Clone, Copy)]
enum IndexSlot {
    /// the key is at this index
    Occupied(u64),
    /// the key is absent, and goes at `at` once the entries in `at..free` move forward one
    Vacant { at: u64, free: u64 },
}

impl<'b, T: Clone + Copy + PartialEq + std::fmt::Debug + 'static> Bucket<T> {
    pub(crate) fn new(
        drives: Arc<Vec<PathBuf>>,
        max_search: MaxSearch,
        stats: Arc<BucketMapStats>,
        count: Arc<AtomicU64>,
        mut restartable_bucket: RestartableBucket,
    ) -> Self {
        let reuse_path = std::mem::take(&mut restartable_bucket.path);
        let elem_size = NonZeroU64::new(std::mem::size_of::<IndexEntry<T>>() as u64).unwrap();
        let (index, random) = reuse_path
            .and_then(|path| {
                // try to reuse the file this bucket was using last time we were running
                restartable_bucket.get().and_then(|(_file_name, random)| {
                    let result = BucketStorage::load_on_restart(
                        path.clone(),
                        elem_size,
                        max_search,
                        Arc::clone(&stats.index),
                        count.clone(),
                    )
                    .map(|index| (index, random));
                    if result.is_none() {
                        // we couldn't reuse it, so delete it
                        _ = fs::remove_file(path);
                    }
                    result
                })
            })
            .unwrap_or_else(|| {
                // no file to reuse, so create a new file
                let (index, file_name) = BucketStorage::new(
                    Arc::clone(&drives),
                    1,
                    elem_size.into(),
                    max_search,
                    Arc::clone(&stats.index),
                    count,
                );
                let random = rng().random();
                restartable_bucket.set_file(file_name, random);
                (index, random)
            });

        stats.index.resize_grow(0, index.capacity_bytes());

        Self {
            random,
            drives,
            index,
            data: vec![],
            stats,
            reallocated: Reallocated::default(),
            anticipated_size: 0,
            at_least_one_entry_deleted: false,
            restartable_bucket,
        }
    }

    pub fn keys(&self) -> Vec<Pubkey> {
        let mut rv = vec![];
        for i in 0..self.index.capacity() {
            if self.index.is_free(i) {
                continue;
            }
            let ix: &IndexEntry<T> = self.index.get(i);
            rv.push(ix.key);
        }
        rv
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn items(&self) -> Vec<BucketItem<T>> {
        let mut result = Vec::with_capacity(self.index.count.load(Ordering::Relaxed) as usize);
        for i in 0..self.index.capacity() {
            let ii = i % self.index.capacity();
            if self.index.is_free(ii) {
                continue;
            }
            let ix = IndexEntryPlaceInBucket::new(ii);
            let key = ix.key(&self.index);
            let v = ix.read_value(&self.index, &self.data);
            result.push(BucketItem {
                pubkey: *key,
                slot_list: v.to_vec(),
            });
        }
        result
    }

    pub fn find_index_entry(&self, key: &Pubkey) -> Option<(IndexEntryPlaceInBucket<T>, u64)> {
        Self::bucket_find_index_entry(&self.index, key, self.random)
    }

    /// slot `key` hashes to in an index of `capacity` entries
    fn home_ix(key: &Pubkey, random: u64, capacity: u64) -> u64 {
        Self::bucket_index_ix(key, random) % capacity
    }

    /// how many slots past `home` `ix` is, wrapping at `capacity`
    fn distance_from_home(home: u64, ix: u64, capacity: u64) -> u64 {
        (ix + capacity - home) % capacity
    }

    /// Find where `key` is in `index`, or where it would be inserted.
    /// Returns an error if inserting `key` would leave an entry more than `max_search` from home.
    fn find_index_slot(
        index: &BucketStorage<IndexBucket<T>>,
        key: &Pubkey,
        random: u64,
    ) -> Result<IndexSlot, BucketMapError> {
        let mut m = Measure::start("bucket_find_index_entry_mut");
        let result = Self::find_index_slot_internal(index, key, random);
        m.stop();
        index
            .stats
            .find_index_entry_mut_us
            .fetch_add(m.as_us(), Ordering::Relaxed);
        result
    }

    fn find_index_slot_internal(
        index: &BucketStorage<IndexBucket<T>>,
        key: &Pubkey,
        random: u64,
    ) -> Result<IndexSlot, BucketMapError> {
        let capacity = index.capacity();
        let max_search = index.max_search().min(capacity);
        let no_space = || BucketMapError::IndexNoSpace(index.contents.capacity());
        let home = Self::home_ix(key, random, capacity);
        for distance in 0..max_search {
            let ix = (home + distance) % capacity;
            if index.is_free(ix) {
                return Ok(IndexSlot::Vacant { at: ix, free: ix });
            }
            let occupant = IndexEntryPlaceInBucket::<T>::new(ix).key(index);
            if occupant == key {
                return Ok(IndexSlot::Occupied(ix));
            }
            let occupant_home = Self::home_ix(occupant, random, capacity);
            if Self::distance_from_home(occupant_home, ix, capacity) < distance {
                // `key` goes before this entry, which moves forward one along with the rest of the run
                let mut free = ix;
                loop {
                    free = (free + 1) % capacity;
                    if free == ix {
                        return Err(no_space());
                    }
                    if index.is_free(free) {
                        return Ok(IndexSlot::Vacant { at: ix, free });
                    }
                    let moved_home = Self::home_ix(
                        IndexEntryPlaceInBucket::<T>::new(free).key(index),
                        random,
                        capacity,
                    );
                    if Self::distance_from_home(moved_home, free, capacity) + 1 >= max_search {
                        return Err(no_space());
                    }
                }
            }
        }
        Err(no_space())
    }

    fn bucket_find_index_entry(
        index: &BucketStorage<IndexBucket<T>>,
        key: &Pubkey,
        random: u64,
    ) -> Option<(IndexEntryPlaceInBucket<T>, u64)> {
        let capacity = index.capacity();
        let home = Self::home_ix(key, random, capacity);
        for distance in 0..index.max_search().min(capacity) {
            let ix = (home + distance) % capacity;
            if index.is_free(ix) {
                return None;
            }
            let elem = IndexEntryPlaceInBucket::new(ix);
            let occupant = elem.key(index);
            if occupant == key {
                return Some((elem, ix));
            }
            let occupant_home = Self::home_ix(occupant, random, capacity);
            if Self::distance_from_home(occupant_home, ix, capacity) < distance {
                // `key` would have been placed before this entry
                return None;
            }
        }
        None
    }

    /// Moves the entries in `at..free` forward one, then occupies `at` with `key` and returns `at`.
    /// The caller must set the new entry's value.
    fn insert_index_entry(
        index: &mut BucketStorage<IndexBucket<T>>,
        key: &Pubkey,
        at: u64,
        free: u64,
        is_resizing: bool,
    ) -> u64 {
        let capacity = index.capacity();
        let mut to = free;
        while to != at {
            let from = (to + capacity - 1) % capacity;
            Self::move_index_entry(index, from, to);
            to = from;
        }
        index.occupy(at, is_resizing).unwrap();
        // These fields will be overwritten after allocation by callers.
        // Since this part of the mmapped file could have previously been used by someone else, there can be garbage here.
        IndexEntryPlaceInBucket::new(at).init(index, key);
        at
    }

    /// Moves the entry at `from` to the free entry at `to`, leaving `from` free.
    fn move_index_entry(index: &mut BucketStorage<IndexBucket<T>>, from: u64, to: u64) {
        let entry: IndexEntry<T> = *index.get(from);
        *index.get_mut::<IndexEntry<T>>(to) = entry;
        index.contents.move_enum_tag(from, to);
    }

    fn bucket_create_key(
        index: &mut BucketStorage<IndexBucket<T>>,
        key: &Pubkey,
        random: u64,
        is_resizing: bool,
    ) -> Result<u64, BucketMapError> {
        match Self::find_index_slot(index, key, random)? {
            IndexSlot::Occupied(ix) => panic!("{key} is already in the index at {ix}"),
            IndexSlot::Vacant { at, free } => {
                Ok(Self::insert_index_entry(index, key, at, free, is_resizing))
            }
        }
    }

    pub(crate) fn read_value(&self, key: &Pubkey) -> Option<&[T]> {
        //debug!("READ_VALUE: {:?}", key);
        let (elem, _) = self.find_index_entry(key)?;
        Some(elem.read_value(&self.index, &self.data))
    }

    /// for each item in `items`, get the hash value when hashed with `random`.
    /// Return a vec of tuples:
    /// (hash_value, index in `items`)
    fn index_entries(items: &[(Pubkey, T)], random: u64) -> Vec<(u64, usize)> {
        let mut inserts = Vec::with_capacity(items.len());
        items.iter().enumerate().for_each(|(i, (key, _v))| {
            let ix = Self::bucket_index_ix(key, random);
            inserts.push((ix, i));
        });
        inserts
    }

    /// batch insert of `items`. Assumption is a single slot list element
    /// For any pubkeys that already exist, the index in `items` of the failed insertion and the existing data (previously put in the index) are returned.
    pub(crate) fn batch_insert_non_duplicates(&mut self, items: &[(Pubkey, T)]) -> Vec<(usize, T)> {
        assert!(
            !self.at_least_one_entry_deleted,
            "efficient batch insertion can only occur prior to any deletes"
        );
        let current_len = self.index.count.load(Ordering::Relaxed);
        let anticipated = items.len() as u64;
        self.set_anticipated_count((anticipated).saturating_add(current_len));
        let mut entries = Self::index_entries(items, self.random);
        let mut duplicates = Vec::default();
        let mut entries_created_on_disk = 0;
        // insert, but resizes may be necessary
        loop {
            let cap = self.index.capacity();
            // sort entries by their index % cap, so we'll search over the same spots in the file close to each other
            // `reverse()` is so we can efficiently pop off the end but get ascending order index values
            // sort before calling to make `batch_insert_non_duplicates_internal` easier to test.
            entries.sort_unstable_by(|a, b| (a.0 % cap).cmp(&(b.0 % cap)).reverse());

            let result = Self::batch_insert_non_duplicates_internal(
                &mut self.index,
                &self.data,
                items,
                &mut entries,
                &mut entries_created_on_disk,
                &mut duplicates,
                self.random,
            );
            match result {
                Ok(_result) => {
                    // everything added
                    self.set_anticipated_count(0);
                    self.index.count.fetch_add(
                        items.len().saturating_sub(duplicates.len()) as u64,
                        Ordering::Relaxed,
                    );
                    let stats = &self.index.stats.startup;
                    stats.entries_reused.fetch_add(
                        items
                            .len()
                            .saturating_sub(duplicates.len())
                            .saturating_sub(entries_created_on_disk) as u64,
                        Ordering::Relaxed,
                    );
                    stats
                        .entries_created
                        .fetch_add(entries_created_on_disk as u64, Ordering::Relaxed);
                    return duplicates;
                }
                Err(error) => {
                    // resize and add more
                    // `entries` will have had items removed from it
                    self.grow(error);
                    self.handle_delayed_grows();
                }
            }
        }
    }

    /// insert as much of `entries` as possible into `index`.
    /// return an error if the index needs to resize.
    /// for every entry that already exists in `index`, add it (and the value already in the index) to `duplicates`
    /// `reverse_sorted_entries` is (raw index (range = U64::MAX) in hash map, index in `items`)
    pub fn batch_insert_non_duplicates_internal(
        index: &mut BucketStorage<IndexBucket<T>>,
        data_buckets: &[BucketStorage<DataBucket>],
        items: &[(Pubkey, T)],
        reverse_sorted_entries: &mut Vec<(u64, usize)>,
        entries_created_on_disk: &mut usize,
        duplicates: &mut Vec<(usize, T)>,
        random: u64,
    ) -> Result<(), BucketMapError> {
        // pop one entry at a time to insert
        while let Some((ix_entry_raw, i)) = reverse_sorted_entries.pop() {
            let (k, v) = &items[i];
            match Self::find_index_slot(index, k, random) {
                Ok(IndexSlot::Occupied(ix)) => {
                    // found a duplicate entry
                    let v_existing =
                        IndexEntryPlaceInBucket::new(ix).read_value(index, data_buckets);
                    duplicates.push((i, *v_existing.first().unwrap()));
                }
                Ok(IndexSlot::Vacant { at, free }) => {
                    // the caller adds the inserted entries to the count, so don't count them here
                    let is_resizing = true;
                    let elem = IndexEntryPlaceInBucket::new(Self::insert_index_entry(
                        index,
                        k,
                        at,
                        free,
                        is_resizing,
                    ));
                    // new data stored should be stored in IndexEntry and NOT in data file
                    // new data len is 1
                    elem.set_slot_count_enum_value(index, OccupiedEnum::OneSlotInIndex(v));
                    *entries_created_on_disk += 1;
                }
                Err(err) => {
                    // remember the item we were trying to insert for next time after resizing
                    reverse_sorted_entries.push((ix_entry_raw, i));
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    pub fn try_write(
        &mut self,
        key: &Pubkey,
        mut data: impl Iterator<Item = &'b T>,
        data_len: usize,
    ) -> Result<(), BucketMapError> {
        let num_slots = data_len as u64;
        let best_fit_bucket = MultipleSlots::data_bucket_from_num_slots(data_len as u64);
        // num_slots > 1 because we can store num_slots = 0 or num_slots = 1 in the index entry
        let requires_data_bucket = num_slots > 1;
        if requires_data_bucket && self.data.get(best_fit_bucket as usize).is_none() {
            // fail early if the data bucket we need doesn't exist - we don't want the index entry partially allocated
            return Err(BucketMapError::DataNoSpace((best_fit_bucket, 0)));
        }

        let max_search = self.index.max_search();
        let slot = Self::find_index_slot(&self.index, key, self.random)?;
        let elem = match slot {
            IndexSlot::Occupied(ix) => Some(IndexEntryPlaceInBucket::new(ix)),
            IndexSlot::Vacant { .. } => None,
        };
        // inserts `key` when it is not in the index yet
        let insert_key = |index: &mut BucketStorage<IndexBucket<T>>| match slot {
            IndexSlot::Occupied(ix) => unreachable!("{key} is already in the index at {ix}"),
            IndexSlot::Vacant { at, free } => {
                let is_resizing = false;
                IndexEntryPlaceInBucket::new(Self::insert_index_entry(
                    index,
                    key,
                    at,
                    free,
                    is_resizing,
                ))
            }
        };
        if !requires_data_bucket {
            let elem = elem.unwrap_or_else(|| insert_key(&mut self.index));
            // new data stored should be stored in IndexEntry and NOT in data file
            // new data len is 0 or 1
            if let OccupiedEnum::MultipleSlots(multiple_slots) =
                elem.get_slot_count_enum(&self.index)
            {
                let bucket_ix = multiple_slots.data_bucket_ix() as usize;
                // free the entry in the data bucket the data was previously stored in
                let loc = multiple_slots.data_loc(&self.data[bucket_ix]);
                self.data[bucket_ix].free(loc);
            }
            elem.set_slot_count_enum_value(
                &mut self.index,
                if let Some(single_element) = data.next() {
                    OccupiedEnum::OneSlotInIndex(single_element)
                } else {
                    self.stats
                        .index
                        .index_uses_uncommon_slot_list_len
                        .store(true, Ordering::Relaxed);
                    OccupiedEnum::ZeroSlots
                },
            );
            return Ok(());
        }

        // storing the slot list requires using the data file
        let mut old_data_entry_to_free = None;
        // see if old elements were in a data file
        if let Some(multiple_slots) = elem
            .as_ref()
            .and_then(|elem| elem.get_multiple_slots_mut(&mut self.index))
        {
            let bucket_ix = multiple_slots.data_bucket_ix() as usize;
            let current_bucket = &mut self.data[bucket_ix];
            let elem_loc = multiple_slots.data_loc(current_bucket);

            if best_fit_bucket == bucket_ix as u64 {
                // write data
                assert!(!current_bucket.is_free(elem_loc));
                let slice: &mut [T] = current_bucket.get_slice_mut(
                    elem_loc,
                    data_len as u64,
                    IncludeHeader::NoHeader,
                );
                multiple_slots.set_num_slots(num_slots);

                slice.iter_mut().zip(data).for_each(|(dest, src)| {
                    *dest = *src;
                });
                return Ok(());
            }

            // not updating in place, so remember old entry to free
            // Wait to free until we make sure we don't have to resize the best_fit_bucket
            old_data_entry_to_free = Some(DataFileEntryToFree {
                bucket_ix,
                location: elem_loc,
            });
        }

        // need to move the allocation to a best fit spot
        let best_bucket = &mut self.data[best_fit_bucket as usize];
        let cap_power = best_bucket.contents.capacity_pow2();
        let cap = best_bucket.capacity();
        let pos = rng().random_range(0..cap);
        let mut success = false;
        // max search is increased here by a lot for this search. The idea is that we just have to find an empty bucket somewhere.
        // We don't mind waiting on a new write (by searching longer). Writing is done in the background only.
        // Wasting space by doubling the bucket size is worse behavior. We expect more
        // updates and fewer inserts, so we optimize for more compact data.
        // We can accomplish this by increasing how many locations we're willing to search for an empty data cell.
        // For the index bucket, it is more like a hash table and we have to exhaustively search 'max_search' to prove an item does not exist.
        // And we do have to support the 'does not exist' case with good performance. So, it makes sense to grow the index bucket when it is too large.
        // For data buckets, the offset is stored in the index, so it is directly looked up. So, the only search is on INSERT or update to a new sized value.
        for i in pos..pos + (max_search * 10).min(cap) {
            let ix = i % cap;
            if best_bucket.is_free(ix) {
                let mut multiple_slots = MultipleSlots::default();
                multiple_slots.set_storage_offset(ix);
                multiple_slots
                    .set_storage_capacity_when_created_pow2(best_bucket.contents.capacity_pow2());
                multiple_slots.set_num_slots(num_slots);

                //debug!(                        "DATA ALLOC {:?} {} {} {}",                        key, elem.data_location, best_bucket.capacity, elem_uid                    );
                let best_bucket = &mut self.data[best_fit_bucket as usize];
                best_bucket.occupy(ix, false).unwrap();
                if num_slots > 0 {
                    // copy slotlist into the data bucket
                    let slice = best_bucket.get_slice_mut(ix, num_slots, IncludeHeader::NoHeader);
                    slice.iter_mut().zip(data).for_each(|(dest, src)| {
                        *dest = *src;
                    });
                }

                // update index bucket after data bucket has been updated.
                elem.unwrap_or_else(|| insert_key(&mut self.index))
                    .set_slot_count_enum_value(
                        &mut self.index,
                        OccupiedEnum::MultipleSlots(&multiple_slots),
                    );
                self.stats
                    .index
                    .index_uses_uncommon_slot_list_len
                    .store(true, Ordering::Relaxed);
                success = true;
                break;
            }
        }
        if !success {
            return Err(BucketMapError::DataNoSpace((best_fit_bucket, cap_power)));
        }
        if let Some(DataFileEntryToFree {
            bucket_ix,
            location,
        }) = old_data_entry_to_free
        {
            // free the entry in the data bucket the data was previously stored in
            self.data[bucket_ix].free(location);
        }
        Ok(())
    }

    pub fn delete_key(&mut self, key: &Pubkey) {
        if let Some((elem, elem_ix)) = self.find_index_entry(key) {
            self.at_least_one_entry_deleted = true;
            if let OccupiedEnum::MultipleSlots(multiple_slots) =
                elem.get_slot_count_enum(&self.index)
            {
                let ix = multiple_slots.data_bucket_ix() as usize;
                let data_bucket = &self.data[ix];
                let loc = multiple_slots.data_loc(data_bucket);
                let data_bucket = &mut self.data[ix];
                //debug!(                    "DATA FREE {:?} {} {} {}",                    key, elem.data_location, data_bucket.capacity, elem_uid                );
                data_bucket.free(loc);
            }
            //debug!("INDEX FREE {:?} {}", key, elem_uid);
            self.index.free(elem_ix);
            // move the rest of the run back one, so it stays contiguous and in order of home slots
            let capacity = self.index.capacity();
            let mut free = elem_ix;
            loop {
                let next = (free + 1) % capacity;
                if next == elem_ix || self.index.is_free(next) {
                    break;
                }
                let next_home = Self::home_ix(
                    IndexEntryPlaceInBucket::<T>::new(next).key(&self.index),
                    self.random,
                    capacity,
                );
                if next_home == next {
                    break;
                }
                Self::move_index_entry(&mut self.index, next, free);
                free = next;
            }
        }
    }

    pub(crate) fn set_anticipated_count(&mut self, count: u64) {
        self.anticipated_size = count;
    }

    pub fn grow_index(&self, mut current_capacity: u64) {
        if self.index.contents.capacity() == current_capacity
            && self.reallocated.try_reserve_reallocation()
        {
            // make sure to grow to at least % more than the anticipated size
            // The indexing algorithm expects to require some over-allocation.
            let anticipated_size = self.anticipated_size * 140 / 100;
            let mut m = Measure::start("grow_index");
            //debug!("GROW_INDEX: {}", current_capacity_pow2);
            let mut count = 0;
            loop {
                count += 1;
                // grow relative to the current capacity
                let new_capacity = (current_capacity * 110 / 100).max(anticipated_size);
                let (mut index, file_name) = BucketStorage::new_with_capacity(
                    Arc::clone(&self.drives),
                    1,
                    std::mem::size_of::<IndexEntry<T>>() as u64,
                    Capacity::Actual(new_capacity),
                    self.index.max_search,
                    Arc::clone(&self.stats.index),
                    Arc::clone(&self.index.count),
                );
                // index may have allocated something larger than we asked for,
                // so, in case we fail to reindex into this larger size, grow from this size next iteration.
                current_capacity = index.capacity();
                let mut valid = true;
                for ix in 0..self.index.capacity() {
                    if !self.index.is_free(ix) {
                        let elem: &IndexEntry<T> = self.index.get(ix);
                        let new_ix =
                            Self::bucket_create_key(&mut index, &elem.key, self.random, true);
                        if new_ix.is_err() {
                            valid = false;
                            break;
                        }
                        let new_ix = new_ix.unwrap();
                        let new_elem: &mut IndexEntry<T> = index.get_mut(new_ix);
                        *new_elem = *elem;
                        index.copying_entry(new_ix, &self.index, ix);
                    }
                }
                if valid {
                    self.stats.index.update_max_size(index.capacity());
                    let mut items = self.reallocated.items.lock().unwrap();
                    items.index = Some(index);
                    self.restartable_bucket.set_file(file_name, self.random);
                    break;
                }
            }
            m.stop();
            if count > 1 {
                self.stats
                    .index
                    .failed_resizes
                    .fetch_add(count - 1, Ordering::Relaxed);
            }
            self.stats.index.resizes.fetch_add(1, Ordering::Relaxed);
            self.stats
                .index
                .resize_us
                .fetch_add(m.as_us(), Ordering::Relaxed);
        }
    }

    pub fn apply_grow_index(&mut self, mut index: BucketStorage<IndexBucket<T>>) {
        self.stats
            .index
            .resize_grow(self.index.capacity_bytes(), index.capacity_bytes());

        if self.restartable_bucket.restart.is_some() {
            // we are keeping track of which files we use for restart.
            // And we are resizing.
            // So, delete the old file and set the new file to NOT delete.
            // This way the new file will still be around on startup.
            // We are completely done with the old file.
            self.index.delete_file_on_drop = true;
            index.delete_file_on_drop = false;
        }
        self.index = index;
    }

    fn elem_size() -> u64 {
        std::mem::size_of::<T>() as u64
    }

    fn add_data_bucket(&mut self, bucket: BucketStorage<DataBucket>) {
        self.stats.data.file_count.fetch_add(1, Ordering::Relaxed);
        self.stats.data.resize_grow(0, bucket.capacity_bytes());
        self.data.push(bucket);
    }

    pub fn apply_grow_data(&mut self, ix: usize, bucket: BucketStorage<DataBucket>) {
        if self.data.get(ix).is_none() {
            for i in self.data.len()..ix {
                // insert empty data buckets
                self.add_data_bucket(
                    BucketStorage::new(
                        Arc::clone(&self.drives),
                        1 << i,
                        Self::elem_size(),
                        self.index.max_search,
                        Arc::clone(&self.stats.data),
                        Arc::default(),
                    )
                    .0,
                );
            }
            self.add_data_bucket(bucket);
        } else {
            let data_bucket = &mut self.data[ix];
            self.stats
                .data
                .resize_grow(data_bucket.capacity_bytes(), bucket.capacity_bytes());
            self.data[ix] = bucket;
        }
    }

    /// grow a data bucket
    /// The application of the new bucket is deferred until the next write lock.
    pub fn grow_data(&self, data_index: u64, current_capacity_pow2: u8) {
        if self
            .data
            .get(data_index as usize)
            .is_some_and(|bucket| bucket.contents.capacity_pow2() != current_capacity_pow2)
            || !self.reallocated.try_reserve_reallocation()
        {
            return;
        }
        let (new_bucket, _file_name) = BucketStorage::new_resized(
            &self.drives,
            self.index.max_search,
            self.data.get(data_index as usize),
            Capacity::Pow2(std::cmp::max(
                current_capacity_pow2 + 1,
                DEFAULT_CAPACITY_POW2,
            )),
            1 << data_index,
            Self::elem_size(),
            &self.stats.data,
        );
        let mut items = self.reallocated.items.lock().unwrap();
        items.data = Some((data_index, new_bucket));
    }

    fn bucket_index_ix(key: &Pubkey, random: u64) -> u64 {
        // the locally generated random will make it hard for an attacker
        // to deterministically cause all the pubkeys to land in the same
        // location in any bucket on all validators
        let hasher_builder = ahash::RandomState::with_seeds(random, random, random, random);
        hasher_builder.hash_one(key)
    }

    /// grow the appropriate piece. Note this takes an immutable ref.
    /// The actual grow is set into self.reallocated and applied later on a write lock
    pub(crate) fn grow(&self, err: BucketMapError) {
        match err {
            BucketMapError::DataNoSpace((data_index, current_capacity_pow2)) => {
                //debug!("GROWING SPACE {:?}", (data_index, current_capacity_pow2));
                self.grow_data(data_index, current_capacity_pow2);
            }
            BucketMapError::IndexNoSpace(current_capacity) => {
                //debug!("GROWING INDEX {}", sz);
                self.grow_index(current_capacity);
            }
        }
    }

    /// if a bucket was resized previously with a read lock, then apply that resize now
    pub fn handle_delayed_grows(&mut self) {
        if self.reallocated.get_reallocated() {
            // swap out the bucket that was resized previously with a read lock
            let mut items = std::mem::take(&mut *self.reallocated.items.lock().unwrap());

            if let Some(bucket) = items.index.take() {
                self.apply_grow_index(bucket);
            } else {
                // data bucket
                let (i, new_bucket) = items.data.take().unwrap();
                self.apply_grow_data(i as usize, new_bucket);
            }
        }
    }

    pub fn insert(&mut self, key: &Pubkey, value: &[T]) {
        loop {
            let Err(err) = self.try_write(key, value.iter(), value.len()) else {
                return;
            };
            self.grow(err);
            self.handle_delayed_grows();
        }
    }

    pub fn update<F>(&mut self, key: &Pubkey, updatefn: F)
    where
        F: FnOnce(Option<&[T]>) -> Option<Vec<T>>,
    {
        let current = self.read_value(key);
        let new = updatefn(current);
        if current == new.as_deref() {
            // unchanged, so skip the write
            return;
        }
        if new.is_none() {
            self.delete_key(key);
            return;
        }
        let new = new.unwrap();
        self.insert(key, &new);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, tempfile::tempdir};

    #[test]
    fn test_index_entries() {
        for v in 10..12u64 {
            for random in 1..3 {
                for len in 1..3 {
                    let raw = (0..len)
                        .map(|l| {
                            let k = Pubkey::from([l as u8; 32]);
                            (k, v + (l as u64))
                        })
                        .collect::<Vec<_>>();
                    let hashed = Bucket::index_entries(&raw, random);
                    assert_eq!(hashed.len(), len);
                    (0..len).for_each(|i| {
                        let raw = raw[i];
                        let hashed = hashed[i];
                        assert_eq!(Bucket::<u64>::bucket_index_ix(&raw.0, random), hashed.0);
                        assert_eq!(i, hashed.1);
                    });
                }
            }
        }
    }

    fn create_test_index(max_search: Option<u8>) -> BucketStorage<IndexBucket<u64>> {
        let tmpdir = tempdir().unwrap();
        let paths: Vec<PathBuf> = vec![tmpdir.path().to_path_buf()];
        assert!(!paths.is_empty());
        let max_search = max_search.unwrap_or(2);
        BucketStorage::<IndexBucket<u64>>::new(
            Arc::new(paths),
            1,
            std::mem::size_of::<crate::index_entry::IndexEntry<u64>>() as u64,
            max_search,
            Arc::default(),
            Arc::default(),
        )
        .0
    }

    #[test]
    fn batch_insert_duplicates_internal_simple() {
        // add the same duplicate key several times.
        // make sure the resulting index and returned `duplicates` is correct.
        let random = 1;
        let data_buckets = Vec::default();
        let k = Pubkey::from([1u8; 32]);
        for v in 10..12u64 {
            for len in 1..4 {
                let raw = (0..len).map(|l| (k, v + (l as u64))).collect::<Vec<_>>();
                let mut hashed = Bucket::index_entries(&raw, random);
                let hashed_raw = hashed.clone();

                let mut index = create_test_index(None);

                let mut entries_created = 0;
                let mut duplicates = Vec::default();
                assert!(
                    Bucket::<u64>::batch_insert_non_duplicates_internal(
                        &mut index,
                        &Vec::default(),
                        &raw,
                        &mut hashed,
                        &mut entries_created,
                        &mut duplicates,
                        random,
                    )
                    .is_ok()
                );

                assert_eq!(duplicates.len(), len as usize - 1);
                assert_eq!(hashed.len(), 0);
                let single_hashed_raw_inserted = hashed_raw.last().unwrap();
                let elem =
                    IndexEntryPlaceInBucket::new(single_hashed_raw_inserted.0 % index.capacity());
                let value = elem.read_value(&index, &data_buckets);
                assert_eq!(value, &[raw[single_hashed_raw_inserted.1].1]);
                let expected_duplicates = hashed_raw
                    .iter()
                    .rev()
                    .skip(1)
                    .map(|(_hash, i)| (*i, raw[single_hashed_raw_inserted.1].1))
                    .collect::<Vec<_>>();
                assert_eq!(expected_duplicates, duplicates);
            }
        }
    }

    #[test]
    fn batch_insert_non_duplicates_internal_simple() {
        // add 2 entries, make sure they are added in the buckets we expect
        let random = 1;
        let data_buckets = Vec::default();
        for v in 10..12u64 {
            for len in 1..3 {
                let raw = (0..len)
                    .map(|l| {
                        let k = Pubkey::from([l as u8; 32]);
                        (k, v + (l as u64))
                    })
                    .collect::<Vec<_>>();
                let mut hashed = Bucket::index_entries(&raw, random);
                let hashed_raw = hashed.clone();

                let mut index = create_test_index(None);

                let mut duplicates = Vec::default();
                let mut entries_created = 0;
                assert!(
                    Bucket::<u64>::batch_insert_non_duplicates_internal(
                        &mut index,
                        &Vec::default(),
                        &raw,
                        &mut hashed,
                        &mut entries_created,
                        &mut duplicates,
                        random,
                    )
                    .is_ok()
                );

                assert_eq!(hashed.len(), 0);
                (0..len).for_each(|i| {
                    let raw2 = hashed_raw[i];
                    let elem = IndexEntryPlaceInBucket::new(raw2.0 % index.capacity());
                    let value = elem.read_value(&index, &data_buckets);
                    assert_eq!(value, &[raw[hashed_raw[i].1].1]);
                });
            }
        }
    }

    #[should_panic(expected = "batch insertion can only occur prior to any deletes")]
    #[test]
    fn batch_insert_after_delete() {
        let tmpdir = tempdir().unwrap();
        let paths: Vec<PathBuf> = vec![tmpdir.path().to_path_buf()];
        assert!(!paths.is_empty());
        let max_search = 2;
        let mut bucket = Bucket::new(
            Arc::new(paths),
            max_search,
            Arc::default(),
            Arc::default(),
            RestartableBucket::default(),
        );

        let key = Pubkey::new_unique();
        assert_eq!(bucket.read_value(&key), None);

        bucket.update(&key, |_| Some(vec![0]));
        bucket.delete_key(&key);

        bucket.batch_insert_non_duplicates(&[]);
    }

    /// Ensure bucket_index_ix() is deterministic for a given seed.
    ///
    /// `ahash` selects an implementation based on target features, so the exact
    /// hash value is not portable across build configurations.
    #[test]
    fn test_bucket_index_ix_is_deterministic() {
        const PUBKEY: Pubkey = Pubkey::new_from_array([0xC3; 32]);
        const RANDOM1: u64 = 0x18E7_9D0B_94D8_E428;
        const RANDOM2: u64 = 0x60AE_DA87_48E9_A887;

        let ix1 = Bucket::<()>::bucket_index_ix(&PUBKEY, RANDOM1);
        assert_eq!(ix1, Bucket::<()>::bucket_index_ix(&PUBKEY, RANDOM1));

        let ix2 = Bucket::<()>::bucket_index_ix(&PUBKEY, RANDOM2);
        assert_eq!(ix2, Bucket::<()>::bucket_index_ix(&PUBKEY, RANDOM2));
        assert_ne!(ix2, ix1);
    }

    fn new_test_bucket(tmpdir: &tempfile::TempDir, max_search: MaxSearch) -> Bucket<u64> {
        Bucket::new(
            Arc::new(vec![tmpdir.path().to_path_buf()]),
            max_search,
            Arc::default(),
            Arc::default(),
            RestartableBucket::default(),
        )
    }

    /// Asserts every occupied entry is within `max_search` of its home, and that each run of
    /// occupied entries is in order of the entries' home slots
    fn assert_robin_hood_order(bucket: &Bucket<u64>) {
        let index = &bucket.index;
        let capacity = index.capacity();
        let distance = |ix: u64| {
            let key = IndexEntryPlaceInBucket::<u64>::new(ix).key(index);
            let home = Bucket::<u64>::home_ix(key, bucket.random, capacity);
            Bucket::<u64>::distance_from_home(home, ix, capacity)
        };
        for ix in 0..capacity {
            if index.is_free(ix) {
                continue;
            }
            let distance_here = distance(ix);
            assert!(distance_here < index.max_search(), "{ix}: {distance_here}");
            if distance_here > 0 {
                let prev = (ix + capacity - 1) % capacity;
                assert!(!index.is_free(prev), "hole before {ix}");
                assert!(distance(prev) + 1 >= distance_here, "{ix} is out of order");
            }
        }
    }

    #[test]
    fn test_robin_hood_matches_hashmap() {
        let tmpdir = tempdir().unwrap();
        let mut bucket = new_test_bucket(&tmpdir, 8);
        let mut expected = std::collections::HashMap::<Pubkey, Vec<u64>>::default();
        let mut rng = rand::rng();
        let keys: Vec<_> = (0..2000).map(|_| Pubkey::new_unique()).collect();
        for round in 0..20_000u64 {
            let key = keys[rng.random_range(0..keys.len())];
            match rng.random_range(0..10) {
                // delete
                0..3 => {
                    bucket.delete_key(&key);
                    expected.remove(&key);
                }
                // insert or update, sometimes with a multi-slot value that lives in a data bucket
                _ => {
                    let len = if rng.random_range(0..4) == 0 { 3 } else { 1 };
                    let value: Vec<_> = (0..len).map(|i| round * 10 + i).collect();
                    bucket.insert(&key, &value);
                    expected.insert(key, value);
                }
            }
            if round % 1000 == 0 {
                assert_robin_hood_order(&bucket);
            }
        }
        assert_robin_hood_order(&bucket);
        for key in &keys {
            assert_eq!(
                bucket.read_value(key),
                expected.get(key).map(Vec::as_slice),
                "{key}"
            );
        }
        assert_eq!(
            bucket.index.count.load(Ordering::Relaxed),
            expected.len() as u64
        );
    }

    #[test]
    fn test_robin_hood_fill() {
        let tmpdir = tempdir().unwrap();
        let mut bucket = new_test_bucket(&tmpdir, 32);
        let num_keys = 200_000;
        for i in 0..num_keys {
            bucket.insert(&Pubkey::new_unique(), &[i]);
        }
        assert_robin_hood_order(&bucket);
        let fill = num_keys as f64 / bucket.index.capacity() as f64;
        // linear probing within a 32 entry window grows at about half full
        assert!(fill > 0.7, "fill: {fill}");
    }
}
