use {
    super::{
        DiskIndexValue, IndexValue, SlotList, SlotListItem,
        bucket_map_holder::{AGE_MASK, Age, BucketMapHolder},
    },
    crate::{account_info::AccountInfo, is_zero_lamport::IsZeroLamport},
    solana_clock::Slot,
    std::{
        fmt::Debug,
        marker::PhantomData,
        sync::atomic::{AtomicU64, Ordering},
    },
};

/// one entry in the in-mem accounts index
/// Represents the value for an account key in the in-memory accounts index
///
/// The index holds a single `(Slot, T)` per pubkey, packed into one 128 bit cell together with
/// the `dirty` and `age` metadata, so updates are a compare-and-exchange rather than a lock.
#[derive(Debug)]
pub struct AccountMapEntry<T> {
    entry: AtomicU64,
    _phantom: PhantomData<T>,
}

/// bit layout of `AccountMapEntry::entry`: the account info, the slot it is stored at, and the
/// metadata the in-mem index keeps for it
const VALUE_BITS: u32 = 26;
const VALUE_MASK: u64 = (1 << VALUE_BITS) - 1;
const SLOT_BITS: u32 = 30;
const SLOT_SHIFT: u32 = VALUE_BITS;
const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
/// 2^30 slots, ~13 years at 400ms per slot
pub const MAX_INDEXED_SLOT: Slot = SLOT_MASK;
const DIRTY_SHIFT: u32 = SLOT_SHIFT + SLOT_BITS;
const AGE_SHIFT: u32 = DIRTY_SHIFT + 1;

// Ensure the size of AccountMapEntry never changes unexpectedly
const _: () = assert!(size_of::<AccountMapEntry<AccountInfo>>() == 8);

impl<T: IndexValue> AccountMapEntry<T> {
    pub fn new(slot_list: SlotList<T>, meta: AccountMapEntryMeta) -> Self {
        let (slot, account_info) = slot_list[0];
        Self {
            entry: AtomicU64::new(Self::pack(slot, account_info, meta.dirty, meta.age)),
            _phantom: PhantomData,
        }
    }

    fn pack(slot: Slot, account_info: T, dirty: bool, age: Age) -> u64 {
        assert!(
            slot <= MAX_INDEXED_SLOT,
            "slot {slot} does not fit in the index entry"
        );
        let value = account_info.to_bits();
        debug_assert_eq!(
            value & !VALUE_MASK,
            0,
            "account info does not fit the entry"
        );
        (value & VALUE_MASK)
            | ((slot & SLOT_MASK) << SLOT_SHIFT)
            | ((dirty as u64) << DIRTY_SHIFT)
            | ((age as u64) << AGE_SHIFT)
    }

    fn unpack_entry(packed: u64) -> SlotListItem<T> {
        let slot = (packed >> SLOT_SHIFT) & SLOT_MASK;
        (slot, T::from_bits(packed & VALUE_MASK))
    }

    fn load(&self) -> u64 {
        self.entry.load(Ordering::Acquire)
    }

    /// The single `(slot, account_info)` this pubkey is stored at
    pub fn entry(&self) -> SlotListItem<T> {
        Self::unpack_entry(self.load())
    }

    /// The index holds exactly one entry per pubkey
    pub fn slot_list(&self) -> SlotList<T> {
        [self.entry()]
    }

    /// Replace the entry, returning the entry that was there
    pub fn replace(&self, item: SlotListItem<T>) -> SlotListItem<T> {
        let mut current = self.load();
        loop {
            // replacing an entry always dirties it
            let next = Self::pack(item.0, item.1, true, Self::age_of(current));
            match self.entry.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Self::unpack_entry(current),
                Err(actual) => current = actual,
            }
        }
    }

    /// Replace the entry with `item` unless the entry held is from a newer slot, in which case
    /// `item` is the older duplicate. `other_slot` names an entry to replace regardless of
    /// ordering. Returns whichever version did not survive.
    ///
    /// The comparison is inside the compare-exchange loop: deciding it against a stale load
    /// could overwrite an entry newer than `item`.
    pub fn replace_if_newer(
        &self,
        item: SlotListItem<T>,
        other_slot: Option<Slot>,
    ) -> SlotListItem<T> {
        let old_slot = other_slot.unwrap_or(item.0);
        let mut current = self.load();
        loop {
            let (current_slot, current_account_info) = Self::unpack_entry(current);
            if current_slot > item.0 && current_slot != old_slot {
                return item;
            }
            let next = Self::pack(item.0, item.1, true, Self::age_of(current));
            match self.entry.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return (current_slot, current_account_info),
                Err(actual) => current = actual,
            }
        }
    }

    fn age_of(packed: u64) -> Age {
        ((packed >> AGE_SHIFT) as Age) & AGE_MASK
    }

    /// set the bit at `shift` to `value`, returning the previous packed cell
    fn set_bit(&self, shift: u32, value: bool) -> u64 {
        if value {
            self.entry.fetch_or(1 << shift, Ordering::AcqRel)
        } else {
            self.entry.fetch_and(!(1u64 << shift), Ordering::AcqRel)
        }
    }

    pub fn dirty(&self) -> bool {
        (self.load() >> DIRTY_SHIFT) & 1 == 1
    }

    pub fn mark_dirty(&self) {
        self.set_bit(DIRTY_SHIFT, true);
    }

    /// set dirty to false, return true if was dirty
    pub fn clear_dirty(&self) -> bool {
        (self.set_bit(DIRTY_SHIFT, false) >> DIRTY_SHIFT) & 1 == 1
    }

    pub fn age(&self) -> Age {
        Self::age_of(self.load())
    }

    /// Age is only a hint for eviction, so this is best effort: a racing update wins and the
    /// new age is dropped rather than retried.
    pub fn set_age(&self, value: Age) {
        let current = self.load();
        let next = (current & !((AGE_MASK as u64) << AGE_SHIFT)) | ((value as u64) << AGE_SHIFT);
        let _ = self
            .entry
            .compare_exchange(current, next, Ordering::AcqRel, Ordering::Relaxed);
    }

    /// set age to 'next_age' if 'self.age' is 'expected_age'
    pub fn try_exchange_age(&self, next_age: Age, expected_age: Age) {
        let current = self.load();
        if Self::age_of(current) != expected_age {
            return;
        }
        let next = (current & !((AGE_MASK as u64) << AGE_SHIFT)) | ((next_age as u64) << AGE_SHIFT);
        let _ = self
            .entry
            .compare_exchange(current, next, Ordering::AcqRel, Ordering::Relaxed);
    }
}

/// data per entry in in-mem accounts index
/// used to keep track of consistency with disk index
#[derive(Debug, Default)]
pub struct AccountMapEntryMeta {
    /// true if entry in in-mem idx has changes and needs to be written to disk
    dirty: bool,
    /// 'age' at which this entry should be purged from the cache (implements lru)
    age: Age,
}

impl AccountMapEntryMeta {
    pub fn new_dirty<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>>(
        storage: &BucketMapHolder<T, U>,
        is_cached: bool,
    ) -> Self {
        AccountMapEntryMeta {
            dirty: true,
            age: storage.future_age_to_flush(is_cached),
        }
    }
    pub fn new_clean<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>>(
        storage: &BucketMapHolder<T, U>,
    ) -> Self {
        AccountMapEntryMeta {
            dirty: false,
            age: storage.future_age_to_flush(false),
        }
    }
}

/// can be used to pre-allocate structures for insertion into accounts index outside of lock
pub enum PreAllocatedAccountMapEntry<T: IndexValue> {
    Entry(AccountMapEntry<T>),
    Raw(SlotListItem<T>),
}

impl<T: IndexValue> IsZeroLamport for PreAllocatedAccountMapEntry<T> {
    fn is_zero_lamport(&self) -> bool {
        match self {
            PreAllocatedAccountMapEntry::Entry(entry) => entry.slot_list()[0].1.is_zero_lamport(),
            PreAllocatedAccountMapEntry::Raw(raw) => raw.1.is_zero_lamport(),
        }
    }
}

impl<T: IndexValue> From<PreAllocatedAccountMapEntry<T>> for SlotListItem<T> {
    fn from(source: PreAllocatedAccountMapEntry<T>) -> SlotListItem<T> {
        match source {
            PreAllocatedAccountMapEntry::Entry(entry) => entry.slot_list()[0],
            PreAllocatedAccountMapEntry::Raw(raw) => raw,
        }
    }
}

impl<T: IndexValue> PreAllocatedAccountMapEntry<T> {
    /// create an entry that is equivalent to this process:
    /// 1. new empty (slot_list={})
    /// 2. update(slot, account_info)
    ///
    /// This code is called when the first entry [ie. (slot,account_info)] for a pubkey is inserted into the index.
    pub fn new<U: DiskIndexValue + From<T> + Into<T>>(
        slot: Slot,
        account_info: T,
        storage: &BucketMapHolder<T, U>,
        store_raw: bool,
    ) -> PreAllocatedAccountMapEntry<T> {
        if store_raw {
            Self::Raw((slot, account_info))
        } else {
            Self::Entry(Self::allocate(slot, account_info, storage))
        }
    }

    fn allocate<U: DiskIndexValue + From<T> + Into<T>>(
        slot: Slot,
        account_info: T,
        storage: &BucketMapHolder<T, U>,
    ) -> AccountMapEntry<T> {
        let meta = AccountMapEntryMeta::new_dirty(storage, false);
        AccountMapEntry::new(SlotList::from([(slot, account_info)]), meta)
    }

    pub fn into_account_map_entry<U: DiskIndexValue + From<T> + Into<T>>(
        self,
        storage: &BucketMapHolder<T, U>,
    ) -> AccountMapEntry<T> {
        match self {
            Self::Entry(entry) => entry,
            Self::Raw((slot, account_info)) => Self::allocate(slot, account_info, storage),
        }
    }
}
