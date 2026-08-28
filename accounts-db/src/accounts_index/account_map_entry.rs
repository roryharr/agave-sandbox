use {
    super::{
        DiskIndexValue, IndexValue, SlotList, SlotListItem,
        bucket_map_holder::{Age, BucketMapHolder},
    },
    crate::{account_info::AccountInfo, is_zero_lamport::IsZeroLamport},
    portable_atomic::AtomicU128,
    solana_clock::Slot,
    std::{fmt::Debug, marker::PhantomData, sync::atomic::Ordering},
};

/// one entry in the in-mem accounts index
/// Represents the value for an account key in the in-memory accounts index
///
/// The index holds a single `(Slot, T)` per pubkey, packed into one 128 bit cell together with
/// the `dirty` and `age` metadata, so updates are a compare-and-exchange rather than a lock.
#[derive(Debug)]
pub struct AccountMapEntry<T> {
    entry: AtomicU128,
    _phantom: PhantomData<T>,
}

/// bit layout of `AccountMapEntry::entry`
const VALUE_BITS: u32 = 64;
const SLOT_BITS: u32 = 40;
const SLOT_SHIFT: u32 = VALUE_BITS;
const SLOT_MASK: u128 = (1 << SLOT_BITS) - 1;
const DIRTY_SHIFT: u32 = SLOT_SHIFT + SLOT_BITS;
const AGE_SHIFT: u32 = DIRTY_SHIFT + 1;
const AGE_MASK: u128 = u8::MAX as u128;

// Ensure the size of AccountMapEntry never changes unexpectedly
const _: () = assert!(size_of::<AccountMapEntry<AccountInfo>>() == 16);

impl<T: IndexValue> AccountMapEntry<T> {
    pub fn new(slot_list: SlotList<T>, meta: AccountMapEntryMeta) -> Self {
        let (slot, account_info) = slot_list[0];
        Self {
            entry: AtomicU128::new(Self::pack(slot, account_info, meta.dirty, meta.age)),
            _phantom: PhantomData,
        }
    }

    fn pack(slot: Slot, account_info: T, dirty: bool, age: Age) -> u128 {
        assert!(
            slot <= SLOT_MASK as Slot,
            "slot {slot} does not fit in the index entry"
        );
        u128::from(account_info.to_bits())
            | ((slot as u128 & SLOT_MASK) << SLOT_SHIFT)
            | ((dirty as u128) << DIRTY_SHIFT)
            | ((age as u128) << AGE_SHIFT)
    }

    fn unpack_entry(packed: u128) -> SlotListItem<T> {
        let slot = ((packed >> SLOT_SHIFT) & SLOT_MASK) as Slot;
        (slot, T::from_bits(packed as u64))
    }

    fn load(&self) -> u128 {
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

    fn age_of(packed: u128) -> Age {
        ((packed >> AGE_SHIFT) & AGE_MASK) as Age
    }

    /// set the bit at `shift` to `value`, returning the previous packed cell
    fn set_bit(&self, shift: u32, value: bool) -> u128 {
        if value {
            self.entry.fetch_or(1 << shift, Ordering::AcqRel)
        } else {
            self.entry.fetch_and(!(1u128 << shift), Ordering::AcqRel)
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
        let next = (current & !(AGE_MASK << AGE_SHIFT)) | ((value as u128) << AGE_SHIFT);
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
        let next = (current & !(AGE_MASK << AGE_SHIFT)) | ((next_age as u128) << AGE_SHIFT);
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
    Entry(Box<AccountMapEntry<T>>),
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
    ) -> Box<AccountMapEntry<T>> {
        let meta = AccountMapEntryMeta::new_dirty(storage, false);
        Box::new(AccountMapEntry::new(
            SlotList::from([(slot, account_info)]),
            meta,
        ))
    }

    pub fn into_account_map_entry<U: DiskIndexValue + From<T> + Into<T>>(
        self,
        storage: &BucketMapHolder<T, U>,
    ) -> Box<AccountMapEntry<T>> {
        match self {
            Self::Entry(entry) => entry,
            Self::Raw((slot, account_info)) => Self::allocate(slot, account_info, storage),
        }
    }
}
