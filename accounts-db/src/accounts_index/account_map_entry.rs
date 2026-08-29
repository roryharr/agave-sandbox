use {
    super::{IndexValue, SlotList, SlotListItem},
    crate::account_info::AccountInfo,
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
/// The index holds a single `(Slot, T)` per pubkey, packed into one 64 bit cell,
/// so updates are a compare-and-exchange rather than a lock.
#[derive(Debug)]
pub struct AccountMapEntry<T> {
    entry: AtomicU64,
    _phantom: PhantomData<T>,
}

/// bit layout of `AccountMapEntry::entry`: the account info and the slot it is stored at
const VALUE_BITS: u32 = 26;
const VALUE_MASK: u64 = (1 << VALUE_BITS) - 1;
const SLOT_BITS: u32 = 30;
const SLOT_SHIFT: u32 = VALUE_BITS;
const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;
/// 2^30 slots, ~13 years at 400ms per slot
pub const MAX_INDEXED_SLOT: Slot = SLOT_MASK;

// Ensure the size of AccountMapEntry never changes unexpectedly
const _: () = assert!(size_of::<AccountMapEntry<AccountInfo>>() == 8);

impl<T: IndexValue> AccountMapEntry<T> {
    pub fn new(slot_list: SlotList<T>) -> Self {
        let (slot, account_info) = slot_list[0];
        Self {
            entry: AtomicU64::new(Self::pack(slot, account_info)),
            _phantom: PhantomData,
        }
    }

    fn pack(slot: Slot, account_info: T) -> u64 {
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
        (value & VALUE_MASK) | ((slot & SLOT_MASK) << SLOT_SHIFT)
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
        let previous = self
            .entry
            .swap(Self::pack(item.0, item.1), Ordering::AcqRel);
        Self::unpack_entry(previous)
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
        let next = Self::pack(item.0, item.1);
        let mut current = self.load();
        loop {
            let (current_slot, current_account_info) = Self::unpack_entry(current);
            if current_slot > item.0 && current_slot != old_slot {
                return item;
            }
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
}
