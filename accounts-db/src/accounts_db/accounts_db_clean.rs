//! AccountsDb clean pipeline extraction - part 1: candidate construction & dirty tracking.

use {
    crate::{
        account_info::AccountInfo,
        accounts_db::AccountsDb,
        accounts_index::{RefCount, SlotList},
        is_zero_lamport::IsZeroLamport,
    },
    log::trace,
    rayon::prelude::*,
    solana_clock::Slot,
    solana_epoch_schedule::EpochSchedule,
    solana_measure::measure::Measure,
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            RwLock,
        },
    },
};

#[derive(Default)]
pub(crate) struct CleanKeyTimings {
    pub(crate) collect_delta_keys_us: u64,
    pub(crate) delta_insert_us: u64,
    pub(crate) dirty_store_processing_us: u64,
    pub(crate) delta_key_count: u64,
    pub(crate) dirty_pubkeys_count: u64,
    pub(crate) oldest_dirty_slot: Slot,
    /// number of ancient append vecs that were scanned because they were dirty when clean started
    pub(crate) dirty_ancient_stores: usize,
}

#[derive(Default, Debug)]
pub(crate) struct CleaningInfo {
    pub(crate) slot_list: SlotList<AccountInfo>,
    pub(crate) ref_count: RefCount,
    /// Indicates if this account might have a zero lamport index entry.
    pub(crate) might_contain_zero_lamport_entry: bool,
}

/// (candidates_by_bin, min_dirty_slot)
pub(crate) type CleaningCandidates = (Box<[RwLock<HashMap<Pubkey, CleaningInfo>>]>, Option<Slot>);

/// Collect all the uncleaned slots, up to a max slot
///
/// Search through the uncleaned Pubkeys and return all the slots, up to a maximum slot.
pub(crate) fn collect_uncleaned_slots_up_to_slot(
    db: &AccountsDb,
    max_slot_inclusive: Slot,
) -> Vec<Slot> {
    db.uncleaned_pubkeys
        .iter()
        .filter_map(|entry| {
            let slot = *entry.key();
            (slot <= max_slot_inclusive).then_some(slot)
        })
        .collect()
}

/// For each slot in the list of uncleaned slots, up to a maximum
/// slot, remove it from the `uncleaned_pubkeys` and move all the
/// pubkeys to `candidates` for cleaning.
pub(crate) fn remove_uncleaned_slots_up_to_slot_and_move_pubkeys(
    db: &AccountsDb,
    max_slot_inclusive: Slot,
    candidates: &[RwLock<HashMap<Pubkey, CleaningInfo>>],
) {
    let uncleaned_slots = collect_uncleaned_slots_up_to_slot(db, max_slot_inclusive);
    for uncleaned_slot in uncleaned_slots.into_iter() {
        if let Some((_removed_slot, mut removed_pubkeys)) =
            db.uncleaned_pubkeys.remove(&uncleaned_slot)
        {
            // Sort all keys by bin index so that we can insert
            // them in `candidates` more efficiently.
            removed_pubkeys.sort_by(|a, b| {
                db.accounts_index
                    .bin_calculator
                    .bin_from_pubkey(a)
                    .cmp(&db.accounts_index.bin_calculator.bin_from_pubkey(b))
            });

            if let Some(first_removed_pubkey) = removed_pubkeys.first() {
                let mut prev_bin = db
                    .accounts_index
                    .bin_calculator
                    .bin_from_pubkey(first_removed_pubkey);
                let mut candidates_bin = candidates[prev_bin].write().unwrap();
                for removed_pubkey in removed_pubkeys {
                    let bin = db
                        .accounts_index
                        .bin_calculator
                        .bin_from_pubkey(&removed_pubkey);
                    if bin != prev_bin {
                        prev_bin = bin;
                        candidates_bin = candidates[prev_bin].write().unwrap();
                    }
                    // Conservatively mark the candidate might have a zero lamport entry for
                    // correctness so that scan WILL try to look in disk if it is
                    // not in-mem. These keys are from 1) recently processed
                    // slots, 2) zero lamports found in shrink. Therefore, they are very likely
                    // to be in-memory, and seldomly do we need to look them up in disk.
                    candidates_bin.insert(
                        removed_pubkey,
                        CleaningInfo {
                            might_contain_zero_lamport_entry: true,
                            ..Default::default()
                        },
                    );
                }
            }
        }
    }
}

pub(crate) fn count_pubkeys(candidates: &[RwLock<HashMap<Pubkey, CleaningInfo>>]) -> u64 {
    candidates
        .iter()
        .map(|x| x.read().unwrap().len())
        .sum::<usize>() as u64
}

/// Construct a list of candidates for cleaning from:
/// - dirty_stores      -- set of stores which had accounts removed or recently rooted
/// - uncleaned_pubkeys -- the delta set of updated pubkeys in rooted slots from the last clean
///
/// The function also returns the minimum slot we encountered.
pub(crate) fn construct_candidate_clean_keys(
    db: &AccountsDb,
    max_clean_root_inclusive: Option<Slot>,
    is_startup: bool,
    timings: &mut CleanKeyTimings,
    epoch_schedule: &EpochSchedule,
) -> CleaningCandidates {
    let oldest_non_ancient_slot = db.get_oldest_non_ancient_slot(epoch_schedule);
    let mut dirty_store_processing_time = Measure::start("dirty_store_processing");
    let max_root_inclusive = db.accounts_index.max_root_inclusive();
    let max_slot_inclusive = max_clean_root_inclusive.unwrap_or(max_root_inclusive);
    // find the oldest dirty slot
    // we'll add logging if that append vec cannot be marked dead
    let mut dirty_stores = Vec::with_capacity(db.dirty_stores.len());
    let mut min_dirty_slot = None::<u64>;
    db.dirty_stores.retain(|slot, store| {
        if *slot > max_slot_inclusive {
            true
        } else {
            min_dirty_slot = min_dirty_slot.map(|min| min.min(*slot)).or(Some(*slot));
            dirty_stores.push((*slot, store.clone()));
            false
        }
    });
    let dirty_stores_len = dirty_stores.len();

    let num_bins = db.accounts_index.bins();
    let candidates: Box<_> =
        std::iter::repeat_with(|| RwLock::new(HashMap::<Pubkey, CleaningInfo>::new()))
            .take(num_bins)
            .collect();

    let insert_candidate = |pubkey: Pubkey, is_zero_lamport: bool| {
        let index = db.accounts_index.bin_calculator.bin_from_pubkey(&pubkey);
        let mut candidates_bin = candidates[index].write().unwrap();
        candidates_bin
            .entry(pubkey)
            .or_default()
            .might_contain_zero_lamport_entry |= is_zero_lamport;
    };

    let dirty_ancient_stores = AtomicUsize::default();
    let mut dirty_store_routine = || {
        let chunk_size = 1.max(dirty_stores_len.saturating_div(rayon::current_num_threads()));
        let oldest_dirty_slots: Vec<u64> = dirty_stores
            .par_chunks(chunk_size)
            .map(|dirty_store_chunk| {
                let mut oldest_dirty_slot = max_slot_inclusive.saturating_add(1);
                dirty_store_chunk.iter().for_each(|(slot, store)| {
                    if *slot < oldest_non_ancient_slot {
                        dirty_ancient_stores.fetch_add(1, Ordering::Relaxed);
                    }
                    oldest_dirty_slot = oldest_dirty_slot.min(*slot);

                    store
                        .accounts
                        .scan_accounts_without_data(|_offset, account| {
                            let pubkey = *account.pubkey();
                            let is_zero_lamport = account.is_zero_lamport();
                            insert_candidate(pubkey, is_zero_lamport);
                        })
                        .expect("must scan accounts storage");
                });
                oldest_dirty_slot
            })
            .collect();
        timings.oldest_dirty_slot = *oldest_dirty_slots
            .iter()
            .min()
            .unwrap_or(&max_slot_inclusive.saturating_add(1));
    };

    if is_startup {
        // Free to consume all the cores during startup
        dirty_store_routine();
    } else {
        db.thread_pool_background.install(|| dirty_store_routine());
    }

    timings.dirty_pubkeys_count = count_pubkeys(&candidates);
    trace!(
        "dirty_stores.len: {} pubkeys.len: {}",
        dirty_stores_len,
        timings.dirty_pubkeys_count,
    );
    dirty_store_processing_time.stop();
    timings.dirty_store_processing_us += dirty_store_processing_time.as_us();
    timings.dirty_ancient_stores = dirty_ancient_stores.load(Ordering::Relaxed);

    let mut collect_delta_keys = Measure::start("key_create");
    remove_uncleaned_slots_up_to_slot_and_move_pubkeys(db, max_slot_inclusive, &candidates);
    collect_delta_keys.stop();
    timings.collect_delta_keys_us += collect_delta_keys.as_us();

    timings.delta_key_count = count_pubkeys(&candidates);

    // Check if we should purge any of the
    // zero_lamport_accounts_to_purge_later, based on the
    // latest_full_snapshot_slot.
    let latest_full_snapshot_slot = db.latest_full_snapshot_slot();
    assert!(
        latest_full_snapshot_slot.is_some()
            || db
                .zero_lamport_accounts_to_purge_after_full_snapshot
                .is_empty(),
        "if snapshots are disabled, then zero_lamport_accounts_to_purge_after_full_snapshot \
         should always be empty"
    );
    if let Some(latest_full_snapshot_slot) = latest_full_snapshot_slot {
        db.zero_lamport_accounts_to_purge_after_full_snapshot
            .retain(|(slot, pubkey)| {
                let is_candidate_for_clean =
                    max_slot_inclusive >= *slot && latest_full_snapshot_slot >= *slot;
                if is_candidate_for_clean {
                    insert_candidate(*pubkey, true);
                }
                !is_candidate_for_clean
            });
    }

    (candidates, min_dirty_slot)
}

#[cfg(test)]
use crate::{
    accounts_db::AccountSharedData,
    contains::Contains,
};
#[test]
fn test_collect_uncleaned_slots_up_to_slot() {
    agave_logger::setup();
    let db = AccountsDb::new_single_for_tests();

    let slot1 = 11;
    let slot2 = 222;
    let slot3 = 3333;

    let pubkey1 = Pubkey::new_unique();
    let pubkey2 = Pubkey::new_unique();
    let pubkey3 = Pubkey::new_unique();

    db.uncleaned_pubkeys.insert(slot1, vec![pubkey1]);
    db.uncleaned_pubkeys.insert(slot2, vec![pubkey2]);
    db.uncleaned_pubkeys.insert(slot3, vec![pubkey3]);

    let mut uncleaned_slots1 = collect_uncleaned_slots_up_to_slot(&db, slot1);
    let mut uncleaned_slots2 = collect_uncleaned_slots_up_to_slot(&db, slot2);
    let mut uncleaned_slots3 = collect_uncleaned_slots_up_to_slot(&db, slot3);

    uncleaned_slots1.sort_unstable();
    uncleaned_slots2.sort_unstable();
    uncleaned_slots3.sort_unstable();

    assert_eq!(uncleaned_slots1, [slot1]);
    assert_eq!(uncleaned_slots2, [slot1, slot2]);
    assert_eq!(uncleaned_slots3, [slot1, slot2, slot3]);
}

#[test]
fn test_remove_uncleaned_slots_and_collect_pubkeys_up_to_slot() {
    agave_logger::setup();
    let db = AccountsDb::new_single_for_tests();

    let slot1 = 11;
    let slot2 = 222;
    let slot3 = 3333;

    let pubkey1 = Pubkey::new_unique();
    let pubkey2 = Pubkey::new_unique();
    let pubkey3 = Pubkey::new_unique();

    let account1 = AccountSharedData::new(0, 0, &pubkey1);
    let account2 = AccountSharedData::new(0, 0, &pubkey2);
    let account3 = AccountSharedData::new(0, 0, &pubkey3);

    db.store_for_tests((slot1, [(&pubkey1, &account1)].as_slice()));
    db.store_for_tests((slot2, [(&pubkey2, &account2)].as_slice()));
    db.store_for_tests((slot3, [(&pubkey3, &account3)].as_slice()));

    // slot 1 is _not_ a root on purpose
    db.add_root(slot2);
    db.add_root(slot3);

    db.uncleaned_pubkeys.insert(slot1, vec![pubkey1]);
    db.uncleaned_pubkeys.insert(slot2, vec![pubkey2]);
    db.uncleaned_pubkeys.insert(slot3, vec![pubkey3]);

    let num_bins = db.accounts_index.bins();
    let candidates: Box<_> =
        std::iter::repeat_with(|| RwLock::new(HashMap::<Pubkey, CleaningInfo>::new()))
            .take(num_bins)
            .collect();
    remove_uncleaned_slots_up_to_slot_and_move_pubkeys(&db, slot3, &candidates);

    let candidates_contain = |pubkey: &Pubkey| {
        candidates
            .iter()
            .any(|bin| bin.read().unwrap().contains(pubkey))
    };
    assert!(candidates_contain(&pubkey1));
    assert!(candidates_contain(&pubkey2));
    assert!(candidates_contain(&pubkey3));
}