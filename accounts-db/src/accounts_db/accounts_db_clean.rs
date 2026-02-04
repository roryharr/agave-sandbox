//! AccountsDb clean pipeline extraction
//!
//! Part 1: candidate construction & dirty tracking.
//! Part 2: reclaim gathering + delete-dependency analysis.

use {
    crate::{
        account_info::AccountInfo,
        accounts_db::{
            AccountsDb,
            PubkeysRemovedFromAccountsIndex, // type alias from accounts_db.rs
        },
        accounts_index::{RefCount, SlotList},
        is_zero_lamport::IsZeroLamport,
    },
    log::{debug, info, trace},
    rayon::prelude::*,
    solana_clock::Slot,
    solana_epoch_schedule::EpochSchedule,
    solana_measure::measure::Measure,
    solana_nohash_hasher::IntSet,
    solana_pubkey::Pubkey,
    std::{
        collections::{HashMap, HashSet},
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Mutex, RwLock,
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

/// While scanning cleaning candidates obtain slots that can be
/// reclaimed for each pubkey. In addition, if the pubkey is
/// removed from the index, insert in pubkeys_removed_from_accounts_index.
pub(crate) fn collect_reclaims(
    db: &AccountsDb,
    pubkey: &Pubkey,
    max_clean_root_inclusive: Option<Slot>,
    ancient_account_cleans: &AtomicU64,
    epoch_schedule: &EpochSchedule,
    pubkeys_removed_from_accounts_index: &Mutex<PubkeysRemovedFromAccountsIndex>,
) -> crate::accounts_index::ReclaimsSlotList<AccountInfo> {
    let one_epoch_old = db.get_oldest_non_ancient_slot(epoch_schedule);
    let mut clean_rooted = Measure::start("clean_old_root-ms");
    let mut reclaims = crate::accounts_index::ReclaimsSlotList::new();
    let removed_from_index =
        db.accounts_index
            .clean_rooted_entries(pubkey, &mut reclaims, max_clean_root_inclusive);
    if removed_from_index {
        pubkeys_removed_from_accounts_index
            .lock()
            .unwrap()
            .insert(*pubkey);
    }
    if !reclaims.is_empty() {
        // figure out how many ancient accounts have been reclaimed
        let old_reclaims = reclaims
            .iter()
            .filter_map(|(slot, _)| (slot < &one_epoch_old).then_some(1))
            .sum();
        ancient_account_cleans.fetch_add(old_reclaims, Ordering::Relaxed);
    }
    clean_rooted.stop();
    db.clean_accounts_stats
        .clean_old_root_us
        .fetch_add(clean_rooted.as_us(), Ordering::Relaxed);
    reclaims
}

/// increment store_counts to non-zero for all stores that can not be deleted.
/// a store cannot be deleted if:
/// 1. one of the pubkeys in the store has account info to a store whose store count is not going to zero
/// 2. a pubkey we were planning to remove is not removing all stores that contain the account
pub(crate) fn calc_delete_dependencies(
    db: &AccountsDb,
    candidates: &[HashMap<Pubkey, CleaningInfo>],
    store_counts: &mut HashMap<Slot, (usize, HashSet<Pubkey>)>,
    min_slot: Option<Slot>,
) {
    // Another pass to check if there are some filtered accounts which
    // do not match the criteria of deleting all appendvecs which contain them
    // then increment their storage count.
    let mut already_counted = IntSet::default();
    for (bin_index, bin) in candidates.iter().enumerate() {
        for (pubkey, cleaning_info) in bin.iter() {
            let slot_list = &cleaning_info.slot_list;
            let ref_count = &cleaning_info.ref_count;
            let mut failed_slot = None;
            let all_stores_being_deleted = slot_list.len() as RefCount == *ref_count;
            if all_stores_being_deleted {
                let mut delete = true;
                for (slot, _account_info) in slot_list {
                    if let Some(count) = store_counts.get(slot).map(|s| s.0) {
                        debug!("calc_delete_dependencies() slot: {slot}, count len: {count}");
                        if count == 0 {
                            // this store CAN be removed
                            continue;
                        }
                    }
                    // One of the pubkeys in the store has account info to a store whose store count is not going to zero.
                    // If the store cannot be found, that also means store isn't being deleted.
                    failed_slot = Some(*slot);
                    delete = false;
                    break;
                }
                if delete {
                    // this pubkey can be deleted from all stores it is in
                    continue;
                }
            } else {
                debug!(
                    "calc_delete_dependencies: pubkey {} ref_count {} slot_list.len {}",
                    pubkey,
                    ref_count,
                    slot_list.len()
                );
            }

            // increment store_counts to non-zero for all stores that can not be deleted.
            let mut pending_stores = IntSet::default();
            for (slot, _account_info) in slot_list {
                if !already_counted.contains(slot) {
                    pending_stores.insert(*slot);
                }
            }
            while !pending_stores.is_empty() {
                let slot = pending_stores.iter().next().cloned().unwrap();
                if Some(slot) == min_slot {
                    if let Some(failed_slot) = failed_slot.take() {
                        info!(
                            "calc_delete_dependencies, oldest slot is not able to be deleted \
                             because of {pubkey} in slot {failed_slot}"
                        );
                    } else {
                        info!(
                            "calc_delete_dependencies, oldest slot is not able to be deleted \
                             because of {pubkey}, slot list len: {}, ref count: {ref_count}",
                            slot_list.len()
                        );
                    }
                }

                pending_stores.remove(&slot);
                if !already_counted.insert(slot) {
                    continue;
                }
                // the point of all this code: remove the store count for all stores we cannot remove
                if let Some(store_count) = store_counts.remove(&slot) {
                    // all pubkeys in this store also cannot be removed from all stores they are in
                    let affected_pubkeys = &store_count.1;
                    for key in affected_pubkeys {
                        let candidates_bin_index =
                            db.accounts_index.bin_calculator.bin_from_pubkey(key);
                        let mut update_pending_stores = |bin: &HashMap<Pubkey, CleaningInfo>| {
                            for (slot, _account_info) in &bin.get(key).unwrap().slot_list {
                                if !already_counted.contains(slot) {
                                    pending_stores.insert(*slot);
                                }
                            }
                        };
                        if candidates_bin_index == bin_index {
                            update_pending_stores(bin);
                        } else {
                            update_pending_stores(&candidates[candidates_bin_index]);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
use crate::{
    account_info::StorageLocation,
    accounts_db::AccountSharedData,
    accounts_index::{AccountSecondaryIndexes, AccountsIndex, ReclaimsSlotList, UpsertReclaim},
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

#[test]
fn test_delete_dependencies() {
    agave_logger::setup();
    let accounts_index = AccountsIndex::<AccountInfo, AccountInfo>::default_for_tests();
    let key0 = Pubkey::new_from_array([0u8; 32]);
    let key1 = Pubkey::new_from_array([1u8; 32]);
    let key2 = Pubkey::new_from_array([2u8; 32]);
    let info0 = AccountInfo::new(StorageLocation::AppendVec(0, 0), true);
    let info1 = AccountInfo::new(StorageLocation::AppendVec(1, 0), true);
    let info2 = AccountInfo::new(StorageLocation::AppendVec(2, 0), true);
    let info3 = AccountInfo::new(StorageLocation::AppendVec(3, 0), true);
    let mut reclaims = ReclaimsSlotList::new();
    accounts_index.upsert(
        0,
        0,
        &key0,
        &AccountSharedData::default(),
        &AccountSecondaryIndexes::default(),
        info0,
        &mut reclaims,
        UpsertReclaim::IgnoreReclaims,
    );
    accounts_index.upsert(
        1,
        1,
        &key0,
        &AccountSharedData::default(),
        &AccountSecondaryIndexes::default(),
        info1,
        &mut reclaims,
        UpsertReclaim::IgnoreReclaims,
    );
    accounts_index.upsert(
        1,
        1,
        &key1,
        &AccountSharedData::default(),
        &AccountSecondaryIndexes::default(),
        info1,
        &mut reclaims,
        UpsertReclaim::IgnoreReclaims,
    );
    accounts_index.upsert(
        2,
        2,
        &key1,
        &AccountSharedData::default(),
        &AccountSecondaryIndexes::default(),
        info2,
        &mut reclaims,
        UpsertReclaim::IgnoreReclaims,
    );
    accounts_index.upsert(
        2,
        2,
        &key2,
        &AccountSharedData::default(),
        &AccountSecondaryIndexes::default(),
        info2,
        &mut reclaims,
        UpsertReclaim::IgnoreReclaims,
    );
    accounts_index.upsert(
        3,
        3,
        &key2,
        &AccountSharedData::default(),
        &AccountSecondaryIndexes::default(),
        info3,
        &mut reclaims,
        UpsertReclaim::IgnoreReclaims,
    );
    accounts_index.add_root(0);
    accounts_index.add_root(1);
    accounts_index.add_root(2);
    accounts_index.add_root(3);
    let num_bins = accounts_index.bins();
    let mut candidates: Box<_> = std::iter::repeat_with(HashMap::<Pubkey, CleaningInfo>::new)
        .take(num_bins)
        .collect();
    for key in [&key0, &key1, &key2] {
        let (rooted_entries, ref_count) = accounts_index.get_and_then(key, |entry| {
            let slot_list_lock = entry.unwrap().slot_list_read_lock();
            let rooted = accounts_index.get_rooted_entries(slot_list_lock.as_ref(), None);
            (false, (rooted, entry.unwrap().ref_count()))
        });
        let index = accounts_index.bin_calculator.bin_from_pubkey(key);
        let candidates_bin = &mut candidates[index];
        candidates_bin.insert(
            *key,
            CleaningInfo {
                slot_list: rooted_entries,
                ref_count,
                ..Default::default()
            },
        );
    }
    for candidates_bin in candidates.iter() {
        for (
            key,
            CleaningInfo {
                slot_list: list,
                ref_count,
                ..
            },
        ) in candidates_bin.iter()
        {
            info!(" purge {key} ref_count {ref_count} =>");
            for x in list {
                info!("  {x:?}");
            }
        }
    }

    let mut store_counts = HashMap::new();
    store_counts.insert(0, (0, HashSet::from_iter(vec![key0])));
    store_counts.insert(1, (0, HashSet::from_iter(vec![key0, key1])));
    store_counts.insert(2, (0, HashSet::from_iter(vec![key1, key2])));
    store_counts.insert(3, (1, HashSet::from_iter(vec![key2])));

    let db = AccountsDb::new_single_for_tests();
    calc_delete_dependencies(&db, &candidates, &mut store_counts, None);
    let mut stores: Vec<_> = store_counts.keys().cloned().collect();
    stores.sort_unstable();
    for store in &stores {
        info!(
            "store: {:?} : {:?}",
            store,
            store_counts.get(store).unwrap()
        );
    }
    for x in 0..3 {
        // if the store count doesn't exist for this id, then it is implied to be > 0
        assert!(store_counts
            .get(&x)
            .map(|entry| entry.0 >= 1)
            .unwrap_or(true));
    }
}
