use super::*;

#[derive(Default)]
pub(crate) struct CleanKeyTimings {
    pub(crate) collect_delta_keys_us: u64,
    pub(crate) delta_insert_us: u64,
    pub(crate) dirty_store_processing_us: u64,
    pub(crate) delta_key_count: u64,
    pub(crate) dirty_pubkeys_count: u64,
    pub(crate) oldest_dirty_slot: Slot,
}

#[derive(Default, Debug)]
pub(crate) struct CleaningInfo {
    pub(crate) slot_list: SlotList<AccountInfo>,
    pub(crate) ref_count: RefCount,
    /// Indicates if this account might have a zero lamport index entry.
    /// If false, the account *shall* not have zero lamport index entries.
    /// If true, the account *might* have zero lamport index entries.
    pub(crate) might_contain_zero_lamport_entry: bool,
}

/// This is the return type of AccountsDb::construct_candidate_clean_keys.
/// It's a collection of pubkeys with associated information to
/// facilitate the decision making about which accounts can be removed
/// from the accounts index. In addition, the minimal dirty slot is
/// included in the returned value.
pub(crate) type CleaningCandidates = (RwLock<HashMap<Pubkey, CleaningInfo>>, Option<Slot>);

fn count_pubkeys(candidates: &RwLock<HashMap<Pubkey, CleaningInfo>>) -> u64 {
    candidates.read().unwrap().len() as u64
}

impl AppendVecBackend {
    /// Body of `AccountsDb::clean_accounts`. The caller is responsible for
    /// resolving `max_clean_root_inclusive` against any in-flight scans, and
    /// for snapshotting the scan-tracker counters used in telemetry.
    pub(in super::super) fn clean_accounts(
        &self,
        max_clean_root_inclusive: Option<Slot>,
        is_startup: bool,
    ) {
        let b = self;
        if b.exhaustively_verify_refcounts {
            //at startup use all cores to verify refcounts
            if is_startup {
                b.exhaustively_verify_refcounts(max_clean_root_inclusive);
            } else {
                // otherwise, use the background thread pool
                b.thread_pool_background
                    .install(|| b.exhaustively_verify_refcounts(max_clean_root_inclusive));
            }
        }

        let mut measure_all = Measure::start("clean_accounts");

        b.report_store_stats();

        let mut measure_construct_candidates = Measure::start("construct_candidates");
        let mut key_timings = CleanKeyTimings::default();
        let (mut candidates, min_dirty_slot) = b.construct_candidate_clean_keys(
            max_clean_root_inclusive,
            is_startup,
            &mut key_timings,
        );
        measure_construct_candidates.stop();

        let num_candidates = count_pubkeys(&candidates);
        let found_not_zero_accum = AtomicU64::new(0);
        let missing_accum = AtomicU64::new(0);
        let useful_accum = AtomicU64::new(0);
        let do_clean_scan = || {
            let mut found_not_zero = 0u64;
            let mut missing = 0u64;
            let mut useful = 0u64;
            candidates
                .write()
                .unwrap()
                .retain(|candidate_pubkey, candidate_info| {
                    let mut useless = true;
                    if let Some((slot, account_info)) = b.accounts_index.get(candidate_pubkey) {
                        let slot_list = [(slot, account_info)];
                        if max_clean_root_inclusive.is_none_or(|max| slot <= max) {
                            if account_info.is_zero_lamport() {
                                useless = false;
                                candidate_info.slot_list = b
                                    .accounts_index
                                    .get_rooted_entries(&slot_list, max_clean_root_inclusive);
                                candidate_info.ref_count = 1;
                            } else {
                                found_not_zero += 1;
                            }
                        }
                    } else {
                        missing += 1;
                    }
                    if !useless {
                        useful += 1;
                    }
                    !candidate_info.slot_list.is_empty()
                });
            found_not_zero_accum.fetch_add(found_not_zero, Ordering::Relaxed);
            missing_accum.fetch_add(missing, Ordering::Relaxed);
            useful_accum.fetch_add(useful, Ordering::Relaxed);
        };
        let mut accounts_scan = Measure::start("accounts_scan");
        if is_startup {
            do_clean_scan();
        } else {
            b.thread_pool_background.install(do_clean_scan);
        }
        accounts_scan.stop();

        let mut candidates = mem::take(&mut *candidates.get_mut().unwrap());

        let retained_keys_count: usize = candidates.len();
        let mut pubkeys_removed_from_accounts_index: PubkeysRemovedFromAccountsIndex =
            HashSet::new();

        // Calculate store counts as if everything was purged
        // Then purge if we can
        let mut store_counts_time = Measure::start("store_counts");
        let mut store_counts: HashMap<Slot, (usize, HashSet<Pubkey>)> = HashMap::new();
        for (pubkey, cleaning_info) in candidates.iter_mut() {
            let slot_list = &mut cleaning_info.slot_list;
            debug_assert!(!slot_list.is_empty(), "candidate slot_list can't be empty");
            slot_list.retain(|(slot, account_info)| {
                if let Some(store_count) = store_counts.get_mut(slot) {
                    store_count.0 -= 1;
                    store_count.1.insert(*pubkey);
                } else {
                    let mut key_set = HashSet::new();
                    key_set.insert(*pubkey);
                    assert!(
                        !account_info.is_cached(),
                        "The Accounts Cache must be flushed first for this account info. pubkey: \
                         {}, slot: {}",
                        *pubkey,
                        *slot
                    );
                    let count = b
                        .storage
                        .get_account_storage_entry(*slot, account_info.store_id())
                        .map(|store| store.count())
                        .unwrap()
                        - 1;
                    debug!(
                        "store_counts, inserting slot: {}, store id: {}, count: {}",
                        slot,
                        account_info.store_id(),
                        count
                    );
                    store_counts.insert(*slot, (count, key_set));
                }
                true
            });
        }
        store_counts_time.stop();

        let mut calc_deps_time = Measure::start("calc_deps");
        b.calc_delete_dependencies(&candidates, &mut store_counts, min_dirty_slot);
        calc_deps_time.stop();

        let mut purge_filter = Measure::start("purge_filter");
        b.filter_zero_lamport_clean_for_incremental_snapshots(
            max_clean_root_inclusive,
            &store_counts,
            &mut candidates,
        );
        purge_filter.stop();

        let mut reclaims_time = Measure::start("reclaims");
        let pubkey_to_slot_set: Vec<_> = candidates
            .into_iter()
            .filter_map(|(pubkey, cleaning_info)| {
                let slot_list = cleaning_info.slot_list;
                (!slot_list.is_empty()).then_some((
                    pubkey,
                    slot_list
                        .iter()
                        .map(|(slot, _)| *slot)
                        .collect::<HashSet<Slot>>(),
                ))
            })
            .collect();

        let (reclaims, pubkeys_removed_from_accounts_index2) =
            self.purge_keys_exact(pubkey_to_slot_set);
        pubkeys_removed_from_accounts_index.extend(pubkeys_removed_from_accounts_index2);

        if !reclaims.is_empty() {
            b.handle_reclaims(
                reclaims.iter(),
                None,
                &b.clean_accounts_stats.purge_stats,
                MarkAccountsObsolete::No,
            );
        }

        reclaims_time.stop();

        measure_all.stop();

        b.clean_accounts_stats.report();
        datapoint_info!(
            "clean_accounts",
            ("max_clean_root", max_clean_root_inclusive, Option<i64>),
            ("total_us", measure_all.as_us(), i64),
            (
                "collect_delta_keys_us",
                key_timings.collect_delta_keys_us,
                i64
            ),
            ("oldest_dirty_slot", key_timings.oldest_dirty_slot, i64),
            (
                "pubkeys_removed_from_accounts_index",
                pubkeys_removed_from_accounts_index.len(),
                i64
            ),
            (
                "dirty_store_processing_us",
                key_timings.dirty_store_processing_us,
                i64
            ),
            ("construct_candidates_us", measure_construct_candidates.as_us(), i64),
            ("accounts_scan", accounts_scan.as_us(), i64),
            ("store_counts", store_counts_time.as_us(), i64),
            ("purge_filter", purge_filter.as_us(), i64),
            ("calc_deps", calc_deps_time.as_us(), i64),
            ("reclaims", reclaims_time.as_us(), i64),
            ("delta_insert_us", key_timings.delta_insert_us, i64),
            ("delta_key_count", key_timings.delta_key_count, i64),
            ("dirty_pubkeys_count", key_timings.dirty_pubkeys_count, i64),
            ("useful_keys", useful_accum.load(Ordering::Relaxed), i64),
            ("total_keys_count", num_candidates, i64),
            ("retained_keys_count", retained_keys_count, i64),
            (
                "scan_found_not_zero",
                found_not_zero_accum.load(Ordering::Relaxed),
                i64
            ),
            ("scan_missing", missing_accum.load(Ordering::Relaxed), i64),
            (
                "get_account_sizes_us",
                b.clean_accounts_stats
                    .get_account_sizes_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "slots_cleaned",
                b.clean_accounts_stats
                    .slots_cleaned
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "remove_dead_accounts_remove_us",
                b.clean_accounts_stats
                    .remove_dead_accounts_remove_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "remove_dead_accounts_shrink_us",
                b.clean_accounts_stats
                    .remove_dead_accounts_shrink_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "clean_stored_dead_slots_us",
                b.clean_accounts_stats
                    .clean_stored_dead_slots_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "roots_added",
                b.storage_roots.read().unwrap().roots_added.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "roots_removed",
                b.storage_roots.read().unwrap().roots_removed.swap(0, Ordering::Relaxed),
                i64
            ),
            ("next_store_id", b.next_id.load(Ordering::Relaxed), i64),
        );
    }
}

impl AppendVecBackend {
    #[cfg(any(test, feature = "dev-context-only-utils"))]
    pub fn get_len_of_slots_with_uncleaned_pubkeys(&self) -> usize {
        self.uncleaned_pubkeys.len()
    }

    #[cfg(test)]
    pub fn storage_access(&self) -> StorageAccess {
        self.storage_access
    }

    #[cfg(test)]
    pub fn check_storage(&self, slot: Slot, alive_count: usize, total_count: usize) {
        let store = self.storage.get_slot_storage_entry(slot).unwrap();
        assert_eq!(store.count(), alive_count);
        assert_eq!(store.accounts_count(), total_count);
    }

    #[cfg(test)]
    pub fn all_account_count_in_accounts_file(&self, slot: Slot) -> usize {
        self.storage
            .get_slot_storage_entry(slot)
            .map_or(0, |store| store.accounts_count())
    }

    pub(super) fn clean_stored_dead_slots(
        &self,
        dead_slots: &IntSet<Slot>,
        purged_account_slots: Option<&mut AccountSlots>,
    ) {
        let mut measure = Measure::start("clean_stored_dead_slots-ms");
        let mut stores = vec![];
        // get all stores in a vec so we can iterate in parallel
        for slot in dead_slots.iter() {
            if let Some(slot_storage) = self.storage.get_slot_storage_entry(*slot) {
                stores.push(slot_storage);
            }
        }
        // get all pubkeys in all dead slots
        let purged_slot_pubkeys: HashSet<(Slot, Pubkey)> = {
            self.thread_pool_background.install(|| {
                stores
                    .into_par_iter()
                    .map(|store| {
                        let slot = store.slot();
                        let mut pubkeys = Vec::with_capacity(store.count());
                        // Obsolete accounts are already unreffed before this point, so do not add
                        // them to the pubkeys list.
                        let obsolete_accounts: HashSet<_> = store
                            .obsolete_accounts_read_lock()
                            .filter_obsolete_accounts(None)
                            .collect();
                        store
                            .accounts
                            .scan_accounts_without_data(|offset, account| {
                                if !obsolete_accounts.contains(&(offset, account.data_len)) {
                                    pubkeys.push((slot, *account.pubkey));
                                }
                            })
                            .expect("must scan accounts storage");
                        pubkeys
                    })
                    .flatten()
                    .collect::<HashSet<_>>()
            })
        };

        if let Some(purged_account_slots) = purged_account_slots {
            for (slot, pubkey) in purged_slot_pubkeys {
                purged_account_slots.entry(pubkey).or_default().insert(slot);
            }
        }

        measure.stop();
        self.clean_accounts_stats
            .clean_stored_dead_slots_us
            .fetch_add(measure.as_us(), Ordering::Relaxed);
    }

    /// increment store_counts to non-zero for all stores that can not be deleted.
    /// a store cannot be deleted if:
    /// 1. one of the pubkeys in the store has account info to a store whose store count is not going to zero
    /// 2. a pubkey we were planning to remove is not removing all stores that contain the account
    fn calc_delete_dependencies(
        &self,
        candidates: &HashMap<Pubkey, CleaningInfo>,
        store_counts: &mut HashMap<Slot, (usize, HashSet<Pubkey>)>,
        min_slot: Option<Slot>,
    ) {
        let mut already_counted = IntSet::default();
        for (pubkey, cleaning_info) in candidates.iter() {
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
                            continue;
                        }
                    }
                    failed_slot = Some(*slot);
                    delete = false;
                    break;
                }
                if delete {
                    continue;
                }
            } else {
                debug!(
                    "calc_delete_dependencies(), pubkey: {pubkey}, slot list len: {}, ref count: \
                     {ref_count}, slot list: {slot_list:?}",
                    slot_list.len(),
                );
            }

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
                if let Some(store_count) = store_counts.remove(&slot) {
                    let affected_pubkeys = &store_count.1;
                    for key in affected_pubkeys {
                        if let Some(info) = candidates.get(key) {
                            for (slot, _account_info) in &info.slot_list {
                                if !already_counted.contains(slot) {
                                    pending_stores.insert(*slot);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Collect all the uncleaned slots, up to a max slot
    ///
    /// Search through the uncleaned Pubkeys and return all the slots, up to a maximum slot.
    pub(super) fn collect_uncleaned_slots_up_to_slot(&self, max_slot_inclusive: Slot) -> Vec<Slot> {
        self.uncleaned_pubkeys
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
    pub(super) fn remove_uncleaned_slots_up_to_slot_and_move_pubkeys(
        &self,
        max_slot_inclusive: Slot,
        candidates: &RwLock<HashMap<Pubkey, CleaningInfo>>,
    ) {
        let uncleaned_slots = self.collect_uncleaned_slots_up_to_slot(max_slot_inclusive);
        for uncleaned_slot in uncleaned_slots.into_iter() {
            if let Some((_removed_slot, removed_pubkeys)) =
                self.uncleaned_pubkeys.remove(&uncleaned_slot)
            {
                let mut candidates_map = candidates.write().unwrap();
                for removed_pubkey in removed_pubkeys {
                    candidates_map.insert(
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

    /// Construct a list of candidates for cleaning from:
    /// - dirty_stores      -- set of stores which had accounts removed or recently rooted
    /// - uncleaned_pubkeys -- the delta set of updated pubkeys in rooted slots from the last clean
    ///
    /// The function also returns the minimum slot we encountered.
    fn construct_candidate_clean_keys(
        &self,
        max_clean_root_inclusive: Option<Slot>,
        is_startup: bool,
        timings: &mut CleanKeyTimings,
    ) -> CleaningCandidates {
        let mut dirty_store_processing_time = Measure::start("dirty_store_processing");
        let max_root_inclusive = self.storage_roots.read().unwrap().max_root_inclusive();
        let max_slot_inclusive = max_clean_root_inclusive.unwrap_or(max_root_inclusive);
        let mut dirty_stores = Vec::with_capacity(self.dirty_stores.len());
        // find the oldest dirty slot
        // we'll add logging if that append vec cannot be marked dead
        let mut min_dirty_slot = None::<u64>;
        self.dirty_stores.retain(|slot, store| {
            if *slot > max_slot_inclusive {
                true
            } else {
                min_dirty_slot = min_dirty_slot.map(|min| min.min(*slot)).or(Some(*slot));
                dirty_stores.push((*slot, store.clone()));
                false
            }
        });
        let dirty_stores_len = dirty_stores.len();
        let candidates = RwLock::new(HashMap::<Pubkey, CleaningInfo>::new());

        let insert_candidate = |pubkey, is_zero_lamport| {
            candidates
                .write()
                .unwrap()
                .entry(pubkey)
                .or_default()
                .might_contain_zero_lamport_entry |= is_zero_lamport;
        };

        let mut dirty_store_routine = || {
            let chunk_size = 1.max(dirty_stores_len.saturating_div(rayon::current_num_threads()));
            let oldest_dirty_slots: Vec<u64> = dirty_stores
                .par_chunks(chunk_size)
                .map(|dirty_store_chunk| {
                    let mut oldest_dirty_slot = max_slot_inclusive.saturating_add(1);
                    dirty_store_chunk.iter().for_each(|(slot, store)| {
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
            self.thread_pool_background.install(|| {
                dirty_store_routine();
            });
        }
        timings.dirty_pubkeys_count = count_pubkeys(&candidates);
        trace!(
            "dirty_stores.len: {} pubkeys.len: {}",
            dirty_stores_len, timings.dirty_pubkeys_count,
        );
        dirty_store_processing_time.stop();
        timings.dirty_store_processing_us += dirty_store_processing_time.as_us();

        let mut collect_delta_keys = Measure::start("key_create");
        self.remove_uncleaned_slots_up_to_slot_and_move_pubkeys(max_slot_inclusive, &candidates);
        collect_delta_keys.stop();
        timings.collect_delta_keys_us += collect_delta_keys.as_us();

        timings.delta_key_count = count_pubkeys(&candidates);

        // Check if we should purge any of the
        // zero_lamport_accounts_to_purge_later, based on the
        // latest_full_snapshot_slot.
        let latest_full_snapshot_slot = self.latest_full_snapshot_slot.read();
        assert!(
            latest_full_snapshot_slot.is_some()
                || self
                    .zero_lamport_accounts_to_purge_after_full_snapshot
                    .is_empty(),
            "if snapshots are disabled, then zero_lamport_accounts_to_purge_later should always \
             be empty"
        );
        if let Some(latest_full_snapshot_slot) = latest_full_snapshot_slot {
            self.zero_lamport_accounts_to_purge_after_full_snapshot
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

    /// called with cli argument to verify refcounts are correct on all accounts
    /// this is very slow
    /// this function will call Rayon par_iter, so you will want to have thread pool installed if
    /// you want to call this without consuming all the cores on the CPU.
    fn exhaustively_verify_refcounts(&self, max_slot_inclusive: Option<Slot>) {
        let max_slot_inclusive = max_slot_inclusive
            .unwrap_or_else(|| self.storage_roots.read().unwrap().max_root_inclusive());
        info!("exhaustively verifying refcounts as of slot: {max_slot_inclusive}");
        let pubkey_refcount = DashMap::<Pubkey, Vec<Slot>>::default();
        let mut storages = self.storage.all_storages();
        storages.retain(|s| s.slot() <= max_slot_inclusive);
        // populate
        storages.par_iter().for_each_init(
            || Box::new(append_vec::new_scan_accounts_reader()),
            |reader, storage| {
                let slot = storage.slot();
                storage
                    .accounts
                    .scan_accounts(reader.as_mut(), |_offset, account| {
                        let pk = account.pubkey();
                        match pubkey_refcount.entry(*pk) {
                            dashmap::mapref::entry::Entry::Occupied(mut occupied_entry) => {
                                if !occupied_entry.get().iter().any(|s| s == &slot) {
                                    occupied_entry.get_mut().push(slot);
                                }
                            }
                            dashmap::mapref::entry::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(vec![slot]);
                            }
                        }
                    })
                    .expect("must scan accounts storage")
            },
        );
        let total = pubkey_refcount.len();
        if total == 0 {
            return;
        }
        let failed = AtomicBool::default();
        let threads = quarter_thread_count();
        let per_batch = total.div_ceil(threads);
        (0..=threads).into_par_iter().for_each(|attempt| {
            pubkey_refcount
                .iter()
                .skip(attempt * per_batch)
                .take(per_batch)
                .for_each(|entry| {
                    if failed.load(Ordering::Relaxed) {
                        return;
                    }

                    if let Some((slot, account_info)) = self.accounts_index.get(entry.key()) {
                        let ref_count = 1usize; // always 1 on this branch
                        let expected = entry.value().len();
                        let num_too_new = (slot > max_slot_inclusive) as usize;
                        match ref_count.cmp(&expected) {
                            std::cmp::Ordering::Equal => {
                                // ref counts match, nothing to do here
                            }
                            std::cmp::Ordering::Greater => {
                                if (ref_count - num_too_new) > expected {
                                    failed.store(true, Ordering::Relaxed);
                                    error!(
                                        "exhaustively_verify_refcounts: {} refcount too large: \
                                         {}, should be: {}, {:?}, {:?}, too_new: {num_too_new}",
                                        entry.key(),
                                        ref_count,
                                        expected,
                                        *entry.value(),
                                        [(slot, account_info)]
                                    );
                                }
                            }
                            std::cmp::Ordering::Less => {
                                error!(
                                    "exhaustively_verify_refcounts: {} refcount too small: {}, \
                                     should be: {}, {:?}, {:?}",
                                    entry.key(),
                                    ref_count,
                                    expected,
                                    *entry.value(),
                                    [(slot, account_info)]
                                );
                            }
                        }
                    }
                });
        });
        if failed.load(Ordering::Relaxed) {
            panic!("exhaustively_verify_refcounts failed");
        }
    }

    /// During clean, some zero-lamport accounts that are marked for purge should *not* actually
    /// get purged.  Filter out those accounts here by removing them from 'candidates'.
    /// Candidates may contain entries with empty slots list in CleaningInfo.
    /// The function removes such entries from 'candidates'.
    ///
    /// When using incremental snapshots, do not purge zero-lamport accounts if the slot is higher
    /// than the latest full snapshot slot.  This is to protect against the following scenario:
    ///
    ///   ```text
    ///   A full snapshot is taken, including account 'alpha' with a non-zero balance.  In a later slot,
    ///   alpha's lamports go to zero.  Eventually, cleaning runs.  Without this change,
    ///   alpha would be cleaned up and removed completely. Finally, an incremental snapshot is taken.
    ///
    ///   Later, the incremental and full snapshots are used to rebuild the bank and accounts
    ///   database (e.x. if the node restarts).  The full snapshot _does_ contain alpha
    ///   and its balance is non-zero.  However, since alpha was cleaned up in a slot after the full
    ///   snapshot slot (due to having zero lamports), the incremental snapshot would not contain alpha.
    ///   Thus, the accounts database will contain the old, incorrect info for alpha with a non-zero
    ///   balance.  Very bad!
    ///   ```
    ///
    /// This filtering step can be skipped if there is no `latest_full_snapshot_slot`, or if the
    /// `max_clean_root_inclusive` is less-than-or-equal-to the `latest_full_snapshot_slot`.
    pub(super) fn filter_zero_lamport_clean_for_incremental_snapshots(
        &self,
        max_clean_root_inclusive: Option<Slot>,
        store_counts: &HashMap<Slot, (usize, HashSet<Pubkey>)>,
        candidates: &mut HashMap<Pubkey, CleaningInfo>,
    ) {
        let latest_full_snapshot_slot = self.latest_full_snapshot_slot.read();
        let should_filter_for_incremental_snapshots = max_clean_root_inclusive.unwrap_or(Slot::MAX)
            > latest_full_snapshot_slot.unwrap_or(Slot::MAX);
        assert!(
            latest_full_snapshot_slot.is_some() || !should_filter_for_incremental_snapshots,
            "if filtering for incremental snapshots, then snapshots should be enabled",
        );

        candidates.retain(|pubkey, cleaning_info| {
            let slot_list = &cleaning_info.slot_list;
            debug_assert!(!slot_list.is_empty(), "candidate slot_list can't be empty");
            // Only keep candidates where the entire history of the account in the root set
            // can be purged. All AppendVecs for those updates are dead.
            for (slot, _account_info) in slot_list.iter() {
                if let Some(store_count) = store_counts.get(slot) {
                    if store_count.0 != 0 {
                        // one store this pubkey is in is not being removed, so this pubkey cannot be removed at all
                        return false;
                    }
                } else {
                    // store is not being removed, so this pubkey cannot be removed at all
                    return false;
                }
            }

            // Exit early if not filtering more for incremental snapshots
            if !should_filter_for_incremental_snapshots {
                return true;
            }

            // Safety: We exited early if the slot list was empty,
            // so we're guaranteed here that `.max_by_key()` returns Some.
            let (slot, account_info) = slot_list
                .iter()
                .max_by_key(|(slot, _account_info)| slot)
                .unwrap();

            // Do *not* purge zero-lamport accounts if the slot is greater than the last full
            // snapshot slot.  Since we're `retain`ing the accounts-to-purge, I felt creating
            // the `cannot_purge` variable made this easier to understand.  Accounts that do
            // not get purged here are added to a list so they be considered for purging later
            // (i.e. after the next full snapshot).
            assert!(account_info.is_zero_lamport());
            let cannot_purge = *slot > latest_full_snapshot_slot.unwrap();
            if cannot_purge {
                self.zero_lamport_accounts_to_purge_after_full_snapshot
                    .insert((*slot, *pubkey));
            }
            !cannot_purge
        });
    }
}

#[allow(dead_code)]
pub(crate) trait AccountsClean {
    fn clean_accounts(&self, max_clean_root_inclusive: Option<Slot>, is_startup: bool);
}

impl AccountsClean for AccountsDb {
    fn clean_accounts(&self, max_clean_root_inclusive: Option<Slot>, is_startup: bool) {
        AccountsDb::clean_accounts(self, max_clean_root_inclusive, is_startup)
    }
}
