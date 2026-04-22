use super::*;

#[derive(Default, Debug)]
/// hold alive accounts
/// alive means in the accounts index
pub(crate) struct AliveAccounts<'a> {
    /// slot the accounts are currently stored in
    pub(crate) slot: Slot,
    pub(crate) accounts: Vec<&'a AccountFromStorage>,
    pub(crate) bytes: usize,
}

/// separate pubkeys into those with a single refcount and those with > 1 refcount
#[derive(Debug)]
pub(crate) struct ShrinkCollectAliveSeparatedByRefs<'a> {
    /// accounts where ref_count = 1
    pub(crate) one_ref: AliveAccounts<'a>,
    /// account where ref_count > 1, but this slot contains the alive entry with the highest slot
    pub(crate) many_refs_this_is_newest_alive: AliveAccounts<'a>,
    /// account where ref_count > 1, and this slot is NOT the highest alive entry in the index for the pubkey
    pub(crate) many_refs_old_alive: AliveAccounts<'a>,
}

pub(crate) trait ShrinkCollectRefs<'a>: Sync + Send {
    fn with_capacity(capacity: usize, slot: Slot) -> Self;
    fn collect(&mut self, other: Self);
    fn add(
        &mut self,
        ref_count: RefCount,
        account: &'a AccountFromStorage,
        slot_list: &[(Slot, AccountInfo)],
    );
    fn len(&self) -> usize;
    fn alive_bytes(&self) -> usize;
    fn alive_accounts(&self) -> &Vec<&'a AccountFromStorage>;
}

impl<'a> ShrinkCollectRefs<'a> for AliveAccounts<'a> {
    fn collect(&mut self, mut other: Self) {
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.accounts.append(&mut other.accounts);
    }
    fn with_capacity(capacity: usize, slot: Slot) -> Self {
        Self {
            accounts: Vec::with_capacity(capacity),
            bytes: 0,
            slot,
        }
    }
    fn add(
        &mut self,
        _ref_count: RefCount,
        account: &'a AccountFromStorage,
        _slot_list: &[(Slot, AccountInfo)],
    ) {
        self.accounts.push(account);
        self.bytes = self.bytes.saturating_add(account.stored_size());
    }
    fn len(&self) -> usize {
        self.accounts.len()
    }
    fn alive_bytes(&self) -> usize {
        self.bytes
    }
    fn alive_accounts(&self) -> &Vec<&'a AccountFromStorage> {
        &self.accounts
    }
}

impl<'a> ShrinkCollectRefs<'a> for ShrinkCollectAliveSeparatedByRefs<'a> {
    fn collect(&mut self, other: Self) {
        self.one_ref.collect(other.one_ref);
        self.many_refs_this_is_newest_alive
            .collect(other.many_refs_this_is_newest_alive);
        self.many_refs_old_alive.collect(other.many_refs_old_alive);
    }
    fn with_capacity(capacity: usize, slot: Slot) -> Self {
        Self {
            one_ref: AliveAccounts::with_capacity(capacity, slot),
            many_refs_this_is_newest_alive: AliveAccounts::with_capacity(0, slot),
            many_refs_old_alive: AliveAccounts::with_capacity(0, slot),
        }
    }
    fn add(
        &mut self,
        ref_count: RefCount,
        account: &'a AccountFromStorage,
        slot_list: &[(Slot, AccountInfo)],
    ) {
        let other = if ref_count == 1 {
            &mut self.one_ref
        } else if slot_list.len() == 1
            || !slot_list
                .iter()
                .any(|(slot_list_slot, _info)| slot_list_slot > &self.many_refs_old_alive.slot)
        {
            // this entry is alive but is newer than any other slot in the index
            &mut self.many_refs_this_is_newest_alive
        } else {
            // This entry is alive but is older than at least one other slot in the index.
            // We would expect clean to get rid of the entry for THIS slot at some point, but clean hasn't done that yet.
            &mut self.many_refs_old_alive
        };
        other.add(ref_count, account, slot_list);
    }
    fn len(&self) -> usize {
        self.one_ref
            .len()
            .saturating_add(self.many_refs_old_alive.len())
            .saturating_add(self.many_refs_this_is_newest_alive.len())
    }
    fn alive_bytes(&self) -> usize {
        self.one_ref
            .alive_bytes()
            .saturating_add(self.many_refs_old_alive.alive_bytes())
            .saturating_add(self.many_refs_this_is_newest_alive.alive_bytes())
    }
    fn alive_accounts(&self) -> &Vec<&'a AccountFromStorage> {
        unimplemented!("illegal use");
    }
}

#[derive(Debug)]
pub(crate) struct ShrinkCollect<'a, T: ShrinkCollectRefs<'a>> {
    pub(crate) slot: Slot,
    pub(crate) capacity: u64,
    pub(crate) zero_lamport_single_ref_pubkeys: Vec<&'a Pubkey>,
    pub(crate) alive_accounts: T,
    /// total size in storage of all alive accounts
    pub(crate) alive_total_bytes: usize,
    pub(crate) total_starting_accounts: usize,
    /// true if all alive accounts are zero lamports
    pub(crate) all_are_zero_lamports: bool,
}

pub(super) struct LoadAccountsIndexForShrink<'a, T: ShrinkCollectRefs<'a>> {
    /// all alive accounts
    pub(super) alive_accounts: T,
    /// pubkeys that are the last remaining zero lamport instance of an account
    pub(super) zero_lamport_single_ref_pubkeys: Vec<&'a Pubkey>,
    /// true if all alive accounts are zero lamport accounts
    pub(super) all_are_zero_lamports: bool,
}

pub struct GetUniqueAccountsResult {
    pub stored_accounts: Vec<AccountFromStorage>,
    pub capacity: u64,
}

impl AccountsDb {
    /// Run a periodic shrink pass.
    ///
    /// When `include_ancient` is true, ancient slots are squashed first
    /// (callers typically pair this with `clean_accounts`). The candidate
    /// queue is always processed.
    ///
    /// Returns the number of candidate slots selected for shrinking.
    pub fn shrink(&self, epoch_schedule: &EpochSchedule, include_ancient: bool) -> usize {
        let _shrink_guard = self.active_stats.activate(ActiveStatItem::Shrink);
        let _ancient_guard = include_ancient
            .then(|| self.active_stats.activate(ActiveStatItem::SquashAncient));
        self.backend.shrink(epoch_schedule, include_ancient)
    }
}

impl AppendVecBackend {
    /// Run a periodic shrink pass.
    ///
    /// When `include_ancient` is true, squash ancient slots first via
    /// `shrink_ancient_slots`. The candidate queue is always processed.
    /// Returns the number of candidate slots selected for shrinking.
    pub fn shrink(&self, epoch_schedule: &EpochSchedule, include_ancient: bool) -> usize {
        if include_ancient {
            self.shrink_ancient_slots(epoch_schedule);
        }
        self.shrink_candidate_slots(epoch_schedule)
    }

    /// Shrink one slot's storage by rewriting alive accounts. No-op if no
    /// productive shrink is available. Test-only; production uses the
    /// queue-driven `shrink` path.
    #[cfg(test)]
    pub(in super::super) fn shrink_slot_forced(&self, slot: Slot) {
        debug!("shrink_slot_forced: slot: {slot}");

        if let Some(store) = self
            .storage
            .get_slot_storage_entry_shrinking_in_progress_ok(slot)
        {
            if AppendVecBackend::is_shrinking_productive(&store) {
                self.shrink_storage(store)
            }
        }
    }

    pub(in super::super) fn shrink_candidate_slots(&self, epoch_schedule: &EpochSchedule) -> usize {
        let max_root = self.storage_roots.read().unwrap().max_root_inclusive();
        let oldest_non_ancient_slot = self.get_oldest_non_ancient_slot(epoch_schedule, max_root);

        let shrink_candidates_slots =
            std::mem::take(&mut *self.shrink_candidate_slots.lock().unwrap());
        self.shrink_stats
            .initial_candidates_count
            .store(shrink_candidates_slots.len() as u64, Ordering::Relaxed);

        let candidates_count = shrink_candidates_slots.len();
        let ((mut shrink_slots, shrink_slots_next_batch), select_time_us) = measure_us!({
            if let AccountShrinkThreshold::TotalSpace { shrink_ratio } = self.shrink_ratio {
                let (shrink_slots, shrink_slots_next_batch) =
                    self.select_candidates_by_total_usage(&shrink_candidates_slots, shrink_ratio);
                (shrink_slots, Some(shrink_slots_next_batch))
            } else {
                (
                    // lookup storage for each slot
                    shrink_candidates_slots
                        .into_iter()
                        .filter_map(|slot| {
                            self.storage
                                .get_slot_storage_entry(slot)
                                .map(|storage| (slot, storage))
                        })
                        .collect(),
                    None,
                )
            }
        });

        // If there are too few slots to shrink, add an ancient slot
        // for shrinking.
        if shrink_slots.len() < SHRINK_INSERT_ANCIENT_THRESHOLD {
            let mut ancients = self.best_ancient_slots_to_shrink.write().unwrap();
            while let Some((slot, capacity)) = ancients.pop_front() {
                if let Some(store) = self.storage.get_slot_storage_entry(slot) {
                    if !shrink_slots.contains(&slot)
                        && capacity == store.capacity()
                        && self.is_candidate_for_shrink(&store)
                    {
                        let ancient_bytes_added_to_shrink = store.alive_bytes() as u64;
                        shrink_slots.insert(slot, store);
                        self.shrink_stats
                            .ancient_bytes_added_to_shrink
                            .fetch_add(ancient_bytes_added_to_shrink, Ordering::Relaxed);
                        self.shrink_stats
                            .ancient_slots_added_to_shrink
                            .fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }
        if shrink_slots.is_empty()
            && shrink_slots_next_batch
                .as_ref()
                .map(|s| s.is_empty())
                .unwrap_or(true)
        {
            return 0;
        }

        let num_selected = shrink_slots.len();
        let (_, shrink_all_us) = measure_us!({
            self.thread_pool_background.install(|| {
                shrink_slots
                    .into_par_iter()
                    .for_each(|(slot, slot_shrink_candidate)| {
                        if self.ancient_append_vec_offset.is_some()
                            && slot < oldest_non_ancient_slot
                        {
                            self.shrink_stats
                                .num_ancient_slots_shrunk
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        self.shrink_storage(slot_shrink_candidate);
                    });
            })
        });

        let mut pended_counts: usize = 0;
        if let Some(shrink_slots_next_batch) = shrink_slots_next_batch {
            let mut shrink_slots = self.shrink_candidate_slots.lock().unwrap();
            pended_counts = shrink_slots_next_batch.len();
            for slot in shrink_slots_next_batch {
                shrink_slots.insert(slot);
            }
        }

        datapoint_info!(
            "shrink_candidate_slots",
            ("select_time_us", select_time_us, i64),
            ("shrink_all_us", shrink_all_us, i64),
            ("candidates_count", candidates_count, i64),
            ("selected_count", num_selected, i64),
            ("deferred_to_next_round_count", pended_counts, i64)
        );

        num_selected
    }

    /// Shrinks `store` by rewriting the alive accounts to a new storage
    pub(super) fn shrink_storage(&self, store: Arc<AccountStorageEntry>) {
        let slot = store.slot();
        let mut unique_accounts =
            self.get_unique_accounts_from_storage_for_shrink(&store, &self.shrink_stats);
        debug!("do_shrink_slot_store: slot: {slot}");
        let shrink_collect = self.shrink_collect::<AliveAccounts<'_>>(
            &store,
            &mut unique_accounts,
            &self.shrink_stats,
        );

        // This shouldn't happen if alive_bytes is accurate.
        // However, it is possible that the remaining alive bytes could be 0. In that case, the whole slot should be marked dead by clean.
        if AppendVecBackend::should_not_shrink(
            shrink_collect.alive_total_bytes as u64,
            shrink_collect.capacity,
        ) || shrink_collect.alive_total_bytes == 0
        {
            if shrink_collect.alive_total_bytes == 0 {
                // clean needs to take care of this dead slot
                self.dirty_stores.insert(slot, store.clone());
            }

            if !shrink_collect.all_are_zero_lamports {
                // if all are zero lamports, then we expect that we would like to mark the whole slot dead, but we cannot. That's clean's job.
                info!(
                    "Unexpected shrink for slot {} alive {} capacity {}, likely caused by a bug \
                     for calculating alive bytes.",
                    slot, shrink_collect.alive_total_bytes, shrink_collect.capacity
                );
            }

            self.shrink_stats
                .skipped_shrink
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        let total_accounts_after_shrink = shrink_collect.alive_accounts.len();
        debug!(
            "shrinking: slot: {}, accounts: ({} => {}) bytes: {} original: {}",
            slot,
            shrink_collect.total_starting_accounts,
            total_accounts_after_shrink,
            shrink_collect.alive_total_bytes,
            shrink_collect.capacity,
        );

        let mut stats_sub = ShrinkStatsSub::default();
        let mut rewrite_elapsed = Measure::start("rewrite_elapsed");
        let (shrink_in_progress, time_us) =
            measure_us!(self.get_store_for_shrink(slot, shrink_collect.alive_total_bytes as u64));
        stats_sub.create_and_insert_store_elapsed_us = Saturating(time_us);

        // here, we're writing back alive_accounts. That should be an atomic operation
        // without use of rather wide locks in this whole function, because we're
        // mutating rooted slots; There should be no writers to them.
        let accounts = [(slot, &shrink_collect.alive_accounts.alive_accounts()[..])];
        let storable_accounts = StorableAccountsBySlot::new(slot, &accounts, &self.storage);
        stats_sub.store_accounts_timing = self.store_accounts_frozen(
            storable_accounts,
            shrink_in_progress.new_storage(),
            UpdateIndexThreadSelection::PoolWithThreshold,
        );

        rewrite_elapsed.stop();
        stats_sub.rewrite_elapsed_us = Saturating(rewrite_elapsed.as_us());

        // `store_accounts_frozen()` above may have purged accounts from some
        // other storage entries (the ones that were just overwritten by this
        // new storage entry). This means some of those stores might have caused
        // this slot to be read to `self.shrink_candidate_slots`, so delete
        // those here
        self.shrink_candidate_slots.lock().unwrap().remove(&slot);

        self.remove_old_stores_shrink(
            &shrink_collect,
            &self.shrink_stats,
            Some(shrink_in_progress),
            false,
        );

        self.reopen_storage_as_readonly_shrinking_in_progress_ok(slot);

        AppendVecBackend::update_shrink_stats(&self.shrink_stats, stats_sub, true);
        self.shrink_stats.report();
    }

    /// get a sorted list of slots older than an epoch
    /// squash those slots into ancient append vecs
    pub(in super::super) fn shrink_ancient_slots(&self, epoch_schedule: &EpochSchedule) {
        if self.ancient_append_vec_offset.is_none() {
            return;
        }

        let max_root = self.storage_roots.read().unwrap().max_root_inclusive();
        let oldest_non_ancient_slot = self.get_oldest_non_ancient_slot(epoch_schedule, max_root);
        let can_randomly_shrink = true;
        let sorted_slots = self.get_sorted_potential_ancient_slots(oldest_non_ancient_slot);
        self.combine_ancient_slots_packed(sorted_slots, can_randomly_shrink);
    }

    /// load the account index entry for the first `count` items in `accounts`
    /// store a reference to all alive accounts in `alive_accounts`
    /// return sum of account size for all alive accounts
    fn load_accounts_index_for_shrink<'a, T: ShrinkCollectRefs<'a>>(
        &self,
        accounts: &'a [AccountFromStorage],
        stats: &ShrinkStats,
        slot_to_shrink: Slot,
    ) -> LoadAccountsIndexForShrink<'a, T> {
        let count = accounts.len();
        let mut alive_accounts = T::with_capacity(count, slot_to_shrink);
        let mut zero_lamport_single_ref_pubkeys = Vec::with_capacity(count);

        let mut alive = 0u64;
        let mut dead = 0u64;
        let mut all_are_zero_lamports = true;
        let latest_full_snapshot_slot = self.latest_full_snapshot_slot.read();

        for stored_account in accounts {
            let pubkey = stored_account.pubkey();
            let is_alive = self
                .accounts_index
                .get(pubkey)
                .map(|(slot, _info)| slot == slot_to_shrink)
                .unwrap_or(false);

            if is_alive {
                let ref_count = 1;
                let slot_list = [(slot_to_shrink, AccountInfo::default())];
                if stored_account.is_zero_lamport()
                    && latest_full_snapshot_slot
                        .map(|s| s >= slot_to_shrink)
                        .unwrap_or(true)
                {
                    zero_lamport_single_ref_pubkeys.push(pubkey);
                    self.add_uncleaned_pubkeys_after_shrink(slot_to_shrink, [*pubkey].into_iter());
                } else {
                    all_are_zero_lamports &= stored_account.is_zero_lamport();
                    alive_accounts.add(ref_count, stored_account, &slot_list);
                    alive += 1;
                }
            } else {
                dead += 1;
            }
        }

        stats.alive_accounts.fetch_add(alive, Ordering::Relaxed);
        stats.dead_accounts.fetch_add(dead, Ordering::Relaxed);

        LoadAccountsIndexForShrink {
            alive_accounts,
            zero_lamport_single_ref_pubkeys,
            all_are_zero_lamports,
        }
    }

    /// get all accounts in all the storages passed in
    /// for duplicate pubkeys, the account with the highest write_value is returned
    pub fn get_unique_accounts_from_storage(
        &self,
        store: &AccountStorageEntry,
    ) -> GetUniqueAccountsResult {
        let capacity = store.capacity();
        let mut stored_accounts = Vec::with_capacity(store.count());
        store
            .accounts
            .scan_accounts_without_data(|offset, account| {
                // file_id is unused and can be anything. We will always be loading whatever storage is in the slot.
                let file_id = 0;
                stored_accounts.push(AccountFromStorage {
                    index_info: AccountInfo::new(
                        StorageLocation::AppendVec(file_id, offset),
                        account.is_zero_lamport(),
                    ),
                    pubkey: *account.pubkey(),
                    data_len: account.data_len as u64,
                });
            })
            .expect("must scan accounts storage");

        GetUniqueAccountsResult {
            stored_accounts,
            capacity,
        }
    }

    pub(crate) fn get_unique_accounts_from_storage_for_shrink(
        &self,
        store: &AccountStorageEntry,
        stats: &ShrinkStats,
    ) -> GetUniqueAccountsResult {
        let (result, storage_read_elapsed_us) =
            measure_us!(self.get_unique_accounts_from_storage(store));
        stats
            .storage_read_elapsed
            .fetch_add(storage_read_elapsed_us, Ordering::Relaxed);
        result
    }

    /// shared code for shrinking normal slots and combining into ancient append vecs
    /// note 'unique_accounts' is passed by ref so we can return references to data within it, avoiding self-references
    pub(crate) fn shrink_collect<'a: 'b, 'b, T: ShrinkCollectRefs<'b>>(
        &self,
        store: &'a AccountStorageEntry,
        unique_accounts: &'b mut GetUniqueAccountsResult,
        stats: &ShrinkStats,
    ) -> ShrinkCollect<'b, T> {
        let slot = store.slot();

        let GetUniqueAccountsResult {
            stored_accounts,
            capacity,
        } = unique_accounts;

        let mut index_read_elapsed = Measure::start("index_read_elapsed");

        // Get a set of all obsolete offsets
        // Slot is not needed, as all obsolete accounts can be considered
        // dead for shrink. Zero lamport accounts are not marked obsolete
        let obsolete_offsets: IntSet<_> = store
            .obsolete_accounts_read_lock()
            .filter_obsolete_accounts(None)
            .map(|(offset, _)| offset)
            .collect();

        // Filter all the accounts that are marked obsolete
        let total_starting_accounts = stored_accounts.len();
        stored_accounts.retain(|account| !obsolete_offsets.contains(&account.index_info.offset()));

        let len = stored_accounts.len();
        let shrink_collect = Mutex::new(ShrinkCollect {
            slot,
            capacity: *capacity,
            zero_lamport_single_ref_pubkeys: Vec::new(),
            alive_accounts: T::with_capacity(len, slot),
            total_starting_accounts,
            all_are_zero_lamports: true,
            alive_total_bytes: 0, // will be updated after `alive_accounts` is populated
        });

        stats
            .accounts_loaded
            .fetch_add(len as u64, Ordering::Relaxed);
        stats
            .obsolete_accounts_filtered
            .fetch_add((total_starting_accounts - len) as u64, Ordering::Relaxed);
        self.thread_pool_background.install(|| {
            stored_accounts
                .par_chunks(SHRINK_COLLECT_CHUNK_SIZE)
                .for_each(|stored_accounts| {
                    let LoadAccountsIndexForShrink {
                        alive_accounts,
                        all_are_zero_lamports,
                        mut zero_lamport_single_ref_pubkeys,
                    } = self.load_accounts_index_for_shrink(stored_accounts, stats, slot);

                    // collect
                    let mut shrink_collect = shrink_collect.lock().unwrap();
                    shrink_collect.alive_accounts.collect(alive_accounts);
                    shrink_collect
                        .zero_lamport_single_ref_pubkeys
                        .append(&mut zero_lamport_single_ref_pubkeys);
                    if !all_are_zero_lamports {
                        shrink_collect.all_are_zero_lamports = false;
                    }
                });
        });

        index_read_elapsed.stop();

        let mut shrink_collect = shrink_collect.into_inner().unwrap();
        let alive_total_bytes = shrink_collect.alive_accounts.alive_bytes();
        shrink_collect.alive_total_bytes = alive_total_bytes;

        stats
            .index_read_elapsed
            .fetch_add(index_read_elapsed.as_us(), Ordering::Relaxed);

        stats.accounts_removed.fetch_add(
            total_starting_accounts - shrink_collect.alive_accounts.len(),
            Ordering::Relaxed,
        );
        stats.bytes_removed.fetch_add(
            capacity.saturating_sub(alive_total_bytes as u64),
            Ordering::Relaxed,
        );
        stats
            .bytes_written
            .fetch_add(alive_total_bytes as u64, Ordering::Relaxed);

        shrink_collect
    }

    /// These accounts were found during shrink of `slot` to be slot_list=[slot] and ref_count == 1 and lamports = 0.
    /// This means this slot contained the only account data for this pubkey and it is zero lamport.
    /// Thus, we did NOT treat this as an alive account, so we did NOT copy the zero lamport account to the new
    /// storage. So, the account will no longer be alive or exist at `slot`.
    /// So, first, remove the ref count since this newly shrunk storage will no longer access it.
    /// Second, remove `slot` from the index entry's slot list. If the slot list is now empty, then the
    /// pubkey can be removed completely from the index.
    /// In parallel with this code (which is running in the bg), the same pubkey could be revived and written to
    /// as part of tx processing. In that case, the slot list will contain a slot in the write cache and the
    /// index entry will NOT be deleted.
    pub(super) fn remove_zero_lamport_single_ref_accounts_after_shrink(
        &self,
        zero_lamport_single_ref_pubkeys: &[&Pubkey],
        slot: Slot,
        stats: &ShrinkStats,
    ) {
        stats.purged_zero_lamports.fetch_add(
            zero_lamport_single_ref_pubkeys.len() as u64,
            Ordering::Relaxed,
        );

        zero_lamport_single_ref_pubkeys.iter().for_each(|k| {
            _ = self.purge_keys_exact([(**k, slot)]);
        });
    }

    /// common code from shrink and combine_ancient_slots
    /// get rid of all original store_ids in the slot
    pub(crate) fn remove_old_stores_shrink<'a, T: ShrinkCollectRefs<'a>>(
        &self,
        shrink_collect: &ShrinkCollect<'a, T>,
        stats: &ShrinkStats,
        shrink_in_progress: Option<ShrinkInProgress>,
        shrink_can_be_active: bool,
    ) {
        let mut time = Measure::start("remove_old_stores_shrink");

        // handle the zero lamport alive accounts before calling clean
        // We have to update the index entries for these zero lamport pubkeys before we remove the storage in `mark_dirty_dead_stores`
        // that contained the accounts.
        self.remove_zero_lamport_single_ref_accounts_after_shrink(
            &shrink_collect.zero_lamport_single_ref_pubkeys,
            shrink_collect.slot,
            stats,
        );

        // Purge old, overwritten storage entries
        // This has the side effect of dropping `shrink_in_progress`, which removes the old storage completely. The
        // index has to be correct before we drop the old storage.
        let dead_storages = self.mark_dirty_dead_stores(
            shrink_collect.slot,
            // If all accounts are zero lamports, then we want to mark the entire OLD append vec as dirty.
            // otherwise, we'll call 'add_uncleaned_pubkeys_after_shrink' just on the unref'd keys below.
            shrink_collect.all_are_zero_lamports,
            shrink_in_progress,
            shrink_can_be_active,
        );
        let dead_storages_len = dead_storages.len();

        let (_, drop_storage_entries_elapsed) = measure_us!(drop(dead_storages));
        time.stop();

        self.appendvec_stats
            .dropped_stores
            .fetch_add(dead_storages_len as u64, Ordering::Relaxed);
        stats
            .drop_storage_entries_elapsed
            .fetch_add(drop_storage_entries_elapsed, Ordering::Relaxed);
        stats
            .remove_old_stores_shrink_us
            .fetch_add(time.as_us(), Ordering::Relaxed);
    }

    pub(crate) fn update_shrink_stats(
        shrink_stats: &ShrinkStats,
        stats_sub: ShrinkStatsSub,
        increment_count: bool,
    ) {
        if increment_count {
            shrink_stats
                .num_slots_shrunk
                .fetch_add(1, Ordering::Relaxed);
        }
        shrink_stats.create_and_insert_store_elapsed.fetch_add(
            stats_sub.create_and_insert_store_elapsed_us.0,
            Ordering::Relaxed,
        );
        shrink_stats.store_accounts_elapsed.fetch_add(
            stats_sub.store_accounts_timing.store_accounts_elapsed,
            Ordering::Relaxed,
        );
        shrink_stats.update_index_elapsed.fetch_add(
            stats_sub.store_accounts_timing.update_index_elapsed,
            Ordering::Relaxed,
        );
        shrink_stats.handle_reclaims_elapsed.fetch_add(
            stats_sub.store_accounts_timing.handle_reclaims_elapsed,
            Ordering::Relaxed,
        );
        shrink_stats
            .rewrite_elapsed
            .fetch_add(stats_sub.rewrite_elapsed_us.0, Ordering::Relaxed);
        shrink_stats
            .unpackable_slots_count
            .fetch_add(stats_sub.unpackable_slots_count.0 as u64, Ordering::Relaxed);
        shrink_stats.newest_alive_packed_count.fetch_add(
            stats_sub.newest_alive_packed_count.0 as u64,
            Ordering::Relaxed,
        );
    }

    /// get stores for 'slot'
    /// Drop 'shrink_in_progress', which will cause the old store to be removed from the storage map.
    /// For 'shrink_in_progress'.'old_storage' which is not retained, insert in 'dead_storages' and optionally 'dirty_stores'
    /// This is the end of the life cycle of `shrink_in_progress`.
    pub fn mark_dirty_dead_stores(
        &self,
        slot: Slot,
        add_dirty_stores: bool,
        shrink_in_progress: Option<ShrinkInProgress>,
        shrink_can_be_active: bool,
    ) -> Vec<Arc<AccountStorageEntry>> {
        let mut dead_storages = Vec::default();

        let mut not_retaining_store = |store: &Arc<AccountStorageEntry>| {
            if add_dirty_stores {
                self.dirty_stores.insert(slot, store.clone());
            }
            dead_storages.push(store.clone());
        };

        if let Some(shrink_in_progress) = shrink_in_progress {
            // shrink is in progress, so 1 new append vec to keep, 1 old one to throw away
            not_retaining_store(shrink_in_progress.old_storage());
            // dropping 'shrink_in_progress' removes the old append vec that was being shrunk from db's storage
        } else if let Some(store) = self.storage.remove(&slot, shrink_can_be_active) {
            // no shrink in progress, so all append vecs in this slot are dead
            not_retaining_store(&store);
        }

        dead_storages
    }

    fn get_roots_less_than(&self, slot: Slot) -> Vec<Slot> {
        self.storage_roots
            .read()
            .unwrap()
            .alive_roots
            .get_all_less_than(slot)
    }

    /// return all slots that are more than one epoch old and thus could already be an ancient append vec
    /// or which could need to be combined into a new or existing ancient append vec
    /// offset is used to combine newer slots than we normally would. This is designed to be used for testing.
    pub(super) fn get_sorted_potential_ancient_slots(
        &self,
        oldest_non_ancient_slot: Slot,
    ) -> Vec<Slot> {
        let mut ancient_slots = self.get_roots_less_than(oldest_non_ancient_slot);
        ancient_slots.sort_unstable();
        ancient_slots
    }

    /// Given the input `ShrinkCandidates`, this function sorts the stores by their alive ratio
    /// in increasing order with the most sparse entries in the front. It will then simulate the
    /// shrinking by working on the most sparse entries first and if the overall alive ratio is
    /// achieved, it will stop and return:
    /// first tuple element: the filtered-down candidates and
    /// second duple element: the candidates which
    /// are skipped in this round and might be eligible for the future shrink.
    pub(super) fn select_candidates_by_total_usage(
        &self,
        shrink_slots: &ShrinkCandidates,
        shrink_ratio: f64,
    ) -> (IntMap<Slot, Arc<AccountStorageEntry>>, ShrinkCandidates) {
        struct StoreUsageInfo {
            slot: Slot,
            alive_ratio: f64,
            store: Arc<AccountStorageEntry>,
        }
        let mut store_usage: Vec<StoreUsageInfo> = Vec::with_capacity(shrink_slots.len());
        let mut total_alive_bytes: u64 = 0;
        let mut total_bytes: u64 = 0;
        for slot in shrink_slots {
            let Some(store) = self.storage.get_slot_storage_entry(*slot) else {
                continue;
            };
            let alive_bytes = store.alive_bytes();
            total_alive_bytes += alive_bytes as u64;
            total_bytes += store.capacity();
            let alive_ratio = alive_bytes as f64 / store.capacity() as f64;
            store_usage.push(StoreUsageInfo {
                slot: *slot,
                alive_ratio,
                store: store.clone(),
            });
        }
        store_usage.sort_by(|a, b| {
            a.alive_ratio
                .partial_cmp(&b.alive_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Working from the beginning of store_usage which are the most sparse and see when we can stop
        // shrinking while still achieving the overall goals.
        let mut shrink_slots = IntMap::default();
        let mut shrink_slots_next_batch = ShrinkCandidates::default();
        for usage in &store_usage {
            let store = &usage.store;
            let alive_ratio = (total_alive_bytes as f64) / (total_bytes as f64);
            debug!(
                "alive_ratio: {:?} store_id: {:?}, store_ratio: {:?} requirement: {:?}, \
                 total_bytes: {:?} total_alive_bytes: {:?}",
                alive_ratio,
                usage.store.id(),
                usage.alive_ratio,
                shrink_ratio,
                total_bytes,
                total_alive_bytes
            );
            if alive_ratio > shrink_ratio {
                // we have reached our goal, stop
                debug!(
                    "Shrinking goal can be achieved at slot {:?}, total_alive_bytes: {:?} \
                     total_bytes: {:?}, alive_ratio: {:}, shrink_ratio: {:?}",
                    usage.slot, total_alive_bytes, total_bytes, alive_ratio, shrink_ratio
                );
                if usage.alive_ratio < shrink_ratio {
                    shrink_slots_next_batch.insert(usage.slot);
                } else {
                    break;
                }
            } else {
                let current_store_size = store.capacity();
                let after_shrink_size = store.alive_bytes() as u64;
                let bytes_saved = current_store_size.saturating_sub(after_shrink_size);
                total_bytes -= bytes_saved;
                shrink_slots.insert(usage.slot, Arc::clone(store));
            }
        }
        (shrink_slots, shrink_slots_next_batch)
    }

    /// return a store that can contain 'size' bytes
    pub fn get_store_for_shrink(&self, slot: Slot, size: u64) -> ShrinkInProgress<'_> {
        let shrunken_store = self.create_store(slot, size, "shrink", self.shrink_paths.as_slice());
        self.storage.shrinking_in_progress(slot, shrunken_store)
    }

    /// add all 'pubkeys' into the set of pubkeys that are 'uncleaned', associated with 'slot'
    /// clean will visit these pubkeys next time it runs
    pub(super) fn add_uncleaned_pubkeys_after_shrink(
        &self,
        slot: Slot,
        pubkeys: impl Iterator<Item = Pubkey>,
    ) {
        let mut uncleaned_pubkeys = self.uncleaned_pubkeys.entry(slot).or_default();
        uncleaned_pubkeys.extend(pubkeys);
    }

    /// each slot in 'dropped_roots' has been combined into an ancient append vec.
    /// We are done with the slot now forever.
    pub(crate) fn handle_dropped_roots_for_ancient(
        &self,
        dropped_roots: impl Iterator<Item = Slot>,
    ) {
        dropped_roots.for_each(|slot| {
            self.storage_roots.write().unwrap().clean_dead_slot(slot);
            // the storage has been removed from this slot and recycled or dropped
            assert!(self.storage.remove(&slot, false).is_none());
            debug_assert!(
                !self
                    .storage_roots
                    .read()
                    .unwrap()
                    .alive_roots
                    .contains(&slot),
                "slot: {slot}"
            );
        });
    }

    pub(crate) fn reopen_storage_as_readonly_shrinking_in_progress_ok(&self, slot: Slot) {
        if let Some(storage) = self
            .storage
            .get_slot_storage_entry_shrinking_in_progress_ok(slot)
        {
            if let Some(new_storage) = storage.reopen_as_readonly(self.storage_access) {
                assert_eq!(storage.id(), new_storage.id());
                assert_eq!(storage.accounts.len(), new_storage.accounts.len());
                self.storage
                    .replace_storage_with_equivalent(slot, Arc::new(new_storage));
            }
        }
    }

    fn should_not_shrink(alive_bytes: u64, total_bytes: u64) -> bool {
        alive_bytes >= total_bytes
    }

    pub(super) fn is_shrinking_productive(store: &AccountStorageEntry) -> bool {
        let alive_count = store.count();
        let total_bytes = store.capacity();
        let alive_bytes = store.alive_bytes_exclude_zero_lamport_single_ref_accounts() as u64;
        if AppendVecBackend::should_not_shrink(alive_bytes, total_bytes) {
            trace!(
                "shrink_slot_forced ({}): not able to shrink at all: num alive: {}, bytes alive: \
                 {}, bytes total: {}, bytes saved: {}",
                store.slot(),
                alive_count,
                alive_bytes,
                total_bytes,
                total_bytes.saturating_sub(alive_bytes),
            );
            return false;
        }

        true
    }

    /// Determines whether a given AccountStorageEntry instance is a
    /// candidate for shrinking.
    pub(crate) fn is_candidate_for_shrink(&self, store: &AccountStorageEntry) -> bool {
        // appended ancient append vecs should not be shrunk by the normal shrink codepath.
        // It is not possible to identify ancient append vecs when we pack, so no check for ancient when we are not appending.
        let total_bytes = store.capacity();

        let alive_bytes = store.alive_bytes_exclude_zero_lamport_single_ref_accounts() as u64;
        match self.shrink_ratio {
            AccountShrinkThreshold::TotalSpace { shrink_ratio: _ } => alive_bytes < total_bytes,
            AccountShrinkThreshold::IndividualStore { shrink_ratio } => {
                (alive_bytes as f64 / total_bytes as f64) < shrink_ratio
            }
        }
    }
}

#[allow(dead_code)]
pub(crate) trait AccountsShrink {
    fn shrink(&self, epoch_schedule: &EpochSchedule, include_ancient: bool) -> usize;
}

impl AccountsShrink for AccountsDb {
    fn shrink(&self, epoch_schedule: &EpochSchedule, include_ancient: bool) -> usize {
        AccountsDb::shrink(self, epoch_schedule, include_ancient)
    }
}
