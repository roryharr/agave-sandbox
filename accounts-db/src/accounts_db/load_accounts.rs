//! Loads of many accounts at once, each handed to the caller as soon as it is loaded.
//!
//! Accounts a cache holds are handed over first. The rest are read from storage, in parallel on
//! io_uring: each thread that loads has a ring of its own, so a caller never waits on another's
//! reads. Without io_uring, or when the thread's ring is in use, they are read one at a time.

#[cfg(target_os = "linux")]
use {
    super::ring_account_read::{AccountRead, AccountReadContext, ReadOp},
    crate::account_storage::meta_entry_read::MetaEntryBuffer,
    agave_io_uring::{Ring, RingAccess},
    io_uring::IoUring,
    log::warn,
    std::{
        cell::{OnceCell, RefCell},
        mem,
        time::Duration,
    },
};
use {
    super::{stats::LoadAccountsBatchStatsSub, AccountsDb, LoadedAccountAccessor},
    crate::{ancestors::Ancestors, is_zero_lamport::IsZeroLamport},
    solana_account::AccountSharedData,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{sync::atomic::Ordering, time::Instant},
};

/// Accounts a thread reads at once, each with one read at a time: its meta entry, then its data
/// entry. The ring's queues and op slab are sized to it.
#[cfg(target_os = "linux")]
const MAX_IN_FLIGHT: u32 = 64;
/// Maximum number of kernel workers for reads of account data, per thread
#[cfg(target_os = "linux")]
const MAX_IOWQ_WORKERS: u32 = 4;

#[cfg(target_os = "linux")]
thread_local! {
    /// This thread's ring, created on its first load from storage; `None` if it cannot be
    static LOAD_RING: OnceCell<Option<RefCell<Ring<LoadState, ReadOp>>>> = const { OnceCell::new() };
}

impl AccountsDb {
    /// Loads each of `pubkeys` as `load` does, populating the read cache and with no load
    /// filter, and hands it to `on_load` as soon as it is loaded: those a cache holds first, then
    /// the rest as their reads from storage complete, in no particular order.
    pub fn load_accounts(
        &self,
        ancestors: &Ancestors,
        pubkeys: impl IntoIterator<Item = Pubkey>,
        mut on_load: impl FnMut(Pubkey, Option<(AccountSharedData, Slot)>),
    ) {
        let mut stats_sub = LoadAccountsBatchStatsSub::default();
        let mut stored = Vec::new();
        for pubkey in pubkeys {
            stats_sub.num_pubkeys += 1;
            match self.load_cached_or_locate(ancestors, &pubkey) {
                LoadOrLocate::Loaded(account) => on_load(pubkey, account),
                LoadOrLocate::Stored(slot, accessor) => stored.push((pubkey, slot, accessor)),
            }
        }
        if !stored.is_empty() {
            stats_sub.num_stored = stored.len() as u64;
            let start = Instant::now();
            self.load_stored(stored, &mut stats_sub, &mut on_load);
            stats_sub.storage_us = start.elapsed().as_micros() as u64;
        }
        self.load_accounts_batch_stats
            .accumulate_sub_stats(stats_sub);
    }

    /// Loads `stored` from storage: on this thread's ring if it can, else one at a time
    fn load_stored(
        &self,
        stored: Vec<(Pubkey, Slot, LoadedAccountAccessor)>,
        stats_sub: &mut LoadAccountsBatchStatsSub,
        on_load: &mut impl FnMut(Pubkey, Option<(AccountSharedData, Slot)>),
    ) {
        #[cfg(target_os = "linux")]
        let stored = match self.load_on_ring(stored, stats_sub, on_load) {
            Ok(()) => return,
            Err(stored) => stored,
        };
        #[cfg(not(target_os = "linux"))]
        {
            stats_sub.num_no_ring_reads += stored.len() as u64;
        }
        for (pubkey, slot, mut accessor) in stored {
            let account = accessor.get_account_shared_data_if(None::<fn(_, &_, _) -> _>);
            self.finish_load(pubkey, slot, account, on_load);
        }
    }

    /// Loads `pubkey` from a cache, as `do_load` does; failing that, looks up where it is stored
    fn load_cached_or_locate(&self, ancestors: &Ancestors, pubkey: &Pubkey) -> LoadOrLocate {
        if let Some((cached_account, slot)) = self.accounts_cache.load_latest(pubkey, ancestors) {
            self.load_account_stats
                .num_loaded_from_write_cache
                .fetch_add(1, Ordering::Relaxed);
            let account = cached_account.account.clone();
            return LoadOrLocate::Loaded((!account.is_zero_lamport()).then_some((account, slot)));
        }
        if let Some((account, slot)) = self
            .read_only_accounts_cache
            .load(pubkey, |slot| ancestors.is_ancestor(slot))
        {
            self.load_account_stats
                .num_loaded_from_read_cache
                .fetch_add(1, Ordering::Relaxed);
            return LoadOrLocate::Loaded((!account.is_zero_lamport()).then_some((account, slot)));
        }
        let Some((slot, accessor)) = self.get_account_accessor_from_index(pubkey, ancestors) else {
            return LoadOrLocate::Loaded(None);
        };
        self.load_account_stats
            .num_loaded_from_index_storage
            .fetch_add(1, Ordering::Relaxed);
        LoadOrLocate::Stored(slot, accessor)
    }

    /// Hands over an account read from storage, storing it in the read cache as `do_load` does
    fn finish_load(
        &self,
        pubkey: Pubkey,
        slot: Slot,
        account: Option<AccountSharedData>,
        on_load: &mut impl FnMut(Pubkey, Option<(AccountSharedData, Slot)>),
    ) {
        let account = account.filter(|account| !account.is_zero_lamport());
        if let Some(account) = &account {
            self.store_in_read_cache_if_newest(&pubkey, slot, account);
        }
        on_load(pubkey, account.map(|account| (account, slot)));
    }

    /// Reads `stored` on this thread's ring, handing each account over as its read completes.
    /// Gives `stored` back, unread, if the ring cannot be created or is in use.
    #[cfg(target_os = "linux")]
    fn load_on_ring(
        &self,
        stored: Vec<(Pubkey, Slot, LoadedAccountAccessor)>,
        stats_sub: &mut LoadAccountsBatchStatsSub,
        on_load: &mut impl FnMut(Pubkey, Option<(AccountSharedData, Slot)>),
    ) -> Result<(), Vec<(Pubkey, Slot, LoadedAccountAccessor)>> {
        LOAD_RING.with(|ring| {
            let Some(ring) = ring.get_or_init(new_load_ring) else {
                stats_sub.num_no_ring_reads += stored.len() as u64;
                return Err(stored);
            };
            // in use when `on_load` loads again
            let Ok(mut ring) = ring.try_borrow_mut() else {
                stats_sub.num_ring_busy_reads += stored.len() as u64;
                return Err(stored);
            };
            // counted in the ring's state, which completions count reads in, for this call
            ring.context_mut().stats_sub = mem::take(stats_sub);
            let mut ring_wait = Duration::ZERO;
            let mut num_in_flight = 0;
            for (pubkey, slot, mut accessor) in stored {
                let Some(meta_entry_read) = accessor.construct_meta_entry_read() else {
                    // the offset is at or past the file's end: the blocking read says what that
                    // means
                    ring.context_mut().stats_sub.num_past_end += 1;
                    let account = accessor.get_account_shared_data_if(None::<fn(_, &_, _) -> _>);
                    self.finish_load(pubkey, slot, account, on_load);
                    continue;
                };
                let buffer = ring
                    .context_mut()
                    .meta_entry_buffers
                    .pop()
                    .unwrap_or_else(|| Box::new(MetaEntryBuffer::new()));
                let account = AccountRead {
                    pubkey,
                    slot,
                    accessor,
                };
                // as a blocking read panics on a storage it cannot read, so does this on a ring
                // it cannot use
                ring.push(ReadOp::meta_entry(account, meta_entry_read, buffer))
                    .expect("push an account read");
                num_in_flight += 1;
                // submit now, so the first accounts are handed over while the rest are pushed;
                // those in the page cache complete during submission
                ring.submit().expect("submit account reads");
                ring.context_mut().handling_submitted = true;
                ring.process_completions()
                    .expect("process account read completions");
                ring.context_mut().handling_submitted = false;
                num_in_flight -= self.finish_ring_loads(ring.context_mut(), on_load);
            }
            while num_in_flight > 0 {
                let wait_start = Instant::now();
                // also submits the data entry reads completions pushed
                ring.submit_and_wait(1, None)
                    .expect("wait for account reads");
                ring_wait += wait_start.elapsed();
                ring.process_completions()
                    .expect("process account read completions");
                num_in_flight -= self.finish_ring_loads(ring.context_mut(), on_load);
            }
            *stats_sub = mem::take(&mut ring.context_mut().stats_sub);
            stats_sub.ring_wait_us = ring_wait.as_micros() as u64;
            Ok(())
        })
    }

    /// Hands over the accounts whose reads are done with, reading again those that failed.
    /// Returns how many reads were done with.
    #[cfg(target_os = "linux")]
    fn finish_ring_loads(
        &self,
        state: &mut LoadState,
        on_load: &mut impl FnMut(Pubkey, Option<(AccountSharedData, Slot)>),
    ) -> usize {
        let num_finished = state.finished.len();
        for (pubkey, slot, account) in state.finished.drain(..) {
            self.finish_load(pubkey, slot, Some(account), on_load);
        }
        let num_failed = state.failed.len();
        state.stats_sub.num_failed_reads += num_failed as u64;
        for (pubkey, slot, mut accessor) in state.failed.drain(..) {
            let account = accessor.get_account_shared_data_if(None::<fn(_, &_, _) -> _>);
            self.finish_load(pubkey, slot, account, on_load);
        }
        num_finished + num_failed
    }
}

/// What `load_cached_or_locate` found
enum LoadOrLocate {
    /// the account `load` would return: it is in a cache, or nowhere
    Loaded(Option<(AccountSharedData, Slot)>),
    /// in storage, at the slot and accessor the index refers to
    Stored(Slot, LoadedAccountAccessor),
}

#[cfg(target_os = "linux")]
fn new_load_ring() -> Option<RefCell<Ring<LoadState, ReadOp>>> {
    if !agave_io_uring::io_uring_supported() {
        return None;
    }
    let ring = IoUring::builder()
        // the op slab is sized to the completion queue, so this is what caps reads in flight
        .setup_cqsize(MAX_IN_FLIGHT)
        .build(MAX_IN_FLIGHT)
        .inspect_err(|err| warn!("loads read accounts one at a time: {err}"))
        .ok()?;
    if let Err(err) = ring
        .submitter()
        .register_iowq_max_workers(&mut [MAX_IOWQ_WORKERS, 1])
    {
        warn!("load kernel workers are not capped: {err}");
    }
    let state = LoadState {
        handling_submitted: false,
        stats_sub: LoadAccountsBatchStatsSub::default(),
        meta_entry_buffers: Vec::new(),
        finished: Vec::new(),
        failed: Vec::new(),
    };
    Some(RefCell::new(Ring::new(ring, state)))
}

/// What completions need, held by the ring
#[cfg(target_os = "linux")]
struct LoadState {
    /// Whether completions are handled right after their submission: the reads completed during
    /// it, so their meta entry was in the page cache
    handling_submitted: bool,
    /// what the current call did so far
    stats_sub: LoadAccountsBatchStatsSub,
    /// Buffers for meta entry reads, allocated as the reads in flight first need them
    meta_entry_buffers: Vec<Box<MetaEntryBuffer>>,
    /// Accounts read, to hand over
    finished: Vec<(Pubkey, Slot, AccountSharedData)>,
    /// Accounts whose read did not produce one, to read again one at a time
    failed: Vec<(Pubkey, Slot, LoadedAccountAccessor)>,
}

#[cfg(target_os = "linux")]
impl AccountReadContext for LoadState {
    fn meta_entry_buffers(&mut self) -> &mut Vec<Box<MetaEntryBuffer>> {
        &mut self.meta_entry_buffers
    }

    fn count_read(&mut self, is_data_read: bool, read_us: u64) {
        let stats_sub = &mut self.stats_sub;
        if is_data_read {
            stats_sub.num_data_reads += 1;
        } else if self.handling_submitted {
            stats_sub.num_page_cache_reads += 1;
        } else {
            stats_sub.num_disk_reads += 1;
        }
        stats_sub.read_us += read_us;
        stats_sub.max_read_us = stats_sub.max_read_us.max(read_us);
    }

    fn finish(&mut self, account: &AccountRead, loaded: Option<AccountSharedData>) {
        match loaded {
            Some(loaded) => self.finished.push((account.pubkey, account.slot, loaded)),
            None => self
                .failed
                .push((account.pubkey, account.slot, account.accessor.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::accounts_db::PopulateReadCache, std::collections::HashMap};

    /// Stores, at rooted slot 0, an account whose data fits its meta entry, one whose data does
    /// not, and a zero lamport one, then one at unrooted slot 1. Returns their pubkeys, and one
    /// that is stored nowhere.
    fn store_accounts(db: &AccountsDb) -> Vec<Pubkey> {
        let owner = Pubkey::new_unique();
        let small = AccountSharedData::new(1, 10, &owner);
        let mut large = AccountSharedData::new(2, 10_000, &owner);
        large.data_as_mut_slice().fill(7);
        let zero_lamport = AccountSharedData::new(0, 0, &owner);
        let cached = AccountSharedData::new(3, 10, &owner);
        let pubkeys: Vec<_> = (0..5).map(|_| Pubkey::new_unique()).collect();
        db.store_for_tests((
            0,
            &[
                (&pubkeys[0], &small),
                (&pubkeys[1], &large),
                (&pubkeys[2], &zero_lamport),
            ][..],
        ));
        db.add_root_and_flush_write_cache(0);
        db.store_for_tests((1, &[(&pubkeys[3], &cached)][..]));
        pubkeys
    }

    #[test]
    fn test_load_accounts_matches_load() {
        let db = AccountsDb::default_for_tests();
        let pubkeys = store_accounts(&db);
        let ancestors = Ancestors::from(vec![0, 1]);

        let mut loaded = HashMap::new();
        db.load_accounts(&ancestors, pubkeys.iter().copied(), |pubkey, account| {
            assert!(loaded.insert(pubkey, account).is_none());
        });

        assert_eq!(loaded.len(), pubkeys.len());
        for pubkey in &pubkeys {
            let expected = db.load(
                &ancestors,
                pubkey,
                PopulateReadCache::False,
                None::<fn(_, &_, _) -> _>,
            );
            assert_eq!(loaded[pubkey], expected);
        }
        // read from storage, so stored in the read cache, as `load` would
        assert!(db.read_only_accounts_cache.contains_key(&pubkeys[0]));
        assert!(db.read_only_accounts_cache.contains_key(&pubkeys[1]));
        assert!(!db.read_only_accounts_cache.contains_key(&pubkeys[2]));
        // the three accounts at slot 0 are read from storage
        let stats = &db.load_accounts_batch_stats;
        assert_eq!(stats.num_calls.load(Ordering::Relaxed), 1);
        assert_eq!(stats.num_pubkeys.load(Ordering::Relaxed), 5);
        assert_eq!(stats.num_stored.load(Ordering::Relaxed), 3);
        assert_eq!(
            stats.num_calls_many_storage_reads.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_load_accounts_from_on_load() {
        let db = AccountsDb::default_for_tests();
        let pubkeys = store_accounts(&db);
        let ancestors = Ancestors::from(vec![0, 1]);

        // a load from `on_load` finds the thread's ring in use, and reads one at a time
        let mut loaded = HashMap::new();
        db.load_accounts(&ancestors, [pubkeys[0]], |_, _| {
            db.load_accounts(&ancestors, [pubkeys[1]], |pubkey, account| {
                loaded.insert(pubkey, account);
            });
        });

        let expected = db.load(
            &ancestors,
            &pubkeys[1],
            PopulateReadCache::False,
            None::<fn(_, &_, _) -> _>,
        );
        assert!(expected.is_some());
        assert_eq!(loaded[&pubkeys[1]], expected);
    }
}
