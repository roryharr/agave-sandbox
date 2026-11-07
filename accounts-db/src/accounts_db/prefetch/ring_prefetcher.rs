//! The prefetch thread on io_uring. Each pass of its loop does two things:
//! - dispatch: takes up to `MAX_PUBKEYS_PER_PASS` pubkeys from the queue and, for each one
//!   worth reading, looks up where the account is stored and pushes a read of it onto the ring
//! - handle completions: handles completed reads, waiting briefly for one only when the queue
//!   is empty
//!
//! Completions only read: each one leaves its account in `PrefetchState::finished`, and the
//! thread stores it in the read cache right after the ring call that completed it. So the db is
//! touched only by the thread's own loop, which holds it for one pass at a time.

use {
    super::{FinishedRead, PrefetchAccountsStatsSub, PrefetchesInFlight},
    crate::{
        account_storage::meta_entry_read::MetaEntryBuffer,
        accounts_db::{
            AccountsDb, LoadedAccountAccessor,
            ring_account_read::{AccountRead, AccountReadContext, ReadOp},
        },
    },
    agave_io_uring::{Ring, RingAccess},
    io_uring::IoUring,
    log::*,
    shaq::mpmc::{Consumer, ReadBatch},
    solana_account::AccountSharedData,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        io, mem,
        num::NonZeroUsize,
        sync::{
            Weak,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

/// Accounts being read at once, each with one read at a time: its meta entry, then its data
/// entry. The ring's queues and op slab are sized to it. Bounds the prefetcher's share of the
/// device whatever pubkeys it is sent: the read rate is this over the read latency. Mainnet
/// peaked at 64, at an epoch boundary.
const MAX_IN_FLIGHT: u32 = 64;
/// Maximum number of kernel workers for reads of account data, and for reads that cannot complete
/// from the page cache and that the filesystem cannot wait on asynchronously. Used to offload long
/// copies from the page cache, and to avoid stalling the ring for milliseconds
const MAX_IOWQ_WORKERS: u32 = 4;
/// With reads in flight and the queue empty, how long to wait for one to complete before
/// checking the queue again: the most new pubkeys wait behind the reads already in flight, as a
/// caller cannot wake a wait on the ring
const COMPLETION_WAIT: Duration = Duration::from_micros(10);
/// Most pubkeys a pass takes from the queue: a run that pushes no reads, all cached or absent,
/// would otherwise keep completed reads waiting until it ends. Counts pubkeys, not reads: most
/// are cached or absent and push none, so this can exceed `MAX_IN_FLIGHT`. When a pass pushes
/// more reads than there are free slots, `Ring::push` waits for completions to free them.
const MAX_PUBKEYS_PER_PASS: NonZeroUsize = NonZeroUsize::new(128).unwrap();
/// With nothing in flight, how long to wait for pubkeys before checking whether to exit
const IDLE_WAIT: Duration = Duration::from_millis(100);

pub(super) struct RingPrefetcher {
    ring: Ring<PrefetchState, ReadOp>,
    /// upgraded for one pass at a time: held while idle, the db could never drop
    accounts_db: Weak<AccountsDb>,
    in_flight: PrefetchesInFlight,
}

impl RingPrefetcher {
    pub(super) fn new(accounts_db: Weak<AccountsDb>) -> io::Result<Self> {
        let ring = IoUring::builder()
            // the op slab is sized to the completion queue, so this is what caps reads in flight
            .setup_cqsize(MAX_IN_FLIGHT)
            .build(MAX_IN_FLIGHT)?;
        if let Err(err) = ring
            .submitter()
            .register_iowq_max_workers(&mut [MAX_IOWQ_WORKERS, 1])
        {
            warn!("prefetch kernel workers are not capped: {err}");
        }
        let state = PrefetchState {
            handling_submitted: false,
            stats_sub: PrefetchAccountsStatsSub::default(),
            meta_entry_buffers: (0..MAX_IN_FLIGHT)
                .map(|_| Box::new(MetaEntryBuffer::new()))
                .collect(),
            finished: Vec::with_capacity(MAX_IN_FLIGHT as usize),
        };
        Ok(Self {
            ring: Ring::new(ring, state),
            accounts_db,
            in_flight: PrefetchesInFlight::default(),
        })
    }

    pub(super) fn run(mut self, consumer: &Consumer<Pubkey>, exit: &AtomicBool) {
        if let Err(err) = self.do_run(consumer, exit) {
            error!("prefetch thread stopped: {err}");
        }
        // the ring's ops point into buffers they own; let them finish before freeing them
        if let Err(err) = self.ring.drain() {
            error!("prefetch thread failed to drain: {err}");
        }
    }

    fn do_run(&mut self, consumer: &Consumer<Pubkey>, exit: &AtomicBool) -> io::Result<()> {
        loop {
            let pubkeys = if self.in_flight.is_empty() {
                // nothing in flight means no completions to handle, so block until pubkeys arrive
                let Some(pubkeys) = wait_for_pubkeys(consumer, exit) else {
                    return Ok(());
                };
                Some(pubkeys)
            } else {
                consumer.try_reserve_read_batch(MAX_PUBKEYS_PER_PASS)
            };
            let Some(accounts_db) = self.accounts_db.upgrade() else {
                return Ok(());
            };
            // fewer pubkeys than asked for means the queue is empty, so `handle_completions` may
            // wait on the ring
            let queue_empty = match pubkeys {
                Some(pubkeys) => {
                    let queue_empty = pubkeys.len() < MAX_PUBKEYS_PER_PASS.get();
                    // read in place, in the queue's slots, released when `pubkeys` drops
                    self.dispatch(&accounts_db, pubkeys.iter())?;
                    queue_empty
                }
                None => true,
            };
            self.handle_completions(&accounts_db, queue_empty)?;
            let stats_sub = mem::take(&mut self.ring.context_mut().stats_sub);
            accounts_db.prefetcher.stats.accumulate_sub_stats(stats_sub);
        }
    }

    /// For each pubkey worth reading, looks up where the account is stored and pushes a read of
    /// it
    fn dispatch<'a>(
        &mut self,
        accounts_db: &AccountsDb,
        pubkeys: impl ExactSizeIterator<Item = &'a Pubkey>,
    ) -> io::Result<()> {
        self.ring.context_mut().stats_sub.num_received += pubkeys.len() as u64;
        for pubkey in pubkeys {
            let stats_sub = &mut self.ring.context_mut().stats_sub;
            if let Some((slot, accessor)) = self.in_flight.start(accounts_db, *pubkey, stats_sub) {
                self.push_read(*pubkey, slot, accessor)?;
                // pushing and submitting handle completions too
                let state = self.ring.context_mut();
                self.in_flight
                    .finish(accounts_db, state.finished.drain(..), &mut state.stats_sub);
            }
        }
        Ok(())
    }

    /// Pushes the read of the meta entry of the account at `accessor`
    fn push_read(
        &mut self,
        pubkey: Pubkey,
        slot: Slot,
        accessor: LoadedAccountAccessor,
    ) -> io::Result<()> {
        let state = self.ring.context_mut();
        state.stats_sub.max_in_flight = state.stats_sub.max_in_flight.max(self.in_flight.len());
        let Some(meta_entry_read) = accessor.construct_meta_entry_read() else {
            // the offset is at or past the file's end: nothing to prefetch
            state.stats_sub.num_past_end += 1;
            state.finished.push((pubkey, slot, None));
            return Ok(());
        };
        let account = AccountRead {
            pubkey,
            slot,
            accessor,
        };
        // a buffer from the pool of one per read in flight
        let buffer = state
            .meta_entry_buffers
            .pop()
            .unwrap_or_else(|| Box::new(MetaEntryBuffer::new()));
        if let Err(err) = self
            .ring
            .push(ReadOp::meta_entry(account, meta_entry_read, buffer))
        {
            // the read is abandoned, so nothing else finishes it
            self.ring.context_mut().finished.push((pubkey, slot, None));
            return Err(err);
        }
        // `push` only queues the read; submit it now rather than after the whole batch is
        // dispatched, which can be too late for the account's use
        self.submit()
    }

    /// Handles completed reads, first waiting briefly for one if `wait`
    fn handle_completions(&mut self, accounts_db: &AccountsDb, wait: bool) -> io::Result<()> {
        if self.in_flight.is_empty() {
            return Ok(());
        }
        if wait {
            self.ring.submit_and_wait(1, Some(COMPLETION_WAIT))?;
        }
        self.ring.process_completions()?;
        // submit the data reads completions queued, rather than after the next dispatch
        self.submit()?;
        let state = self.ring.context_mut();
        self.in_flight
            .finish(accounts_db, state.finished.drain(..), &mut state.stats_sub);
        Ok(())
    }

    /// Submits queued reads, then handles the ones that completed during submission: their data
    /// was in the page cache, and handling them now finishes their accounts without waiting for
    /// the next `handle_completions`
    fn submit(&mut self) -> io::Result<()> {
        self.ring.submit()?;
        self.ring.context_mut().handling_submitted = true;
        let result = self.ring.process_completions();
        self.ring.context_mut().handling_submitted = false;
        result
    }
}

/// Blocks until pubkeys arrive, waking every `IDLE_WAIT` to check `exit`. `None` once it is set.
fn wait_for_pubkeys<'a>(
    consumer: &'a Consumer<Pubkey>,
    exit: &AtomicBool,
) -> Option<ReadBatch<'a, Pubkey>> {
    loop {
        if let Ok(pubkeys) = consumer.reserve_read_batch_timeout(MAX_PUBKEYS_PER_PASS, IDLE_WAIT) {
            return Some(pubkeys);
        }
        if exit.load(Ordering::Relaxed) {
            return None;
        }
    }
}

/// What completions need, held by the ring
struct PrefetchState {
    /// set while handling the completions posted during their own submission
    handling_submitted: bool,
    /// what this pass did so far
    stats_sub: PrefetchAccountsStatsSub,
    /// Buffers for meta entry reads, one per read in flight, allocated once
    meta_entry_buffers: Vec<Box<MetaEntryBuffer>>,
    /// Reads done with, for the thread to finish: the account read, or `None` when there is
    /// none to store
    finished: Vec<FinishedRead>,
}

impl AccountReadContext for PrefetchState {
    fn meta_entry_buffers(&mut self) -> &mut Vec<Box<MetaEntryBuffer>> {
        &mut self.meta_entry_buffers
    }

    fn count_read(&mut self, is_data_read: bool, read_us: u64) {
        let stats_sub = &mut self.stats_sub;
        if is_data_read {
            // always on a kernel worker
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
        self.finished.push((account.pubkey, account.slot, loaded));
    }
}
