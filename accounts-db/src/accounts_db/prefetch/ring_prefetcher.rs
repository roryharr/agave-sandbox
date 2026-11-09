//! The prefetch thread on io_uring. Each pass of its loop does two things:
//! - dispatch: takes incoming batches and, for each pubkey worth reading, looks up where the
//!   account is stored and pushes a read of it onto the ring
//! - reap: waits briefly for reads to complete; each completion stores its account in the read
//!   cache, or reads the account's data when it continues past what was read
//!
//! An account's size is unknown until its header is read, so the first read is a page; nearly
//! every account fits. The rest of the account's data is read straight into the account's own
//! buffer, on a kernel worker: even from the page cache, copying a large account would stall
//! every other read for milliseconds.

use {
    super::{Outstanding, PrefetchAccountsStatsSub},
    crate::{
        account_storage_entry::AccountStorageEntry,
        accounts_db::{AccountsDb, LoadedAccountAccessor},
        accounts_file::{AccountDataRead, AccountFromPage},
        append_vec::PAGE_SIZE,
    },
    agave_io_uring::{Completion, Ring, RingAccess, RingOp},
    crossbeam_channel::Receiver,
    io_uring::{IoUring, opcode, squeue, types},
    log::*,
    solana_account::AccountSharedData,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        io::{self, IoSliceMut},
        mem,
        os::fd::AsRawFd,
        slice,
        sync::{Arc, Weak},
        time::{Duration, Instant},
    },
};

/// Reads in flight at once; the ring's completion queue and op slab are sized to it
const MAX_IN_FLIGHT: u32 = 256;
/// Submission queue entries; a full queue is submitted to the kernel to make room
const SUBMISSION_QUEUE_SIZE: u32 = 64;
/// Maximum number of kernel workers for reads of account data, and for reads that cannot complete
/// from the page cache and that the filesystem cannot wait on asynchronously. Used to offload long
/// copies from the page cache, and to avoid stalling the ring for milliseconds
const MAX_IOWQ_WORKERS: u32 = 4;
/// While reads are in flight, how long to wait for one to complete before taking new batches:
/// the most a batch waits behind the reads already in flight. Roughly the latency of 1 ssd read
const COMPLETION_WAIT: Duration = Duration::from_micros(50);
/// Pubkeys dispatched between reaps: a batch of mostly cached pubkeys pushes nothing, and its
/// completions must not wait for the whole batch. Two cache probes per pubkey keep this near
/// `COMPLETION_WAIT`.
const DISPATCH_REAP_INTERVAL: usize = 128;

pub(super) struct RingPrefetcher {
    ring: Ring<PrefetchState, ReadOp>,
}

impl RingPrefetcher {
    pub(super) fn new(accounts_db: Weak<AccountsDb>) -> io::Result<Self> {
        let ring = IoUring::builder()
            .setup_cqsize(MAX_IN_FLIGHT)
            .build(SUBMISSION_QUEUE_SIZE)?;
        if let Err(err) = ring
            .submitter()
            .register_iowq_max_workers(&mut [MAX_IOWQ_WORKERS, 1])
        {
            warn!("prefetch kernel workers are not capped: {err}");
        }
        let state = PrefetchState {
            accounts_db,
            pass_db: None,
            outstanding: Outstanding::default(),
            num_in_flight: 0,
            reaping_submitted: false,
            stats_sub: PrefetchAccountsStatsSub::default(),
            page_buffers: (0..MAX_IN_FLIGHT)
                .map(|_| Vec::with_capacity(PAGE_SIZE))
                .collect(),
        };
        Ok(Self {
            ring: Ring::new(ring, state),
        })
    }

    pub(super) fn run(mut self, batch_receiver: Receiver<Vec<Pubkey>>) {
        if let Err(err) = self.do_run(&batch_receiver) {
            error!("prefetch thread stopped: {err}");
        }
        // the ring's ops point into buffers they own; let them finish before freeing them
        if let Err(err) = self.ring.drain() {
            error!("prefetch thread failed to drain: {err}");
        }
    }

    fn do_run(&mut self, batch_receiver: &Receiver<Vec<Pubkey>>) -> io::Result<()> {
        loop {
            // Nothing in flight means nothing to reap, so block until a batch arrives and hold
            // the db for the pass it starts. With reads in flight, `reap` waits on the ring
            // instead, and new batches wait for it.
            if self.ring.context().num_in_flight == 0 {
                let Ok(pubkeys) = batch_receiver.recv() else {
                    return Ok(());
                };
                let Some(accounts_db) = self.ring.context().accounts_db.upgrade() else {
                    return Ok(());
                };
                self.ring.context_mut().pass_db = Some(accounts_db);
                self.dispatch(pubkeys)?;
            }
            for pubkeys in batch_receiver.try_iter() {
                self.dispatch(pubkeys)?;
            }
            self.reap()?;
            let state = self.ring.context_mut();
            let stats_sub = mem::take(&mut state.stats_sub);
            state.db().prefetcher.stats.accumulate_sub_stats(stats_sub);
            if state.num_in_flight == 0 {
                state.pass_db = None;
            }
        }
    }

    /// For each pubkey worth reading, looks up where the account is stored and pushes a read of
    /// it
    fn dispatch(&mut self, pubkeys: Vec<Pubkey>) -> io::Result<()> {
        let state = self.ring.context_mut();
        state.stats_sub.num_batches += 1;
        state.stats_sub.max_batch_len = state.stats_sub.max_batch_len.max(pubkeys.len());
        let accounts_db = Arc::clone(state.db());
        // a full ring makes `push` reap completions until there is room
        for pubkeys in pubkeys.chunks(DISPATCH_REAP_INTERVAL) {
            for pubkey in pubkeys {
                let state = self.ring.context_mut();
                if let Some((slot, accessor)) = accounts_db.read_for_prefetch(
                    *pubkey,
                    &mut state.outstanding,
                    &mut state.stats_sub,
                ) {
                    self.push_read(*pubkey, slot, accessor)?;
                }
            }
            self.ring.process_completions()?;
        }
        Ok(())
    }

    /// Pushes the first read of the account at `accessor`, or loads it on this thread when there
    /// is nothing to read
    fn push_read(
        &mut self,
        pubkey: Pubkey,
        slot: Slot,
        mut accessor: LoadedAccountAccessor,
    ) -> io::Result<()> {
        let state = self.ring.context_mut();
        state.num_in_flight += 1;
        state.stats_sub.max_in_flight = state.stats_sub.max_in_flight.max(state.num_in_flight);
        let LoadedAccountAccessor {
            storage_entry: storage,
            offset,
        } = &accessor;
        let Some((offset, len)) = storage.accounts.account_page_read(*offset) else {
            // nothing to read at the offset: a load on this thread settles it
            state.stats_sub.num_blocking_reads += 1;
            let account = accessor.get_account_shared_data_if(None::<fn(_, &_, _) -> _>);
            self.ring.context_mut().finish(pubkey, slot, account);
            return Ok(());
        };
        let storage = Arc::clone(storage);
        // the first read's buffer: a page, from the pool of one per read in flight
        let buf = self
            .ring
            .context_mut()
            .page_buffers
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(PAGE_SIZE));
        if let Err(err) = self.ring.push(ReadOp {
            pubkey,
            slot,
            storage,
            offset,
            buf,
            len,
            data_read: None,
            iovecs: None,
            pushed: Instant::now(),
        }) {
            // the read is abandoned, so nothing else finishes it
            self.ring.context_mut().finish(pubkey, slot, None);
            return Err(err);
        }
        // `push` only queues the read; submit it now rather than after the whole batch is
        // dispatched, which can be too late for the account's use
        self.submit()
    }

    /// Waits briefly for reads to complete; each completion finishes its read or reads more
    fn reap(&mut self) -> io::Result<()> {
        if self.ring.context().num_in_flight == 0 {
            return Ok(());
        }
        self.ring.submit_and_wait(1, Some(COMPLETION_WAIT))?;
        self.ring.process_completions()?;
        // submit the whole-account reads completions queued, rather than after the next dispatch
        self.submit()
    }

    /// Submits queued reads, then handles the ones that completed during submission: their data
    /// was in the page cache, and handling them now stores their accounts without waiting for
    /// the next reap
    fn submit(&mut self) -> io::Result<()> {
        self.ring.submit()?;
        self.ring.context_mut().reaping_submitted = true;
        let result = self.ring.process_completions();
        self.ring.context_mut().reaping_submitted = false;
        result
    }
}

/// What completions need, held by the ring so they can finish their reads themselves
struct PrefetchState {
    accounts_db: Weak<AccountsDb>,
    /// held for a pass, and let go while idle on the channel: held then, the db could never drop
    pass_db: Option<Arc<AccountsDb>>,
    outstanding: Outstanding,
    /// accounts whose reads have been pushed and not finished
    num_in_flight: u32,
    /// set while handling the completions posted during their own submission
    reaping_submitted: bool,
    /// what this pass did so far
    stats_sub: PrefetchAccountsStatsSub,
    /// Buffers for first reads, one per read in flight, allocated once. A first read is a page,
    /// whatever the account's size: the header in it says how much more to read, and that is
    /// read into a buffer of its own, which becomes the account's.
    page_buffers: Vec<Vec<u8>>,
}

impl PrefetchState {
    /// The db, held for the pass
    fn db(&self) -> &Arc<AccountsDb> {
        self.pass_db.as_ref().expect("held for the pass")
    }

    /// Stores a finished read's account in the read cache and clears its pubkey from
    /// `outstanding`
    fn finish(&mut self, pubkey: Pubkey, slot: Slot, account: Option<AccountSharedData>) {
        self.num_in_flight -= 1;
        self.pass_db
            .as_ref()
            .expect("held for the pass")
            .finish_prefetch(pubkey, slot, account, &mut self.outstanding);
    }
}

/// A read at `offset`: the first `len` bytes of an account, or the rest of it
struct ReadOp {
    pubkey: Pubkey,
    slot: Slot,
    /// keeps the file open, and the account in it, until the read completes
    storage: Arc<AccountStorageEntry>,
    offset: u64,
    /// read into its spare capacity, which holds `len` bytes: a page buffer for the first read,
    /// the account's own for its data
    buf: Vec<u8>,
    len: usize,
    /// set for the read finishing the account, after the first read parsed its header
    data_read: Option<AccountDataRead>,
    /// that read's buffers, set when it is submitted: `data_read`'s data entry, then `buf`
    iovecs: Option<[IoSliceMut<'static>; 2]>,
    pushed: Instant,
}

impl RingOp<PrefetchState> for ReadOp {
    fn entry(&mut self) -> squeue::Entry {
        let Some(data_read) = &mut self.data_read else {
            let fd = types::Fd(self.storage.accounts.file().as_raw_fd());
            return opcode::Read::new(fd, self.buf.as_mut_ptr(), self.len as u32)
                .offset(self.offset)
                .build();
        };
        let (data_file, _) = self
            .storage
            .accounts
            .account_data_file()
            .expect("a read finishing an account is of its data file");
        let data_entry = data_read.data_entry_mut();
        // SAFETY: both point into this op, which the ring keeps in place until the read completes
        let iovecs = self.iovecs.insert(unsafe {
            [
                IoSliceMut::new(slice::from_raw_parts_mut(
                    data_entry.as_mut_ptr(),
                    data_entry.len(),
                )),
                IoSliceMut::new(slice::from_raw_parts_mut(self.buf.as_mut_ptr(), self.len)),
            ]
        });
        let fd = types::Fd(data_file.as_raw_fd());
        opcode::Readv::new(fd, iovecs.as_ptr().cast(), iovecs.len() as u32)
            .offset(self.offset)
            .build()
            // copy on a worker, not on this thread during submit
            .flags(squeue::Flags::ASYNC)
    }

    fn complete(
        &mut self,
        completion: &mut Completion<PrefetchState, Self>,
        res: io::Result<i32>,
    ) -> io::Result<()> {
        let state = completion.context_mut();
        let read_us = self.pushed.elapsed().as_micros() as u64;
        // only an account's first read: its data read always goes to a kernel worker
        if self.data_read.is_none() {
            if state.reaping_submitted {
                state.stats_sub.num_page_cache_reads += 1;
            } else {
                state.stats_sub.num_disk_reads += 1;
            }
        } else {
            state.stats_sub.num_data_reads += 1;
        }
        state.stats_sub.read_us += read_us;
        state.stats_sub.max_read_us = state.stats_sub.max_read_us.max(read_us);
        let bytes_read = res.ok().map(|bytes_read| bytes_read as usize);
        let account = match self.data_read.take() {
            None => {
                let from_page = bytes_read.and_then(|bytes_read| {
                    // SAFETY: the read initialized the first `bytes_read` bytes of spare capacity
                    unsafe { self.buf.set_len(bytes_read) };
                    self.storage
                        .accounts
                        .account_from_page(self.offset, &self.buf)
                });
                // the page buffer is done with: what it held is parsed or copied out
                let mut page_buffer = mem::take(&mut self.buf);
                page_buffer.clear();
                completion.context_mut().page_buffers.push(page_buffer);
                match from_page {
                    Some(AccountFromPage::Complete(account)) => Some(account),
                    Some(AccountFromPage::Partial(data_read)) => {
                        let len = data_read.data_len();
                        // still in flight: the next read finishes the account
                        return completion.push(ReadOp {
                            pubkey: self.pubkey,
                            slot: self.slot,
                            storage: Arc::clone(&self.storage),
                            offset: data_read.offset(),
                            buf: Vec::with_capacity(len),
                            len,
                            data_read: Some(data_read),
                            iovecs: None,
                            pushed: Instant::now(),
                        });
                    }
                    None => None,
                }
            }
            Some(mut data_read) => {
                let read_len = data_read.data_entry_mut().len() + self.len;
                // a failed or short read is left to a load on the critical path
                (bytes_read == Some(read_len))
                    .then(|| {
                        // SAFETY: the read filled the data entry, then `len` bytes of spare
                        // capacity
                        unsafe { self.buf.set_len(self.len) };
                        data_read.account(mem::take(&mut self.buf))
                    })
                    .flatten()
            }
        };
        completion
            .context_mut()
            .finish(self.pubkey, self.slot, account);
        Ok(())
    }
}
