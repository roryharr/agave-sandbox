//! Background loads that warm the read cache ahead of the caller's critical path.
//!
//! One thread does all of it. For each batch it drops pubkeys a cache holds or a prefetch is
//! already loading, looks the rest up in the index and reads them from storage; for each finished
//! read it stores the account in the read cache. On io_uring many reads are in flight at once;
//! without io_uring there is no prefetch.
//!
//! The thread alone owns the set of outstanding pubkeys, so the set needs no lock.
//!
//! With nothing in flight, the thread blocks on its queue, so a batch starts at once, at the
//! cost of the sender waking it. With reads in flight it waits on the ring instead, so a sender
//! has nothing to wake: the batch waits for the next completion or a short timeout.

// The io_uring prefetcher is the only one; without it, the db's half of the protocol is unused
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

#[cfg(target_os = "linux")]
mod ring_prefetcher;

use {
    super::{
        AccountsDb, LoadedAccountAccessor,
        stats::{PrefetchAccountsStats, PrefetchAccountsStatsSub},
    },
    crate::{ancestors::Ancestors, is_zero_lamport::IsZeroLamport},
    crossbeam_channel::{Receiver, Sender, TrySendError, bounded},
    solana_account::AccountSharedData,
    solana_clock::Slot,
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    std::{
        collections::HashSet,
        sync::{Weak, atomic::Ordering},
    },
};
#[cfg(target_os = "linux")]
use {log::warn, ring_prefetcher::RingPrefetcher, std::thread};

/// Batches waiting for the prefetch thread; once full, new batches are dropped
const MAX_QUEUED_BATCHES: usize = 64;

type Outstanding = HashSet<Pubkey, PubkeyHasherBuilder>;

#[derive(Debug)]
pub(crate) struct AccountsPrefetcher {
    batch_sender: Sender<Vec<Pubkey>>,
    /// Kept apart from `load_account_stats` so a prefetch's own storage read is not counted
    /// as a miss on the critical path
    pub(crate) stats: PrefetchAccountsStats,
}

impl AccountsPrefetcher {
    /// Starts the prefetch thread, where there is io_uring; elsewhere there is no prefetch, and
    /// every batch is a disconnected send.
    ///
    /// The thread holds only a `Weak` to `accounts_db`, so it never keeps it alive. Dropping it
    /// drops `batch_sender`, and the thread exits once its input disconnects. The thread first
    /// touches `accounts_db` on its first batch, which cannot be sent before the `Arc` exists.
    pub(super) fn new(accounts_db: Weak<AccountsDb>) -> Self {
        let (batch_sender, batch_receiver) = bounded(MAX_QUEUED_BATCHES);
        start_prefetch_thread(accounts_db, batch_receiver);
        Self {
            batch_sender,
            stats: PrefetchAccountsStats::default(),
        }
    }
}

#[cfg(target_os = "linux")]
fn start_prefetch_thread(accounts_db: Weak<AccountsDb>, batch_receiver: Receiver<Vec<Pubkey>>) {
    if !agave_io_uring::io_uring_supported() {
        warn!("no prefetch: io_uring is unsupported");
        return;
    }
    thread::Builder::new()
        .name("solAcctPrefetch".to_string())
        .spawn(move || match RingPrefetcher::new(accounts_db) {
            Ok(ring_prefetcher) => ring_prefetcher.run(batch_receiver),
            Err(err) => warn!("no prefetch: {err}"),
        })
        .expect("spawn prefetch thread");
}

/// Without io_uring there is no prefetch: the receiver drops here
#[cfg(not(target_os = "linux"))]
fn start_prefetch_thread(_accounts_db: Weak<AccountsDb>, _batch_receiver: Receiver<Vec<Pubkey>>) {}

impl AccountsDb {
    /// Loads `pubkeys` from storage off this thread and stores them in the read cache, so that
    /// a later load on the caller's critical path finds them there.
    ///
    /// `pubkeys` is in the caller's order, which is the order the accounts are about to be used.
    /// The caller's only cost is queueing it for the prefetch thread; when that queue is full
    /// the batch is dropped. Pubkeys that either cache already holds are dropped: a write-cache
    /// version shadows storage, and a read-cache version is already warm. So are pubkeys an
    /// earlier prefetch is still loading. The rest are read from storage and stored only while
    /// their version is still the newest in the index.
    ///
    /// The loads run against the rooted view rather than any fork's. The read cache only ever
    /// holds the newest version in storage, so a fork's ancestors would not change what gets
    /// cached, and loading against a snapshot of them here is not safe: this call does not keep
    /// the bank they came from alive, and the index entry of a minor fork may lose its storage
    /// before the read.
    ///
    /// Fire and forget. The caller is not told when, or whether, a load lands: a prefetch that
    /// finds nothing, races a flush, or is evicted before the account is used is wasted work
    /// and nothing worse. Without io_uring there is no prefetch thread, and every batch is
    /// dropped.
    pub fn prefetch_accounts(&self, pubkeys: Vec<Pubkey>) {
        if pubkeys.is_empty() {
            return;
        }
        match self.prefetcher.batch_sender.try_send(pubkeys) {
            Ok(()) => {
                self.prefetcher
                    .stats
                    .max_queued_batches
                    .fetch_max(self.prefetcher.batch_sender.len() as u64, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_)) => {
                self.prefetcher
                    .stats
                    .num_batches_dropped
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.prefetcher
                    .stats
                    .num_batches_disconnected
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Returns the slot and storage of `pubkey`'s newest version if no cache holds it and no
    /// prefetch is loading it, marking it outstanding
    fn read_for_prefetch(
        &self,
        pubkey: Pubkey,
        outstanding: &mut Outstanding,
        stats_sub: &mut PrefetchAccountsStatsSub,
    ) -> Option<(Slot, LoadedAccountAccessor)> {
        if self.accounts_cache.contains_pubkey(&pubkey)
            || self.read_only_accounts_cache.contains_key(&pubkey)
        {
            stats_sub.num_skipped_cached += 1;
            return None;
        }
        if !outstanding.insert(pubkey) {
            stats_sub.num_skipped_outstanding += 1;
            return None;
        }
        let Some((slot, accessor)) =
            self.get_account_accessor_from_index(&pubkey, &Ancestors::default())
        else {
            outstanding.remove(&pubkey);
            stats_sub.num_not_in_index += 1;
            return None;
        };
        stats_sub.num_issued += 1;
        Some((slot, accessor))
    }

    /// Stores a finished read's account if its version is still the newest, and clears its
    /// pubkey from `outstanding` either way, or it would never be prefetched again
    fn finish_prefetch(
        &self,
        pubkey: Pubkey,
        slot: Slot,
        account: Option<AccountSharedData>,
        outstanding: &mut Outstanding,
    ) {
        // Skip zero lamport accounts, as `do_load` does
        if let Some(account) = account
            && !account.is_zero_lamport()
        {
            // held already means a load on the critical path beat this read to it
            let already_cached = self.read_only_accounts_cache.contains_key(&pubkey);
            if self.store_in_read_cache_if_newest(&pubkey, slot, &account) {
                let stats = &self.prefetcher.stats;
                stats.num_stored.fetch_add(1, Ordering::Relaxed);
                if already_cached {
                    stats
                        .num_stored_already_cached
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        outstanding.remove(&pubkey);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_for_prefetch_skips_outstanding() {
        let db = AccountsDb::default_for_tests();
        let pubkey = Pubkey::new_unique();
        let account = AccountSharedData::new(42, 1, &Pubkey::default());
        db.store_for_tests((0, &[(&pubkey, &account)][..]));
        db.add_root_and_flush_write_cache(0);
        let mut outstanding = Outstanding::default();
        let mut stats_sub = PrefetchAccountsStatsSub::default();

        assert!(
            db.read_for_prefetch(pubkey, &mut outstanding, &mut stats_sub)
                .is_some()
        );

        // still outstanding, so not read again
        assert!(
            db.read_for_prefetch(pubkey, &mut outstanding, &mut stats_sub)
                .is_none()
        );
        assert_eq!(stats_sub.num_issued, 1);
        assert_eq!(stats_sub.num_skipped_outstanding, 1);

        // once finished, it is read again
        db.finish_prefetch(pubkey, 0, None, &mut outstanding);
        assert!(
            db.read_for_prefetch(pubkey, &mut outstanding, &mut stats_sub)
                .is_some()
        );
    }

    #[test]
    fn test_read_for_prefetch_not_in_index() {
        let db = AccountsDb::default_for_tests();
        let pubkey = Pubkey::new_unique();
        let mut outstanding = Outstanding::default();
        let mut stats_sub = PrefetchAccountsStatsSub::default();

        assert!(
            db.read_for_prefetch(pubkey, &mut outstanding, &mut stats_sub)
                .is_none()
        );
        // nothing is in flight, so nothing is left outstanding
        assert!(outstanding.is_empty());
        assert_eq!(stats_sub.num_not_in_index, 1);
    }
}
