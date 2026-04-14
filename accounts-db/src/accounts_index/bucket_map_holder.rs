use {
    super::{
        AccountsIndexConfig, DiskIndexValue, IndexLimit, IndexValue,
        in_mem_accounts_index::{InMemAccountsIndex, StartupStats},
        stats::Stats,
    },
    crate::waitable_condvar::WaitableCondvar,
    log::*,
    solana_bucket_map::bucket_map::{BucketMap, BucketMapConfig},
    solana_clock::Slot,
    solana_measure::measure::Measure,
    std::{
        fmt::Debug,
        marker::PhantomData,
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

/// The number of entries below an in-mem index bin's usable capacity at which to begin evicting.
///
/// This number should be *at least* the worst case rate that entries are added to the in-mem
/// index per bin.  This ensures we start evicting early enough so that we do not exceed the
/// configured index threshold limit.
///
/// At the same time, we want this value to be as small as possible.  The smaller this value, the
/// higher the utilization of the in-mem index bins.
///
/// This value is used to compute the high watermark.
pub const DEFAULT_NUM_ENTRIES_OVERHEAD: usize = 5_000;

/// The number of entries to evict, once we've hit the high watermark.
///
/// We want this number to be small, similar to `NUM_ENTRIES_OVERHEAD`, to keep utilization high.
/// It also must be large enough to ensure once an eviction is triggered that scanning + flushing +
/// evicting completes before the high watermark is crossed again.
/// We also want to avoid/ammortize scanning the bins for flush/evict, so a larger number helps
/// with that goal.
///
/// This value is used to compute the low watermark.
pub const DEFAULT_NUM_ENTRIES_TO_EVICT: usize = 10_000;

pub struct BucketMapHolder<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> {
    pub disk: Option<BucketMap<(Slot, U)>>,

    pub stats: Stats,

    // used to wake bg threads on shutdown
    pub wait_exit: Arc<WaitableCondvar>,
    pub(crate) bins: usize,

    pub threads: usize,

    /// startup is a special time for flush to focus on moving everything to disk as fast and efficiently as possible
    /// with less thread count limitations. LRU and access patterns are not important. Freeing memory
    /// and writing to disk in parallel are.
    /// Note startup is an optimization and is not required for correctness.
    startup: AtomicBool,
    _phantom: PhantomData<T>,

    pub(crate) startup_stats: Arc<StartupStats>,

    /// Precomputed thresholds per bin for flushing and eviction
    /// None for Minimal/InMemOnly, Some(threshold_entries_per_bin) for Threshold
    pub(super) threshold_entries_per_bin: Option<ThresholdEntriesPerBin>,
}

impl<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> Debug for BucketMapHolder<T, U> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[allow(clippy::mutex_atomic)]
impl<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> BucketMapHolder<T, U> {
    /// is the accounts index using disk as a backing store
    pub fn is_disk_index_enabled(&self) -> bool {
        self.disk.is_some()
    }

    /// Check if flushing to disk should occur based on entry count threshold per bin
    pub fn should_flush(&self, entries_in_bin: usize) -> bool {
        match &self.threshold_entries_per_bin {
            None => self.is_disk_index_enabled(),
            Some(threshold_entries_per_bin) => {
                entries_in_bin > threshold_entries_per_bin.high_water_mark
            }
        }
    }

    /// Calculate maximum evictions to perform for threshold-based flushing
    /// Returns current_entries for Minimal disk index
    /// Returns the max_evictions for Threshold mode to bring count to the low water mark
    pub fn max_evictions_for_threshold(&self, current_entries: usize) -> NonZeroUsize {
        let evictions = match &self.threshold_entries_per_bin {
            None => current_entries,
            Some(threshold_entries_per_bin) => {
                // Low water mark: evict down to specified ratio of the per-bin threshold
                current_entries.saturating_sub(threshold_entries_per_bin.low_water_mark)
            }
        }
        .max(1);
        // SAFETY: evictions is ensured to be non-zero above.
        NonZeroUsize::new(evictions).unwrap()
    }

    /// used by bg processes to determine # active threads and how aggressively to flush
    pub fn get_startup(&self) -> bool {
        self.startup.load(Ordering::Relaxed)
    }

    /// startup=true causes:
    ///      in mem to act in a way that flushes to disk asap
    /// startup=false is 'normal' operation
    pub fn set_startup(&self, value: bool) {
        self.startup.store(value, Ordering::Relaxed)
    }

    pub fn new(bins: usize, config: &AccountsIndexConfig, threads: usize) -> Self {
        let mut bucket_config = BucketMapConfig::new(bins);
        bucket_config.drives = config.drives.as_ref().cloned();
        bucket_config.restart_config_file = bucket_config
            .drives
            .as_ref()
            .and_then(|drives| drives.first())
            .map(|drive| drive.join("accounts_index_restart"));

        let disk = match config.index_limit {
            IndexLimit::InMemOnly => None,
            IndexLimit::Minimal | IndexLimit::Threshold(_) => Some(BucketMap::new(bucket_config)),
        };

        // Compute threshold_entries once here
        let threshold_entries_per_bin = match &config.index_limit {
            IndexLimit::InMemOnly | IndexLimit::Minimal => None,
            IndexLimit::Threshold(threshold) => {
                let limit_bytes = threshold.num_bytes;
                let bytes_per_entry = InMemAccountsIndex::<T, U>::size_of_uninitialized()
                    + InMemAccountsIndex::<T, U>::size_of_single_entry();
                let limit_entries = (limit_bytes as usize) / bytes_per_entry;
                let entries_per_bin = limit_entries / bins;
                let target_entries_per_bin =
                    Self::calculate_target_entries_per_bin(entries_per_bin);
                let high_water_mark = target_entries_per_bin
                    .checked_sub(threshold.num_entries_overhead)
                    .expect("limit too small for high watermark");
                let low_water_mark = high_water_mark
                    .checked_sub(threshold.num_entries_to_evict)
                    .expect("limit too small for low watermark");
                #[rustfmt::skip]
                info!(
                    "AccountsIndex threshold configuration: \
                     num_bins: {bins}, \
                     bytes_per_entry: {bytes_per_entry}, \
                     limit_bytes_total: {limit_bytes}, \
                     limit_entries_total: {limit_entries}, \
                     limit_entries_per_bin: {entries_per_bin}, \
                     target_entries_per_bin: {target_entries_per_bin}, \
                     high_water_mark_entries_per_bin: {high_water_mark}, \
                     low_water_mark_entries_per_bin: {low_water_mark}",
                );
                Some(ThresholdEntriesPerBin {
                    high_water_mark,
                    low_water_mark,
                })
            }
        };

        Self {
            disk,
            stats: Stats::new(bins),
            wait_exit: Arc::default(),
            bins,
            startup: AtomicBool::default(),
            threads,
            _phantom: PhantomData,
            startup_stats: Arc::default(),
            threshold_entries_per_bin,
        }
    }

    fn calculate_target_entries_per_bin(entries_per_bin: usize) -> usize {
        if entries_per_bin == 0 {
            0
        } else {
            // HashMap capacities grow in powers of 2 and typically trigger a reallocation
            // when ~7/8 full. Derive the target as the highest power-of-two capacity that
            // fits within entries_per_bin, then apply the 7/8 factor to stay below the
            // reallocation threshold.
            let entries_per_bin_log2 = entries_per_bin.ilog2();
            (1usize << entries_per_bin_log2) * 7 / 8
        }
    }

    // intended to execute in a bg thread
    pub fn background(
        &self,
        exit: Vec<Arc<AtomicBool>>,
        in_mem: Vec<Arc<InMemAccountsIndex<T, U>>>,
    ) {
        let flush = self.is_disk_index_enabled();
        loop {
            // When there's nothing to flush (disk disabled, or not in startup mode), sleep until
            // the stats interval elapses or we're woken by a dirty/exit signal.
            if !flush || !self.get_startup() {
                let mut m = Measure::start("wait");
                self.wait_exit.wait_timeout(Duration::from_millis(
                    self.stats.remaining_until_next_interval(),
                ));
                m.stop();
                self.stats
                    .bg_waiting_us
                    .fetch_add(m.as_us(), Ordering::Relaxed);
            }

            if exit.iter().any(|exit| exit.load(Ordering::Relaxed)) {
                break;
            }

            self.stats.active_threads.fetch_add(1, Ordering::Relaxed);
            if flush && self.get_startup() {
                // During startup, flush all bins to disk as fast as possible.
                for bin in &in_mem {
                    bin.flush();
                }
            }
            self.stats.report_stats(self, &in_mem);
            self.stats.active_threads.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// Precomputed thresholds derived from the configured per-bin target.
#[derive(Clone, Copy, Debug)]
pub struct ThresholdEntriesPerBin {
    /// Entry count above which a bin triggers flushing to disk and eviction
    /// from in-memory index.
    pub high_water_mark: usize,
    /// Entry count to reach after flushing/evicting from a bin.
    pub low_water_mark: usize,
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::accounts_index::IndexLimitThreshold, test_case::test_case,
    };

    #[test]
    fn test_disk_index_enabled() {
        let bins = 1;
        let config = AccountsIndexConfig {
            index_limit: IndexLimit::Minimal,
            ..Default::default()
        };
        let test = BucketMapHolder::<u64, u64>::new(bins, &config, 1);
        assert!(test.is_disk_index_enabled());
    }

    /// Ensure that should_flush() is correct when using IndexLimit::Threshold
    #[test]
    fn test_should_flush_threshold() {
        let bins = 1;
        let num_entries_overhead = DEFAULT_NUM_ENTRIES_OVERHEAD;
        let num_entries_to_evict = DEFAULT_NUM_ENTRIES_TO_EVICT;
        let num_entries = (num_entries_overhead + num_entries_to_evict) * 3;
        let bytes_per_entry = InMemAccountsIndex::<u64, u64>::size_of_uninitialized()
            + InMemAccountsIndex::<u64, u64>::size_of_single_entry();
        let config = AccountsIndexConfig {
            index_limit: IndexLimit::Threshold(IndexLimitThreshold {
                num_bytes: (num_entries * bytes_per_entry) as u64,
                num_entries_overhead,
                num_entries_to_evict,
            }),
            ..Default::default()
        };
        let test = BucketMapHolder::<u64, u64>::new(bins, &config, 1);
        assert!(test.is_disk_index_enabled());

        let thresholds = test.threshold_entries_per_bin.unwrap();
        // the high water mark must be non-zero and less than num_entries
        assert!((1..num_entries).contains(&thresholds.high_water_mark));
        // the low water mark must be non-zero and less than the high water mark
        assert!((1..thresholds.high_water_mark).contains(&thresholds.low_water_mark));

        // Test: Below, at, and above the should_flush() boundary
        assert!(!test.should_flush(thresholds.high_water_mark - 1));
        assert!(!test.should_flush(thresholds.high_water_mark));
        assert!(test.should_flush(thresholds.high_water_mark + 1));
    }

    /// Ensure that should_flush() is always true when using IndexLimit::Minimal
    #[test]
    fn test_should_flush_minimal() {
        let bins = 1;
        let config = AccountsIndexConfig {
            index_limit: IndexLimit::Minimal,
            ..Default::default()
        };
        let test = BucketMapHolder::<u64, u64>::new(bins, &config, 1);

        assert!(test.should_flush(0));
        assert!(test.should_flush(1000));
        assert!(test.should_flush(usize::MAX));
    }

    /// Ensure that should_flush() is always false when using IndexLimit::InMemOnly
    #[test]
    fn test_should_flush_in_mem_only() {
        let bins = 1;
        let config = AccountsIndexConfig {
            index_limit: IndexLimit::InMemOnly,
            ..Default::default()
        };
        let test = BucketMapHolder::<u64, u64>::new(bins, &config, 1);

        assert!(!test.should_flush(0));
        assert!(!test.should_flush(1000));
        assert!(!test.should_flush(usize::MAX));
    }

    #[test]
    fn test_max_evictions_minimal() {
        let bins = 1;
        let config = AccountsIndexConfig {
            index_limit: IndexLimit::Minimal,
            ..Default::default()
        };
        let test = BucketMapHolder::<u64, u64>::new(bins, &config, 1);

        assert_eq!(
            test.max_evictions_for_threshold(0),
            NonZeroUsize::new(1).unwrap()
        );
        assert_eq!(
            test.max_evictions_for_threshold(1000),
            NonZeroUsize::new(1000).unwrap()
        );
    }

    #[test_case(1; "bins=1")]
    #[test_case(2; "bins=2")]
    #[test_case(4; "bins=4")]
    #[test_case(8; "bins=8")]
    fn test_max_evictions_threshold(num_bins: usize) {
        let num_entries = 1_234_567_890;
        let bytes_per_entry = InMemAccountsIndex::<u64, u64>::size_of_uninitialized()
            + InMemAccountsIndex::<u64, u64>::size_of_single_entry();
        let limit_bytes = (num_entries * bytes_per_entry) as u64;
        let config = AccountsIndexConfig {
            index_limit: IndexLimit::Threshold(IndexLimitThreshold {
                num_bytes: limit_bytes,
                num_entries_overhead: DEFAULT_NUM_ENTRIES_OVERHEAD,
                num_entries_to_evict: DEFAULT_NUM_ENTRIES_TO_EVICT,
            }),
            ..Default::default()
        };
        let test = BucketMapHolder::<u64, u64>::new(num_bins, &config, 1);
        let threshold = test.threshold_entries_per_bin.unwrap();
        let low_water_mark = threshold.low_water_mark;
        // these two asserts ensure we actually have entries to evict
        assert!(low_water_mark > 0);
        assert!(low_water_mark < num_entries);

        let expected = NonZeroUsize::new(num_entries - low_water_mark).unwrap();
        assert_eq!(test.max_evictions_for_threshold(num_entries), expected);
    }
}
