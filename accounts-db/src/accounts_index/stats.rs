use {
    super::{IndexValue, in_mem_accounts_index::InMemAccountsIndex},
    solana_time_utils::AtomicInterval,
    std::{
        fmt::Debug,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        },
    },
};

// stats logged every 10 s
const STATS_INTERVAL_MS: u64 = 10_000;

#[derive(Debug, Default)]
pub struct Stats {
    pub get_mem_us: AtomicU64,
    pub gets_from_mem: AtomicU64,
    pub get_missing_us: AtomicU64,
    pub gets_missing: AtomicU64,
    pub entry_mem_us: AtomicU64,
    pub entries_from_mem: AtomicU64,
    pub entry_missing_us: AtomicU64,
    pub entries_missing: AtomicU64,
    pub updates_in_mem: AtomicU64,
    pub keys: AtomicU64,
    pub deletes: AtomicU64,
    pub inserts: AtomicU64,
    count: AtomicUsize,
    pub count_in_mem: AtomicUsize,
    pub capacity_in_mem: AtomicUsize,
    last_was_startup: AtomicBool,
    last_time: AtomicInterval,
}

impl Stats {
    pub fn new(_bins: usize) -> Stats {
        Stats::default()
    }

    pub fn inc_insert(&self) {
        self.inc_insert_count(1);
    }

    pub fn inc_insert_count(&self, count: u64) {
        self.inserts.fetch_add(count, Ordering::Relaxed);
        self.count.fetch_add(count as usize, Ordering::Relaxed);
    }

    pub fn inc_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn inc_mem_count(&self) {
        self.add_mem_count(1);
    }

    pub fn dec_mem_count(&self) {
        self.sub_mem_count(1);
    }

    pub fn add_mem_count(&self, count: usize) {
        self.count_in_mem.fetch_add(count, Ordering::Relaxed);
    }

    pub fn sub_mem_count(&self, count: usize) {
        self.count_in_mem.fetch_sub(count, Ordering::Relaxed);
    }

    /// Updates the 'in-mem capacity' stat, given a bin's pre and post values
    pub fn update_in_mem_capacity(&self, pre: usize, post: usize) {
        match post.cmp(&pre) {
            std::cmp::Ordering::Equal => {
                // nothing to do here
            }
            std::cmp::Ordering::Greater => {
                self.capacity_in_mem
                    .fetch_add(post - pre, Ordering::Relaxed);
            }
            std::cmp::Ordering::Less => {
                self.capacity_in_mem
                    .fetch_sub(pre - post, Ordering::Relaxed);
            }
        }
    }

    /// return min, max, sum, median of data
    fn get_stats(mut data: Vec<usize>) -> (usize, usize, usize, usize) {
        if data.is_empty() {
            (0, 0, 0, 0)
        } else {
            data.sort_unstable();
            (
                *data.first().unwrap(),
                *data.last().unwrap(),
                data.iter().sum(),
                data[data.len() / 2],
            )
        }
    }

    pub fn total_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn report_stats<T: IndexValue>(
        &self,
        startup: bool,
        in_mem: &[Arc<InMemAccountsIndex<T>>],
    ) {
        let elapsed_ms = self.last_time.elapsed_ms();
        if elapsed_ms < STATS_INTERVAL_MS {
            return;
        }

        if !self.last_time.should_update(STATS_INTERVAL_MS) {
            return;
        }

        let mem_per_bucket_counts = in_mem.iter().map(|bin| bin.len()).collect();
        let mem_stats = Self::get_stats(mem_per_bucket_counts);

        // all metrics during startup are written to a different data point
        let was_startup = self.last_was_startup.swap(startup, Ordering::Relaxed);

        let count_in_mem = self.count_in_mem.load(Ordering::Relaxed);
        let capacity_in_mem = self.capacity_in_mem.load(Ordering::Relaxed);

        let datapoint_name = if startup || was_startup {
            "accounts_index_startup"
        } else {
            "accounts_index"
        };
        datapoint_info!(
            datapoint_name,
            (
                "estimate_mem_bytes",
                (
                    // hash map mem usage is based on capacity, and the footprint of a KV-pair
                    // (we ignore other hash map details, such as load factor)
                    capacity_in_mem * InMemAccountsIndex::<T>::size_of_uninitialized()
                    // each value in use we assume has a single entry in the slot list
                    + count_in_mem * InMemAccountsIndex::<T>::size_of_single_entry()
                ),
                i64
            ),
            ("count_in_mem", count_in_mem, i64),
            ("capacity_in_mem", capacity_in_mem, i64),
            ("count", self.total_count(), i64),
            (
                "gets_from_mem",
                self.gets_from_mem.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_mem_us",
                self.get_mem_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "gets_missing",
                self.gets_missing.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_missing_us",
                self.get_missing_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "entries_from_mem",
                self.entries_from_mem.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "entry_mem_us",
                self.entry_mem_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "entries_missing",
                self.entries_missing.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "entry_missing_us",
                self.entry_missing_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "updates_in_mem",
                self.updates_in_mem.swap(0, Ordering::Relaxed),
                i64
            ),
            ("inserts", self.inserts.swap(0, Ordering::Relaxed), i64),
            ("deletes", self.deletes.swap(0, Ordering::Relaxed), i64),
            ("keys", self.keys.swap(0, Ordering::Relaxed), i64),
            ("min_in_bin_mem", mem_stats.0, i64),
            ("max_in_bin_mem", mem_stats.1, i64),
            ("count_from_bins_mem", mem_stats.2, i64),
            ("median_from_bins_mem", mem_stats.3, i64),
        );
    }
}
