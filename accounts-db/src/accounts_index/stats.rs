use {
    super::{
        DiskIndexValue, IndexValue,
        bucket_map_holder::BucketMapHolder,
        in_mem_accounts_index::InMemAccountsIndex,
    },
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
    pub load_disk_found_count: AtomicU64,
    pub load_disk_found_us: AtomicU64,
    pub load_disk_missing_count: AtomicU64,
    pub load_disk_missing_us: AtomicU64,
    pub updates_in_mem: AtomicU64,
    pub keys: AtomicU64,
    pub deletes: AtomicU64,
    pub inserts: AtomicU64,
    count: AtomicUsize,
    pub bg_waiting_us: AtomicU64,
    pub count_in_mem: AtomicUsize,
    pub capacity_in_mem: AtomicUsize,
    pub flush_entries_updated_on_disk: AtomicU64,
    pub flush_entries_evicted_from_mem: AtomicU64,
    pub active_threads: AtomicU64,
    last_was_startup: AtomicBool,
    last_time: AtomicInterval,
    bins: u64,
}

impl Stats {
    pub fn new(bins: usize) -> Stats {
        Stats {
            bins: bins as u64,
            ..Stats::default()
        }
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

    pub fn remaining_until_next_interval(&self) -> u64 {
        self.last_time
            .remaining_until_next_interval(STATS_INTERVAL_MS)
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

    fn calc_percent(ms: u64, elapsed_ms: u64) -> f32 {
        if elapsed_ms == 0 {
            0.0
        } else {
            (ms as f32 / elapsed_ms as f32) * 100.0
        }
    }

    pub fn total_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn report_stats<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>>(
        &self,
        storage: &BucketMapHolder<T, U>,
        in_mem: &[Arc<InMemAccountsIndex<T, U>>],
    ) {
        let elapsed_ms = self.last_time.elapsed_ms();
        if elapsed_ms < STATS_INTERVAL_MS {
            return;
        }

        if !self.last_time.should_update(STATS_INTERVAL_MS) {
            return;
        }

        let disk = storage.disk.as_ref();
        let disk_per_bucket_counts = disk
            .map(|disk| {
                (0..self.bins)
                    .map(|i| disk.get_bucket_from_index(i as usize).bucket_len() as usize)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let disk_stats = Self::get_stats(disk_per_bucket_counts);
        let mem_per_bucket_counts = in_mem.iter().map(|bin| bin.len()).collect();
        let mem_stats = Self::get_stats(mem_per_bucket_counts);
        let dirty_per_bin: Vec<usize> = in_mem
            .iter()
            .map(|bin| bin.dirty_entry_count().max(0) as usize)
            .collect();
        let dirty_stats = Self::get_stats(dirty_per_bin);

        const US_PER_MS: u64 = 1_000;

        // all metrics during startup are written to a different data point
        let startup = storage.get_startup();
        let was_startup = self.last_was_startup.swap(startup, Ordering::Relaxed);

        let count_in_mem = self.count_in_mem.load(Ordering::Relaxed);
        let capacity_in_mem = self.capacity_in_mem.load(Ordering::Relaxed);

        // sum of elapsed time in each thread
        let thread_time_elapsed_ms = elapsed_ms * storage.threads as u64;
        let datapoint_name = if startup || was_startup {
            "accounts_index_startup"
        } else {
            "accounts_index"
        };
        if storage.is_disk_index_enabled() {
            if was_startup {
                // these stats only apply at startup
                datapoint_info!(
                    "accounts_index_startup",
                    (
                        "entries_created",
                        disk.map(|disk| disk
                            .stats
                            .index
                            .startup
                            .entries_created
                            .swap(0, Ordering::Relaxed))
                            .unwrap_or_default(),
                        i64
                    ),
                    (
                        "entries_reused",
                        disk.map(|disk| disk
                            .stats
                            .index
                            .startup
                            .entries_reused
                            .swap(0, Ordering::Relaxed))
                            .unwrap_or_default(),
                        i64
                    ),
                );
            }
            let estimate_mem_bytes =
                // hash map mem usage is based on capacity, and the footprint of a KV-pair
                // (we ignore other hash map details, such as load factor)
                capacity_in_mem * InMemAccountsIndex::<T, U>::size_of_uninitialized()
                // each value in use we assume has a single entry in the slot list
                + count_in_mem * InMemAccountsIndex::<T, U>::size_of_single_entry();
            datapoint_info!(
                datapoint_name,
                ("estimate_mem_bytes", estimate_mem_bytes, i64),
                ("count_in_mem", count_in_mem, i64),
                ("capacity_in_mem", capacity_in_mem, i64),
                ("count", self.total_count(), i64),
                (
                    "bg_waiting_percent",
                    Self::calc_percent(
                        self.bg_waiting_us.swap(0, Ordering::Relaxed) / US_PER_MS,
                        thread_time_elapsed_ms
                    ),
                    f64
                ),
                ("min_in_bin_disk", disk_stats.0, i64),
                ("max_in_bin_disk", disk_stats.1, i64),
                ("count_from_bins_disk", disk_stats.2, i64),
                ("median_from_bins_disk", disk_stats.3, i64),
                ("min_in_bin_mem", mem_stats.0, i64),
                ("max_in_bin_mem", mem_stats.1, i64),
                ("count_from_bins_mem", mem_stats.2, i64),
                ("median_from_bins_mem", mem_stats.3, i64),
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
                    "load_disk_found_count",
                    self.load_disk_found_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "load_disk_found_us",
                    self.load_disk_found_us.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "load_disk_missing_count",
                    self.load_disk_missing_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "load_disk_missing_us",
                    self.load_disk_missing_us.swap(0, Ordering::Relaxed),
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
                (
                    "active_threads",
                    self.active_threads.load(Ordering::Relaxed),
                    i64
                ),
                ("keys", self.keys.swap(0, Ordering::Relaxed), i64),
                (
                    "disk_index_resizes",
                    disk.map(|disk| disk.stats.index.resizes.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_index_failed_resizes",
                    disk.map(|disk| disk.stats.index.failed_resizes.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_index_max_size",
                    disk.map(|disk| { disk.stats.index.max_size.swap(0, Ordering::Relaxed) })
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_index_new_file_us",
                    disk.map(|disk| disk.stats.index.new_file_us.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_index_resize_us",
                    disk.map(|disk| disk.stats.index.resize_us.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_index_flush_file_us",
                    disk.map(|disk| disk.stats.index.flush_file_us.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_index_file_size",
                    disk.map(|disk| disk.stats.index.total_file_size.load(Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_index_find_index_entry_mut_us",
                    disk.map(|disk| disk
                        .stats
                        .index
                        .find_index_entry_mut_us
                        .swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_index_flush_mmap_us",
                    disk.map(|disk| disk.stats.index.mmap_us.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "index_exceptional_entry",
                    disk.map(|disk| disk
                        .stats
                        .index
                        .index_uses_uncommon_slot_list_len_or_refcount
                        .load(Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_data_file_size",
                    disk.map(|disk| disk.stats.data.total_file_size.load(Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_data_file_count",
                    disk.map(|disk| disk.stats.data.file_count.load(Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_data_resizes",
                    disk.map(|disk| disk.stats.data.resizes.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_data_max_size",
                    disk.map(|disk| { disk.stats.data.max_size.swap(0, Ordering::Relaxed) })
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_data_new_file_us",
                    disk.map(|disk| disk.stats.data.new_file_us.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_data_resize_us",
                    disk.map(|disk| disk.stats.data.resize_us.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_data_flush_file_us",
                    disk.map(|disk| disk.stats.data.flush_file_us.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "disk_data_flush_mmap_us",
                    disk.map(|disk| disk.stats.data.mmap_us.swap(0, Ordering::Relaxed))
                        .unwrap_or_default(),
                    i64
                ),
                (
                    "flush_entries_updated_on_disk",
                    self.flush_entries_updated_on_disk
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "flush_entries_evicted_from_mem",
                    self.flush_entries_evicted_from_mem
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                ("min_dirty_entry_count", dirty_stats.0, i64),
                ("max_dirty_entry_count", dirty_stats.1, i64),
                ("total_dirty_entry_count", dirty_stats.2, i64),
                ("median_dirty_entry_count", dirty_stats.3, i64),
            );
        } else {
            datapoint_info!(
                datapoint_name,
                (
                    "estimate_mem_bytes",
                    (
                        // hash map mem usage is based on capacity, and the footprint of a KV-pair
                        // (we ignore other hash map details, such as load factor)
                        capacity_in_mem * InMemAccountsIndex::<T, U>::size_of_uninitialized()
                        // each value in use we assume has a single entry in the slot list
                        + count_in_mem * InMemAccountsIndex::<T, U>::size_of_single_entry()
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
                ("min_dirty_entry_count", dirty_stats.0, i64),
                ("max_dirty_entry_count", dirty_stats.1, i64),
                ("total_dirty_entry_count", dirty_stats.2, i64),
                ("median_dirty_entry_count", dirty_stats.3, i64),
            );
        }
    }
}
