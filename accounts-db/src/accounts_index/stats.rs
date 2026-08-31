use {
    solana_time_utils::AtomicInterval,
    std::{
        fmt::Debug,
        sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
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
    pub deletes: AtomicU64,
    pub inserts: AtomicU64,
    count: AtomicUsize,
    last_was_startup: AtomicBool,
    last_time: AtomicInterval,
}

impl Stats {
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

    pub fn total_count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn report_stats(
        &self,
        startup: bool,
        count_in_mem: usize,
        capacity_in_mem: usize,
        estimate_mem_bytes: usize,
    ) {
        let elapsed_ms = self.last_time.elapsed_ms();
        if elapsed_ms < STATS_INTERVAL_MS {
            return;
        }

        if !self.last_time.should_update(STATS_INTERVAL_MS) {
            return;
        }

        // all metrics during startup are written to a different data point
        let was_startup = self.last_was_startup.swap(startup, Ordering::Relaxed);

        let datapoint_name = if startup || was_startup {
            "accounts_index_startup"
        } else {
            "accounts_index"
        };
        datapoint_info!(
            datapoint_name,
            ("estimate_mem_bytes", estimate_mem_bytes, i64),
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
        );
    }
}
