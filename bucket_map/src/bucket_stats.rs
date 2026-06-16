use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

#[derive(Debug, Default)]
pub struct StartupBucketStats {
    pub entries_created: AtomicU64,
    pub entries_reused: AtomicU64,
}

#[derive(Debug, Default)]
pub struct BucketStats {
    pub resizes: AtomicU64,
    pub failed_resizes: AtomicU64,
    pub max_size: AtomicU64,
    pub resize_us: AtomicU64,
    pub new_file_us: AtomicU64,
    pub flush_file_us: AtomicU64,
    pub mmap_us: AtomicU64,
    pub find_index_entry_mut_us: AtomicU64,
    pub file_count: AtomicU64,
    pub total_file_size: AtomicU64,
    pub startup: StartupBucketStats,
    pub index_uses_uncommon_slot_list_len_or_refcount: AtomicBool,
}

impl BucketStats {
    pub fn update_max_size(&self, size: u64) {
        self.max_size.fetch_max(size, Ordering::Relaxed);
    }

    pub fn resize_grow(&self, old_size: u64, new_size: u64) {
        let size_change = new_size.saturating_sub(old_size);
        let total = self
            .total_file_size
            .fetch_add(size_change, Ordering::Relaxed)
            + size_change;
        // Safety valve for the inline-account-index experiment: the index file is expected to
        // settle well under this. If a bucket file blows past 1 TiB, the index is either not
        // converging or the box can't hold it -- abort hard rather than fill the disk out from
        // under the node.
        const MAX_BUCKET_FILE_SIZE: u64 = 1 << 40; // 1 TiB
        if total > MAX_BUCKET_FILE_SIZE {
            eprintln!(
                "FATAL: bucket map file size {total} bytes exceeded {MAX_BUCKET_FILE_SIZE} (1 TiB); \
                 aborting to avoid filling the disk"
            );
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Default)]
pub struct BucketMapStats {
    pub index: Arc<BucketStats>,
    pub data: Arc<BucketStats>,
}
