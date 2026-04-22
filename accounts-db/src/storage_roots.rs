use {
    crate::rolling_bit_field::RollingBitField,
    solana_clock::Slot,
    solana_measure::measure::Measure,
    std::sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Default)]
pub struct StorageRootsStats {
    pub roots_len: Option<usize>,
    pub uncleaned_roots_len: Option<usize>,
    pub roots_range: Option<u64>,
    pub rooted_cleaned_count: usize,
    pub unrooted_cleaned_count: usize,
    pub clean_unref_from_storage_us: u64,
    pub clean_dead_slot_us: u64,
}

#[derive(Debug)]
pub struct StorageRoots {
    /// Current roots where appendvecs or write cache has account data.
    /// Range is approximately the last N slots where N is # slots per epoch.
    pub alive_roots: RollingBitField,
    /// # roots added since last stats collection
    pub roots_added: AtomicUsize,
    /// # roots removed since last stats collection
    pub roots_removed: AtomicUsize,
}

impl Default for StorageRoots {
    fn default() -> Self {
        // we expect to keep a rolling set of 400k slots around at a time
        // 4M gives us plenty of extra room to handle a width 10x what we should need.
        // cost is 4M bits of memory, which is .5MB
        Self::new(4194304)
    }
}

impl StorageRoots {
    pub fn new(max_width: u64) -> Self {
        Self {
            alive_roots: RollingBitField::new(max_width),
            roots_added: AtomicUsize::default(),
            roots_removed: AtomicUsize::default(),
        }
    }

    pub fn add_root(&mut self, slot: Slot) {
        self.roots_added.fetch_add(1, Ordering::Relaxed);
        self.alive_roots.insert(slot);
    }

    pub fn is_alive_root(&self, slot: Slot) -> bool {
        self.alive_roots.contains(&slot)
    }

    pub fn get_rooted_from_list<'a>(&self, slots: impl Iterator<Item = &'a Slot>) -> Vec<Slot> {
        slots
            .filter_map(|s| self.alive_roots.contains(s).then_some(*s))
            .collect()
    }

    pub fn max_root_inclusive(&self) -> Slot {
        self.alive_roots.max_inclusive()
    }

    /// Remove the slot when the storage for the slot is freed.
    /// Returns true if slot was a root.
    pub fn clean_dead_slot(&mut self, slot: Slot) -> bool {
        if self.alive_roots.remove(&slot) {
            self.roots_removed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn all_alive_roots(&self) -> Vec<Slot> {
        self.alive_roots.get_all()
    }

    pub fn update_roots_stats(&self, stats: &mut StorageRootsStats) {
        stats.roots_len = Some(self.alive_roots.len());
        stats.roots_range = Some(self.alive_roots.range_width());
    }

    pub fn clean_dead_slots<'a>(
        &mut self,
        dead_slots_iter: impl Iterator<Item = &'a Slot>,
    ) -> StorageRootsStats {
        let mut stats = StorageRootsStats::default();
        let mut measure = Measure::start("clean_dead_slot");
        let mut rooted_cleaned_count = 0;
        let mut unrooted_cleaned_count = 0;
        for slot in dead_slots_iter {
            if self.clean_dead_slot(*slot) {
                rooted_cleaned_count += 1;
            } else {
                unrooted_cleaned_count += 1;
            }
        }
        measure.stop();
        stats.clean_dead_slot_us += measure.as_us();
        self.update_roots_stats(&mut stats);
        stats.rooted_cleaned_count += rooted_cleaned_count;
        stats.unrooted_cleaned_count += unrooted_cleaned_count;
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_alive_root() {
        let mut storage_roots = StorageRoots::default();
        assert!(!storage_roots.is_alive_root(0));
        storage_roots.add_root(0);
        assert!(storage_roots.is_alive_root(0));
    }

    #[test]
    fn test_clean_first() {
        let mut storage_roots = StorageRoots::default();
        storage_roots.add_root(0);
        storage_roots.add_root(1);
        storage_roots.clean_dead_slot(0);
        assert!(storage_roots.is_alive_root(1));
        assert!(!storage_roots.is_alive_root(0));
    }

    #[test]
    fn test_clean_last() {
        // this behavior might be undefined, clean up should only occur on older slots
        let mut storage_roots = StorageRoots::default();
        storage_roots.add_root(0);
        storage_roots.add_root(1);
        storage_roots.clean_dead_slot(1);
        assert!(!storage_roots.is_alive_root(1));
        assert!(storage_roots.is_alive_root(0));
    }
}
