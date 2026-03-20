use {
    crate::ancestors::Ancestors,
    solana_clock::{BankId, Slot},
    std::{
        collections::{HashSet, btree_map::BTreeMap},
        sync::{
            Arc, Mutex, RwLock,
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        },
    },
    thiserror::Error,
};

pub type ScanResult<T> = Result<T, ScanError>;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ScanError {
    #[error(
        "Node detected it replayed bad version of slot {slot:?} with id {bank_id:?}, thus the \
         scan on said slot was aborted"
    )]
    SlotRemoved { slot: Slot, bank_id: BankId },
    #[error("scan aborted: {0}")]
    Aborted(String),
}

/// In what order should items be scanned?
///
/// Users should prefer `Unsorted`, unless required otherwise,
/// as sorting incurs additional runtime cost.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ScanOrder {
    /// Scan items in any order
    Unsorted,
    /// Scan items in sorted order
    Sorted,
}

#[derive(Debug)]
pub struct ScanConfig {
    /// checked by the scan. When true, abort scan.
    pub abort: Option<Arc<AtomicBool>>,

    /// In what order should items be scanned?
    pub scan_order: ScanOrder,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            abort: None,
            scan_order: ScanOrder::Unsorted,
        }
    }
}

impl ScanConfig {
    pub fn new(scan_order: ScanOrder) -> Self {
        Self {
            scan_order,
            ..Default::default()
        }
    }

    /// mark the scan as aborted
    pub fn abort(&self) {
        if let Some(abort) = self.abort.as_ref() {
            abort.store(true, Ordering::Relaxed)
        }
    }

    /// use existing 'abort' if available, otherwise allocate one
    pub fn recreate_with_abort(&self) -> Self {
        ScanConfig {
            abort: Some(self.abort.clone().unwrap_or_default()),
            scan_order: self.scan_order,
        }
    }

    /// true if scan should abort
    pub fn is_aborted(&self) -> bool {
        if let Some(abort) = self.abort.as_ref() {
            abort.load(Ordering::Relaxed)
        } else {
            false
        }
    }
}

/// Holds all state related to scan lifecycle management.
///
/// This includes tracking ongoing scans, removed bank IDs (to detect
/// corrupted scans), scan statistics, and scan configuration.
#[derive(Debug)]
pub struct ScanState {
    /// Tracks the `max_root` pinned by each active scan. The value is a
    /// reference count so multiple scans can pin the same root.
    pub ongoing_scan_roots: RwLock<BTreeMap<Slot, u64>>,
    /// Bank IDs removed via `remove_unrooted_slots()`. Scans check this to
    /// detect whether the bank they were scanning was removed mid-scan.
    pub removed_bank_ids: Mutex<HashSet<BankId>>,
    /// When a scan's accumulated data exceeds this limit, abort the scan.
    pub scan_results_limit_bytes: Option<usize>,
    /// Number of scans currently active.
    pub active_scans: AtomicUsize,
    /// Max distance between the latest `max_root` and the oldest ongoing scan root.
    pub max_distance_to_min_scan_slot: AtomicU64,
}

impl ScanState {
    pub fn new(scan_results_limit_bytes: Option<usize>) -> Self {
        Self {
            ongoing_scan_roots: RwLock::<BTreeMap<Slot, u64>>::default(),
            removed_bank_ids: Mutex::<HashSet<BankId>>::default(),
            scan_results_limit_bytes,
            active_scans: AtomicUsize::default(),
            max_distance_to_min_scan_slot: AtomicU64::default(),
        }
    }

    pub fn min_ongoing_scan_root(&self) -> Option<Slot> {
        self.min_ongoing_scan_root_from_btree(&self.ongoing_scan_roots.read().unwrap())
    }

    pub fn min_ongoing_scan_root_from_btree(
        &self,
        ongoing_scan_roots: &BTreeMap<Slot, u64>,
    ) -> Option<Slot> {
        ongoing_scan_roots.keys().next().cloned()
    }
}

/// Guard that protects account state during an accounts scan.
///
/// Pins `max_root` in `ongoing_scan_roots` on creation and unpins it on drop,
/// preventing clean from advancing past the pinned root while the scan is active.
#[derive(Debug)]
pub(crate) struct ScanGuard<'a> {
    scan_state: &'a ScanState,
    max_root: Slot,
    scan_bank_id: BankId,
    ancestors_max_slot: Slot,
    use_empty_ancestors: bool,
}

impl<'a> ScanGuard<'a> {
    /// Begin a scan: checks the bank hasn't been removed, pins `max_root` in
    /// `ongoing_scan_roots`, and resolves whether ancestors should be used.
    pub fn new(
        scan_state: &'a ScanState,
        ancestors: &Ancestors,
        scan_bank_id: BankId,
        max_root_inclusive: Slot,
    ) -> Result<Self, ScanError> {
        let ancestors_max_slot = ancestors.max_slot();

        {
            let locked_removed_bank_ids = scan_state.removed_bank_ids.lock().unwrap();
            if locked_removed_bank_ids.contains(&scan_bank_id) {
                return Err(ScanError::SlotRemoved {
                    slot: ancestors_max_slot,
                    bank_id: scan_bank_id,
                });
            }
        }

        scan_state
            .active_scans
            .fetch_add(1, Ordering::Relaxed);
        {
            let mut w_ongoing_scan_roots = scan_state
                // This lock is also grabbed by clean_accounts(), so clean
                // has at most cleaned up to the current `max_root` (since
                // clean only happens *after* BankForks::set_root() which sets
                // the `max_root`)
                .ongoing_scan_roots
                .write()
                .unwrap();
            if let Some(min_ongoing_scan_root) =
                scan_state.min_ongoing_scan_root_from_btree(&w_ongoing_scan_roots)
            {
                if min_ongoing_scan_root < max_root_inclusive {
                    let current = max_root_inclusive - min_ongoing_scan_root;
                    scan_state
                        .max_distance_to_min_scan_slot
                        .fetch_max(current, Ordering::Relaxed);
                }
            }
            *w_ongoing_scan_roots.entry(max_root_inclusive).or_default() += 1;
        }

        let use_empty_ancestors = !ancestors.contains_key(&max_root_inclusive);

        Ok(Self {
            scan_state,
            max_root: max_root_inclusive,
            scan_bank_id,
            ancestors_max_slot,
            use_empty_ancestors,
        })
    }

    pub fn max_root(&self) -> Slot {
        self.max_root
    }

    /// Resolve which ancestors to use for this scan.
    ///
    /// For any bank `B` descended from the current `max_root`, it must be true
    /// that `B.ancestors.contains(max_root)`, regardless of squash behavior.
    /// (Proof: at startup max_root is the greatest root from the snapshot, and
    /// on each `set_root(R_new)` where `R_new > R`, every surviving descendant
    /// of `R_new` was also a descendant of `R` and therefore has `R_new` in its
    /// ancestors.)
    ///
    /// If `max_root` is **not** in `ancestors`, the bank is either:
    /// 1. on a different fork, or
    /// 2. an ancestor of `max_root`.
    ///
    /// In both cases the provided ancestors may reference slots that have
    /// already been cleaned, so we fall back to an empty ancestor set and rely
    /// only on roots (bounded by `max_root`).
    ///
    /// When ancestors **is** used:
    /// - Ancestors <= `max_root` are all rooted, protected by `ongoing_scan_roots`.
    /// - Ancestors > `max_root` are kept alive by the `Bank::parent` reference
    ///   chain, so they cannot be cleaned mid-scan.
    pub fn resolve_ancestors<'b>(
        &self,
        ancestors: &'b Ancestors,
        empty: &'b Ancestors,
    ) -> &'b Ancestors {
        if self.use_empty_ancestors {
            empty
        } else {
            ancestors
        }
    }

    /// Finalize the scan: checks whether the bank was removed during the scan.
    /// The `Drop` impl handles unpinning regardless of whether this is called.
    pub fn finish(self) -> Result<(), ScanError> {
        let was_scan_corrupted = self
            .scan_state
            .removed_bank_ids
            .lock()
            .unwrap()
            .contains(&self.scan_bank_id);

        if was_scan_corrupted {
            Err(ScanError::SlotRemoved {
                slot: self.ancestors_max_slot,
                bank_id: self.scan_bank_id,
            })
        } else {
            Ok(())
        }
    }
}

impl Drop for ScanGuard<'_> {
    fn drop(&mut self) {
        self.scan_state
            .active_scans
            .fetch_sub(1, Ordering::Relaxed);
        let mut ongoing_scan_roots = self
            .scan_state
            .ongoing_scan_roots
            .write()
            .unwrap();
        let count = ongoing_scan_roots.get_mut(&self.max_root).unwrap();
        *count -= 1;
        if *count == 0 {
            ongoing_scan_roots.remove(&self.max_root);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_scan_state() -> ScanState {
        ScanState::new(None)
    }

    // --- ScanConfig tests ---

    #[test]
    fn test_scan_config_default() {
        let config = ScanConfig::default();
        assert_eq!(config.scan_order, ScanOrder::Unsorted);
        assert!(config.abort.is_none());
        assert!(!config.is_aborted());
    }

    #[test]
    fn test_scan_config_new_sorted() {
        let config = ScanConfig::new(ScanOrder::Sorted);
        assert_eq!(config.scan_order, ScanOrder::Sorted);
    }

    #[test]
    fn test_scan_config_abort() {
        let config = ScanConfig::default();
        // no abort handle — abort is a no-op, is_aborted stays false
        config.abort();
        assert!(!config.is_aborted());

        let config = config.recreate_with_abort();
        assert!(!config.is_aborted());
        config.abort();
        assert!(config.is_aborted());
    }

    #[test]
    fn test_scan_config_recreate_shares_abort() {
        let config = ScanConfig::default().recreate_with_abort();
        let config2 = config.recreate_with_abort();
        config.abort();
        assert!(config2.is_aborted());
    }

    // --- ScanState tests ---

    #[test]
    fn test_scan_state_new() {
        let state = ScanState::new(Some(1024));
        assert_eq!(state.scan_results_limit_bytes, Some(1024));
        assert_eq!(state.active_scans.load(Ordering::Relaxed), 0);
        assert_eq!(state.min_ongoing_scan_root(), None);
    }

    #[test]
    fn test_scan_state_min_ongoing_scan_root() {
        let state = new_scan_state();
        assert_eq!(state.min_ongoing_scan_root(), None);

        state.ongoing_scan_roots.write().unwrap().insert(10, 1);
        assert_eq!(state.min_ongoing_scan_root(), Some(10));

        state.ongoing_scan_roots.write().unwrap().insert(5, 1);
        assert_eq!(state.min_ongoing_scan_root(), Some(5));

        state.ongoing_scan_roots.write().unwrap().remove(&5);
        assert_eq!(state.min_ongoing_scan_root(), Some(10));
    }

    // --- ScanGuard tests ---

    #[test]
    fn test_lifecycle_new_pins_root() {
        let state = new_scan_state();
        let ancestors = Ancestors::default();
        let scan = ScanGuard::new(&state, &ancestors, 0, 42).unwrap();

        assert_eq!(scan.max_root(), 42);
        assert_eq!(state.active_scans.load(Ordering::Relaxed), 1);
        assert_eq!(state.min_ongoing_scan_root(), Some(42));

        drop(scan);
        assert_eq!(state.active_scans.load(Ordering::Relaxed), 0);
        assert_eq!(state.min_ongoing_scan_root(), None);
    }

    #[test]
    fn test_lifecycle_refcounts_same_root() {
        let state = new_scan_state();
        let ancestors = Ancestors::default();
        let scan1 = ScanGuard::new(&state, &ancestors, 0, 10).unwrap();
        let scan2 = ScanGuard::new(&state, &ancestors, 0, 10).unwrap();

        assert_eq!(state.active_scans.load(Ordering::Relaxed), 2);
        assert_eq!(*state.ongoing_scan_roots.read().unwrap().get(&10).unwrap(), 2);

        drop(scan1);
        assert_eq!(state.active_scans.load(Ordering::Relaxed), 1);
        assert_eq!(state.min_ongoing_scan_root(), Some(10));

        drop(scan2);
        assert_eq!(state.min_ongoing_scan_root(), None);
    }

    #[test]
    fn test_lifecycle_rejected_for_removed_bank() {
        let state = new_scan_state();
        state.removed_bank_ids.lock().unwrap().insert(7);

        let ancestors = Ancestors::default();
        let result = ScanGuard::new(&state, &ancestors, 7, 100);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ScanError::SlotRemoved { slot: 0, bank_id: 7 }
        );
        // should not have incremented active_scans
        assert_eq!(state.active_scans.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_lifecycle_finish_ok_when_bank_not_removed() {
        let state = new_scan_state();
        let ancestors = Ancestors::default();
        let scan = ScanGuard::new(&state, &ancestors, 0, 50).unwrap();
        assert!(scan.finish().is_ok());
    }

    #[test]
    fn test_lifecycle_finish_err_when_bank_removed_during_scan() {
        let state = new_scan_state();
        let ancestors = Ancestors::default();
        let scan = ScanGuard::new(&state, &ancestors, 5, 50).unwrap();

        // simulate bank removal mid-scan
        state.removed_bank_ids.lock().unwrap().insert(5);

        let result = scan.finish();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ScanError::SlotRemoved { slot: 0, bank_id: 5 }
        );
    }

    #[test]
    fn test_lifecycle_drop_unpins_even_without_finish() {
        let state = new_scan_state();
        let ancestors = Ancestors::default();
        let scan = ScanGuard::new(&state, &ancestors, 0, 30).unwrap();
        assert_eq!(state.min_ongoing_scan_root(), Some(30));

        // drop without calling finish
        drop(scan);
        assert_eq!(state.min_ongoing_scan_root(), None);
        assert_eq!(state.active_scans.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_lifecycle_tracks_max_distance() {
        let state = new_scan_state();
        let ancestors = Ancestors::default();

        // first scan pins root at 10
        let scan1 = ScanGuard::new(&state, &ancestors, 0, 10).unwrap();
        assert_eq!(state.max_distance_to_min_scan_slot.load(Ordering::Relaxed), 0);

        // second scan at 20: distance = 20 - 10 = 10
        let _scan2 = ScanGuard::new(&state, &ancestors, 0, 20).unwrap();
        assert_eq!(state.max_distance_to_min_scan_slot.load(Ordering::Relaxed), 10);

        // third scan at 50: distance = 50 - 10 = 40
        let _scan3 = ScanGuard::new(&state, &ancestors, 0, 50).unwrap();
        assert_eq!(state.max_distance_to_min_scan_slot.load(Ordering::Relaxed), 40);

        drop(scan1);
        // after dropping the oldest, min is now 20; new scan at 25 → distance = 25 - 20 = 5
        // but max_distance is still 40 (fetch_max)
        let _scan4 = ScanGuard::new(&state, &ancestors, 0, 25).unwrap();
        assert_eq!(state.max_distance_to_min_scan_slot.load(Ordering::Relaxed), 40);
    }

    #[test]
    fn test_lifecycle_resolve_ancestors_uses_provided_when_max_root_in_ancestors() {
        let state = new_scan_state();
        let mut ancestors = Ancestors::default();
        ancestors.insert(42);

        let scan = ScanGuard::new(&state, &ancestors, 0, 42).unwrap();
        let empty = Ancestors::default();
        let resolved = scan.resolve_ancestors(&ancestors, &empty);
        // max_root (42) is in ancestors, so we use the provided ancestors
        assert!(resolved.contains_key(&42));
    }

    #[test]
    fn test_lifecycle_resolve_ancestors_uses_empty_when_max_root_not_in_ancestors() {
        let state = new_scan_state();
        let mut ancestors = Ancestors::default();
        ancestors.insert(10);

        // max_root_inclusive = 42, which is NOT in ancestors
        let scan = ScanGuard::new(&state, &ancestors, 0, 42).unwrap();
        let empty = Ancestors::default();
        let resolved = scan.resolve_ancestors(&ancestors, &empty);
        // should get the empty ancestors back
        assert!(!resolved.contains_key(&10));
    }
}
