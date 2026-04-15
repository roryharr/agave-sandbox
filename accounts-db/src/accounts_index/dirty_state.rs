use std::sync::atomic::{AtomicU8, Ordering};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirtyState {
    /// Entry is in sync with the disk index. Safe to evict
    Clean = 0,
    /// Item is being flushed to the disk. Not safe to evict
    Flushing = 1,
    /// Entry has changes not yet written to disk. Not safe to evict
    Dirty = 2,
}

impl From<u8> for DirtyState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Clean,
            1 => Self::Flushing,
            2 => Self::Dirty,
            _ => panic!("invalid value for DirtyState: {value}"),
        }
    }
}

/// Atomic wrapper for DirtyState, owning all state-transition operations
#[derive(Debug)]
pub struct AtomicDirtyState(AtomicU8);

impl Default for AtomicDirtyState {
    fn default() -> Self {
        Self(AtomicU8::new(DirtyState::Clean as u8))
    }
}

impl AtomicDirtyState {
    pub fn new(state: DirtyState) -> Self {
        Self(AtomicU8::new(state as u8))
    }

    pub fn load(&self) -> DirtyState {
        DirtyState::from(self.0.load(Ordering::Acquire))
    }

    /// Returns true when the entry is Dirty or Flushing, and thus not safe to evict
    pub fn is_dirty(&self) -> bool {
        self.load() != DirtyState::Clean
    }

    /// Returns true when the entry is Clean and thus safe to evict
    pub fn is_evictable(&self) -> bool {
        self.load() == DirtyState::Clean
    }

    /// Mark the entry as Dirty, having changes that are not yet flushed to disk
    pub fn mark_dirty(&self) {
        self.0.store(DirtyState::Dirty as u8, Ordering::Release);
    }

    /// Transitions from Dirty to Flushing. Returns true if the transition succeeded
    pub fn begin_flush(&self) -> bool {
        self.0
            .compare_exchange(
                DirtyState::Dirty as u8,
                DirtyState::Flushing as u8,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }

    /// Transitions from Flushing to Clean. Returns true if the transition succeeded
    pub fn end_flush(&self) -> bool {
        self.0
            .compare_exchange(
                DirtyState::Flushing as u8,
                DirtyState::Clean as u8,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use {super::*, test_case::test_case};

    #[test_case(DirtyState::Clean, false, true; "clean")]
    #[test_case(DirtyState::Flushing, true, false; "flushing")]
    #[test_case(DirtyState::Dirty, true, false; "dirty")]
    fn test_state_properties(
        state: DirtyState,
        expected_is_dirty: bool,
        expected_is_evictable: bool,
    ) {
        let s = AtomicDirtyState::new(state);
        assert_eq!(s.is_dirty(), expected_is_dirty);
        assert_eq!(s.is_evictable(), expected_is_evictable);
    }

    #[test]
    fn default_is_clean() {
        let s = AtomicDirtyState::default();
        assert_eq!(s.load(), DirtyState::Clean);
    }

    #[test_case(DirtyState::Clean; "from_clean")]
    #[test_case(DirtyState::Flushing; "from_flushing")]
    #[test_case(DirtyState::Dirty; "from_dirty")]
    fn test_mark_dirty(initial: DirtyState) {
        let s = AtomicDirtyState::new(initial);
        s.mark_dirty();
        assert_eq!(s.load(), DirtyState::Dirty);
    }

    #[test_case(DirtyState::Dirty, true, DirtyState::Flushing; "from_dirty_succeeds")]
    #[test_case(DirtyState::Clean, false, DirtyState::Clean; "from_clean_fails")]
    #[test_case(DirtyState::Flushing, false, DirtyState::Flushing; "from_flushing_fails")]
    fn test_begin_flush(initial: DirtyState, expected_return: bool, expected_state: DirtyState) {
        let s = AtomicDirtyState::new(initial);
        assert_eq!(s.begin_flush(), expected_return);
        assert_eq!(s.load(), expected_state);
    }

    #[test_case(DirtyState::Flushing, true, DirtyState::Clean; "transitions_to_clean")]
    #[test_case(DirtyState::Dirty, false, DirtyState::Dirty; "noop_when_dirty")]
    #[test_case(DirtyState::Clean, false, DirtyState::Clean; "noop_when_clean")]
    fn test_end_flush(initial: DirtyState, expected_return: bool, expected_state: DirtyState) {
        let s = AtomicDirtyState::new(initial);
        assert_eq!(s.end_flush(), expected_return);
        assert_eq!(s.load(), expected_state);
    }

    #[test_case(0, DirtyState::Clean)]
    #[test_case(1, DirtyState::Flushing)]
    #[test_case(2, DirtyState::Dirty)]
    fn test_from_u8(value: u8, expected: DirtyState) {
        assert_eq!(DirtyState::from(value), expected);
    }

    #[test_case(3)]
    #[test_case(255)]
    #[should_panic(expected = "invalid value for DirtyState")]
    fn test_from_u8_invalid(value: u8) {
        let _ = DirtyState::from(value);
    }

    /// Normal Path: Dirty to Flushing to Clean
    #[test]
    fn test_flush_cycle_happy_path() {
        let s = AtomicDirtyState::new(DirtyState::Dirty);
        assert!(s.begin_flush());
        assert_eq!(s.load(), DirtyState::Flushing);
        assert!(s.end_flush());
        assert_eq!(s.load(), DirtyState::Clean);
    }

    /// A write arriving while a flush is in-flight must not be lost.
    ///
    /// Sequence: Dirty -> Flushing (begin_flush), then a concurrent
    /// write calls mark_dirty() -> Dirty, then the flush completes and
    /// end_flush() must leave the entry Dirty for the next cycle.
    #[test]
    fn test_concurrent_write_during_flush_is_not_lost() {
        let s = AtomicDirtyState::new(DirtyState::Dirty);
        assert!(s.begin_flush());
        s.mark_dirty(); // concurrent write while disk write is in-flight
        assert_eq!(s.load(), DirtyState::Dirty);
        assert!(!s.end_flush()); // disk write completes -- must not clear the new dirty
        assert_eq!(s.load(), DirtyState::Dirty);
    }
}
