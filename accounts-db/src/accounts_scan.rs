use {
    solana_clock::{BankId, Slot},
    std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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

#[derive(Debug, Default)]
pub struct ScanConfig {
    /// checked by the scan. When true, abort scan.
    pub abort: Option<Arc<AtomicBool>>,
}

impl ScanConfig {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_config() {
        let config = ScanConfig::default();
        assert!(config.abort.is_none()); // not allocated
        assert!(!config.is_aborted());
        config.abort(); // has no effect
        assert!(!config.is_aborted());

        let config = config.recreate_with_abort();
        assert!(config.abort.is_some());
        assert!(!config.is_aborted());
        config.abort();
        assert!(config.is_aborted());

        let config = config.recreate_with_abort();
        assert!(config.is_aborted());
    }

    #[test]
    fn test_scan_config_abort() {
        let config = ScanConfig::default();
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
}
