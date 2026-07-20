//! Write sessions over account data.
//!
//! `AccountData::begin_write` opens a contiguous writable view with growth
//! headroom; `commit` produces the new data. Large data on Linux uses the
//! kernel-COW gather mmap (see `gather`), so untouched pages stay shared
//! with the original and only written pages are copied. Everywhere else the
//! session is a plain unique contiguous buffer — exactly the pre-existing
//! unshare-by-copy semantics.

use crate::AccountData;

/// Minimum data length for the kernel-COW gather path.
///
/// Set to one page while the machinery is being proven, so every multi-page
/// account exercises it. For production this wants raising to roughly the
/// break-even point where the fixed syscall cost of a gather session (a few
/// mmaps, a memfd, a pagemap read: ~15-20us measured) matches copying the
/// bytes instead — around 256KiB-1MiB; tune from replay benchmarks.
#[cfg(target_os = "linux")]
const GATHER_MIN_DATA_LEN: usize = 4096;

/// An open write session on account data. The view is contiguous and its
/// base pointer is stable for the session's lifetime; `resize` within the
/// reservation never moves it.
pub struct AccountDataWrite {
    inner: WriteInner,
}

enum WriteInner {
    /// A unique contiguous buffer, copied up front.
    Contiguous { data: Vec<u8>, reserved_len: usize },
    #[cfg(target_os = "linux")]
    Gather(crate::gather::GatherWrite),
}

impl AccountData {
    /// Opens a write session on a snapshot of `self`, with at least
    /// `reserve_extra` bytes of growth headroom. `self` is unaffected;
    /// [`AccountDataWrite::commit`] returns the successor data.
    pub fn begin_write(&self, reserve_extra: usize) -> AccountDataWrite {
        #[cfg(target_os = "linux")]
        if self.len() >= GATHER_MIN_DATA_LEN {
            // Gather construction can fail under fd or VMA pressure; the
            // contiguous copy below always works as the fallback.
            if let Ok(gather) = crate::gather::GatherWrite::build(self, reserve_extra) {
                return AccountDataWrite {
                    inner: WriteInner::Gather(gather),
                };
            }
        }
        let data = self.to_contiguous_vec(reserve_extra);
        AccountDataWrite {
            inner: WriteInner::Contiguous {
                reserved_len: data.len().saturating_add(reserve_extra),
                data,
            },
        }
    }
}

impl AccountDataWrite {
    pub fn len(&self) -> usize {
        match &self.inner {
            WriteInner::Contiguous { data, .. } => data.len(),
            #[cfg(target_os = "linux")]
            WriteInner::Gather(gather) => gather.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The size the session can grow to without moving; at least the
    /// starting length plus the requested headroom.
    pub fn reserved_len(&self) -> usize {
        match &self.inner {
            WriteInner::Contiguous { reserved_len, .. } => *reserved_len,
            #[cfg(target_os = "linux")]
            WriteInner::Gather(gather) => gather.reserved_len(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match &self.inner {
            WriteInner::Contiguous { data, .. } => data,
            #[cfg(target_os = "linux")]
            WriteInner::Gather(gather) => gather.as_slice(),
        }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match &mut self.inner {
            WriteInner::Contiguous { data, .. } => data.as_mut_slice(),
            #[cfg(target_os = "linux")]
            WriteInner::Gather(gather) => gather.as_mut_slice(),
        }
    }

    /// Grows (filling with `fill`) or shrinks the view within the
    /// reservation. Returns false, changing nothing, past the reservation.
    pub fn resize(&mut self, new_len: usize, fill: u8) -> bool {
        match &mut self.inner {
            WriteInner::Contiguous { data, reserved_len } => {
                if new_len > *reserved_len {
                    return false;
                }
                data.resize(new_len, fill);
                true
            }
            #[cfg(target_os = "linux")]
            WriteInner::Gather(gather) => gather.resize(new_len, fill),
        }
    }

    /// Closes the session, capturing its bytes as the successor data. On
    /// the gather path untouched pages are shared with the original, and a
    /// session that changed nothing returns the original by identity.
    pub fn commit(self) -> AccountData {
        match self.inner {
            WriteInner::Contiguous { data, .. } => AccountData::from(data),
            #[cfg(target_os = "linux")]
            WriteInner::Gather(gather) => gather.commit(),
        }
    }

    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn is_gather_for_tests(&self) -> bool {
        matches!(self.inner, WriteInner::Gather(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_write_session() {
        let base = AccountData::from(b"hello world".to_vec());
        let mut write = base.begin_write(16);
        assert_eq!(write.len(), 11);
        assert_eq!(write.reserved_len(), 27);
        assert_eq!(write.as_slice(), b"hello world");

        write.as_mut_slice()[0] = b'H';
        assert!(write.resize(13, b'!'));
        assert!(!write.resize(28, 0));

        let committed = write.commit();
        assert_eq!(committed.as_slice(), b"Hello world!!");
        // the original is untouched
        assert_eq!(base.as_slice(), b"hello world");
    }

    #[test]
    fn test_small_fragmented_input_materializes() {
        let base = AccountData::from_chunks_for_tests(&[b"hello", b" ", b"world"]);
        let mut write = base.begin_write(0);
        write.as_mut_slice()[0] = b'H';
        let committed = write.commit();
        assert_eq!(committed.as_slice(), b"Hello world");
        assert!(!committed.is_shared());
    }

    #[test]
    fn test_empty_data_write_session() {
        let base = AccountData::default();
        let mut write = base.begin_write(8);
        assert!(write.is_empty());
        assert!(write.resize(3, 5));
        let committed = write.commit();
        assert_eq!(committed.as_slice(), &[5, 5, 5]);
    }
}
