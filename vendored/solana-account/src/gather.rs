//! Kernel copy-on-write support for account data (Linux only).
//!
//! A write session on large account data builds a *gather mmap*: one
//! contiguous virtual-address range assembled from `MAP_FIXED | MAP_PRIVATE`
//! mappings of the memfd-backed segment sources, plus anonymous zero pages
//! for growth headroom. Writes through the gather make the kernel copy the
//! touched 4KB page; untouched pages keep reading the shared source
//! physical pages. At commit, a `/proc/self/pagemap` scan finds the
//! copied (now anonymous) pages, and the new segment list splices freshly
//! captured dirty runs with references to the untouched ranges of the old
//! sources.
//!
//! Design doc: cow_accounts_shared_data.md. Kernel behavior validated by
//! cow_accounts_probe.c.

use {
    crate::{AccountData, Repr, Segment, SegmentList, SegmentSource},
    std::{
        fmt, io,
        os::{
            fd::{AsRawFd, FromRawFd, OwnedFd},
            unix::fs::FileExt,
        },
        ptr::NonNull,
        slice,
        sync::{Arc, OnceLock},
    },
};

pub(crate) const PAGE_SIZE: usize = 4096;

/// A commit whose splice would exceed this many segments is compacted into
/// a single fresh source instead. Bounds both the per-account metadata and
/// the VMA count of future gathers of this data.
const MAX_SEGMENTS: usize = 512;

fn round_up_to_page(n: usize) -> usize {
    n.div_ceil(PAGE_SIZE) * PAGE_SIZE
}

fn io_err(context: &str) -> io::Error {
    let err = io::Error::last_os_error();
    io::Error::new(err.kind(), format!("{context}: {err}"))
}

/// Immutable memfd-backed bytes: the gather-mappable segment source.
///
/// The bytes are written once at construction and never change afterwards;
/// physical memory is released when the last `Arc` drops (the memfd closes
/// and its pages die with it).
pub(crate) struct MemfdSource {
    fd: OwnedFd,
    map: NonNull<u8>,
    /// Page-rounded; bytes past the written content read as zeros.
    mapped_len: usize,
}

// SAFETY: the mapping is immutable after construction and owned by this
// struct; gather mappings of the fd are MAP_PRIVATE and never write through.
unsafe impl Send for MemfdSource {}
unsafe impl Sync for MemfdSource {}

impl MemfdSource {
    /// Creates a source holding the concatenation of `chunks` (`total_len`
    /// bytes). Every chunk but the last must be a multiple of the page size
    /// if callers rely on page-aligned interior offsets (commit does).
    pub(crate) fn from_chunks<'a>(
        chunks: impl Iterator<Item = &'a [u8]>,
        total_len: usize,
    ) -> io::Result<Self> {
        debug_assert!(total_len > 0);
        let mapped_len = round_up_to_page(total_len);
        let fd = unsafe {
            let raw = libc::memfd_create(c"solana-account-data".as_ptr(), libc::MFD_CLOEXEC);
            if raw < 0 {
                return Err(io_err("memfd_create"));
            }
            OwnedFd::from_raw_fd(raw)
        };
        if unsafe { libc::ftruncate(fd.as_raw_fd(), mapped_len as libc::off_t) } != 0 {
            return Err(io_err("ftruncate"));
        }
        let file = std::fs::File::from(fd);
        let mut offset = 0u64;
        for chunk in chunks {
            file.write_all_at(chunk, offset)?;
            offset += chunk.len() as u64;
        }
        debug_assert_eq!(offset as usize, total_len);
        let fd = OwnedFd::from(file);
        let map = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                mapped_len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if map == libc::MAP_FAILED {
            return Err(io_err("mmap source"));
        }
        Ok(Self {
            fd,
            map: NonNull::new(map.cast()).expect("mmap never returns null"),
            mapped_len,
        })
    }

    /// The full page-rounded content.
    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: the mapping is valid for mapped_len bytes for the lifetime
        // of self and never mutated.
        unsafe { slice::from_raw_parts(self.map.as_ptr(), self.mapped_len) }
    }
}

impl Drop for MemfdSource {
    fn drop(&mut self) {
        // SAFETY: base/len are the mapping created in from_chunks.
        unsafe { libc::munmap(self.map.as_ptr().cast(), self.mapped_len) };
    }
}

impl fmt::Debug for MemfdSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemfdSource")
            .field("fd", &self.fd.as_raw_fd())
            .field("mapped_len", &self.mapped_len)
            .finish()
    }
}

/// The reserved gather address range; unmapped wholesale on drop.
struct Reservation {
    base: NonNull<u8>,
    reserved_len: usize,
}

impl Reservation {
    fn new(reserved_len: usize) -> io::Result<Self> {
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                reserved_len,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io_err("mmap reserve"));
        }
        Ok(Self {
            base: NonNull::new(base.cast()).expect("mmap never returns null"),
            reserved_len,
        })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        // SAFETY: unmaps exactly the range reserved in new(); MAP_FIXED
        // overlays within it are torn down by the same call.
        unsafe { libc::munmap(self.base.as_ptr().cast(), self.reserved_len) };
    }
}

/// One `MAP_FIXED | MAP_PRIVATE` run of the gather: which source bytes are
/// mapped at which gather offset. Commit maps clean pages back to their
/// source through this.
struct GatherRun {
    gather_offset: usize,
    mapped_len: usize,
    source: Arc<MemfdSource>,
    source_offset: usize,
}

/// An active kernel-COW write session. See the module docs.
pub(crate) struct GatherWrite {
    reservation: Reservation,
    /// Current logical data length; can move within the reservation.
    len: usize,
    /// Page-rounded extent mapped from sources; pages past it are anonymous.
    file_extent: usize,
    layout: Vec<GatherRun>,
    /// The data this session started from, for the read-only fast path.
    original: AccountData,
    original_len: usize,
    /// True if `original`'s own segments are mapped (no conversion copy was
    /// needed), making the no-write commit an identity.
    original_mapped: bool,
}

// SAFETY: the gather range is exclusively owned by this session; sources are
// Sync. The raw base pointer is only dereferenced through &self/&mut self.
unsafe impl Send for GatherWrite {}

impl GatherWrite {
    pub(crate) fn build(data: &AccountData, reserve_extra: usize) -> io::Result<Self> {
        let len = data.len();
        debug_assert!(len > 0);
        let (runs, original_mapped) = match mappable_runs(data) {
            Some(runs) => (runs, true),
            None => {
                // Not gather-mappable (heap-backed or unaligned segments):
                // convert once into a single memfd source. This is the
                // one-time O(len) copy a freshly loaded or heap-built
                // account pays to enter the kernel-COW regime.
                let source = Arc::new(MemfdSource::from_chunks(data.data_chunks(), len)?);
                (vec![(source, 0, round_up_to_page(len))], false)
            }
        };

        let file_extent = round_up_to_page(len);
        debug_assert_eq!(
            file_extent,
            runs.iter()
                .map(|(_, _, mapped_len)| mapped_len)
                .sum::<usize>()
        );
        let reserved_len = round_up_to_page(len.saturating_add(reserve_extra));
        let reservation = Reservation::new(reserved_len)?;
        let base = reservation.base.as_ptr();

        let mut layout = Vec::with_capacity(runs.len());
        let mut gather_offset = 0;
        for (source, source_offset, mapped_len) in runs {
            debug_assert!(source_offset + mapped_len <= source.mapped_len);
            let mapped = unsafe {
                libc::mmap(
                    base.add(gather_offset).cast(),
                    mapped_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_FIXED,
                    source.fd.as_raw_fd(),
                    source_offset as libc::off_t,
                )
            };
            if mapped == libc::MAP_FAILED {
                return Err(io_err("mmap gather run"));
            }
            layout.push(GatherRun {
                gather_offset,
                mapped_len,
                source,
                source_offset,
            });
            gather_offset += mapped_len;
        }

        if reserved_len > file_extent {
            let tail = unsafe {
                libc::mmap(
                    base.add(file_extent).cast(),
                    reserved_len - file_extent,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED,
                    -1,
                    0,
                )
            };
            if tail == libc::MAP_FAILED {
                return Err(io_err("mmap gather tail"));
            }
        }

        Ok(Self {
            reservation,
            len,
            file_extent,
            layout,
            original: data.clone(),
            original_len: len,
            original_mapped,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn reserved_len(&self) -> usize {
        self.reservation.reserved_len
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // SAFETY: [base, len) is mapped readable for the session's lifetime;
        // untouched anonymous pages read as zeros.
        unsafe { slice::from_raw_parts(self.reservation.base.as_ptr(), self.len) }
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as as_slice, plus the range is writable and exclusively
        // owned by this session (writes COW, never reaching the sources).
        unsafe { slice::from_raw_parts_mut(self.reservation.base.as_ptr(), self.len) }
    }

    /// Grows or shrinks within the reservation; returns false (unchanged)
    /// beyond it. Growth explicitly fills the new bytes, which also makes
    /// shrink-then-regrow read deterministically.
    pub(crate) fn resize(&mut self, new_len: usize, fill: u8) -> bool {
        if new_len > self.reservation.reserved_len {
            return false;
        }
        if new_len > self.len {
            // SAFETY: [len, new_len) is within the mapped writable range.
            unsafe {
                self.reservation
                    .base
                    .as_ptr()
                    .add(self.len)
                    .write_bytes(fill, new_len - self.len)
            };
        }
        self.len = new_len;
        true
    }

    /// Captures the session's bytes as new account data: dirty (COW'd) page
    /// runs are copied into one fresh source, untouched ranges reference the
    /// old sources. Falls back to a plain contiguous copy if the kernel
    /// bookkeeping fails.
    pub(crate) fn commit(self) -> AccountData {
        self.try_commit()
            .unwrap_or_else(|_| AccountData::from(self.as_slice().to_vec()))
    }

    fn try_commit(&self) -> io::Result<AccountData> {
        let final_len = self.len;
        if final_len == 0 {
            return Ok(AccountData::default());
        }
        let n_pages = final_len.div_ceil(PAGE_SIZE);
        let entries = self.read_pagemap(n_pages)?;
        let file_pages = self.file_extent / PAGE_SIZE;
        // A page holds new bytes iff the kernel COW'd it (it left the file
        // backing), or it lies past the file extent (growth pages, anonymous
        // by construction). Fault-around maps pages file-backed only, so it
        // never produces a false dirty; a swapped-out COW page still counts.
        let page_is_dirty = |page: usize| {
            page >= file_pages || {
                let entry = entries[page];
                entry & (PM_PRESENT | PM_SWAPPED) != 0 && entry & PM_FILE == 0
            }
        };

        let any_dirty = (0..n_pages).any(page_is_dirty);
        if !any_dirty && final_len == self.original_len && self.original_mapped {
            // Read-only session: keep the existing data (and its identity).
            return Ok(self.original.clone());
        }

        // Split [0, n_pages) into maximal clean/dirty page runs.
        let mut page_runs = Vec::new(); // (start_page, page_count, dirty)
        for page in 0..n_pages {
            let dirty = page_is_dirty(page);
            match page_runs.last_mut() {
                Some((_, count, last_dirty)) if *last_dirty == dirty => *count += 1,
                _ => page_runs.push((page, 1usize, dirty)),
            }
        }

        // All dirty runs go into one fresh source, packed in order. Runs are
        // page-multiples except possibly the final one, so every run starts
        // page-aligned within the source.
        let byte_len = |start_page: usize, page_count: usize| {
            (start_page * PAGE_SIZE + page_count * PAGE_SIZE).min(final_len)
                - start_page * PAGE_SIZE
        };
        let dirty_total: usize = page_runs
            .iter()
            .filter(|(_, _, dirty)| *dirty)
            .map(|&(start, count, _)| byte_len(start, count))
            .sum();
        let dirty_source = if dirty_total > 0 {
            let gather = self.as_slice();
            Some(Arc::new(MemfdSource::from_chunks(
                page_runs
                    .iter()
                    .filter(|(_, _, dirty)| *dirty)
                    .map(|&(start, count, _)| {
                        &gather[start * PAGE_SIZE..start * PAGE_SIZE + byte_len(start, count)]
                    }),
                dirty_total,
            )?))
        } else {
            None
        };

        let mut segments = Vec::new();
        let mut dirty_offset = 0;
        for &(start_page, page_count, dirty) in &page_runs {
            let run_start = start_page * PAGE_SIZE;
            let run_len = byte_len(start_page, page_count);
            if dirty {
                segments.push(Segment {
                    source: SegmentSource::Memfd(Arc::clone(
                        dirty_source.as_ref().expect("dirty run implies source"),
                    )),
                    offset: dirty_offset,
                    len: run_len,
                });
                dirty_offset += round_up_to_page(run_len);
            } else {
                // Map the clean range back to its sources; it may span
                // several layout runs.
                let mut remaining_start = run_start;
                let run_end = run_start + run_len;
                for gather_run in &self.layout {
                    let mapped_end = gather_run.gather_offset + gather_run.mapped_len;
                    if mapped_end <= remaining_start {
                        continue;
                    }
                    if gather_run.gather_offset >= run_end {
                        break;
                    }
                    let piece_end = run_end.min(mapped_end);
                    segments.push(Segment {
                        source: SegmentSource::Memfd(Arc::clone(&gather_run.source)),
                        offset: gather_run.source_offset
                            + (remaining_start - gather_run.gather_offset),
                        len: piece_end - remaining_start,
                    });
                    remaining_start = piece_end;
                    if remaining_start == run_end {
                        break;
                    }
                }
                debug_assert_eq!(remaining_start, run_end);
            }
        }
        debug_assert_eq!(segments.iter().map(|s| s.len).sum::<usize>(), final_len);

        if segments.len() > MAX_SEGMENTS {
            // Compact: one contiguous copy resets fragmentation (and the
            // VMA count of future gathers) to a single run.
            let source = MemfdSource::from_chunks(std::iter::once(self.as_slice()), final_len)?;
            segments = vec![Segment {
                source: SegmentSource::Memfd(Arc::new(source)),
                offset: 0,
                len: final_len,
            }];
        }

        Ok(AccountData {
            repr: Repr::Fragmented(Arc::new(SegmentList {
                segments,
                len: final_len,
                contiguous: OnceLock::new(),
            })),
        })
    }

    fn read_pagemap(&self, n_pages: usize) -> io::Result<Vec<u64>> {
        let pagemap = std::fs::File::open("/proc/self/pagemap")?;
        let mut buf = vec![0u8; n_pages * 8];
        let first_page = self.reservation.base.as_ptr() as u64 / PAGE_SIZE as u64;
        pagemap.read_exact_at(&mut buf, first_page * 8)?;
        Ok(buf
            .chunks_exact(8)
            .map(|chunk| u64::from_ne_bytes(chunk.try_into().expect("chunks of 8")))
            .collect())
    }
}

// /proc/self/pagemap entry flags (unprivileged: PFN bits are masked but
// these flag bits are readable; validated by the probe).
const PM_PRESENT: u64 = 1 << 63;
const PM_SWAPPED: u64 = 1 << 62;
const PM_FILE: u64 = 1 << 61;

/// The gather-mappable runs of `data`: coalesced (source, source_offset,
/// mapped_len) covering all bytes, or None if any part is heap-backed or
/// not page-aligned.
#[allow(clippy::type_complexity)]
fn mappable_runs(data: &AccountData) -> Option<Vec<(Arc<MemfdSource>, usize, usize)>> {
    let Repr::Fragmented(list) = &data.repr else {
        return None;
    };
    let mut runs: Vec<(Arc<MemfdSource>, usize, usize)> = Vec::new();
    for (index, segment) in list.segments.iter().enumerate() {
        let SegmentSource::Memfd(source) = &segment.source else {
            return None;
        };
        if segment.offset % PAGE_SIZE != 0 {
            return None;
        }
        let is_last = index == list.segments.len() - 1;
        if !is_last && segment.len % PAGE_SIZE != 0 {
            return None;
        }
        let mapped_len = round_up_to_page(segment.len);
        if segment.offset + mapped_len > source.mapped_len {
            return None;
        }
        match runs.last_mut() {
            Some((last_source, last_offset, last_len))
                if Arc::ptr_eq(last_source, source)
                    && *last_offset + *last_len == segment.offset =>
            {
                *last_len += mapped_len;
            }
            _ => runs.push((Arc::clone(source), segment.offset, mapped_len)),
        }
    }
    Some(runs)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::write::AccountDataWrite};

    const MB: usize = 1024 * 1024;
    const RESERVE: usize = 10 * 1024; // MAX_PERMITTED_DATA_INCREASE

    fn segment_count(data: &AccountData) -> usize {
        match &data.repr {
            Repr::Contiguous(_) => 1,
            Repr::Fragmented(list) => list.segments.len(),
        }
    }

    fn is_gather(write: &AccountDataWrite) -> bool {
        write.is_gather_for_tests()
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i / PAGE_SIZE) as u8 ^ (i % 251) as u8)
            .collect()
    }

    #[test]
    fn test_first_write_converts_and_cows_one_page() {
        let reference = pattern(2 * MB);
        let base = AccountData::from(reference.clone());
        let mut write = base.begin_write(RESERVE);
        assert!(is_gather(&write));

        let mut expected = reference.clone();
        write.as_mut_slice()[100 * PAGE_SIZE] = 0xAA;
        expected[100 * PAGE_SIZE] = 0xAA;

        let committed = write.commit();
        assert_eq!(committed.as_slice(), expected.as_slice());
        // clean prefix, one dirty page, clean suffix
        assert_eq!(segment_count(&committed), 3);
        // the original is untouched
        assert_eq!(base.as_slice(), reference.as_slice());
    }

    #[test]
    fn test_read_only_commit() {
        let base = AccountData::from(pattern(2 * MB));
        // first session converts heap -> memfd even without writes
        let converted = base.begin_write(RESERVE).commit();
        assert_eq!(converted, base);
        assert_eq!(segment_count(&converted), 1);
        // a read-only session on converted data returns it by identity
        let unchanged = converted.begin_write(RESERVE).commit();
        assert!(unchanged.ptr_eq(&converted));
    }

    #[test]
    fn test_second_cycle_shares_and_isolates() {
        let mut expected = pattern(2 * MB);
        let v1 = AccountData::from(expected.clone());

        let mut write = v1.begin_write(RESERVE);
        write.as_mut_slice()[100 * PAGE_SIZE] = 0xAA;
        expected[100 * PAGE_SIZE] = 0xAA;
        let v2 = write.commit();
        let v2_snapshot = expected.clone();

        let mut write = v2.begin_write(RESERVE);
        assert!(is_gather(&write));
        write.as_mut_slice()[300 * PAGE_SIZE] = 0xBB;
        expected[300 * PAGE_SIZE] = 0xBB;
        let v3 = write.commit();

        assert_eq!(v3.as_slice(), expected.as_slice());
        assert_eq!(segment_count(&v3), 5);
        // COW isolation: v2 still reads its own snapshot
        assert_eq!(v2.as_slice(), v2_snapshot.as_slice());
    }

    #[test]
    fn test_scattered_writes_splice_exactly() {
        let mut expected = pattern(2 * MB);
        let base = AccountData::from(expected.clone());
        let mut write = base.begin_write(RESERVE);
        for page in [3usize, 200, 450] {
            write.as_mut_slice()[page * PAGE_SIZE + 7] = 0xCC;
            expected[page * PAGE_SIZE + 7] = 0xCC;
        }
        let committed = write.commit();
        assert_eq!(committed.as_slice(), expected.as_slice());
        // clean/dirty alternation: c d c d c d c
        assert_eq!(segment_count(&committed), 7);
    }

    #[test]
    fn test_growth_shrink_regrow() {
        let len = 2 * MB;
        let base = AccountData::from(pattern(len));
        let mut write = base.begin_write(RESERVE);
        assert_eq!(write.reserved_len(), round_up_to_page(len + RESERVE));

        // grow with a visible fill
        assert!(write.resize(len + 5000, 7));
        assert_eq!(&write.as_slice()[len..], &[7u8; 5000][..]);
        // beyond the reservation is refused
        assert!(!write.resize(len + RESERVE + PAGE_SIZE, 0));

        // shrink, then regrow with zeros: the refilled range must be zeros
        assert!(write.resize(len - 100, 0));
        assert!(write.resize(len + 5000, 0));
        assert_eq!(&write.as_slice()[len - 100..], &vec![0u8; 5100][..]);

        let committed = write.commit();
        assert_eq!(committed.len(), len + 5000);
        assert_eq!(
            &committed.as_slice()[..len - 100],
            &pattern(len)[..len - 100]
        );
        assert_eq!(&committed.as_slice()[len - 100..], &vec![0u8; 5100][..]);
    }

    #[test]
    fn test_compaction_cap() {
        let len = 8 * MB; // 2048 pages
        let base = AccountData::from(pattern(len));
        let mut write = base.begin_write(RESERVE);
        // dirty every other page: ~1024 dirty runs -> way past MAX_SEGMENTS
        for page in (0..len / PAGE_SIZE).step_by(2) {
            write.as_mut_slice()[page * PAGE_SIZE] = 0xEE;
        }
        let committed = write.commit();
        assert_eq!(segment_count(&committed), 1);
        let mut expected = pattern(len);
        for page in (0..len / PAGE_SIZE).step_by(2) {
            expected[page * PAGE_SIZE] = 0xEE;
        }
        assert_eq!(committed.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_unmappable_fragmented_input_converts() {
        // dev-context fragmented data is heap-backed and unaligned: the
        // session must convert it and still commit correctly
        let half = pattern(MB);
        let base = AccountData::from_chunks_for_tests(&[&half, &half[..MB - 13]]);
        let mut write = base.begin_write(RESERVE);
        assert!(is_gather(&write));
        write.as_mut_slice()[0] = 0xDD;
        let committed = write.commit();
        assert_eq!(committed.len(), 2 * MB - 13);
        assert_eq!(committed.as_slice()[0], 0xDD);
        assert_eq!(committed.as_slice()[1..], base.as_slice()[1..]);
    }

    #[test]
    fn test_commit_interoperates_with_generic_paths() {
        // fragmented arena-backed data must behave under the ordinary
        // AccountData API: eq, chunked iteration, software mutation
        let mut expected = pattern(2 * MB);
        let base = AccountData::from(expected.clone());
        let mut write = base.begin_write(RESERVE);
        write.as_mut_slice()[50 * PAGE_SIZE] = 9;
        expected[50 * PAGE_SIZE] = 9;
        let committed = write.commit();

        assert_eq!(committed, AccountData::from(expected.clone()));
        let collected: Vec<u8> = committed
            .data_chunks()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        assert_eq!(collected, expected);

        // software mutation materializes to contiguous and works
        let mut mutated = committed.clone();
        mutated.as_mut_slice()[0] = 42;
        expected[0] = 42;
        assert_eq!(mutated.as_slice(), expected.as_slice());
        assert!(!mutated.is_shared());
    }
}
