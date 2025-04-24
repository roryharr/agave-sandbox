use {
    crate::{accounts_db::AccountStorageEntry, accounts_file::InternalsForArchive},
    std::{
        fs::File,
        io::{self, Read, Seek, SeekFrom},
        sync::Arc,
    },
};

/// A wrapper type around `AccountStorageEntry` that implements the `Read` trait.
/// This type skips over the data in the sorted dead accounts structure.
pub struct AccountStorageReader<'a> {
    sorted_dead_accounts: Vec<(usize, usize, u64)>,
    current_offset: usize,
    file: Option<File>,
    internals: InternalsForArchive<'a>,
    length: usize,
}

impl<'a> AccountStorageReader<'a> {
    /// Creates a new `AccountStorageReader` from an `AccountStorageEntry`.
    /// The dead accounts structure is sorted during initialization.
    pub fn new(
        storage: &'a Arc<AccountStorageEntry>,
        snapshot_slot: Option<u64>,
    ) -> io::Result<Self> {
        let sorted_dead_accounts = {
            let mut dead_accounts = storage.get_sorted_dead_accounts();
            dead_accounts.reverse();
            dead_accounts
                .into_iter()
                .filter(|&(_, _, slot)| {
                    snapshot_slot.is_none_or(|snapshot_slot| slot <= snapshot_slot)
                })
                .collect::<Vec<_>>()
        };

        let length = storage.accounts.len() - storage.get_dead_account_bytes(snapshot_slot);

        let file = match storage.accounts.internals_for_archive() {
            InternalsForArchive::Mmap(_internals) => None,
            InternalsForArchive::FileIo(_internals) => {
                Some(File::open(storage.accounts.path()).unwrap())
            }
        };

        Ok(Self {
            sorted_dead_accounts,
            current_offset: 0,
            file,
            internals: storage.accounts.internals_for_archive(),
            length,
        })
    }

    pub fn get_length(&self) -> usize {
        self.length
    }
}

impl Read for AccountStorageReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut total_read = 0;
        let buf_len = buf.len();

        while total_read < buf_len {
            let next_dead_account = self.sorted_dead_accounts.last();

            // Check if the current offset is within a dead account range
            if let Some(&(dead_start, dead_size, _)) = next_dead_account {
                if self.current_offset == dead_start {
                    // Skip the dead account range
                    self.current_offset += dead_size;
                    if let Some(file) = &mut self.file {
                        file.seek(SeekFrom::Start(self.current_offset as u64))?;
                    }
                    self.sorted_dead_accounts.pop();
                    continue;
                }
            }

            let remaining = if let Some(&(dead_start, _, _)) = next_dead_account {
                (dead_start as isize - self.current_offset as isize).max(0) as usize
            } else {
                buf_len - total_read
            };

            let read_size = if let Some(file) = &mut self.file {
                file.read(&mut buf[total_read..total_read + remaining.min(buf_len - total_read)])?
            } else if let InternalsForArchive::Mmap(data) = &self.internals {
                (&data[self.current_offset..])
                    .read(&mut buf[total_read..total_read + remaining.min(buf_len - total_read)])?
            } else {
                0
            };

            if read_size == 0 {
                break; // EOF
            }

            self.current_offset += read_size;
            total_read += read_size;
        }

        Ok(total_read)
    }
}
