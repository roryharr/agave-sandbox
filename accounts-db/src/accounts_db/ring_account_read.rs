//! Reads of accounts from storage on io_uring.
//!
//! Each account is read as a split file stores it: first its meta entry, then, if that does not
//! hold all its data, its data entry. The meta entry's size is unknown until it is parsed, so its
//! read is up to a page; nearly every account fits. The data entry is read straight into the
//! account's own buffer, on a kernel worker: even from the page cache, copying a large account
//! would stall every other read for milliseconds.
//!
//! Completions only read: each one hands its account to the ring's context, through
//! `AccountReadContext::finish`, and the context's owner does the rest.

use {
    crate::{
        account_storage::meta_entry_read::{
            AccountDataRead, AccountFromMetaEntry, MetaEntryBuffer, MetaEntryRead,
        },
        accounts_db::LoadedAccountAccessor,
        split_file::DATA_ENTRY_FIXED_SIZE,
    },
    agave_io_uring::{Completion, RingAccess, RingOp},
    io_uring::{opcode, squeue, types},
    solana_account::{Account, AccountSharedData},
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        io::{self, IoSliceMut},
        mem,
        os::fd::AsRawFd,
        slice,
        time::Instant,
    },
};

/// What completions need, held by the ring
pub(super) trait AccountReadContext {
    /// Buffers for meta entry reads, one per read in flight. A meta entry read is up to a page,
    /// whatever the account's size: the meta entry says how much more to read, and that is read
    /// into a buffer of its own, which becomes the account's.
    fn meta_entry_buffers(&mut self) -> &mut Vec<Box<MetaEntryBuffer>>;

    /// Counts a completed read and how long it took
    fn count_read(&mut self, is_data_read: bool, read_us: u64);

    /// Takes a read done with: the account read, or `None` when the read did not produce one
    fn finish(&mut self, account: &AccountRead, loaded: Option<AccountSharedData>);
}

/// The account a read is for
#[derive(Clone)]
pub(super) struct AccountRead {
    pub(super) pubkey: Pubkey,
    pub(super) slot: Slot,
    /// where the account is stored: keeps the file open, and the account in it, until the read
    /// completes
    pub(super) accessor: LoadedAccountAccessor,
}

/// One read of an account: its meta entry, then, if that does not hold all its data, its data
/// entry
pub(super) struct ReadOp {
    account: AccountRead,
    pushed: Instant,
    read: Read,
}

enum Read {
    /// The account's meta entry, into a pooled buffer, which completion returns to the pool
    MetaEntry {
        meta_entry_read: MetaEntryRead,
        buffer: Option<Box<MetaEntryBuffer>>,
    },
    /// The account's data entry: its header, if any, then its data, into its own buffer
    Data {
        /// taken by completion to finish the account
        data_read: Option<AccountDataRead>,
        /// its first `AccountsFile::data_entry_header_len()` bytes hold the header
        header: [u8; DATA_ENTRY_FIXED_SIZE],
        data: Vec<u8>,
        /// set on submission: the header, then `data`
        iovecs: Option<[IoSliceMut<'static>; 2]>,
    },
}

impl ReadOp {
    pub(super) fn meta_entry(
        account: AccountRead,
        meta_entry_read: MetaEntryRead,
        buffer: Box<MetaEntryBuffer>,
    ) -> Self {
        Self {
            account,
            pushed: Instant::now(),
            read: Read::MetaEntry {
                meta_entry_read,
                buffer: Some(buffer),
            },
        }
    }

    fn data(account: AccountRead, data_read: AccountDataRead) -> Self {
        let data = Vec::with_capacity(data_read.data_len);
        Self {
            account,
            pushed: Instant::now(),
            read: Read::Data {
                data_read: Some(data_read),
                header: [0; DATA_ENTRY_FIXED_SIZE],
                data,
                iovecs: None,
            },
        }
    }
}

impl<C: AccountReadContext> RingOp<C> for ReadOp {
    fn entry(&mut self) -> squeue::Entry {
        let accounts = self.account.accessor.accounts_file();
        match &mut self.read {
            Read::MetaEntry {
                meta_entry_read,
                buffer,
            } => {
                let buffer = buffer
                    .as_mut()
                    .expect("a meta entry read holds its buffer until it completes");
                let bytes = buffer.bytes_mut(meta_entry_read.size);
                let fd = types::Fd(accounts.meta_entry_file().as_raw_fd());
                opcode::Read::new(fd, bytes.as_mut_ptr(), bytes.len() as u32)
                    .offset(meta_entry_read.offset)
                    .build()
            }
            Read::Data {
                data_read,
                header,
                data,
                iovecs,
            } => {
                let (data_file, _) = accounts
                    .account_data_file()
                    .expect("a data entry read is of the data file");
                let data_read = data_read
                    .as_ref()
                    .expect("a data read holds its account until it completes");
                let offset = data_read.offset;
                let data_len = data_read.data_len;
                let header_len = accounts.data_entry_header_len();
                // SAFETY: both point into this op, which the ring keeps in place until the read
                // completes
                let iovecs = iovecs.insert(unsafe {
                    [
                        IoSliceMut::new(slice::from_raw_parts_mut(header.as_mut_ptr(), header_len)),
                        IoSliceMut::new(slice::from_raw_parts_mut(data.as_mut_ptr(), data_len)),
                    ]
                });
                let fd = types::Fd(data_file.as_raw_fd());
                opcode::Readv::new(fd, iovecs.as_ptr().cast(), iovecs.len() as u32)
                    .offset(offset)
                    .build()
                    // copy on a worker, not on this thread during submit
                    .flags(squeue::Flags::ASYNC)
            }
        }
    }

    fn complete(
        &mut self,
        completion: &mut Completion<C, Self>,
        res: io::Result<i32>,
    ) -> io::Result<()> {
        let is_data_read = matches!(self.read, Read::Data { .. });
        let read_us = self.pushed.elapsed().as_micros() as u64;
        completion.context_mut().count_read(is_data_read, read_us);
        let bytes_read = res.ok().map(|bytes_read| bytes_read as usize);
        let account = match &mut self.read {
            Read::MetaEntry {
                meta_entry_read,
                buffer,
            } => {
                let mut buffer = buffer
                    .take()
                    .expect("a meta entry read holds its buffer until it completes");
                let from_meta_entry = bytes_read.and_then(|bytes_read| {
                    // the read initialized the first `bytes_read` bytes
                    self.account
                        .accessor
                        .accounts_file()
                        .account_from_meta_entry(
                            meta_entry_read.offset,
                            buffer.bytes_mut(bytes_read),
                        )
                });
                let account = match from_meta_entry {
                    Some(AccountFromMetaEntry::Complete { account, data }) => {
                        Some(AccountSharedData::from(Account {
                            data: data.to_vec(),
                            ..account
                        }))
                    }
                    Some(AccountFromMetaEntry::Partial(data_read)) => {
                        completion.context_mut().meta_entry_buffers().push(buffer);
                        // still in flight: the data entry read finishes the account
                        return completion.push(ReadOp::data(self.account.clone(), data_read));
                    }
                    None => None,
                };
                // what the buffer held is parsed or copied out, so it goes back to the pool
                completion.context_mut().meta_entry_buffers().push(buffer);
                account
            }
            Read::Data {
                data_read,
                header,
                data,
                ..
            } => {
                let accounts = self.account.accessor.accounts_file();
                let data_read = data_read
                    .take()
                    .expect("a data read holds its account until it completes");
                let data_len = data_read.data_len;
                let header_len = accounts.data_entry_header_len();
                // a failed or short read produces no account
                (bytes_read == Some(header_len + data_len))
                    .then(|| {
                        // SAFETY: the read filled the header, then `data_len` bytes of spare
                        // capacity
                        unsafe { data.set_len(data_len) };
                        accounts.account_from_data_entry(
                            data_read,
                            &header[..header_len],
                            mem::take(data),
                        )
                    })
                    .flatten()
            }
        };
        completion.context_mut().finish(&self.account, account);
        Ok(())
    }
}
