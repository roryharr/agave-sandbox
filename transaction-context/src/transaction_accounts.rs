#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    crate::{
        IndexOfAccount, MAX_ACCOUNT_DATA_GROWTH_PER_TRANSACTION, MAX_ACCOUNT_DATA_LEN,
        vm_addresses::{GUEST_ACCOUNT_PAYLOAD_BASE_ADDRESS, GUEST_REGION_SIZE},
        vm_slice::VmSlice,
    },
    solana_account::{
        AccountData, AccountDataWrite, AccountSharedData, ReadableAccount, WritableAccount,
    },
    solana_instruction::error::InstructionError,
    solana_pubkey::Pubkey,
    std::{
        cell::{Cell, UnsafeCell},
        ops::{Deref, DerefMut},
    },
};

/// This struct is shared with programs. Do not alter its fields.
#[repr(C)]
#[derive(Debug, PartialEq)]
struct AccountSharedFields {
    key: Pubkey,
    owner: Pubkey,
    lamports: u64,
    // The payload is going to be filled with the guest virtual address of the account payload
    // vector.
    payload: VmSlice<u8>,
}

#[derive(Debug)]
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
struct AccountPrivateFields {
    rent_epoch: u64,
    executable: bool,
    /// The account data as loaded; the pre-session state while a write
    /// session is open.
    payload: AccountData,
    /// Write session holding the account's working bytes. While open, all
    /// data reads and writes go through it and `into_data` folds it back
    /// into one `AccountData`. Opened on the first write to shared payload
    /// bytes when the kernel-COW gather path applies, and eagerly when a
    /// contiguous view of fragmented payload bytes would otherwise copy.
    write_session: Option<AccountDataWrite>,
}

/// Headroom for a write session. The session's base pointer must stay put
/// for the rest of the transaction (memory regions point into it), so cover
/// any legal growth; gather reservations are virtual address space only, so
/// the headroom is free.
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
fn session_reserve_extra(len: usize) -> usize {
    (MAX_ACCOUNT_DATA_LEN as usize).saturating_sub(len)
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl AccountPrivateFields {
    fn new(account: &AccountSharedData) -> Self {
        let payload = account.data_clone();
        // A gather session is the only contiguous view of fragmented data
        // that does not copy the bytes, so open it up front.
        let write_session = payload
            .as_slice_would_copy()
            .then(|| payload.begin_write(session_reserve_extra(payload.len())));
        Self {
            rent_epoch: account.rent_epoch(),
            executable: account.executable(),
            payload,
            write_session,
        }
    }

    fn payload_len(&self) -> usize {
        match &self.write_session {
            Some(session) => session.len(),
            None => self.payload.len(),
        }
    }

    fn data(&self) -> &[u8] {
        match &self.write_session {
            Some(session) => session.as_slice(),
            None => self.payload.as_slice(),
        }
    }

    /// Opens the write session mutations are routed through, so that shared
    /// payload bytes are copied per touched page instead of wholesale. Small
    /// payloads keep the plain copy-on-write of `AccountData` itself.
    fn ensure_write_session(&mut self) {
        if self.write_session.is_none()
            && self.payload.is_shared()
            && self.payload.begin_write_would_gather()
        {
            self.write_session = Some(
                self.payload
                    .begin_write(session_reserve_extra(self.payload.len())),
            );
        }
    }

    /// Folds an open write session back into `payload`. Only for growth past
    /// the session reservation, which no length-checked runtime path reaches
    /// (the reservation covers `MAX_ACCOUNT_DATA_LEN`); callers of a resize
    /// already re-point any memory region at the account afterwards.
    fn materialize_write_session(&mut self) {
        if let Some(session) = self.write_session.take() {
            self.payload = session.commit();
        }
    }

    fn data_as_mut_slice(&mut self) -> &mut [u8] {
        self.ensure_write_session();
        match &mut self.write_session {
            Some(session) => session.as_mut_slice(),
            None => self.payload.as_mut_slice(),
        }
    }

    fn resize(&mut self, new_len: usize, value: u8) {
        self.ensure_write_session();
        if let Some(session) = &mut self.write_session {
            if session.resize(new_len, value) {
                return;
            }
            self.materialize_write_session();
        }
        self.payload.resize(new_len, value);
    }

    fn set_data_from_slice(&mut self, new_data: &[u8]) {
        // With no session open this replaces the payload without copying the
        // old bytes, so there is nothing for a session to save; only route
        // through one that already exists.
        if let Some(session) = &mut self.write_session {
            if session.resize(new_data.len(), 0) {
                session.as_mut_slice().copy_from_slice(new_data);
                return;
            }
            self.materialize_write_session();
        }
        self.payload.set_data_from_slice(new_data);
    }

    fn extend_from_slice(&mut self, data: &[u8]) {
        self.ensure_write_session();
        if let Some(session) = &mut self.write_session {
            let old_len = session.len();
            if session.resize(old_len.saturating_add(data.len()), 0) {
                session
                    .as_mut_slice()
                    .get_mut(old_len..)
                    .expect("within the resized session length")
                    .copy_from_slice(data);
                return;
            }
            self.materialize_write_session();
        }
        self.payload.extend_from_slice(data);
    }

    fn reserve(&mut self, additional: usize) {
        self.ensure_write_session();
        // An open session's reservation already covers any legal growth.
        if self.write_session.is_none() {
            self.payload.reserve(additional);
        }
    }

    /// Closes the account's working state into one `AccountData`, committing
    /// the write session if one is open.
    fn into_data(self) -> AccountData {
        match self.write_session {
            Some(session) => session.commit(),
            None => self.payload,
        }
    }
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl PartialEq for AccountPrivateFields {
    fn eq(&self, other: &Self) -> bool {
        self.rent_epoch == other.rent_epoch
            && self.executable == other.executable
            && self.data() == other.data()
    }
}

#[derive(Debug, PartialEq)]
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
pub struct TransactionAccountView<'a> {
    abi_account: &'a AccountSharedFields,
    private_fields: &'a AccountPrivateFields,
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl ReadableAccount for TransactionAccountView<'_> {
    fn lamports(&self) -> u64 {
        self.abi_account.lamports
    }

    fn data(&self) -> &[u8] {
        self.private_fields.data()
    }

    fn owner(&self) -> &Pubkey {
        &self.abi_account.owner
    }

    fn executable(&self) -> bool {
        self.private_fields.executable
    }

    fn rent_epoch(&self) -> u64 {
        self.private_fields.rent_epoch
    }
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl PartialEq<AccountSharedData> for TransactionAccountView<'_> {
    fn eq(&self, other: &AccountSharedData) -> bool {
        other.lamports() == self.lamports()
            && other.data() == self.data()
            && other.owner() == self.owner()
            && other.executable() == self.executable()
            && other.rent_epoch() == self.rent_epoch()
    }
}

#[derive(Debug)]
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
pub struct TransactionAccountViewMut<'a> {
    abi_account: &'a mut AccountSharedFields,
    private_fields: &'a mut AccountPrivateFields,
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl TransactionAccountViewMut<'_> {
    pub(crate) fn raw_mut_data_slice(&mut self) -> *mut [u8] {
        &raw mut self.private_fields.data_as_mut_slice()[..]
    }

    pub(crate) fn resize(&mut self, new_len: usize, value: u8) {
        self.private_fields.resize(new_len, value);
        self.abi_account.payload.set_len(new_len as u64);
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    pub(crate) fn set_data_from_slice(&mut self, new_data: &[u8]) {
        self.private_fields.set_data_from_slice(new_data);
        self.abi_account.payload.set_len(new_data.len() as u64);
    }

    pub(crate) fn extend_from_slice(&mut self, data: &[u8]) {
        self.private_fields.extend_from_slice(data);
        self.abi_account
            .payload
            .set_len(self.private_fields.payload_len() as u64);
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        self.private_fields.reserve(additional)
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    pub(crate) fn is_shared(&self) -> bool {
        self.private_fields.payload.is_shared()
    }
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl ReadableAccount for TransactionAccountViewMut<'_> {
    fn lamports(&self) -> u64 {
        self.abi_account.lamports
    }

    fn data(&self) -> &[u8] {
        self.private_fields.data()
    }

    fn owner(&self) -> &Pubkey {
        &self.abi_account.owner
    }

    fn executable(&self) -> bool {
        self.private_fields.executable
    }

    fn rent_epoch(&self) -> u64 {
        self.private_fields.rent_epoch
    }
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl WritableAccount for TransactionAccountViewMut<'_> {
    fn set_lamports(&mut self, lamports: u64) {
        self.abi_account.lamports = lamports;
    }

    fn data_as_mut_slice(&mut self) -> &mut [u8] {
        self.private_fields.data_as_mut_slice()
    }

    fn set_owner(&mut self, owner: Pubkey) {
        self.abi_account.owner = owner;
    }

    fn copy_into_owner_from_slice(&mut self, source: &[u8]) {
        self.abi_account.owner.as_mut().copy_from_slice(source);
    }

    fn set_executable(&mut self, executable: bool) {
        self.private_fields.executable = executable;
    }

    fn set_rent_epoch(&mut self, epoch: u64) {
        self.private_fields.rent_epoch = epoch;
    }
}

/// An account key and the matching account
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
pub type KeyedAccountSharedData = (Pubkey, AccountSharedData);
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
pub(crate) type DeconstructedTransactionAccounts =
    (Vec<KeyedAccountSharedData>, Box<[Cell<bool>]>, Cell<i64>);

#[derive(Debug)]
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
pub struct TransactionAccounts {
    shared_account_fields: Box<[UnsafeCell<AccountSharedFields>]>,
    private_account_fields: Box<[UnsafeCell<AccountPrivateFields>]>,
    borrow_counters: Box<[BorrowCounter]>,
    touched_flags: Box<[Cell<bool>]>,
    resize_delta: Cell<i64>,
    lamports_delta: Cell<i128>,
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl TransactionAccounts {
    pub(crate) fn new(accounts: Vec<KeyedAccountSharedData>) -> TransactionAccounts {
        let touched_flags = vec![Cell::new(false); accounts.len()].into_boxed_slice();
        let borrow_counters = vec![BorrowCounter::default(); accounts.len()].into_boxed_slice();
        let (shared_accounts, private_fields) = accounts
            .into_iter()
            .enumerate()
            .map(|(idx, item)| {
                (
                    UnsafeCell::new(AccountSharedFields {
                        key: item.0,
                        owner: *item.1.owner(),
                        lamports: item.1.lamports(),
                        payload: VmSlice::new(
                            GUEST_ACCOUNT_PAYLOAD_BASE_ADDRESS
                                .saturating_add(GUEST_REGION_SIZE.saturating_mul(idx as u64)),
                            item.1.data().len() as u64,
                        ),
                    }),
                    UnsafeCell::new(AccountPrivateFields::new(&item.1)),
                )
            })
            .collect::<(
                Vec<UnsafeCell<AccountSharedFields>>,
                Vec<UnsafeCell<AccountPrivateFields>>,
            )>();

        TransactionAccounts {
            shared_account_fields: shared_accounts.into_boxed_slice(),
            private_account_fields: private_fields.into_boxed_slice(),
            borrow_counters,
            touched_flags,
            resize_delta: Cell::new(0),
            lamports_delta: Cell::new(0),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.shared_account_fields.len()
    }

    pub fn touch(&self, index: IndexOfAccount) -> Result<(), InstructionError> {
        self.touched_flags
            .get(index as usize)
            .ok_or(InstructionError::MissingAccount)?
            .set(true);
        Ok(())
    }

    pub(crate) fn update_accounts_resize_delta(
        &self,
        old_len: usize,
        new_len: usize,
    ) -> Result<(), InstructionError> {
        let accounts_resize_delta = self.resize_delta.get();
        self.resize_delta.set(
            accounts_resize_delta.saturating_add((new_len as i64).saturating_sub(old_len as i64)),
        );
        Ok(())
    }

    pub(crate) fn can_data_be_resized(
        &self,
        old_len: usize,
        new_len: usize,
    ) -> Result<(), InstructionError> {
        // The new length can not exceed the maximum permitted length
        if new_len > MAX_ACCOUNT_DATA_LEN as usize {
            return Err(InstructionError::InvalidRealloc);
        }
        // The resize can not exceed the per-transaction maximum
        let length_delta = (new_len as i64).saturating_sub(old_len as i64);
        if self.resize_delta.get().saturating_add(length_delta)
            > MAX_ACCOUNT_DATA_GROWTH_PER_TRANSACTION
        {
            return Err(InstructionError::MaxAccountsDataAllocationsExceeded);
        }
        Ok(())
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    pub(crate) fn try_borrow_mut(
        &self,
        index: IndexOfAccount,
    ) -> Result<AccountRefMut<'_>, InstructionError> {
        let borrow_counter = self
            .borrow_counters
            .get(index as usize)
            .ok_or(InstructionError::MissingAccount)?;
        borrow_counter.try_borrow_mut()?;

        // SAFETY: The borrow counter guarantees this is the only mutable borrow of this account.
        // The unwrap is safe because accounts.len() == borrow_counters.len(), so the missing
        // account error should have been returned above.
        let svm_account = unsafe {
            &mut *self
                .shared_account_fields
                .get(index as usize)
                .unwrap()
                .get()
        };

        let private_fields = unsafe {
            &mut *self
                .private_account_fields
                .get(index as usize)
                .unwrap()
                .get()
        };

        let account = TransactionAccountViewMut {
            abi_account: svm_account,
            private_fields,
        };

        Ok(AccountRefMut {
            account,
            borrow_counter,
        })
    }

    pub fn try_borrow(&self, index: IndexOfAccount) -> Result<AccountRef<'_>, InstructionError> {
        let borrow_counter = self
            .borrow_counters
            .get(index as usize)
            .ok_or(InstructionError::MissingAccount)?;
        borrow_counter.try_borrow()?;

        // SAFETY: The borrow counter guarantees there are no mutable borrow of this account.
        // The unwrap is safe because accounts.len() == borrow_counters.len(), so the missing
        // account error should have been returned above.
        let svm_account = unsafe {
            &*self
                .shared_account_fields
                .get(index as usize)
                .unwrap()
                .get()
        };

        let private_fields = unsafe {
            &*self
                .private_account_fields
                .get(index as usize)
                .unwrap()
                .get()
        };

        let account = TransactionAccountView {
            abi_account: svm_account,
            private_fields,
        };

        Ok(AccountRef {
            account,
            borrow_counter,
        })
    }

    pub(crate) fn add_lamports_delta(&self, balance: i128) -> Result<(), InstructionError> {
        let delta = self.lamports_delta.get();
        self.lamports_delta.set(
            delta
                .checked_add(balance)
                .ok_or(InstructionError::ArithmeticOverflow)?,
        );
        Ok(())
    }

    pub(crate) fn get_lamports_delta(&self) -> i128 {
        self.lamports_delta.get()
    }

    fn deconstruct_into_keyed_account_shared_data(&mut self) -> Vec<KeyedAccountSharedData> {
        let shared_account_fields = std::mem::take(&mut self.shared_account_fields);
        let private_account_fields = std::mem::take(&mut self.private_account_fields);
        shared_account_fields
            .into_iter()
            .zip(private_account_fields)
            .map(|(shared_fields_cell, private_fields_cell)| {
                let shared_fields = shared_fields_cell.into_inner();
                let private_fields = private_fields_cell.into_inner();
                let executable = private_fields.executable;
                let rent_epoch = private_fields.rent_epoch;
                (
                    shared_fields.key,
                    AccountSharedData::create_from_existing_shared_data(
                        shared_fields.lamports,
                        private_fields.into_data(),
                        shared_fields.owner,
                        executable,
                        rent_epoch,
                    ),
                )
            })
            .collect()
    }

    pub(crate) fn deconstruct_into_account_shared_data(&mut self) -> Vec<AccountSharedData> {
        let shared_account_fields = std::mem::take(&mut self.shared_account_fields);
        let private_account_fields = std::mem::take(&mut self.private_account_fields);
        shared_account_fields
            .into_iter()
            .zip(private_account_fields)
            .map(|(shared_fields_cell, private_fields_cell)| {
                let shared_fields = shared_fields_cell.into_inner();
                let private_fields = private_fields_cell.into_inner();
                let executable = private_fields.executable;
                let rent_epoch = private_fields.rent_epoch;
                AccountSharedData::create_from_existing_shared_data(
                    shared_fields.lamports,
                    private_fields.into_data(),
                    shared_fields.owner,
                    executable,
                    rent_epoch,
                )
            })
            .collect()
    }

    pub(crate) fn take(mut self) -> DeconstructedTransactionAccounts {
        let shared_data = self.deconstruct_into_keyed_account_shared_data();
        (shared_data, self.touched_flags, self.resize_delta)
    }

    pub fn resize_delta(&self) -> i64 {
        self.resize_delta.get()
    }

    pub(crate) fn account_key(&self, index: IndexOfAccount) -> Option<&Pubkey> {
        // SAFETY: We never modify an account key, so returning a reference to it is safe.
        unsafe {
            self.shared_account_fields
                .get(index as usize)
                .map(|acc| &(*acc.get()).key)
        }
    }

    pub(crate) fn account_keys_iter(&self) -> impl Iterator<Item = &Pubkey> {
        // SAFETY: We never modify account keys, so returning an immutable reference to them is safe.
        unsafe {
            self.shared_account_fields
                .iter()
                .map(|item| &(*item.get()).key)
        }
    }
}

#[derive(Default, Debug, Clone)]
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
struct BorrowCounter {
    counter: Cell<i8>,
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl BorrowCounter {
    #[inline]
    fn is_writing(&self) -> bool {
        self.counter.get() < 0
    }

    #[inline]
    fn is_reading(&self) -> bool {
        self.counter.get() > 0
    }

    #[inline]
    fn try_borrow(&self) -> Result<(), InstructionError> {
        if self.is_writing() {
            return Err(InstructionError::AccountBorrowFailed);
        }

        if let Some(counter) = self.counter.get().checked_add(1) {
            self.counter.set(counter);
            return Ok(());
        }

        Err(InstructionError::AccountBorrowFailed)
    }

    #[inline]
    fn try_borrow_mut(&self) -> Result<(), InstructionError> {
        if self.is_writing() || self.is_reading() {
            return Err(InstructionError::AccountBorrowFailed);
        }

        self.counter.set(self.counter.get().saturating_sub(1));

        Ok(())
    }

    #[inline]
    fn release_borrow(&self) {
        self.counter.set(self.counter.get().saturating_sub(1));
    }

    #[inline]
    fn release_borrow_mut(&self) {
        self.counter.set(self.counter.get().saturating_add(1));
    }
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
pub struct AccountRef<'a> {
    account: TransactionAccountView<'a>,
    borrow_counter: &'a BorrowCounter,
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl Drop for AccountRef<'_> {
    fn drop(&mut self) {
        self.borrow_counter.release_borrow();
    }
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl<'a> Deref for AccountRef<'a> {
    type Target = TransactionAccountView<'a>;
    fn deref(&self) -> &Self::Target {
        &self.account
    }
}

#[derive(Debug)]
#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
pub struct AccountRefMut<'a> {
    account: TransactionAccountViewMut<'a>,
    borrow_counter: &'a BorrowCounter,
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl Drop for AccountRefMut<'_> {
    fn drop(&mut self) {
        self.account
            .abi_account
            .payload
            .set_len(self.account.private_fields.payload_len() as u64);
        self.borrow_counter.release_borrow_mut();
    }
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl<'a> Deref for AccountRefMut<'a> {
    type Target = TransactionAccountViewMut<'a>;
    fn deref(&self) -> &Self::Target {
        &self.account
    }
}

#[cfg(not(any(target_arch = "bpf", target_arch = "sbf")))]
impl DerefMut for AccountRefMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.account
    }
}

#[cfg(all(test, not(target_arch = "sbf"), not(target_arch = "bpf")))]
#[allow(clippy::indexing_slicing)]
mod tests {
    use {
        crate::{MAX_ACCOUNT_DATA_LEN, transaction_accounts::TransactionAccounts},
        solana_account::{AccountData, AccountSharedData, ReadableAccount, WritableAccount},
        solana_instruction::error::InstructionError,
        solana_pubkey::Pubkey,
    };

    const PAGE_SIZE: usize = 4096;

    fn account_with_data(data: AccountData) -> AccountSharedData {
        AccountSharedData::create_from_existing_shared_data(1, data, Pubkey::new_unique(), false, 0)
    }

    fn deconstruct(mut tx_accounts: TransactionAccounts) -> Vec<AccountSharedData> {
        tx_accounts.deconstruct_into_account_shared_data()
    }

    #[test]
    fn test_write_session_routes_large_shared_account() {
        let mut expected: Vec<u8> = (0..3 * PAGE_SIZE).map(|i| (i % 251) as u8).collect();
        let account = account_with_data(AccountData::from(expected.clone()));
        // keep a clone alive so the payload is shared, like a freshly
        // loaded account sharing bytes with the accounts-db cache
        let original = account.clone();
        let tx_accounts = TransactionAccounts::new(vec![(Pubkey::new_unique(), account)]);

        {
            // the first in-place write opens the session; the following
            // overwrite must route through it
            let mut borrowed = tx_accounts.try_borrow_mut(0).unwrap();
            borrowed.data_as_mut_slice()[PAGE_SIZE + 10] = 0xAB;
            expected[PAGE_SIZE + 10] = 0xAB;
            expected.truncate(2 * PAGE_SIZE);
            borrowed.set_data_from_slice(&expected);
        }
        assert_eq!(tx_accounts.try_borrow(0).unwrap().data(), &expected[..]);

        let committed = deconstruct(tx_accounts);
        assert_eq!(committed[0].data(), &expected[..]);
        // the shared original is isolated from the writes
        assert_ne!(original.data()[PAGE_SIZE + 10], 0xAB);
        assert_eq!(original.data().len(), 3 * PAGE_SIZE);
    }

    #[test]
    fn test_fragmented_payload_reads_and_writes_through_session() {
        let first = vec![7u8; PAGE_SIZE];
        let second = vec![9u8; PAGE_SIZE + 100];
        let mut expected = [first.as_slice(), second.as_slice()].concat();
        let account = account_with_data(AccountData::from_chunks_for_tests(&[&first, &second]));
        let tx_accounts = TransactionAccounts::new(vec![(Pubkey::new_unique(), account)]);

        // reads see the concatenation through the eager session
        assert_eq!(tx_accounts.try_borrow(0).unwrap().data(), &expected[..]);

        {
            let mut borrowed = tx_accounts.try_borrow_mut(0).unwrap();
            borrowed.data_as_mut_slice()[0] = 42;
            borrowed.extend_from_slice(&[1, 2, 3]);
        }
        expected[0] = 42;
        expected.extend_from_slice(&[1, 2, 3]);

        let committed = deconstruct(tx_accounts);
        assert_eq!(committed[0].data(), &expected[..]);
    }

    #[test]
    fn test_resize_within_session_and_past_reservation() {
        let expected = vec![5u8; 2 * PAGE_SIZE];
        let account = account_with_data(AccountData::from_chunks_for_tests(&[
            &expected[..PAGE_SIZE],
            &expected[PAGE_SIZE..],
        ]));
        let tx_accounts = TransactionAccounts::new(vec![(Pubkey::new_unique(), account)]);

        {
            let mut borrowed = tx_accounts.try_borrow_mut(0).unwrap();
            // grow and shrink within the session reservation
            borrowed.resize(2 * PAGE_SIZE + 100, 3);
            borrowed.resize(PAGE_SIZE, 0);
            // growth past the reservation falls back to a materialized payload
            borrowed.resize(MAX_ACCOUNT_DATA_LEN as usize + PAGE_SIZE, 8);
        }

        let committed = deconstruct(tx_accounts);
        let data = committed[0].data();
        assert_eq!(data.len(), MAX_ACCOUNT_DATA_LEN as usize + PAGE_SIZE);
        assert_eq!(&data[..PAGE_SIZE], &expected[..PAGE_SIZE]);
        assert!(data[PAGE_SIZE..].iter().all(|&byte| byte == 8));
    }

    #[test]
    fn test_untouched_account_keeps_identity() {
        let account = account_with_data(AccountData::from(vec![1u8; 2 * PAGE_SIZE]));
        let original = account.clone();
        let tx_accounts = TransactionAccounts::new(vec![(Pubkey::new_unique(), account)]);
        assert_eq!(
            tx_accounts.try_borrow(0).unwrap().data().len(),
            2 * PAGE_SIZE
        );

        let committed = deconstruct(tx_accounts);
        assert!(committed[0].data_clone().ptr_eq(&original.data_clone()));
    }

    #[test]
    fn test_missing_account() {
        let accounts = vec![
            (
                Pubkey::new_unique(),
                AccountSharedData::new(2, 1, &Pubkey::new_unique()),
            ),
            (
                Pubkey::new_unique(),
                AccountSharedData::new(2, 1, &Pubkey::new_unique()),
            ),
        ];

        let tx_accounts = TransactionAccounts::new(accounts);

        let res = tx_accounts.try_borrow(3);
        assert_eq!(res.err(), Some(InstructionError::MissingAccount));

        let res = tx_accounts.try_borrow_mut(3);
        assert_eq!(res.err(), Some(InstructionError::MissingAccount));
    }

    #[test]
    fn test_invalid_borrow() {
        let accounts = vec![
            (
                Pubkey::new_unique(),
                AccountSharedData::new(2, 1, &Pubkey::new_unique()),
            ),
            (
                Pubkey::new_unique(),
                AccountSharedData::new(2, 1, &Pubkey::new_unique()),
            ),
        ];

        let tx_accounts = TransactionAccounts::new(accounts);

        // Two immutable borrows are valid
        {
            let acc_1 = tx_accounts.try_borrow(0);
            assert!(acc_1.is_ok());

            let acc_2 = tx_accounts.try_borrow(1);
            assert!(acc_2.is_ok());

            let acc_1_new = tx_accounts.try_borrow(0);
            assert!(acc_1_new.is_ok());

            assert_eq!(acc_1.unwrap().account, acc_1_new.unwrap().account);
        }

        // Two mutable borrows are invalid
        {
            let acc_1 = tx_accounts.try_borrow_mut(0);
            assert!(acc_1.is_ok());

            let acc_2 = tx_accounts.try_borrow_mut(1);
            assert!(acc_2.is_ok());

            let acc_1_new = tx_accounts.try_borrow_mut(0);
            assert_eq!(acc_1_new.err(), Some(InstructionError::AccountBorrowFailed));
        }

        // Mutable after immutable must fail
        {
            let acc_1 = tx_accounts.try_borrow(0);
            assert!(acc_1.is_ok());

            let acc_2 = tx_accounts.try_borrow(1);
            assert!(acc_2.is_ok());

            let acc_1_new = tx_accounts.try_borrow_mut(0);
            assert_eq!(acc_1_new.err(), Some(InstructionError::AccountBorrowFailed));
        }

        // Immutable after mutable must fail
        {
            let acc_1 = tx_accounts.try_borrow_mut(0);
            assert!(acc_1.is_ok());

            let acc_2 = tx_accounts.try_borrow_mut(1);
            assert!(acc_2.is_ok());

            let acc_1_new = tx_accounts.try_borrow(0);
            assert_eq!(acc_1_new.err(), Some(InstructionError::AccountBorrowFailed));
        }

        // Different scopes are good
        {
            let acc_1 = tx_accounts.try_borrow_mut(0);
            assert!(acc_1.is_ok());
        }

        {
            let acc_1 = tx_accounts.try_borrow_mut(0);
            assert!(acc_1.is_ok());
        }
    }

    #[test]
    fn too_many_borrows() {
        let accounts = vec![
            (
                Pubkey::new_unique(),
                AccountSharedData::new(2, 1, &Pubkey::new_unique()),
            ),
            (
                Pubkey::new_unique(),
                AccountSharedData::new(2, 1, &Pubkey::new_unique()),
            ),
        ];

        let tx_accounts = TransactionAccounts::new(accounts);
        let mut borrows = Vec::new();
        for i in 0..129 {
            let acc = tx_accounts.try_borrow(1);
            if i < 127 {
                assert!(acc.is_ok());
                borrows.push(acc.unwrap());
            } else {
                assert_eq!(acc.err(), Some(InstructionError::AccountBorrowFailed));
            }
        }
    }
}
