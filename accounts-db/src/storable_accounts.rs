//! trait for abstracting underlying storage of pubkey and account pairs to be written
use {
    crate::{
        account_storage::stored_account_info::StoredAccountInfo, is_zero_lamport::IsZeroLamport,
        utils::create_account_shared_data,
    },
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
};

/// hold a ref to an account to store. The account could be represented in memory a few different ways
#[derive(Debug, Copy, Clone)]
pub enum AccountForStorage<'a> {
    AddressAndAccount((&'a Pubkey, &'a AccountSharedData)),
    StoredAccountInfo(&'a StoredAccountInfo<'a>),
}

impl<'a> From<(&'a Pubkey, &'a AccountSharedData)> for AccountForStorage<'a> {
    fn from(source: (&'a Pubkey, &'a AccountSharedData)) -> Self {
        Self::AddressAndAccount(source)
    }
}

impl<'a> From<&'a StoredAccountInfo<'a>> for AccountForStorage<'a> {
    fn from(source: &'a StoredAccountInfo<'a>) -> Self {
        Self::StoredAccountInfo(source)
    }
}

impl IsZeroLamport for AccountForStorage<'_> {
    fn is_zero_lamport(&self) -> bool {
        self.lamports() == 0
    }
}

impl<'a> AccountForStorage<'a> {
    pub fn pubkey(&self) -> &'a Pubkey {
        match self {
            AccountForStorage::AddressAndAccount((pubkey, _account)) => pubkey,
            AccountForStorage::StoredAccountInfo(account) => account.pubkey(),
        }
    }

    pub fn take_account(&self) -> AccountSharedData {
        match self {
            AccountForStorage::AddressAndAccount((_pubkey, account)) => (*account).clone(),
            AccountForStorage::StoredAccountInfo(account) => create_account_shared_data(*account),
        }
    }
}

impl ReadableAccount for AccountForStorage<'_> {
    fn lamports(&self) -> u64 {
        match self {
            AccountForStorage::AddressAndAccount((_pubkey, account)) => account.lamports(),
            AccountForStorage::StoredAccountInfo(account) => account.lamports(),
        }
    }
    fn data(&self) -> &[u8] {
        match self {
            AccountForStorage::AddressAndAccount((_pubkey, account)) => account.data(),
            AccountForStorage::StoredAccountInfo(account) => account.data(),
        }
    }
    fn owner(&self) -> &Pubkey {
        match self {
            AccountForStorage::AddressAndAccount((_pubkey, account)) => account.owner(),
            AccountForStorage::StoredAccountInfo(account) => account.owner(),
        }
    }
    fn executable(&self) -> bool {
        match self {
            AccountForStorage::AddressAndAccount((_pubkey, account)) => account.executable(),
            AccountForStorage::StoredAccountInfo(account) => account.executable(),
        }
    }
    fn rent_epoch(&self) -> Epoch {
        match self {
            AccountForStorage::AddressAndAccount((_pubkey, account)) => account.rent_epoch(),
            AccountForStorage::StoredAccountInfo(account) => account.rent_epoch(),
        }
    }
}

static DEFAULT_ACCOUNT_SHARED_DATA: std::sync::LazyLock<AccountSharedData> =
    std::sync::LazyLock::new(AccountSharedData::default);

/// abstract access to pubkey, account, slot, target_slot of either:
/// a. (slot, &[&Pubkey, &ReadableAccount])
/// b. (slot, &[Pubkey, ReadableAccount])
/// c. (slot, &[&Pubkey, &ReadableAccount, Slot]) (we will use this later)
/// This trait avoids having to allocate redundant data when there is a duplicated slot parameter.
/// All legacy callers do not have a unique slot per account to store.
pub trait StorableAccounts<'a>: Sync {
    /// account at 'index'
    fn account<Ret>(
        &self,
        index: usize,
        callback: impl for<'local> FnMut(AccountForStorage<'local>) -> Ret,
    ) -> Ret;
    /// Geyser account update notifications need a `&AccountSharedData`. When storing into the
    /// accounts write cache, we should always have an AccountSharedData, so allow access to it
    /// when available, which is an optimization to avoid creating a new AccountSharedData only to
    /// immediately take a reference.
    /// Note: Only implement this fn if underlying type actually holds an AccountSharedData.
    /// Otherwise mark it unimplemented!().
    fn account_for_geyser<Ret>(
        &self,
        index: usize,
        callback: impl for<'local> FnMut(&'local Pubkey, &'local AccountSharedData) -> Ret,
    ) -> Ret;
    /// whether account at 'index' has zero lamports
    fn is_zero_lamport(&self, index: usize) -> bool;
    /// data length of account at 'index'
    fn data_len(&self, index: usize) -> usize;
    /// pubkey of account at 'index'
    fn pubkey(&self, index: usize) -> &Pubkey;
    /// None if account is zero lamports
    fn account_default_if_zero_lamport<Ret>(
        &self,
        index: usize,
        mut callback: impl for<'local> FnMut(AccountForStorage<'local>) -> Ret,
    ) -> Ret {
        // Calling `self.account` may be expensive if backed by disk storage.
        // Check if the account is zero lamports first.
        if self.is_zero_lamport(index) {
            callback(AccountForStorage::AddressAndAccount((
                self.pubkey(index),
                &DEFAULT_ACCOUNT_SHARED_DATA,
            )))
        } else {
            self.account(index, callback)
        }
    }
    // current slot for account at 'index'
    fn slot(&self, index: usize) -> Slot;
    /// slot that all accounts are to be written to
    fn target_slot(&self) -> Slot;
    /// true if no accounts to write
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// # accounts to write
    fn len(&self) -> usize;
}

impl<'a: 'b, 'b> StorableAccounts<'a> for (Slot, &'b [(&'a Pubkey, &'a AccountSharedData)]) {
    fn account<Ret>(
        &self,
        index: usize,
        mut callback: impl for<'local> FnMut(AccountForStorage<'local>) -> Ret,
    ) -> Ret {
        callback((self.1[index].0, self.1[index].1).into())
    }
    fn account_for_geyser<Ret>(
        &self,
        index: usize,
        mut callback: impl for<'local> FnMut(&'local Pubkey, &'local AccountSharedData) -> Ret,
    ) -> Ret {
        let pubkey = self.pubkey(index);
        let account = self.1[index].1;
        callback(pubkey, account)
    }
    fn is_zero_lamport(&self, index: usize) -> bool {
        self.1[index].1.is_zero_lamport()
    }
    fn data_len(&self, index: usize) -> usize {
        self.1[index].1.data().len()
    }
    fn pubkey(&self, index: usize) -> &Pubkey {
        self.1[index].0
    }
    fn slot(&self, _index: usize) -> Slot {
        // per-index slot is not unique per slot when per-account slot is not included in the source data
        self.target_slot()
    }
    fn target_slot(&self) -> Slot {
        self.0
    }
    fn len(&self) -> usize {
        self.1.len()
    }
}

impl<'a: 'b, 'b> StorableAccounts<'a> for (Slot, &'b [(Pubkey, AccountSharedData)]) {
    fn account<Ret>(
        &self,
        index: usize,
        mut callback: impl for<'local> FnMut(AccountForStorage<'local>) -> Ret,
    ) -> Ret {
        callback((&self.1[index].0, &self.1[index].1).into())
    }
    fn account_for_geyser<Ret>(
        &self,
        index: usize,
        mut callback: impl for<'local> FnMut(&'local Pubkey, &'local AccountSharedData) -> Ret,
    ) -> Ret {
        let pubkey = self.pubkey(index);
        let account = &self.1[index].1;
        callback(pubkey, account)
    }
    fn is_zero_lamport(&self, index: usize) -> bool {
        self.1[index].1.is_zero_lamport()
    }
    fn data_len(&self, index: usize) -> usize {
        self.1[index].1.data().len()
    }
    fn pubkey(&self, index: usize) -> &Pubkey {
        &self.1[index].0
    }
    fn slot(&self, _index: usize) -> Slot {
        // per-index slot is not unique per slot when per-account slot is not included in the source data
        self.target_slot()
    }
    fn target_slot(&self) -> Slot {
        self.0
    }
    fn len(&self) -> usize {
        self.1.len()
    }
}
