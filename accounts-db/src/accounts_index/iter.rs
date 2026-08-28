use {
    super::{AccountsIndex, DiskIndexValue, IndexValue, in_mem_accounts_index::InMemAccountsIndex},
    solana_pubkey::Pubkey,
    std::sync::Arc,
};

/// Iterator over AccountsIndex
///
/// One bin is inspected per call to `next()`, and bins may be empty.
/// Thus, clients must be able to handle `next()` returning `Some(vec![])`.
pub struct AccountsIndexPubkeyIterator<'a, T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> {
    account_maps: &'a [Arc<InMemAccountsIndex<T, U>>],
    current_bin: usize,
}

impl<'a, T: IndexValue, U: DiskIndexValue + From<T> + Into<T>>
    AccountsIndexPubkeyIterator<'a, T, U>
{
    pub fn new(index: &'a AccountsIndex<T, U>) -> Self {
        Self {
            account_maps: &index.account_maps,
            current_bin: 0,
        }
    }
}

/// Implement the Iterator trait for AccountsIndexIterator
impl<T: IndexValue, U: DiskIndexValue + From<T> + Into<T>> Iterator
    for AccountsIndexPubkeyIterator<'_, T, U>
{
    type Item = Vec<Pubkey>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_bin < self.account_maps.len() {
            let items = self.account_maps[self.current_bin].keys();
            self.current_bin += 1;
            Some(items)
        } else {
            None
        }
    }
}
