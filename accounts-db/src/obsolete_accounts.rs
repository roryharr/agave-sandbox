use {crate::account_info::Offset, solana_clock::Slot};

#[derive(Debug, Clone, PartialEq)]
struct ObsoleteAccountItem {
    /// Offset of the account in the storage file
    offset: Offset,
    /// Length of the account data
    data_len: usize,
    /// Slot when the account was marked obsolete
    slot: Slot,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ObsoleteAccounts {
    accounts: Vec<ObsoleteAccountItem>,
}

impl ObsoleteAccounts {
    /// Marks the accounts at the given offsets as obsolete
    pub fn mark_accounts_obsolete(
        &mut self,
        newly_obsolete_accounts: impl ExactSizeIterator<Item = (Offset, usize)>,
        slot: Slot,
    ) {
        self.accounts.reserve(newly_obsolete_accounts.len());

        for (offset, data_len) in newly_obsolete_accounts {
            self.accounts.push(ObsoleteAccountItem {
                offset,
                data_len,
                slot,
            });
        }
    }

    /// Returns the accounts that were marked obsolete as of the passed in slot
    /// or earlier. If slot is None, then slot will be assumed to be the max root
    /// and all obsolete accounts will be returned.
    pub fn filter_obsolete_accounts(
        &self,
        slot: Option<Slot>,
    ) -> impl Iterator<Item = (Offset, usize)> + '_ {
        self.accounts
            .iter()
            .filter(move |obsolete_account| slot.map_or(true, |s| obsolete_account.slot <= s))
            .map(|obsolete_account| (obsolete_account.offset, obsolete_account.data_len))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_accounts_obsolete() {
        let mut obsolete_accounts = ObsoleteAccounts::default();
        let new_accounts = vec![(10, 100), (20, 200), (30, 300)];
        let slot: Slot = 42;

        obsolete_accounts.mark_accounts_obsolete(new_accounts.into_iter(), slot);

        assert_eq!(
            obsolete_accounts.accounts,
            vec![
                ObsoleteAccountItem {
                    offset: 10,
                    data_len: 100,
                    slot,
                },
                ObsoleteAccountItem {
                    offset: 20,
                    data_len: 200,
                    slot,
                },
                ObsoleteAccountItem {
                    offset: 30,
                    data_len: 300,
                    slot,
                },
            ]
        );
    }

    #[test]
    fn test_filter_obsolete_accounts() {
        let mut obsolete_accounts = ObsoleteAccounts::default();
        let new_accounts: Vec<ObsoleteAccountItem> = vec![
            ObsoleteAccountItem {
                offset: 10,
                data_len: 100,
                slot: 40,
            },
            ObsoleteAccountItem {
                offset: 20,
                data_len: 200,
                slot: 42,
            },
            ObsoleteAccountItem {
                offset: 30,
                data_len: 300,
                slot: 44,
            },
        ];

        // Mark accounts obsolete with different slots
        new_accounts.into_iter().for_each(|item| {
            obsolete_accounts
                .mark_accounts_obsolete([(item.offset, item.data_len)].into_iter(), item.slot)
        });

        // Filter accounts obsolete as of slot 42
        let filtered_accounts: Vec<_> = obsolete_accounts
            .filter_obsolete_accounts(Some(42))
            .collect();

        assert_eq!(filtered_accounts, vec![(10, 100), (20, 200)]);

        // Filter accounts obsolete passing in no slot (i.e., all obsolete accounts)
        let filtered_accounts: Vec<_> = obsolete_accounts.filter_obsolete_accounts(None).collect();

        assert_eq!(filtered_accounts, vec![(10, 100), (20, 200), (30, 300)]);
    }

    #[test]
    fn test_empty_obsolete_accounts() {
        let obsolete_accounts: ObsoleteAccounts = ObsoleteAccounts::default();

        let filtered_accounts: Vec<_> = obsolete_accounts.filter_obsolete_accounts(None).collect();

        assert!(filtered_accounts.is_empty());
    }
}
