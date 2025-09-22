use {crate::account_info::Offset, solana_clock::Slot};

pub type ObsoleteAccounts = Vec<ObsoleteAccountItem>;
type ObsoleteAccountItem = (Offset, usize, Slot);

pub trait ObsoleteAccount {
    fn mark_accounts_obsolete(
        &mut self,
        newly_obsolete_accounts: impl ExactSizeIterator<Item = (Offset, usize)>,
        slot: Slot,
    );
    fn filter_obsolete_accounts(&self, slot: Option<Slot>) -> Vec<(Offset, usize)>;
}

impl ObsoleteAccount for ObsoleteAccounts {
    /// Marks the accounts at the given offsets as obsolete
    fn mark_accounts_obsolete(
        &mut self,
        newly_obsolete_accounts: impl ExactSizeIterator<Item = (Offset, usize)>,
        slot: Slot,
    ) {
        self.reserve(newly_obsolete_accounts.len());

        for (offset, data_len) in newly_obsolete_accounts {
            self.push((offset, data_len, slot));
        }
    }

    /// Returns the accounts that were marked obsolete as of the passed in slot
    /// or earlier. If slot is None, then slot will be assumed to be the max root
    /// and all obsolete accounts will be returned.
    fn filter_obsolete_accounts(&self, slot: Option<Slot>) -> Vec<(Offset, usize)> {
        self.iter()
            .filter(move |(_, _, obsolete_slot)| slot.is_none_or(|s| *obsolete_slot <= s))
            .map(|(offset, data_len, _)| (*offset, *data_len))
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_accounts_obsolete() {
        let mut obsolete_accounts: ObsoleteAccounts = Vec::new();
        let new_accounts = vec![(10, 100), (20, 200), (30, 300)];
        let slot: Slot = 42;

        obsolete_accounts.mark_accounts_obsolete(new_accounts.into_iter(), slot);

        assert_eq!(
            obsolete_accounts,
            vec![(10, 100, slot), (20, 200, slot), (30, 300, slot),]
        );
    }

    #[test]
    fn test_filter_obsolete_accounts() {
        let mut obsolete_accounts: ObsoleteAccounts = Vec::new();
        let new_accounts: ObsoleteAccounts = vec![(10, 100, 40), (20, 200, 42), (30, 300, 44)];

        // Mark accounts obsolete with different slots
        new_accounts.into_iter().for_each(|item| {
            obsolete_accounts.mark_accounts_obsolete([(item.0, item.1)].into_iter(), item.2)
        });

        // Filter accounts obsolete as of slot 42
        let filtered_accounts: Vec<_> = obsolete_accounts.filter_obsolete_accounts(Some(42));

        assert_eq!(filtered_accounts, vec![(10, 100), (20, 200)]);

        // Filter accounts obsolete passing in no slot (i.e., all obsolete accounts)
        let filtered_accounts: Vec<_> = obsolete_accounts.filter_obsolete_accounts(None);

        assert_eq!(filtered_accounts, vec![(10, 100), (20, 200), (30, 300)]);
    }

    #[test]
    fn test_empty_obsolete_accounts() {
        let obsolete_accounts: ObsoleteAccounts = Vec::new();

        let filtered_accounts: Vec<_> = obsolete_accounts.filter_obsolete_accounts(None);

        assert!(filtered_accounts.is_empty());
    }
}
