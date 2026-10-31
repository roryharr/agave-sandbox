//! Stake delegations as seen by each bank: a map of the delegations as of the latest root, plus
//! the delegations written by each unrooted bank, keyed by bank id. A bank sees the writes of the
//! banks in its ancestors.

use {
    dashmap::DashMap,
    solana_accounts_db::{ancestors::Ancestors, bank_id::BankId},
    solana_nohash_hasher::BuildNoHashHasher,
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    std::{
        collections::HashMap,
        fmt,
        marker::PhantomData,
        sync::{Arc, RwLock, RwLockReadGuard},
    },
    wincode::{
        SchemaWrite, WriteResult, config::ConfigCore, containers, io::Writer, len::BincodeLen,
    },
};

type Map<V> = HashMap<Pubkey, V, PubkeyHasherBuilder>;

/// Delegations written by one bank, or `None` where it removed a delegation.
type Writes<T> = Map<Option<T>>;

/// Shared by every bank derived from the same root; cloning shares it too. Each method takes the
/// ancestors of the bank it acts for. Empty ancestors act on the rooted map alone, which is how a
/// `Stakes` outside a bank is used.
pub struct StakeDelegations<T> {
    /// Delegations as of the latest root. Values are boxed so growing the map moves only the keys
    /// and pointers.
    rooted: Arc<RwLock<Map<Box<T>>>>,
    /// Writes of unrooted banks
    unrooted: Arc<DashMap<BankId, Writes<T>, BuildNoHashHasher<BankId>>>,
}

impl<T: Clone> StakeDelegations<T> {
    pub fn get(&self, ancestors: &Ancestors, pubkey: &Pubkey) -> Option<T> {
        // Hold the read lock while probing so no bank is squashed between the probes
        let rooted = self.rooted.read().unwrap();
        for bank_id in ancestors.iter_bank_ids().rev() {
            if let Some(writes) = self.unrooted.get(&bank_id)
                && let Some(write) = writes.get(pubkey)
            {
                return write.clone();
            }
        }
        rooted.get(pubkey).map(|value| T::clone(value))
    }

    pub fn contains_key(&self, ancestors: &Ancestors, pubkey: &Pubkey) -> bool {
        self.get(ancestors, pubkey).is_some()
    }

    /// Inserts `value` for `pubkey`, returning the previous delegation.
    pub(crate) fn insert(&mut self, ancestors: &Ancestors, pubkey: Pubkey, value: T) -> Option<T> {
        let previous = self.get(ancestors, &pubkey);
        self.write(ancestors, pubkey, Some(value));
        previous
    }

    /// Removes the delegation for `pubkey`, returning it.
    pub(crate) fn remove(&mut self, ancestors: &Ancestors, pubkey: &Pubkey) -> Option<T> {
        let previous = self.get(ancestors, pubkey);
        if previous.is_some() {
            self.write(ancestors, *pubkey, None);
        }
        previous
    }

    /// Records `write` as a write of the newest bank in `ancestors`
    fn write(&mut self, ancestors: &Ancestors, pubkey: Pubkey, write: Option<T>) {
        match ancestors.iter_bank_ids().next_back() {
            Some(bank_id) => {
                self.unrooted
                    .entry(bank_id)
                    .or_default()
                    .insert(pubkey, write);
            }
            None => {
                let mut rooted = self.rooted.write().unwrap();
                match write {
                    Some(value) => rooted.insert(pubkey, Box::new(value)),
                    None => rooted.remove(&pubkey),
                };
            }
        }
    }

    /// Returns a view of all delegations. The view holds the rooted map's read lock, which keeps
    /// banks from being squashed, so drop it promptly.
    pub fn view(&self, ancestors: &Ancestors) -> StakeDelegationsView<'_, T> {
        // Take the read lock first so no bank is squashed while collecting the writes
        let rooted = self.rooted.read().unwrap();
        let mut unrooted = Writes::default();
        for bank_id in ancestors.iter_bank_ids() {
            if let Some(writes) = self.unrooted.get(&bank_id) {
                unrooted.extend(
                    writes
                        .iter()
                        .map(|(pubkey, write)| (*pubkey, write.clone())),
                );
            }
        }
        let shadowed = unrooted
            .keys()
            .filter_map(|pubkey| rooted.get_key_value(pubkey))
            .map(|(pubkey, _)| entry_address(pubkey))
            .collect();
        StakeDelegationsView {
            rooted,
            unrooted,
            shadowed,
        }
    }

    pub fn len(&self, ancestors: &Ancestors) -> usize {
        self.view(ancestors).len()
    }

    pub fn is_empty(&self, ancestors: &Ancestors) -> bool {
        self.len(ancestors) == 0
    }

    /// Returns the delegations as an owned map.
    pub fn to_map(&self, ancestors: &Ancestors) -> HashMap<Pubkey, T> {
        self.view(ancestors)
            .iter()
            .map(|(pubkey, value)| (*pubkey, value.clone()))
            .collect()
    }

    /// Moves the writes of the banks in `ancestors` into the rooted map. Called when the newest
    /// of them is rooted, which roots the others too.
    ///
    /// Banks on forks that do not descend from the new root see the rooted map change under them;
    /// they are about to be pruned.
    pub(crate) fn squash(&self, ancestors: &Ancestors) {
        let mut rooted = self.rooted.write().unwrap();
        for bank_id in ancestors.iter_bank_ids() {
            let Some((_bank_id, writes)) = self.unrooted.remove(&bank_id) else {
                continue;
            };
            for (pubkey, write) in writes {
                match write {
                    Some(value) => rooted.insert(pubkey, Box::new(value)),
                    None => rooted.remove(&pubkey),
                };
            }
        }
    }

    /// Drops the writes of a bank that will not be rooted
    pub(crate) fn purge(&self, bank_id: BankId) {
        self.unrooted.remove(&bank_id);
    }
}

impl<T> Default for StakeDelegations<T> {
    fn default() -> Self {
        Self {
            rooted: Arc::default(),
            unrooted: Arc::default(),
        }
    }
}

impl<T> Clone for StakeDelegations<T> {
    fn clone(&self) -> Self {
        Self {
            rooted: Arc::clone(&self.rooted),
            unrooted: Arc::clone(&self.unrooted),
        }
    }
}

/// Compares the rooted maps
impl<T: Clone + PartialEq> PartialEq for StakeDelegations<T> {
    fn eq(&self, other: &Self) -> bool {
        let ancestors = Ancestors::default();
        self.to_map(&ancestors) == other.to_map(&ancestors)
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for StakeDelegations<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.view(&Ancestors::default()).iter())
            .finish()
    }
}

impl<T> FromIterator<(Pubkey, T)> for StakeDelegations<T> {
    fn from_iter<I: IntoIterator<Item = (Pubkey, T)>>(iter: I) -> Self {
        Self {
            rooted: Arc::new(RwLock::new(
                iter.into_iter()
                    .map(|(pubkey, value)| (pubkey, Box::new(value)))
                    .collect(),
            )),
            unrooted: Arc::default(),
        }
    }
}

/// Iterates the rooted map
impl<T: Clone> IntoIterator for StakeDelegations<T> {
    type Item = (Pubkey, T);
    type IntoIter = std::collections::hash_map::IntoIter<Pubkey, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.to_map(&Ancestors::default()).into_iter()
    }
}

/// Identifies an entry of the rooted map by the address of its key, which iterating the map
/// computes without reading the entry.
fn entry_address(pubkey: &Pubkey) -> usize {
    pubkey as *const Pubkey as usize
}

/// All delegations of a bank, holding the rooted map's read lock.
pub struct StakeDelegationsView<'a, T> {
    rooted: RwLockReadGuard<'a, Map<Box<T>>>,
    /// Writes of unrooted banks, which take precedence over `rooted`.
    unrooted: Writes<T>,
    /// Addresses of the entries in `rooted` that `unrooted` overrides.
    shadowed: ahash::HashSet<usize>,
}

impl<T> StakeDelegationsView<'_, T> {
    pub fn get(&self, pubkey: &Pubkey) -> Option<&T> {
        match self.unrooted.get(pubkey) {
            Some(write) => write.as_ref(),
            None => self.rooted.get(pubkey).map(|value| &**value),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Pubkey, &T)> {
        // Checking the entry's address rather than its key avoids reading every rooted entry
        let rooted = self
            .rooted
            .iter()
            .filter(|(pubkey, _)| {
                self.shadowed.is_empty() || !self.shadowed.contains(&entry_address(pubkey))
            })
            .map(|(pubkey, value)| (pubkey, &**value));
        let unrooted = self
            .unrooted
            .iter()
            .filter_map(|(pubkey, write)| write.as_ref().map(|value| (pubkey, value)));
        rooted.chain(unrooted)
    }

    /// Collects the delegations into a vector, which can be iterated in parallel.
    pub fn to_vec(&self) -> Vec<(&Pubkey, &T)> {
        let mut delegations = Vec::with_capacity(self.len());
        delegations.extend(self.iter());
        delegations
    }

    pub fn len(&self) -> usize {
        let unrooted = self
            .unrooted
            .values()
            .filter(|write| write.is_some())
            .count();
        self.rooted.len() - self.shadowed.len() + unrooted
    }
}

/// Serializes the rooted map of [`StakeDelegations`] in the layout of a map: a length followed by
/// its entries.
pub struct StakeDelegationsSchema<T>(PhantomData<T>);

unsafe impl<T, C> SchemaWrite<C> for StakeDelegationsSchema<T>
where
    T: Clone,
    C: ConfigCore,
    containers::Vec<(Pubkey, T), BincodeLen>: SchemaWrite<C, Src = Vec<(Pubkey, T)>>,
{
    type Src = StakeDelegations<T>;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        let delegations: Vec<_> = src.to_map(&Ancestors::default()).into_iter().collect();
        <containers::Vec<(Pubkey, T), BincodeLen> as SchemaWrite<C>>::size_of(&delegations)
    }

    fn write(writer: impl Writer, src: &Self::Src) -> WriteResult<()> {
        let delegations: Vec<_> = src.to_map(&Ancestors::default()).into_iter().collect();
        <containers::Vec<(Pubkey, T), BincodeLen> as SchemaWrite<C>>::write(writer, &delegations)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_clock::Slot};

    /// Ancestors of a bank whose fork is `banks`, each a (slot, bank id) pair
    fn ancestors(banks: &[(Slot, u64)]) -> Ancestors {
        Ancestors::from(
            banks
                .iter()
                .map(|(slot, bank_id)| (*slot, BankId::new(*bank_id)))
                .collect::<Vec<_>>(),
        )
    }

    fn sorted(delegations: &StakeDelegations<u64>, ancestors: &Ancestors) -> Vec<(Pubkey, u64)> {
        let mut entries: Vec<_> = delegations.to_map(ancestors).into_iter().collect();
        entries.sort_unstable();
        entries
    }

    #[test]
    fn test_bank_sees_ancestor_writes_but_not_descendant_ones() {
        let [a, b] = [Pubkey::new_unique(), Pubkey::new_unique()];
        let mut delegations = StakeDelegations::from_iter([(a, 1)]);
        let parent = ancestors(&[(1, 1)]);
        let child = ancestors(&[(1, 1), (2, 2)]);
        delegations.insert(&parent, b, 2);
        delegations.insert(&child, a, 10);

        assert_eq!(delegations.get(&parent, &a), Some(1));
        assert_eq!(delegations.get(&parent, &b), Some(2));
        assert_eq!(delegations.get(&child, &a), Some(10));
        assert_eq!(delegations.get(&child, &b), Some(2));
    }

    #[test]
    fn test_sibling_forks_are_isolated() {
        let key = Pubkey::new_unique();
        let mut delegations = StakeDelegations::from_iter([(key, 1)]);
        let parent = ancestors(&[(1, 1)]);
        let left = ancestors(&[(1, 1), (2, 2)]);
        let right = ancestors(&[(1, 1), (3, 3)]);

        delegations.insert(&left, key, 2);
        delegations.remove(&right, &key);

        assert_eq!(delegations.get(&parent, &key), Some(1));
        assert_eq!(delegations.get(&left, &key), Some(2));
        assert_eq!(delegations.get(&right, &key), None);
        assert_eq!(delegations.len(&right), 0);
    }

    #[test]
    fn test_squash_applies_newest_write() {
        let [a, b, c] = [
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let mut delegations = StakeDelegations::from_iter([(a, 1), (b, 1)]);
        let grandparent = ancestors(&[(1, 1)]);
        let parent = ancestors(&[(1, 1), (2, 2)]);
        let bank = ancestors(&[(1, 1), (2, 2), (3, 3)]);
        let descendant = ancestors(&[(1, 1), (2, 2), (3, 3), (4, 4)]);
        delegations.insert(&grandparent, a, 2);
        delegations.insert(&parent, a, 3);
        delegations.remove(&parent, &b);
        delegations.insert(&bank, c, 4);

        delegations.squash(&bank);

        let expected = {
            let mut expected = vec![(a, 3), (c, 4)];
            expected.sort_unstable();
            expected
        };
        assert_eq!(sorted(&delegations, &bank), expected);
        assert_eq!(sorted(&delegations, &descendant), expected);
        assert_eq!(
            *delegations.rooted.read().unwrap(),
            Map::from_iter([(a, Box::new(3)), (c, Box::new(4))])
        );
        assert!(delegations.unrooted.is_empty());
    }

    #[test]
    fn test_view_merges_unsquashed_writes() {
        let [a, b, c] = [
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let mut delegations = StakeDelegations::from_iter([(a, 1), (b, 1)]);
        let child = ancestors(&[(1, 1)]);
        let grandchild = ancestors(&[(1, 1), (2, 2)]);
        delegations.insert(&child, a, 2);
        delegations.remove(&child, &b);
        delegations.insert(&grandchild, c, 3);

        // Squash only `child`; `grandchild`'s own write stays unsquashed
        delegations.squash(&child);

        let view = delegations.view(&grandchild);
        assert_eq!(view.len(), 2);
        let mut entries: Vec<_> = view
            .iter()
            .map(|(pubkey, value)| (*pubkey, *value))
            .collect();
        entries.sort_unstable();
        let mut expected = vec![(a, 2), (c, 3)];
        expected.sort_unstable();
        assert_eq!(entries, expected);
        assert_eq!(view.to_vec().len(), 2);
        assert_eq!(view.get(&a), Some(&2));
        assert_eq!(view.get(&b), None);
        assert_eq!(view.get(&c), Some(&3));
    }

    #[test]
    fn test_purge_drops_only_that_bank() {
        let key = Pubkey::new_unique();
        let mut delegations = StakeDelegations::from_iter([(key, 1)]);
        let kept = ancestors(&[(1, 1), (2, 2)]);
        let dead = ancestors(&[(1, 1), (2, 3)]);
        delegations.insert(&kept, key, 2);
        delegations.insert(&dead, key, 3);

        delegations.purge(BankId::new(3));

        assert_eq!(delegations.get(&kept, &key), Some(2));
        assert_eq!(delegations.get(&dead, &key), Some(1));
    }
}
