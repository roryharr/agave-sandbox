//! Stake delegations as seen by each bank: a map of the delegations as of the latest root, shared
//! by every bank, plus the delegations written by the bank and by each of its unrooted parents.

use {
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
pub(crate) type StakeDelegationWrites<T> = Map<Option<T>>;

pub struct StakeDelegations<T> {
    /// Delegations as of the latest root, shared by every bank derived from the same root. Values
    /// are boxed so growing the map moves only the keys and pointers.
    rooted: Arc<RwLock<Map<Box<T>>>>,
    /// Writes of this bank's parents back to the root as of this bank's creation, newest first.
    /// Parents are frozen before children are made from them, so their writes no longer change.
    parents: Vec<Arc<StakeDelegationWrites<T>>>,
    /// Writes of this bank, shared with its children.
    writes: Arc<StakeDelegationWrites<T>>,
}

impl<T: Clone> StakeDelegations<T> {
    pub fn get(&self, pubkey: &Pubkey) -> Option<T> {
        for writes in std::iter::once(&self.writes).chain(&self.parents) {
            if let Some(write) = writes.get(pubkey) {
                return write.clone();
            }
        }
        self.rooted
            .read()
            .unwrap()
            .get(pubkey)
            .map(|value| T::clone(value))
    }

    pub fn contains_key(&self, pubkey: &Pubkey) -> bool {
        self.get(pubkey).is_some()
    }

    /// Inserts `value` for `pubkey`, returning the previous delegation.
    pub(crate) fn insert(&mut self, pubkey: Pubkey, value: T) -> Option<T> {
        let previous = self.get(&pubkey);
        Arc::make_mut(&mut self.writes).insert(pubkey, Some(value));
        previous
    }

    /// Removes the delegation for `pubkey`, returning it.
    pub(crate) fn remove(&mut self, pubkey: &Pubkey) -> Option<T> {
        let previous = self.get(pubkey);
        if previous.is_some() {
            Arc::make_mut(&mut self.writes).insert(*pubkey, None);
        }
        previous
    }

    /// Returns a view of all delegations. The view holds the rooted map's read lock, which keeps
    /// banks from being squashed, so drop it promptly.
    pub fn view(&self) -> StakeDelegationsView<'_, T> {
        let rooted = self.rooted.read().unwrap();
        let mut unrooted = StakeDelegationWrites::default();
        for writes in self
            .parents
            .iter()
            .rev()
            .chain(std::iter::once(&self.writes))
        {
            unrooted.extend(
                writes
                    .iter()
                    .map(|(pubkey, write)| (*pubkey, write.clone())),
            );
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

    pub fn len(&self) -> usize {
        self.view().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the delegations as an owned map.
    pub fn to_map(&self) -> HashMap<Pubkey, T> {
        self.view()
            .iter()
            .map(|(pubkey, value)| (*pubkey, value.clone()))
            .collect()
    }

    /// Returns this bank's writes, for its children.
    pub(crate) fn writes(&self) -> Arc<StakeDelegationWrites<T>> {
        Arc::clone(&self.writes)
    }

    /// Returns the delegations for a child of this bank. `parents` are the writes of this bank's
    /// parents back to the root, newest first.
    pub(crate) fn new_child(
        &self,
        parents: impl IntoIterator<Item = Arc<StakeDelegationWrites<T>>>,
    ) -> Self {
        Self {
            rooted: Arc::clone(&self.rooted),
            parents: std::iter::once(self.writes()).chain(parents).collect(),
            writes: Arc::default(),
        }
    }

    /// Applies the writes of this bank and its parents to the rooted map. Called when this bank is
    /// rooted, which roots its parents too.
    ///
    /// `parents` may include banks rooted earlier. Writes are applied oldest first, so applying
    /// theirs again is harmless.
    ///
    /// Banks on forks that do not descend from this bank see the rooted map change under them;
    /// they are about to be pruned.
    pub(crate) fn squash(&self) {
        let mut rooted = self.rooted.write().unwrap();
        for writes in self
            .parents
            .iter()
            .rev()
            .chain(std::iter::once(&self.writes))
        {
            for (pubkey, write) in writes.iter() {
                match write {
                    Some(value) => rooted.insert(*pubkey, Box::new(value.clone())),
                    None => rooted.remove(pubkey),
                };
            }
        }
    }
}

impl<T> Default for StakeDelegations<T> {
    fn default() -> Self {
        Self {
            rooted: Arc::default(),
            parents: Vec::new(),
            writes: Arc::default(),
        }
    }
}

/// The clone shares the rooted map, so it sees later squashes. Banks use
/// [`StakeDelegations::new_child`] instead.
impl<T> Clone for StakeDelegations<T> {
    fn clone(&self) -> Self {
        Self {
            rooted: Arc::clone(&self.rooted),
            parents: self.parents.clone(),
            writes: Arc::clone(&self.writes),
        }
    }
}

impl<T: Clone + PartialEq> PartialEq for StakeDelegations<T> {
    fn eq(&self, other: &Self) -> bool {
        self.to_map() == other.to_map()
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for StakeDelegations<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.view().iter()).finish()
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
            parents: Vec::new(),
            writes: Arc::default(),
        }
    }
}

impl<T: Clone> IntoIterator for StakeDelegations<T> {
    type Item = (Pubkey, T);
    type IntoIter = std::collections::hash_map::IntoIter<Pubkey, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.to_map().into_iter()
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
    unrooted: StakeDelegationWrites<T>,
    /// Addresses of the entries in `rooted` that `unrooted` overrides.
    shadowed: ahash::HashSet<usize>,
}

impl<T> StakeDelegationsView<'_, T> {
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

/// Serializes [`StakeDelegations`] in the layout of a map: a length followed by its entries.
pub struct StakeDelegationsSchema<T>(PhantomData<T>);

unsafe impl<T, C> SchemaWrite<C> for StakeDelegationsSchema<T>
where
    T: Clone,
    C: ConfigCore,
    containers::Vec<(Pubkey, T), BincodeLen>: SchemaWrite<C, Src = Vec<(Pubkey, T)>>,
{
    type Src = StakeDelegations<T>;

    fn size_of(src: &Self::Src) -> WriteResult<usize> {
        let delegations: Vec<_> = src.to_map().into_iter().collect();
        <containers::Vec<(Pubkey, T), BincodeLen> as SchemaWrite<C>>::size_of(&delegations)
    }

    fn write(writer: impl Writer, src: &Self::Src) -> WriteResult<()> {
        let delegations: Vec<_> = src.to_map().into_iter().collect();
        <containers::Vec<(Pubkey, T), BincodeLen> as SchemaWrite<C>>::write(writer, &delegations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Makes a child of `parent`, whose own parents back to the root are `older`, newest first
    fn child(
        parent: &StakeDelegations<u64>,
        older: &[&StakeDelegations<u64>],
    ) -> StakeDelegations<u64> {
        parent.new_child(older.iter().map(|bank| bank.writes()))
    }

    fn sorted(delegations: &StakeDelegations<u64>) -> Vec<(Pubkey, u64)> {
        let mut entries: Vec<_> = delegations.to_map().into_iter().collect();
        entries.sort_unstable();
        entries
    }

    #[test]
    fn test_child_sees_parent_writes_but_not_later_ones() {
        let [a, b] = [Pubkey::new_unique(), Pubkey::new_unique()];
        let mut parent = StakeDelegations::from_iter([(a, 1)]);
        parent.insert(b, 2);

        let child = child(&parent, &[]);
        parent.insert(a, 10);

        assert_eq!(child.get(&a), Some(1));
        assert_eq!(child.get(&b), Some(2));
        assert_eq!(parent.get(&a), Some(10));
    }

    #[test]
    fn test_sibling_forks_are_isolated() {
        let key = Pubkey::new_unique();
        let parent = StakeDelegations::from_iter([(key, 1)]);
        let mut left = child(&parent, &[]);
        let mut right = child(&parent, &[]);

        left.insert(key, 2);
        right.remove(&key);

        assert_eq!(parent.get(&key), Some(1));
        assert_eq!(left.get(&key), Some(2));
        assert_eq!(right.get(&key), None);
        assert_eq!(right.len(), 0);
    }

    #[test]
    fn test_squash_applies_newest_write() {
        let [a, b, c] = [
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let mut grandparent = StakeDelegations::from_iter([(a, 1), (b, 1)]);
        grandparent.insert(a, 2);
        let mut parent = child(&grandparent, &[]);
        parent.insert(a, 3);
        parent.remove(&b);
        let mut bank = child(&parent, &[&grandparent]);
        bank.insert(c, 4);
        let descendant = child(&bank, &[&parent, &grandparent]);

        bank.squash();

        let expected = {
            let mut expected = vec![(a, 3), (c, 4)];
            expected.sort_unstable();
            expected
        };
        assert_eq!(sorted(&bank), expected);
        assert_eq!(sorted(&descendant), expected);
        assert_eq!(
            *bank.rooted.read().unwrap(),
            Map::from_iter([(a, Box::new(3)), (c, Box::new(4))])
        );
    }

    #[test]
    fn test_squash_again_after_later_root_keeps_newest_write() {
        let key = Pubkey::new_unique();
        let mut root = StakeDelegations::from_iter([(key, 1)]);
        root.insert(key, 2);
        root.squash();
        let mut bank = child(&root, &[]);
        bank.insert(key, 3);

        // `bank`'s parents still include `root`, which was rooted already
        bank.squash();

        assert_eq!(
            *bank.rooted.read().unwrap(),
            Map::from_iter([(key, Box::new(3))])
        );
    }

    #[test]
    fn test_view_merges_unsquashed_writes() {
        let [a, b, c] = [
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let root = StakeDelegations::from_iter([(a, 1), (b, 1)]);
        let mut bank = child(&root, &[]);
        bank.insert(a, 2);
        bank.remove(&b);
        let mut descendant = child(&bank, &[&root]);
        descendant.insert(c, 3);

        // Squash only `bank`; `descendant`'s own write stays unsquashed
        bank.squash();

        let view = descendant.view();
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
    }
}
