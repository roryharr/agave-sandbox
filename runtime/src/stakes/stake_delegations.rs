//! Stake delegations as seen by each bank: a map of the delegations as of the latest root,
//! shared by every bank, plus a small layer per unrooted bank holding the delegations it wrote.

use {
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    std::{
        collections::HashMap,
        fmt,
        marker::PhantomData,
        mem,
        sync::{
            Arc, Mutex, RwLock, RwLockReadGuard,
            atomic::{AtomicBool, Ordering},
        },
    },
    wincode::{
        SchemaWrite, WriteResult, config::ConfigCore, containers, io::Writer, len::BincodeLen,
    },
};

type Map<V> = HashMap<Pubkey, V, PubkeyHasherBuilder>;

/// Delegations written by one bank, or `None` where it removed a delegation.
type Writes<T> = Map<Option<T>>;

/// The writes of a frozen, unrooted bank, shared by its descendants.
struct Layer<T> {
    writes: Writes<T>,
    /// The layer of the closest ancestor that wrote delegations. Cleared once this layer is
    /// squashed, so squashed layers are freed.
    parent: Mutex<Option<Arc<Layer<T>>>>,
    /// Set once `writes` are in the rooted map, after which readers use the rooted map instead.
    squashed: AtomicBool,
}

pub struct StakeDelegations<T> {
    /// Delegations as of the latest root, shared by every bank derived from the same root. Values
    /// are boxed so growing the map moves only the keys and pointers.
    rooted: Arc<RwLock<Map<Box<T>>>>,
    /// Writes of unrooted ancestors, newest first.
    ancestors: Option<Arc<Layer<T>>>,
    /// Writes of this bank.
    writes: Writes<T>,
}

impl<T: Clone> StakeDelegations<T> {
    /// Returns the ancestor layers not yet squashed, newest first.
    fn unsquashed_ancestors(&self) -> Vec<Arc<Layer<T>>> {
        let mut layers = Vec::new();
        let mut next = self.ancestors.clone();
        while let Some(layer) = next {
            if layer.squashed.load(Ordering::Acquire) {
                break;
            }
            next = layer.parent.lock().unwrap().clone();
            layers.push(layer);
        }
        layers
    }

    pub fn get(&self, pubkey: &Pubkey) -> Option<T> {
        if let Some(write) = self.writes.get(pubkey) {
            return write.clone();
        }
        let mut next = self.ancestors.clone();
        while let Some(layer) = next {
            // A squashed layer's writes, and those of all older layers, are in the rooted map
            if layer.squashed.load(Ordering::Acquire) {
                break;
            }
            if let Some(write) = layer.writes.get(pubkey) {
                return write.clone();
            }
            next = layer.parent.lock().unwrap().clone();
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
        self.writes.insert(pubkey, Some(value));
        previous
    }

    /// Removes the delegation for `pubkey`, returning it.
    pub(crate) fn remove(&mut self, pubkey: &Pubkey) -> Option<T> {
        let previous = self.get(pubkey);
        if previous.is_some() {
            self.writes.insert(*pubkey, None);
        }
        previous
    }

    /// Returns a view of all delegations. The view holds the rooted map's read lock, which keeps
    /// banks from being squashed, so drop it promptly.
    pub fn view(&self) -> StakeDelegationsView<'_, T> {
        // Take the read lock first so no layer is squashed while collecting the layers
        let rooted = self.rooted.read().unwrap();
        let mut unrooted = Writes::default();
        for layer in self.unsquashed_ancestors().iter().rev() {
            unrooted.extend(
                layer
                    .writes
                    .iter()
                    .map(|(pubkey, write)| (*pubkey, write.clone())),
            );
        }
        unrooted.extend(
            self.writes
                .iter()
                .map(|(pubkey, write)| (*pubkey, write.clone())),
        );
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

    /// Returns the delegations for a child bank. This bank's writes move into a layer shared with
    /// the child, so later writes to this bank are not seen by the child.
    pub(crate) fn new_child(&mut self) -> Self {
        if !self.writes.is_empty() {
            self.ancestors = Some(Arc::new(Layer {
                writes: mem::take(&mut self.writes),
                parent: Mutex::new(self.ancestors.take()),
                squashed: AtomicBool::new(false),
            }));
        }
        Self {
            rooted: Arc::clone(&self.rooted),
            ancestors: self.ancestors.clone(),
            writes: Writes::default(),
        }
    }

    /// Moves the writes of this bank and its unrooted ancestors into the rooted map. Called when
    /// this bank is rooted, which roots its ancestors too.
    ///
    /// Banks on forks that do not descend from this bank see the rooted map change under them;
    /// they are about to be pruned.
    pub(crate) fn squash(&mut self) {
        let mut rooted = self.rooted.write().unwrap();
        let layers = self.unsquashed_ancestors();
        for layer in layers.iter().rev() {
            for (pubkey, write) in &layer.writes {
                match write {
                    Some(value) => rooted.insert(*pubkey, Box::new(value.clone())),
                    None => rooted.remove(pubkey),
                };
            }
        }
        for (pubkey, write) in mem::take(&mut self.writes) {
            match write {
                Some(value) => rooted.insert(pubkey, Box::new(value)),
                None => rooted.remove(&pubkey),
            };
        }
        for layer in &layers {
            layer.squashed.store(true, Ordering::Release);
            *layer.parent.lock().unwrap() = None;
        }
        drop(rooted);
        self.ancestors = None;
    }
}

impl<T> Default for StakeDelegations<T> {
    fn default() -> Self {
        Self {
            rooted: Arc::default(),
            ancestors: None,
            writes: Writes::default(),
        }
    }
}

/// The clone shares the rooted map, so it sees later squashes. Banks use
/// [`StakeDelegations::new_child`] instead.
impl<T: Clone> Clone for StakeDelegations<T> {
    fn clone(&self) -> Self {
        Self {
            rooted: Arc::clone(&self.rooted),
            ancestors: self.ancestors.clone(),
            writes: self.writes.clone(),
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
            ancestors: None,
            writes: Writes::default(),
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
    unrooted: Writes<T>,
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

        let child = parent.new_child();
        parent.insert(a, 10);

        assert_eq!(child.get(&a), Some(1));
        assert_eq!(child.get(&b), Some(2));
        assert_eq!(parent.get(&a), Some(10));
    }

    #[test]
    fn test_sibling_forks_are_isolated() {
        let key = Pubkey::new_unique();
        let mut parent = StakeDelegations::from_iter([(key, 1)]);
        let mut left = parent.new_child();
        let mut right = parent.new_child();

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
        let mut parent = grandparent.new_child();
        parent.insert(a, 3);
        parent.remove(&b);
        let mut bank = parent.new_child();
        bank.insert(c, 4);
        let descendant = bank.new_child();

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
    fn test_view_merges_unsquashed_writes() {
        let [a, b, c] = [
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        let mut root = StakeDelegations::from_iter([(a, 1), (b, 1)]);
        let mut child = root.new_child();
        child.insert(a, 2);
        child.remove(&b);
        let mut grandchild = child.new_child();
        grandchild.insert(c, 3);

        // Squash only `child`; `grandchild`'s own write stays unsquashed
        child.squash();

        let view = grandchild.view();
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
