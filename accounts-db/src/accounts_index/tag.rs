//! The in-mem accounts index is keyed by a `Tag`, a 64 bit hash of the pubkey, rather than by
//! the pubkey itself. That takes a map slot from 40 bytes (32 pubkey + 8 entry) down to 16.
//!
//! A tag must be a hash of the whole pubkey rather than a slice of it. The bin is chosen from
//! the pubkey's high bits, so every pubkey in a bin shares them, and hashbrown takes its control
//! byte from the high bits of the hash.
//!
//! Tags are only ever compared within a bin, so each bin seeds its own calculator. The seed is
//! random per boot, so tags are not stable across restarts and cannot be ground offline.
//!
//! Two pubkeys in a bin that share a tag share an index entry. That is not handled: the load
//! path asserts the account it reads has the pubkey it asked for, so a collision panics rather
//! than silently returning another account's data.

use {
    solana_pubkey::Pubkey,
    std::hash::{BuildHasher, Hasher},
};

/// The key of the in-mem accounts index: a 64 bit hash of a pubkey.
pub type Tag = u64;

/// Computes the `Tag` for a pubkey, using a seed chosen when the bin is created.
#[derive(Debug, Clone)]
pub struct TagCalculator {
    hasher: ahash::RandomState,
}

impl Default for TagCalculator {
    fn default() -> Self {
        Self {
            hasher: ahash::RandomState::new(),
        }
    }
}

impl TagCalculator {
    #[inline]
    pub fn tag_from_pubkey(&self, pubkey: &Pubkey) -> Tag {
        self.hasher.hash_one(pubkey)
    }
}

/// Hasher for a map already keyed by `Tag`.
///
/// A tag is a hash, so hashbrown's bucket index (the low bits) and control byte (the high bits)
/// are both already well distributed. Hashing it again would only cost time.
#[derive(Debug, Default, Clone)]
pub struct TagHasherBuilder;

impl BuildHasher for TagHasherBuilder {
    type Hasher = TagHasher;

    fn build_hasher(&self) -> TagHasher {
        TagHasher(0)
    }
}

#[derive(Debug, Default)]
pub struct TagHasher(Tag);

impl Hasher for TagHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("the in-mem accounts index is keyed by Tag")
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::collections::HashMap};

    #[test]
    fn test_tag_hasher_is_passthrough() {
        let builder = TagHasherBuilder;
        assert_eq!(builder.hash_one(42u64), 42);
        assert_eq!(builder.hash_one(Tag::MAX), Tag::MAX);
    }

    #[test]
    fn test_tag_map_round_trips() {
        let calculator = TagCalculator::default();
        let mut map = HashMap::<Tag, usize, TagHasherBuilder>::default();
        let pubkeys: Vec<_> = (0..1_000).map(|_| solana_pubkey::new_rand()).collect();
        for (i, pubkey) in pubkeys.iter().enumerate() {
            map.insert(calculator.tag_from_pubkey(pubkey), i);
        }
        // no collisions among 1,000 random pubkeys, and every one is found again
        assert_eq!(map.len(), pubkeys.len());
        for (i, pubkey) in pubkeys.iter().enumerate() {
            assert_eq!(map.get(&calculator.tag_from_pubkey(pubkey)), Some(&i));
        }
    }

    #[test]
    fn test_tag_calculators_are_independently_seeded() {
        let pubkey = solana_pubkey::new_rand();
        let tags: Vec<_> = (0..8)
            .map(|_| TagCalculator::default().tag_from_pubkey(&pubkey))
            .collect();
        assert!(
            tags.iter().any(|tag| *tag != tags[0]),
            "every calculator produced the same tag: {tags:?}"
        );
    }
}
