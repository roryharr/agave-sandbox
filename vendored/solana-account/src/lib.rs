#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! The Solana [`Account`] type.

#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
#[cfg(feature = "serde")]
use serde::ser::{Serialize, Serializer};
#[cfg(feature = "frozen-abi")]
use solana_frozen_abi_macro::{frozen_abi, AbiExample, StableAbi, StableAbiSample};
#[cfg(feature = "bincode")]
use solana_sysvar::SysvarSerialize;
use {
    solana_account_info::{debug_account_data::*, AccountInfo},
    solana_clock::{Epoch, INITIAL_RENT_EPOCH},
    solana_instruction_error::LamportsError,
    solana_pubkey::Pubkey,
    solana_sdk_ids::{bpf_loader, bpf_loader_deprecated, bpf_loader_upgradeable, loader_v4},
    std::{
        cell::RefCell,
        fmt,
        mem::MaybeUninit,
        ops::Deref,
        ptr,
        rc::Rc,
        sync::{Arc, OnceLock},
    },
};
#[cfg(feature = "bincode")]
pub mod state_traits;

#[cfg(target_os = "linux")]
mod gather;
mod write;
pub use write::AccountDataWrite;

/// An Account with data that is stored on chain
#[repr(C)]
#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample, StableAbi, StableAbiSample),
    frozen_abi(
        api_digest = "62EqVoynUFvuui7DVfqWCvZP7bxKGJGioeSBnWrdjRME",
        abi_digest = "G4phLpfhujMpk4wS1WswCe4HqnQjCBPWjrXjvDZ6iUw8"
    )
)]
#[cfg_attr(
    feature = "serde",
    derive(serde_derive::Deserialize),
    serde(rename_all = "camelCase")
)]
#[cfg_attr(feature = "wincode", derive(wincode::SchemaRead, wincode::SchemaWrite))]
#[derive(PartialEq, Eq, Clone, Default)]
pub struct Account {
    /// lamports in the account
    pub lamports: u64,
    /// data held in this account
    #[cfg_attr(feature = "serde", serde(with = "serde_bytes"))]
    #[cfg_attr(
        feature = "frozen-abi",
        stable_abi_sample(
            with = "(0..rng.random_range(0..=1000)).map(|_| rng.random()).collect()"
        )
    )]
    pub data: Vec<u8>,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: Epoch,
}

// mod because we need 'Account' below to have the name 'Account' to match expected serialization
#[cfg(feature = "serde")]
mod account_serialize {
    #[cfg(feature = "frozen-abi")]
    use solana_frozen_abi_macro::{frozen_abi, AbiExample};
    use {
        crate::ReadableAccount,
        serde::{ser::Serializer, Serialize},
        solana_clock::Epoch,
        solana_pubkey::Pubkey,
    };
    #[repr(C)]
    #[cfg_attr(
        feature = "frozen-abi",
        derive(AbiExample),
        frozen_abi(digest = "62EqVoynUFvuui7DVfqWCvZP7bxKGJGioeSBnWrdjRME")
    )]
    #[derive(serde_derive::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Account<'a> {
        lamports: u64,
        #[serde(with = "serde_bytes")]
        // a slice so we don't have to make a copy just to serialize this
        data: &'a [u8],
        owner: &'a Pubkey,
        executable: bool,
        rent_epoch: Epoch,
    }

    /// allows us to implement serialize on AccountSharedData that is equivalent to Account::serialize without making a copy of the Vec<u8>
    pub fn serialize_account<S>(
        account: &impl ReadableAccount,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let temp = Account {
            lamports: account.lamports(),
            data: account.data(),
            owner: account.owner(),
            executable: account.executable(),
            rent_epoch: account.rent_epoch(),
        };
        temp.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl Serialize for Account {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        crate::account_serialize::serialize_account(self, serializer)
    }
}

#[cfg(feature = "serde")]
impl Serialize for AccountSharedData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        crate::account_serialize::serialize_account(self, serializer)
    }
}

/// An Account with data that is stored on chain
/// This will be the in-memory representation of the 'Account' struct data.
/// The existing 'Account' structure cannot easily change due to downstream projects.
#[cfg_attr(feature = "frozen-abi", derive(AbiExample, StableAbi, StableAbiSample))]
#[cfg_attr(
    feature = "serde",
    derive(serde_derive::Deserialize),
    serde(from = "Account")
)]
#[cfg_attr(feature = "wincode", derive(wincode::SchemaRead, wincode::SchemaWrite))]
#[derive(PartialEq, Eq, Clone, Default)]
pub struct AccountSharedData {
    /// lamports in the account
    lamports: u64,
    /// data held in this account
    data: AccountData,
    /// the program that owns this account. If executable, the program that loads this account.
    owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    executable: bool,
    /// the epoch at which this account will next owe rent
    rent_epoch: Epoch,
}

/// The data held in an [`AccountSharedData`].
///
/// Opaque handle to the account's data bytes. Cloning is O(1): clones share
/// the underlying bytes. Mutating methods copy the bytes first if they are
/// shared (copy-on-write).
///
/// The bytes are either one contiguous buffer or a sequence of segments
/// referencing shared sources. Fragmented data reads zero-copy through
/// `data_chunks()`; callers that need one contiguous slice (`as_slice()`)
/// cause a lazily-cached materialization. All mutating methods produce
/// contiguous data, copying the segments together first if needed.
#[derive(Clone)]
pub struct AccountData {
    repr: Repr,
}

#[derive(Clone)]
enum Repr {
    /// One contiguous heap buffer holding all the data.
    Contiguous(Arc<Vec<u8>>),
    /// The data split across segments referencing shared sources.
    Fragmented(Arc<SegmentList>),
}

/// Ordered segments whose concatenation is the account data.
struct SegmentList {
    /// Non-empty segments; their lengths sum to `len`.
    segments: Vec<Segment>,
    len: usize,
    /// Lazily materialized contiguous copy, for callers that need one slice.
    contiguous: OnceLock<Box<[u8]>>,
}

/// A byte range into a shared source.
#[derive(Clone, Debug)]
struct Segment {
    source: SegmentSource,
    offset: usize,
    len: usize,
}

/// Backing bytes shared by segments, potentially across many [`AccountData`]s.
#[derive(Clone, Debug)]
enum SegmentSource {
    /// Plain heap bytes: readable but not gather-mappable. Only built by
    /// the dev-context test constructor today.
    #[cfg_attr(not(feature = "dev-context-only-utils"), allow(dead_code))]
    Owned(Arc<Vec<u8>>),
    /// Memfd-backed and page-aligned: mappable into a kernel-COW gather.
    #[cfg(target_os = "linux")]
    Memfd(Arc<gather::MemfdSource>),
}

impl Segment {
    fn as_slice(&self) -> &[u8] {
        match &self.source {
            SegmentSource::Owned(bytes) => &bytes[self.offset..self.offset + self.len],
            #[cfg(target_os = "linux")]
            SegmentSource::Memfd(source) => &source.as_slice()[self.offset..self.offset + self.len],
        }
    }
}

impl SegmentList {
    /// The whole logical byte range as one slice, materializing it on first
    /// use if there is more than one segment.
    fn as_contiguous_slice(&self) -> &[u8] {
        match self.segments.as_slice() {
            [] => &[],
            [segment] => segment.as_slice(),
            _ => self.contiguous.get_or_init(|| {
                let mut bytes = Vec::with_capacity(self.len);
                for segment in &self.segments {
                    bytes.extend_from_slice(segment.as_slice());
                }
                bytes.into_boxed_slice()
            }),
        }
    }
}

impl AccountData {
    pub fn as_slice(&self) -> &[u8] {
        match &self.repr {
            Repr::Contiguous(data) => data,
            Repr::Fragmented(list) => list.as_contiguous_slice(),
        }
    }

    pub fn len(&self) -> usize {
        match &self.repr {
            Repr::Contiguous(data) => data.len(),
            Repr::Fragmented(list) => list.len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterates the bytes as contiguous chunks whose concatenation is
    /// `as_slice()`, without requiring the whole range to be contiguous.
    pub fn data_chunks(&self) -> DataChunks<'_> {
        match &self.repr {
            Repr::Contiguous(data) => DataChunks::single(data),
            Repr::Fragmented(list) => DataChunks {
                inner: DataChunksInner::Segments(list.segments.iter()),
            },
        }
    }

    /// Returns true if mutating will copy the bytes: they are either shared
    /// with another clone or not held in one contiguous buffer.
    ///
    /// Serialization relies on this to never map a buffer writable into the
    /// VM while other clones can observe it.
    pub fn is_shared(&self) -> bool {
        match &self.repr {
            Repr::Contiguous(data) => Arc::strong_count(data) > 1,
            Repr::Fragmented(_) => true,
        }
    }

    /// True when `as_slice` would assemble (and cache) a contiguous copy of
    /// the bytes instead of returning a view of existing memory.
    pub fn as_slice_would_copy(&self) -> bool {
        match &self.repr {
            Repr::Contiguous(_) => false,
            Repr::Fragmented(list) => list.segments.len() > 1 && list.contiguous.get().is_none(),
        }
    }

    /// Returns true if `self` and `other` share the same underlying bytes,
    /// i.e. they are clones of each other.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&self.repr, &other.repr) {
            (Repr::Contiguous(left), Repr::Contiguous(right)) => Arc::ptr_eq(left, right),
            (Repr::Fragmented(left), Repr::Fragmented(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    /// The size of the contiguous buffer, i.e. `len()` plus any spare
    /// capacity usable without reallocating. Fragmented data has none.
    pub fn capacity(&self) -> usize {
        match &self.repr {
            Repr::Contiguous(data) => data.capacity(),
            Repr::Fragmented(list) => list.len,
        }
    }

    fn to_contiguous_vec(&self, additional_capacity: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.len().saturating_add(additional_capacity));
        for chunk in self.data_chunks() {
            bytes.extend_from_slice(chunk);
        }
        bytes
    }

    fn make_mut(&mut self) -> &mut Vec<u8> {
        if let Repr::Fragmented(_) = &self.repr {
            self.repr = Repr::Contiguous(Arc::new(self.to_contiguous_vec(0)));
        }
        match &mut self.repr {
            Repr::Contiguous(data) => Arc::make_mut(data),
            Repr::Fragmented(_) => unreachable!("made contiguous above"),
        }
    }

    /// Returns a mutable view of the bytes, copying them first if shared.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.make_mut().as_mut_slice()
    }

    pub fn reserve(&mut self, additional: usize) {
        if let Repr::Contiguous(data) = &mut self.repr {
            if let Some(data) = Arc::get_mut(data) {
                data.reserve(additional);
                return;
            }
        }
        // Allocate the final capacity up front so the bytes are copied
        // once, instead of make_mut's copy followed by a reallocation.
        self.repr = Repr::Contiguous(Arc::new(self.to_contiguous_vec(additional)));
    }

    pub fn resize(&mut self, new_len: usize, value: u8) {
        self.make_mut().resize(new_len, value)
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.make_mut().extend_from_slice(data)
    }

    pub fn set_data_from_slice(&mut self, new_data: &[u8]) {
        // If the buffer isn't shared, we're going to memcpy in place.
        let Repr::Contiguous(data) = &mut self.repr else {
            self.repr = Repr::Contiguous(Arc::new(new_data.to_vec()));
            return;
        };
        let Some(data) = Arc::get_mut(data) else {
            // If the buffer is shared, the cheapest thing to do is to clone the
            // incoming slice and replace the buffer.
            self.repr = Repr::Contiguous(Arc::new(new_data.to_vec()));
            return;
        };

        let new_len = new_data.len();

        // Reserve additional capacity if needed. Here we make the assumption
        // that growing the current buffer is cheaper than doing a whole new
        // allocation to make `new_data` owned.
        //
        // This assumption holds true during CPI, especially when the account
        // size doesn't change but the account is only changed in place. And
        // it's also true when the account is grown by a small margin (the
        // realloc limit is quite low), in which case the allocator can just
        // update the allocation metadata without moving.
        //
        // Shrinking and copying in place is always faster than making
        // `new_data` owned, since shrinking boils down to updating the Vec's
        // length.

        data.reserve(new_len.saturating_sub(data.len()));

        // Safety:
        // We just reserved enough capacity. We set data::len to 0 to avoid
        // possible UB on panic (dropping uninitialized elements), do the copy,
        // finally set the new length once everything is initialized.
        #[allow(clippy::uninit_vec)]
        // this is a false positive, the lint doesn't currently special case set_len(0)
        unsafe {
            data.set_len(0);
            ptr::copy_nonoverlapping(new_data.as_ptr(), data.as_mut_ptr(), new_len);
            data.set_len(new_len);
        };
    }

    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.make_mut().spare_capacity_mut()
    }

    fn into_vec(mut self) -> Vec<u8> {
        std::mem::take(self.make_mut())
    }

    /// Builds fragmented data with one segment per chunk, for testing the
    /// fragmented paths. Production fragmented data is only created by the
    /// paged copy-on-write machinery.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn from_chunks_for_tests(chunks: &[&[u8]]) -> Self {
        let segments: Vec<_> = chunks
            .iter()
            .filter(|chunk| !chunk.is_empty())
            .map(|chunk| Segment {
                source: SegmentSource::Owned(Arc::new(chunk.to_vec())),
                offset: 0,
                len: chunk.len(),
            })
            .collect();
        let len = segments.iter().map(|segment| segment.len).sum();
        Self {
            repr: Repr::Fragmented(Arc::new(SegmentList {
                segments,
                len,
                contiguous: OnceLock::new(),
            })),
        }
    }
}

impl PartialEq for AccountData {
    fn eq(&self, other: &Self) -> bool {
        if self.ptr_eq(other) {
            return true;
        }
        if self.len() != other.len() {
            return false;
        }
        // Compare chunkwise; the two sides may be chunked differently, so
        // advance through both chunk sequences comparing the overlap.
        let mut left_chunks = self.data_chunks();
        let mut right_chunks = other.data_chunks();
        let mut left: &[u8] = &[];
        let mut right: &[u8] = &[];
        loop {
            while left.is_empty() {
                match left_chunks.next() {
                    Some(chunk) => left = chunk,
                    // Chunk lengths sum to the (equal) data lengths on both
                    // sides, so the right side is exhausted too.
                    None => return true,
                }
            }
            while right.is_empty() {
                match right_chunks.next() {
                    Some(chunk) => right = chunk,
                    None => return true,
                }
            }
            let overlap = left.len().min(right.len());
            if left[..overlap] != right[..overlap] {
                return false;
            }
            left = &left[overlap..];
            right = &right[overlap..];
        }
    }
}

impl Eq for AccountData {}

impl Default for AccountData {
    fn default() -> Self {
        Self {
            repr: Repr::Contiguous(Arc::default()),
        }
    }
}

impl fmt::Debug for AccountData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_struct("AccountData");
        f.field("len", &self.len());
        debug_account_data(self.as_slice(), &mut f);
        f.finish()
    }
}

impl From<Vec<u8>> for AccountData {
    fn from(data: Vec<u8>) -> Self {
        Self {
            repr: Repr::Contiguous(Arc::new(data)),
        }
    }
}

impl From<Arc<Vec<u8>>> for AccountData {
    fn from(data: Arc<Vec<u8>>) -> Self {
        Self {
            repr: Repr::Contiguous(data),
        }
    }
}

#[cfg(feature = "wincode")]
// SAFETY: TYPE_META is left Dynamic; size_of and write delegate to the
// slice schema, which writes exactly the bytes it sizes.
unsafe impl<C: wincode::config::Config> wincode::SchemaWrite<C> for AccountData {
    type Src = Self;

    // The wire format is the plain byte-vector schema (length prefix plus
    // bytes), independent of the in-memory representation. It must not
    // change: snapshots serialize accounts through this.
    fn size_of(src: &Self::Src) -> wincode::WriteResult<usize> {
        <[u8] as wincode::SchemaWrite<C>>::size_of(src.as_slice())
    }

    fn write(writer: impl wincode::io::Writer, src: &Self::Src) -> wincode::WriteResult<()> {
        <[u8] as wincode::SchemaWrite<C>>::write(writer, src.as_slice())
    }
}

#[cfg(feature = "wincode")]
// SAFETY: TYPE_META is left Dynamic; read initializes dst exactly when it
// returns Ok, delegating to the byte-vector schema.
unsafe impl<'de, C: wincode::config::Config> wincode::SchemaRead<'de, C> for AccountData {
    type Dst = Self;

    fn read(
        reader: impl wincode::io::Reader<'de>,
        dst: &mut MaybeUninit<Self::Dst>,
    ) -> wincode::ReadResult<()> {
        let bytes = <Vec<u8> as wincode::SchemaRead<'de, C>>::get(reader)?;
        dst.write(Self::from(bytes));
        Ok(())
    }
}

#[cfg(feature = "frozen-abi")]
impl solana_frozen_abi::abi_example::AbiExample for AccountData {
    fn example() -> Self {
        Self::from(<Arc<Vec<u8>> as solana_frozen_abi::abi_example::AbiExample>::example())
    }
}

#[cfg(feature = "frozen-abi")]
impl solana_frozen_abi::stable_abi::StableAbi for AccountData {
    // Sample exactly like the previous `Arc<Vec<u8>>` representation so the
    // abi digests of containing types don't change.
    fn random_with_context(
        rng: &mut (impl solana_frozen_abi::rand::RngCore + ?Sized),
        _ctx: (),
    ) -> Self {
        Self::from(<Arc<Vec<u8>> as solana_frozen_abi::stable_abi::StableAbi>::random(rng))
    }
}

/// Iterator over account data as contiguous chunks.
///
/// The concatenation of the chunks is the account data. Consumers that can
/// process the data incrementally (hashing, writing to a file) should iterate
/// chunks instead of requiring one contiguous slice, so that a chunked
/// underlying representation doesn't have to materialize.
#[derive(Debug)]
pub struct DataChunks<'a> {
    inner: DataChunksInner<'a>,
}

#[derive(Debug)]
enum DataChunksInner<'a> {
    Single(Option<&'a [u8]>),
    Segments(std::slice::Iter<'a, Segment>),
}

impl<'a> DataChunks<'a> {
    /// A single contiguous chunk.
    pub fn single(data: &'a [u8]) -> Self {
        Self {
            inner: DataChunksInner::Single(Some(data)),
        }
    }
}

impl<'a> Iterator for DataChunks<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            DataChunksInner::Single(chunk) => chunk.take(),
            DataChunksInner::Segments(segments) => segments.next().map(Segment::as_slice),
        }
    }
}

/// Compares two ReadableAccounts
///
/// Returns true if accounts are essentially equivalent as in all fields are equivalent.
pub fn accounts_equal<T: ReadableAccount, U: ReadableAccount>(me: &T, other: &U) -> bool {
    me.lamports() == other.lamports()
        && me.executable() == other.executable()
        && me.rent_epoch() == other.rent_epoch()
        && me.owner() == other.owner()
        && me.data() == other.data()
}

impl From<AccountSharedData> for Account {
    fn from(other: AccountSharedData) -> Self {
        Self {
            lamports: other.lamports,
            data: other.data.into_vec(),
            owner: other.owner,
            executable: other.executable,
            rent_epoch: other.rent_epoch,
        }
    }
}

impl From<Account> for AccountSharedData {
    fn from(other: Account) -> Self {
        Self {
            lamports: other.lamports,
            data: AccountData::from(other.data),
            owner: other.owner,
            executable: other.executable,
            rent_epoch: other.rent_epoch,
        }
    }
}

pub trait WritableAccount: ReadableAccount {
    fn set_lamports(&mut self, lamports: u64);
    fn checked_add_lamports(&mut self, lamports: u64) -> Result<(), LamportsError> {
        self.set_lamports(
            self.lamports()
                .checked_add(lamports)
                .ok_or(LamportsError::ArithmeticOverflow)?,
        );
        Ok(())
    }
    fn checked_sub_lamports(&mut self, lamports: u64) -> Result<(), LamportsError> {
        self.set_lamports(
            self.lamports()
                .checked_sub(lamports)
                .ok_or(LamportsError::ArithmeticUnderflow)?,
        );
        Ok(())
    }
    fn saturating_add_lamports(&mut self, lamports: u64) {
        self.set_lamports(self.lamports().saturating_add(lamports))
    }
    fn saturating_sub_lamports(&mut self, lamports: u64) {
        self.set_lamports(self.lamports().saturating_sub(lamports))
    }
    fn data_as_mut_slice(&mut self) -> &mut [u8];
    fn set_owner(&mut self, owner: Pubkey);
    fn copy_into_owner_from_slice(&mut self, source: &[u8]);
    fn set_executable(&mut self, executable: bool);
    fn set_rent_epoch(&mut self, epoch: Epoch);
}

pub trait ReadableAccount: Sized {
    fn lamports(&self) -> u64;
    fn data(&self) -> &[u8];
    /// The length of the account data, without requiring it to be contiguous.
    fn data_len(&self) -> usize {
        self.data().len()
    }
    /// Iterates the account data as contiguous chunks whose concatenation is
    /// `data()`, without requiring the whole range to be contiguous.
    fn data_chunks(&self) -> DataChunks<'_> {
        DataChunks::single(self.data())
    }
    fn owner(&self) -> &Pubkey;
    fn executable(&self) -> bool;
    fn rent_epoch(&self) -> Epoch;
}

impl<T> ReadableAccount for T
where
    T: Deref,
    T::Target: ReadableAccount,
{
    fn lamports(&self) -> u64 {
        self.deref().lamports()
    }
    fn data(&self) -> &[u8] {
        self.deref().data()
    }
    fn data_len(&self) -> usize {
        self.deref().data_len()
    }
    fn data_chunks(&self) -> DataChunks<'_> {
        self.deref().data_chunks()
    }
    fn owner(&self) -> &Pubkey {
        self.deref().owner()
    }
    fn executable(&self) -> bool {
        self.deref().executable()
    }
    fn rent_epoch(&self) -> Epoch {
        self.deref().rent_epoch()
    }
}

impl ReadableAccount for Account {
    fn lamports(&self) -> u64 {
        self.lamports
    }
    fn data(&self) -> &[u8] {
        &self.data
    }
    fn owner(&self) -> &Pubkey {
        &self.owner
    }
    fn executable(&self) -> bool {
        self.executable
    }
    fn rent_epoch(&self) -> Epoch {
        self.rent_epoch
    }
}

impl WritableAccount for Account {
    fn set_lamports(&mut self, lamports: u64) {
        self.lamports = lamports;
    }
    fn data_as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    fn set_owner(&mut self, owner: Pubkey) {
        self.owner = owner;
    }
    fn copy_into_owner_from_slice(&mut self, source: &[u8]) {
        self.owner.as_mut().copy_from_slice(source);
    }
    fn set_executable(&mut self, executable: bool) {
        self.executable = executable;
    }
    fn set_rent_epoch(&mut self, epoch: Epoch) {
        self.rent_epoch = epoch;
    }
}

impl WritableAccount for AccountSharedData {
    fn set_lamports(&mut self, lamports: u64) {
        self.lamports = lamports;
    }
    fn data_as_mut_slice(&mut self) -> &mut [u8] {
        self.data.as_mut_slice()
    }
    fn set_owner(&mut self, owner: Pubkey) {
        self.owner = owner;
    }
    fn copy_into_owner_from_slice(&mut self, source: &[u8]) {
        self.owner.as_mut().copy_from_slice(source);
    }
    fn set_executable(&mut self, executable: bool) {
        self.executable = executable;
    }
    fn set_rent_epoch(&mut self, epoch: Epoch) {
        self.rent_epoch = epoch;
    }
}

impl ReadableAccount for AccountSharedData {
    fn lamports(&self) -> u64 {
        self.lamports
    }
    fn data(&self) -> &[u8] {
        self.data.as_slice()
    }
    fn data_len(&self) -> usize {
        self.data.len()
    }
    fn data_chunks(&self) -> DataChunks<'_> {
        self.data.data_chunks()
    }
    fn owner(&self) -> &Pubkey {
        &self.owner
    }
    fn executable(&self) -> bool {
        self.executable
    }
    fn rent_epoch(&self) -> Epoch {
        self.rent_epoch
    }
}

fn debug_fmt<T: ReadableAccount>(item: &T, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut f = f.debug_struct("Account");

    f.field("lamports", &item.lamports())
        .field("data.len", &item.data().len())
        .field("owner", &item.owner())
        .field("executable", &item.executable())
        .field("rent_epoch", &item.rent_epoch());
    debug_account_data(item.data(), &mut f);

    f.finish()
}

impl fmt::Debug for Account {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_fmt(self, f)
    }
}

impl fmt::Debug for AccountSharedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug_fmt(self, f)
    }
}

#[cfg(feature = "bincode")]
fn shared_deserialize_data<T: serde::de::DeserializeOwned, U: ReadableAccount>(
    account: &U,
) -> Result<T, bincode::Error> {
    bincode::deserialize(account.data())
}

#[cfg(feature = "bincode")]
fn shared_serialize_data<T: serde::Serialize, U: WritableAccount>(
    account: &mut U,
    state: &T,
) -> Result<(), bincode::Error> {
    if bincode::serialized_size(state)? > account.data().len() as u64 {
        return Err(Box::new(bincode::ErrorKind::SizeLimit));
    }
    bincode::serialize_into(account.data_as_mut_slice(), state)
}

impl Account {
    pub fn new(lamports: u64, space: usize, owner: &Pubkey) -> Self {
        Account {
            lamports,
            data: vec![0; space],
            owner: *owner,
            executable: false,
            rent_epoch: Epoch::default(),
        }
    }
    pub fn new_ref(lamports: u64, space: usize, owner: &Pubkey) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Account::new(lamports, space, owner)))
    }
    #[cfg(feature = "bincode")]
    pub fn new_data<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        owner: &Pubkey,
    ) -> Result<Self, bincode::Error> {
        let data = bincode::serialize(state)?;
        Ok(Account {
            lamports,
            data,
            owner: *owner,
            executable: false,
            rent_epoch: Epoch::default(),
        })
    }
    #[cfg(feature = "bincode")]
    pub fn new_ref_data<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        owner: &Pubkey,
    ) -> Result<RefCell<Self>, bincode::Error> {
        Account::new_data(lamports, state, owner).map(RefCell::new)
    }
    #[cfg(feature = "bincode")]
    pub fn new_data_with_space<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        space: usize,
        owner: &Pubkey,
    ) -> Result<Self, bincode::Error> {
        let mut account = Account::new(lamports, space, owner);
        shared_serialize_data(&mut account, state)?;
        Ok(account)
    }
    #[cfg(feature = "bincode")]
    pub fn new_ref_data_with_space<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        space: usize,
        owner: &Pubkey,
    ) -> Result<RefCell<Self>, bincode::Error> {
        Account::new_data_with_space(lamports, state, space, owner).map(RefCell::new)
    }
    pub fn new_rent_epoch(lamports: u64, space: usize, owner: &Pubkey, rent_epoch: Epoch) -> Self {
        Account {
            lamports,
            data: vec![0; space],
            owner: *owner,
            executable: false,
            rent_epoch,
        }
    }
    #[cfg(feature = "bincode")]
    pub fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, bincode::Error> {
        shared_deserialize_data(self)
    }
    #[cfg(feature = "bincode")]
    pub fn serialize_data<T: serde::Serialize>(&mut self, state: &T) -> Result<(), bincode::Error> {
        shared_serialize_data(self, state)
    }
}

impl AccountSharedData {
    pub fn is_shared(&self) -> bool {
        self.data.is_shared()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional)
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn data_clone(&self) -> AccountData {
        self.data.clone()
    }

    pub fn resize(&mut self, new_len: usize, value: u8) {
        self.data.resize(new_len, value)
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data)
    }

    pub fn set_data_from_slice(&mut self, new_data: &[u8]) {
        self.data.set_data_from_slice(new_data)
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    #[cfg_attr(not(feature = "dev-context-only-utils"), allow(dead_code))]
    fn set_data(&mut self, data: Vec<u8>) {
        self.data = AccountData::from(data);
    }

    pub fn spare_data_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.data.spare_capacity_mut()
    }

    pub fn new(lamports: u64, space: usize, owner: &Pubkey) -> Self {
        AccountSharedData {
            lamports,
            data: AccountData::from(vec![0u8; space]),
            owner: *owner,
            executable: false,
            rent_epoch: Epoch::default(),
        }
    }
    pub fn new_ref(lamports: u64, space: usize, owner: &Pubkey) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(AccountSharedData::new(lamports, space, owner)))
    }
    #[cfg(feature = "bincode")]
    pub fn new_data<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        owner: &Pubkey,
    ) -> Result<Self, bincode::Error> {
        let data = bincode::serialize(state)?;
        Ok(Self::create_from_existing_shared_data(
            lamports,
            data,
            *owner,
            false,
            Epoch::default(),
        ))
    }
    #[cfg(feature = "bincode")]
    pub fn new_ref_data<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        owner: &Pubkey,
    ) -> Result<RefCell<Self>, bincode::Error> {
        AccountSharedData::new_data(lamports, state, owner).map(RefCell::new)
    }
    #[cfg(feature = "bincode")]
    pub fn new_data_with_space<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        space: usize,
        owner: &Pubkey,
    ) -> Result<Self, bincode::Error> {
        let mut account = AccountSharedData::new(lamports, space, owner);
        shared_serialize_data(&mut account, state)?;
        Ok(account)
    }
    #[cfg(feature = "bincode")]
    pub fn new_ref_data_with_space<T: serde::Serialize>(
        lamports: u64,
        state: &T,
        space: usize,
        owner: &Pubkey,
    ) -> Result<RefCell<Self>, bincode::Error> {
        AccountSharedData::new_data_with_space(lamports, state, space, owner).map(RefCell::new)
    }
    pub fn new_rent_epoch(lamports: u64, space: usize, owner: &Pubkey, rent_epoch: Epoch) -> Self {
        AccountSharedData {
            lamports,
            data: AccountData::from(vec![0; space]),
            owner: *owner,
            executable: false,
            rent_epoch,
        }
    }
    #[cfg(feature = "bincode")]
    pub fn deserialize_data<T: serde::de::DeserializeOwned>(&self) -> Result<T, bincode::Error> {
        shared_deserialize_data(self)
    }
    #[cfg(feature = "bincode")]
    pub fn serialize_data<T: serde::Serialize>(&mut self, state: &T) -> Result<(), bincode::Error> {
        shared_serialize_data(self, state)
    }

    pub fn create_from_existing_shared_data(
        lamports: u64,
        data: impl Into<AccountData>,
        owner: Pubkey,
        executable: bool,
        rent_epoch: Epoch,
    ) -> AccountSharedData {
        AccountSharedData {
            lamports,
            data: data.into(),
            owner,
            executable,
            rent_epoch,
        }
    }
}

pub type InheritableAccountFields = (u64, Epoch);
pub const DUMMY_INHERITABLE_ACCOUNT_FIELDS: InheritableAccountFields = (1, INITIAL_RENT_EPOCH);

#[cfg(feature = "bincode")]
pub fn create_account_with_fields<S: SysvarSerialize>(
    sysvar: &S,
    (lamports, rent_epoch): InheritableAccountFields,
) -> Account {
    let data_len = S::size_of().max(bincode::serialized_size(sysvar).unwrap() as usize);
    let mut account = Account::new(lamports, data_len, &solana_sdk_ids::sysvar::id());
    to_account::<S, Account>(sysvar, &mut account).unwrap();
    account.rent_epoch = rent_epoch;
    account
}

#[cfg(feature = "bincode")]
pub fn create_account_for_test<S: SysvarSerialize>(sysvar: &S) -> Account {
    create_account_with_fields(sysvar, DUMMY_INHERITABLE_ACCOUNT_FIELDS)
}

#[cfg(feature = "bincode")]
/// Create an `Account` from a `Sysvar`.
pub fn create_account_shared_data_with_fields<S: SysvarSerialize>(
    sysvar: &S,
    fields: InheritableAccountFields,
) -> AccountSharedData {
    AccountSharedData::from(create_account_with_fields(sysvar, fields))
}

#[cfg(feature = "bincode")]
pub fn create_account_shared_data_for_test<S: SysvarSerialize>(sysvar: &S) -> AccountSharedData {
    AccountSharedData::from(create_account_with_fields(
        sysvar,
        DUMMY_INHERITABLE_ACCOUNT_FIELDS,
    ))
}

#[cfg(feature = "bincode")]
/// Create a `Sysvar` from an `Account`'s data.
pub fn from_account<S: SysvarSerialize, T: ReadableAccount>(account: &T) -> Option<S> {
    bincode::deserialize(account.data()).ok()
}

#[cfg(feature = "bincode")]
/// Serialize a `Sysvar` into an `Account`'s data.
pub fn to_account<S: SysvarSerialize, T: WritableAccount>(
    sysvar: &S,
    account: &mut T,
) -> Option<()> {
    bincode::serialize_into(account.data_as_mut_slice(), sysvar).ok()
}

/// Return the information required to construct an `AccountInfo`.  Used by the
/// `AccountInfo` conversion implementations.
impl solana_account_info::Account for Account {
    fn get(&mut self) -> (&mut u64, &mut [u8], &Pubkey, bool) {
        (
            &mut self.lamports,
            &mut self.data,
            &self.owner,
            self.executable,
        )
    }
}

/// Create `AccountInfo`s
pub fn create_is_signer_account_infos<'a>(
    accounts: &'a mut [(&'a Pubkey, bool, &'a mut Account)],
) -> Vec<AccountInfo<'a>> {
    accounts
        .iter_mut()
        .map(|(key, is_signer, account)| {
            AccountInfo::new(
                key,
                *is_signer,
                false,
                &mut account.lamports,
                &mut account.data,
                &account.owner,
                account.executable,
            )
        })
        .collect()
}

/// Replacement for the executable flag: An account being owned by one of these contains a program.
#[deprecated(since = "4.3.0", note = "no longer available as a constant")]
pub const PROGRAM_OWNERS: &[Pubkey] = &[
    bpf_loader_upgradeable::id(),
    bpf_loader::id(),
    bpf_loader_deprecated::id(),
    loader_v4::id(),
];

#[cfg(test)]
pub mod tests {
    use super::*;

    fn fragmented(chunks: &[&[u8]]) -> AccountData {
        let account_data = AccountData::from_chunks_for_tests(chunks);
        assert!(matches!(account_data.repr, Repr::Fragmented(_)));
        account_data
    }

    #[test]
    fn test_fragmented_read() {
        let account_data = fragmented(&[b"hello", b" ", b"world"]);
        assert_eq!(account_data.len(), 11);
        assert!(!account_data.is_empty());
        assert_eq!(
            account_data.data_chunks().collect::<Vec<_>>(),
            [b"hello".as_slice(), b" ", b"world"]
        );
        // as_slice materializes (and caches) one contiguous copy
        assert_eq!(account_data.as_slice(), b"hello world");
        assert_eq!(
            account_data.as_slice().as_ptr(),
            account_data.as_slice().as_ptr()
        );
    }

    #[test]
    fn test_fragmented_single_segment_reads_without_materializing() {
        let account_data = fragmented(&[b"hello"]);
        let Repr::Fragmented(list) = &account_data.repr else {
            unreachable!()
        };
        assert_eq!(account_data.as_slice(), b"hello");
        assert!(list.contiguous.get().is_none());
    }

    #[test]
    fn test_fragmented_eq() {
        let contiguous = AccountData::from(b"hello world".to_vec());
        assert_eq!(fragmented(&[b"hello", b" ", b"world"]), contiguous);
        assert_eq!(contiguous, fragmented(&[b"hello", b" ", b"world"]));
        // same bytes, different chunk boundaries
        assert_eq!(
            fragmented(&[b"hel", b"lo world"]),
            fragmented(&[b"hello ", b"world"])
        );
        assert_ne!(
            fragmented(&[b"hello", b" ", "worlD".as_bytes()]),
            contiguous
        );
        // same length, difference within a chunk overlap
        assert_ne!(
            fragmented(&[b"hel", b"lo world"]),
            fragmented(&[b"hellO ", b"world"])
        );
        assert_ne!(
            fragmented(&[b"hello"]),
            AccountData::from(b"hello world".to_vec())
        );
    }

    #[test]
    fn test_fragmented_is_shared_and_mutation_materializes() {
        let account_data = fragmented(&[b"hello", b" ", b"world"]);
        // fragmented data always copies on mutation, so it reports shared
        assert!(account_data.is_shared());

        let mut resized = account_data.clone();
        resized.resize(13, b'!');
        assert!(matches!(resized.repr, Repr::Contiguous(_)));
        assert!(!resized.is_shared());
        assert_eq!(resized.as_slice(), b"hello world!!");

        let mut extended = account_data.clone();
        extended.extend_from_slice(b"!!");
        assert_eq!(extended.as_slice(), b"hello world!!");

        let mut mutated = account_data.clone();
        mutated.as_mut_slice()[0] = b'H';
        assert_eq!(mutated.as_slice(), b"Hello world");

        let mut replaced = account_data.clone();
        replaced.set_data_from_slice(b"bye");
        assert_eq!(replaced.as_slice(), b"bye");

        let mut reserved = account_data.clone();
        reserved.reserve(100);
        assert!(reserved.capacity() >= 111);
        assert_eq!(reserved.as_slice(), b"hello world");

        // the original is untouched throughout
        assert_eq!(account_data.as_slice(), b"hello world");
    }

    #[test]
    fn test_fragmented_ptr_eq() {
        let account_data = fragmented(&[b"hello", b" ", b"world"]);
        let clone = account_data.clone();
        assert!(account_data.ptr_eq(&clone));
        assert!(!account_data.ptr_eq(&fragmented(&[b"hello", b" ", b"world"])));
        // equal bytes, different representation
        let contiguous = AccountData::from(b"hello world".to_vec());
        assert!(!account_data.ptr_eq(&contiguous));
        assert_eq!(account_data, contiguous);
    }

    #[test]
    fn test_fragmented_into_vec() {
        let mut account = AccountSharedData::new(1, 0, &Pubkey::new_unique());
        account.data = fragmented(&[b"hello", b" ", b"world"]);
        let converted = Account::from(account);
        assert_eq!(converted.data, b"hello world");
    }

    fn make_two_accounts(key: &Pubkey) -> (Account, AccountSharedData) {
        let mut account1 = Account::new(1, 2, key);
        account1.executable = true;
        account1.rent_epoch = 4;
        let mut account2 = AccountSharedData::new(1, 2, key);
        account2.executable = true;
        account2.rent_epoch = 4;
        assert!(accounts_equal(&account1, &account2));
        (account1, account2)
    }

    #[test]
    fn test_account_data_copy_as_slice() {
        let key = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        let (mut account1, mut account2) = make_two_accounts(&key);
        account1.copy_into_owner_from_slice(key2.as_ref());
        account2.copy_into_owner_from_slice(key2.as_ref());
        assert!(accounts_equal(&account1, &account2));
        assert_eq!(account1.owner(), &key2);
    }

    #[test]
    fn test_account_set_data_from_slice() {
        let key = Pubkey::new_unique();
        let (_, mut account) = make_two_accounts(&key);
        assert_eq!(account.data(), &vec![0, 0]);
        account.set_data_from_slice(&[1, 2]);
        assert_eq!(account.data(), &vec![1, 2]);
        account.set_data_from_slice(&[1, 2, 3]);
        assert_eq!(account.data(), &vec![1, 2, 3]);
        account.set_data_from_slice(&[4, 5, 6]);
        assert_eq!(account.data(), &vec![4, 5, 6]);
        account.set_data_from_slice(&[4, 5, 6, 0]);
        assert_eq!(account.data(), &vec![4, 5, 6, 0]);
        account.set_data_from_slice(&[]);
        assert_eq!(account.data().len(), 0);
        account.set_data_from_slice(&[44]);
        assert_eq!(account.data(), &vec![44]);
        account.set_data_from_slice(&[44]);
        assert_eq!(account.data(), &vec![44]);
    }

    #[test]
    fn test_account_data_set_data() {
        let key = Pubkey::new_unique();
        let (_, mut account) = make_two_accounts(&key);
        assert_eq!(account.data(), &vec![0, 0]);
        account.set_data(vec![1, 2]);
        assert_eq!(account.data(), &vec![1, 2]);
        account.set_data(vec![]);
        assert_eq!(account.data().len(), 0);
    }

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: Io(Kind(UnexpectedEof))"
    )]
    fn test_account_deserialize() {
        let key = Pubkey::new_unique();
        let (account1, _account2) = make_two_accounts(&key);
        account1.deserialize_data::<String>().unwrap();
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()` on an `Err` value: SizeLimit")]
    fn test_account_serialize() {
        let key = Pubkey::new_unique();
        let (mut account1, _account2) = make_two_accounts(&key);
        account1.serialize_data(&"hello world").unwrap();
    }

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: Io(Kind(UnexpectedEof))"
    )]
    fn test_account_shared_data_deserialize() {
        let key = Pubkey::new_unique();
        let (_account1, account2) = make_two_accounts(&key);
        account2.deserialize_data::<String>().unwrap();
    }

    #[test]
    #[should_panic(expected = "called `Result::unwrap()` on an `Err` value: SizeLimit")]
    fn test_account_shared_data_serialize() {
        let key = Pubkey::new_unique();
        let (_account1, mut account2) = make_two_accounts(&key);
        account2.serialize_data(&"hello world").unwrap();
    }

    #[test]
    fn test_account_shared_data() {
        let key = Pubkey::new_unique();
        let (account1, account2) = make_two_accounts(&key);
        assert!(accounts_equal(&account1, &account2));
        let account = account1;
        assert_eq!(account.lamports, 1);
        assert_eq!(account.lamports(), 1);
        assert_eq!(account.data.len(), 2);
        assert_eq!(account.data().len(), 2);
        assert_eq!(account.owner, key);
        assert_eq!(account.owner(), &key);
        assert!(account.executable);
        assert!(account.executable());
        assert_eq!(account.rent_epoch, 4);
        assert_eq!(account.rent_epoch(), 4);
        let account = account2;
        assert_eq!(account.lamports, 1);
        assert_eq!(account.lamports(), 1);
        assert_eq!(account.data.len(), 2);
        assert_eq!(account.data().len(), 2);
        assert_eq!(account.owner, key);
        assert_eq!(account.owner(), &key);
        assert!(account.executable);
        assert!(account.executable());
        assert_eq!(account.rent_epoch, 4);
        assert_eq!(account.rent_epoch(), 4);
    }

    // test clone and from for both types against expected
    fn test_equal(
        should_be_equal: bool,
        account1: &Account,
        account2: &AccountSharedData,
        account_expected: &Account,
    ) {
        assert_eq!(should_be_equal, accounts_equal(account1, account2));
        if should_be_equal {
            assert!(accounts_equal(account_expected, account2));
        }
        assert_eq!(
            accounts_equal(account_expected, account1),
            accounts_equal(account_expected, &account1.clone())
        );
        assert_eq!(
            accounts_equal(account_expected, account2),
            accounts_equal(account_expected, &account2.clone())
        );
        assert_eq!(
            accounts_equal(account_expected, account1),
            accounts_equal(account_expected, &AccountSharedData::from(account1.clone()))
        );
        assert_eq!(
            accounts_equal(account_expected, account2),
            accounts_equal(account_expected, &Account::from(account2.clone()))
        );
    }

    #[test]
    fn test_account_add_sub_lamports() {
        let key = Pubkey::new_unique();
        let (mut account1, mut account2) = make_two_accounts(&key);
        assert!(accounts_equal(&account1, &account2));
        account1.checked_add_lamports(1).unwrap();
        account2.checked_add_lamports(1).unwrap();
        assert!(accounts_equal(&account1, &account2));
        assert_eq!(account1.lamports(), 2);
        account1.checked_sub_lamports(2).unwrap();
        account2.checked_sub_lamports(2).unwrap();
        assert!(accounts_equal(&account1, &account2));
        assert_eq!(account1.lamports(), 0);
    }

    #[test]
    #[should_panic(expected = "Overflow")]
    fn test_account_checked_add_lamports_overflow() {
        let key = Pubkey::new_unique();
        let (mut account1, _account2) = make_two_accounts(&key);
        account1.checked_add_lamports(u64::MAX).unwrap();
    }

    #[test]
    #[should_panic(expected = "Underflow")]
    fn test_account_checked_sub_lamports_underflow() {
        let key = Pubkey::new_unique();
        let (mut account1, _account2) = make_two_accounts(&key);
        account1.checked_sub_lamports(u64::MAX).unwrap();
    }

    #[test]
    #[should_panic(expected = "Overflow")]
    fn test_account_checked_add_lamports_overflow2() {
        let key = Pubkey::new_unique();
        let (_account1, mut account2) = make_two_accounts(&key);
        account2.checked_add_lamports(u64::MAX).unwrap();
    }

    #[test]
    #[should_panic(expected = "Underflow")]
    fn test_account_checked_sub_lamports_underflow2() {
        let key = Pubkey::new_unique();
        let (_account1, mut account2) = make_two_accounts(&key);
        account2.checked_sub_lamports(u64::MAX).unwrap();
    }

    #[test]
    fn test_account_saturating_add_lamports() {
        let key = Pubkey::new_unique();
        let (mut account, _) = make_two_accounts(&key);

        let remaining = 22;
        account.set_lamports(u64::MAX - remaining);
        account.saturating_add_lamports(remaining * 2);
        assert_eq!(account.lamports(), u64::MAX);
    }

    #[test]
    fn test_account_saturating_sub_lamports() {
        let key = Pubkey::new_unique();
        let (mut account, _) = make_two_accounts(&key);

        let remaining = 33;
        account.set_lamports(remaining);
        account.saturating_sub_lamports(remaining * 2);
        assert_eq!(account.lamports(), 0);
    }

    #[test]
    fn test_account_shared_data_all_fields() {
        let key = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        let key3 = Pubkey::new_unique();
        let (mut account1, mut account2) = make_two_accounts(&key);
        assert!(accounts_equal(&account1, &account2));

        let mut account_expected = account1.clone();
        assert!(accounts_equal(&account1, &account_expected));
        assert!(accounts_equal(&account1, &account2.clone())); // test the clone here

        for field_index in 0..5 {
            for pass in 0..4 {
                if field_index == 0 {
                    if pass == 0 {
                        account1.checked_add_lamports(1).unwrap();
                    } else if pass == 1 {
                        account_expected.checked_add_lamports(1).unwrap();
                        account2.set_lamports(account2.lamports + 1);
                    } else if pass == 2 {
                        account1.set_lamports(account1.lamports + 1);
                    } else if pass == 3 {
                        account_expected.checked_add_lamports(1).unwrap();
                        account2.checked_add_lamports(1).unwrap();
                    }
                } else if field_index == 1 {
                    if pass == 0 {
                        account1.data[0] += 1;
                    } else if pass == 1 {
                        account_expected.data[0] += 1;
                        account2.data_as_mut_slice()[0] = account2.data()[0] + 1;
                    } else if pass == 2 {
                        account1.data_as_mut_slice()[0] = account1.data[0] + 1;
                    } else if pass == 3 {
                        account_expected.data[0] += 1;
                        account2.data_as_mut_slice()[0] += 1;
                    }
                } else if field_index == 2 {
                    if pass == 0 {
                        account1.owner = key2;
                    } else if pass == 1 {
                        account_expected.owner = key2;
                        account2.set_owner(key2);
                    } else if pass == 2 {
                        account1.set_owner(key3);
                    } else if pass == 3 {
                        account_expected.owner = key3;
                        account2.owner = key3;
                    }
                } else if field_index == 3 {
                    if pass == 0 {
                        account1.executable = !account1.executable;
                    } else if pass == 1 {
                        account_expected.executable = !account_expected.executable;
                        account2.set_executable(!account2.executable);
                    } else if pass == 2 {
                        account1.set_executable(!account1.executable);
                    } else if pass == 3 {
                        account_expected.executable = !account_expected.executable;
                        account2.executable = !account2.executable;
                    }
                } else if field_index == 4 {
                    if pass == 0 {
                        account1.rent_epoch += 1;
                    } else if pass == 1 {
                        account_expected.rent_epoch += 1;
                        account2.set_rent_epoch(account2.rent_epoch + 1);
                    } else if pass == 2 {
                        account1.set_rent_epoch(account1.rent_epoch + 1);
                    } else if pass == 3 {
                        account_expected.rent_epoch += 1;
                        account2.rent_epoch += 1;
                    }
                }

                let should_be_equal = pass == 1 || pass == 3;
                test_equal(should_be_equal, &account1, &account2, &account_expected);

                // test new_ref
                if should_be_equal {
                    assert!(accounts_equal(
                        &Account::new_ref(
                            account_expected.lamports(),
                            account_expected.data().len(),
                            account_expected.owner()
                        )
                        .borrow(),
                        &AccountSharedData::new_ref(
                            account_expected.lamports(),
                            account_expected.data().len(),
                            account_expected.owner()
                        )
                        .borrow()
                    ));

                    {
                        // test new_data
                        let account1_with_data = Account::new_data(
                            account_expected.lamports(),
                            &account_expected.data()[0],
                            account_expected.owner(),
                        )
                        .unwrap();
                        let account2_with_data = AccountSharedData::new_data(
                            account_expected.lamports(),
                            &account_expected.data()[0],
                            account_expected.owner(),
                        )
                        .unwrap();

                        assert!(accounts_equal(&account1_with_data, &account2_with_data));
                        assert_eq!(
                            account1_with_data.deserialize_data::<u8>().unwrap(),
                            account2_with_data.deserialize_data::<u8>().unwrap()
                        );
                    }

                    // test new_data_with_space
                    assert!(accounts_equal(
                        &Account::new_data_with_space(
                            account_expected.lamports(),
                            &account_expected.data()[0],
                            1,
                            account_expected.owner()
                        )
                        .unwrap(),
                        &AccountSharedData::new_data_with_space(
                            account_expected.lamports(),
                            &account_expected.data()[0],
                            1,
                            account_expected.owner()
                        )
                        .unwrap()
                    ));

                    // test new_ref_data
                    assert!(accounts_equal(
                        &Account::new_ref_data(
                            account_expected.lamports(),
                            &account_expected.data()[0],
                            account_expected.owner()
                        )
                        .unwrap()
                        .borrow(),
                        &AccountSharedData::new_ref_data(
                            account_expected.lamports(),
                            &account_expected.data()[0],
                            account_expected.owner()
                        )
                        .unwrap()
                        .borrow()
                    ));

                    //new_ref_data_with_space
                    assert!(accounts_equal(
                        &Account::new_ref_data_with_space(
                            account_expected.lamports(),
                            &account_expected.data()[0],
                            1,
                            account_expected.owner()
                        )
                        .unwrap()
                        .borrow(),
                        &AccountSharedData::new_ref_data_with_space(
                            account_expected.lamports(),
                            &account_expected.data()[0],
                            1,
                            account_expected.owner()
                        )
                        .unwrap()
                        .borrow()
                    ));
                }
            }
        }
    }
}
