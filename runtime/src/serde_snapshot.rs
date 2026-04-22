#[cfg(all(target_os = "linux", target_env = "gnu"))]
use std::{
    ffi::{CStr, CString},
    path::Path,
};
use {
    crate::{
        bank::{Bank, BankFieldsToDeserialize, BankFieldsToSerialize, BankHashStats, BankRc},
        epoch_stakes::{DeserializableVersionedEpochStakes, VersionedEpochStakes},
        runtime_config::RuntimeConfig,
        stake_account::StakeAccount,
        stakes::{DeserializableStakes, Stakes, serialize_stake_accounts_to_delegation_format},
    },
    agave_fs::FileInfo,
    agave_snapshots::error::SnapshotError,
    bincode::{self, Error, config::Options},
    dashmap::DashMap,
    log::*,
    serde::{Deserialize, Serialize, de::DeserializeOwned},
    smallvec::SmallVec,
    solana_accounts_db::{
        ObsoleteAccounts,
        account_storage_entry::AccountStorageEntry,
        accounts::Accounts,
        accounts_db::{
            AccountsDb, AccountsDbConfig, AccountsFileId, AtomicAccountsFileId, IndexGenerationInfo,
        },
        accounts_file::{AccountsFile, StorageAccess},
        accounts_hash::AccountsLtHash,
        accounts_update_notifier_interface::AccountsUpdateNotifier,
        blockhash_queue::BlockhashQueue,
        reconstruct_single_storage,
    },
    solana_clock::{Epoch, Slot, UnixTimestamp},
    solana_epoch_schedule::EpochSchedule,
    solana_fee_calculator::FeeRateGovernor,
    solana_genesis_config::GenesisConfig,
    solana_hard_forks::HardForks,
    solana_hash::Hash,
    solana_inflation::Inflation,
    solana_lattice_hash::lt_hash::LtHash,
    solana_leader_schedule::SlotLeader,
    solana_pubkey::Pubkey,
    solana_serde::default_on_eof,
    solana_stake_interface::state::Delegation,
    std::{
        collections::{HashMap, HashSet},
        io::{self, BufReader, Read, Write},
        path::PathBuf,
        result::Result,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread,
        time::Instant,
    },
    types::{SerdeAccountsLtHash, UnusedRentCollector},
    wincode::{SchemaReadOwned, SchemaWrite, io::std_write::WriteAdapter},
};

mod obsolete_accounts;
mod status_cache;
mod storage;
mod tests;
mod types;
mod utils;

pub(crate) use {
    obsolete_accounts::{SerdeObsoleteAccounts, SerdeObsoleteAccountsMap},
    status_cache::{deserialize_status_cache, serialize_status_cache},
    storage::{SerializableAccountStorageEntry, SerializedAccountsFileId},
};

const MAX_STREAM_SIZE: usize = 32 * 1024 * 1024 * 1024;
type MaxStreamSizeConfig = wincode::config::Configuration<true, MAX_STREAM_SIZE>;

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub(crate) struct AccountsDbFields<T>(
    Vec<(Slot, SmallVec<[T; 1]>)>,
    u64, // unused, formerly write_version
    Slot,
    BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    Vec<(Slot, Hash)>,
);

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "dev-context-only-utils", derive(Default, PartialEq))]
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UnusedIncrementalSnapshotPersistence {
    pub full_slot: u64,
    pub full_hash: [u8; 32],
    pub full_capitalization: u64,
    pub incremental_hash: [u8; 32],
    pub incremental_capitalization: u64,
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BankHashInfo {
    _unused_accounts_delta_hash: [u8; 32],
    _unused_accounts_hash: [u8; 32],
    stats: BankHashStats,
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Default, Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
struct UnusedAccounts {
    unused1: HashSet<Pubkey>,
    unused2: HashSet<Pubkey>,
    unused3: HashMap<Pubkey, u64>,
}

// Deserializable version of Bank which need not be serializable,
// because it's handled by SerializableVersionedBank.
// So, sync fields with it!
#[derive(Clone, Deserialize)]
struct DeserializableVersionedBank {
    blockhash_queue: BlockhashQueue,
    _unused_ancestors: HashMap<Slot, usize>,
    hash: Hash,
    parent_hash: Hash,
    parent_slot: Slot,
    hard_forks: HardForks,
    transaction_count: u64,
    tick_height: u64,
    signature_count: u64,
    capitalization: u64,
    max_tick_height: u64,
    hashes_per_tick: Option<u64>,
    ticks_per_slot: u64,
    ns_per_slot: u128,
    genesis_creation_time: UnixTimestamp,
    slots_per_year: f64,
    accounts_data_len: u64,
    slot: Slot,
    _unused_epoch: Epoch,
    block_height: u64,
    leader_id: Pubkey,
    _unused_collector_fees: u64,
    _unused_fee_calculator: u64,
    fee_rate_governor: FeeRateGovernor,
    _unused_collected_rent: u64,
    _unused_rent_collector: UnusedRentCollector,
    epoch_schedule: EpochSchedule,
    inflation: Inflation,
    stakes: DeserializableStakes<Delegation>,
    _unused_accounts: UnusedAccounts,
    unused_epoch_stakes: HashMap<Epoch, ()>,
    is_delta: bool,
}

impl From<DeserializableVersionedBank> for BankFieldsToDeserialize {
    fn from(dvb: DeserializableVersionedBank) -> Self {
        // This serves as a canary for the LtHash.
        // If it is not replaced during deserialization, it indicates a bug.
        const LT_HASH_CANARY: LtHash = LtHash([0xCAFE; LtHash::NUM_ELEMENTS]);
        BankFieldsToDeserialize {
            blockhash_queue: dvb.blockhash_queue,
            hash: dvb.hash,
            parent_hash: dvb.parent_hash,
            parent_slot: dvb.parent_slot,
            hard_forks: dvb.hard_forks,
            transaction_count: dvb.transaction_count,
            tick_height: dvb.tick_height,
            signature_count: dvb.signature_count,
            capitalization: dvb.capitalization,
            max_tick_height: dvb.max_tick_height,
            hashes_per_tick: dvb.hashes_per_tick,
            ticks_per_slot: dvb.ticks_per_slot,
            ns_per_slot: dvb.ns_per_slot,
            genesis_creation_time: dvb.genesis_creation_time,
            slots_per_year: dvb.slots_per_year,
            accounts_data_len: dvb.accounts_data_len,
            slot: dvb.slot,
            block_height: dvb.block_height,
            leader_id: dvb.leader_id,
            fee_rate_governor: dvb.fee_rate_governor,
            epoch_schedule: dvb.epoch_schedule,
            inflation: dvb.inflation,
            stakes: dvb.stakes,
            is_delta: dvb.is_delta,
            versioned_epoch_stakes: vec![], // populated from ExtraFieldsToDeserialize
            accounts_lt_hash: AccountsLtHash(LT_HASH_CANARY), // populated from ExtraFieldsToDeserialize
            block_id: None, // populated from ExtraFieldsToDeserialize
        }
    }
}

// Serializable version of Bank, not Deserializable to avoid cloning by using refs.
// Sync fields with DeserializableVersionedBank!
#[derive(Serialize)]
struct SerializableVersionedBank {
    blockhash_queue: BlockhashQueue,
    unused_ancestors: HashMap<Slot, usize>,
    hash: Hash,
    parent_hash: Hash,
    parent_slot: Slot,
    hard_forks: HardForks,
    transaction_count: u64,
    tick_height: u64,
    signature_count: u64,
    capitalization: u64,
    max_tick_height: u64,
    hashes_per_tick: Option<u64>,
    ticks_per_slot: u64,
    ns_per_slot: u128,
    genesis_creation_time: UnixTimestamp,
    slots_per_year: f64,
    accounts_data_len: u64,
    slot: Slot,
    unused_epoch: Epoch,
    block_height: u64,
    leader_id: Pubkey,
    unused_collector_fees: u64,
    unused_fee_calculator: u64,
    fee_rate_governor: FeeRateGovernor,
    unused_collected_rent: u64,
    unused_rent_collector: UnusedRentCollector,
    epoch_schedule: EpochSchedule,
    inflation: Inflation,
    #[serde(serialize_with = "serialize_stake_accounts_to_delegation_format")]
    stakes: Stakes<StakeAccount<Delegation>>,
    unused_accounts: UnusedAccounts,
    unused_epoch_stakes: HashMap<Epoch, ()>,
    is_delta: bool,
}

impl From<BankFieldsToSerialize> for SerializableVersionedBank {
    fn from(rhs: BankFieldsToSerialize) -> Self {
        Self {
            blockhash_queue: rhs.blockhash_queue,
            unused_ancestors: HashMap::default(),
            hash: rhs.hash,
            parent_hash: rhs.parent_hash,
            parent_slot: rhs.parent_slot,
            hard_forks: rhs.hard_forks,
            transaction_count: rhs.transaction_count,
            tick_height: rhs.tick_height,
            signature_count: rhs.signature_count,
            capitalization: rhs.capitalization,
            max_tick_height: rhs.max_tick_height,
            hashes_per_tick: rhs.hashes_per_tick,
            ticks_per_slot: rhs.ticks_per_slot,
            ns_per_slot: rhs.ns_per_slot,
            genesis_creation_time: rhs.genesis_creation_time,
            slots_per_year: rhs.slots_per_year,
            accounts_data_len: rhs.accounts_data_len,
            slot: rhs.slot,
            unused_epoch: 0,
            block_height: rhs.block_height,
            leader_id: rhs.leader_id,
            unused_collector_fees: 0,
            unused_fee_calculator: 0,
            fee_rate_governor: rhs.fee_rate_governor,
            unused_collected_rent: u64::default(),
            unused_rent_collector: UnusedRentCollector::zeroed(),
            epoch_schedule: rhs.epoch_schedule,
            inflation: rhs.inflation,
            stakes: rhs.stakes,
            unused_accounts: UnusedAccounts::default(),
            unused_epoch_stakes: HashMap::default(),
            is_delta: rhs.is_delta,
        }
    }
}

#[cfg(feature = "frozen-abi")]
impl solana_frozen_abi::abi_example::TransparentAsHelper for SerializableVersionedBank {}

/// Helper type to wrap BufReader streams when deserializing and reconstructing from either just a
/// full snapshot, or both a full and incremental snapshot
pub struct SnapshotStreams<'a, R> {
    pub full_snapshot_stream: &'a mut BufReader<R>,
    pub incremental_snapshot_stream: Option<&'a mut BufReader<R>>,
}

/// Helper type to wrap BankFields when reconstructing Bank from either just a full
/// snapshot, or both a full and incremental snapshot
#[derive(Debug)]
pub struct SnapshotBankFields {
    full: BankFieldsToDeserialize,
    incremental: Option<BankFieldsToDeserialize>,
}

impl SnapshotBankFields {
    pub fn new(
        full: BankFieldsToDeserialize,
        incremental: Option<BankFieldsToDeserialize>,
    ) -> Self {
        Self { full, incremental }
    }

    /// Collapse the SnapshotBankFields into a single (the latest) BankFieldsToDeserialize.
    pub fn collapse_into(self) -> BankFieldsToDeserialize {
        self.incremental.unwrap_or(self.full)
    }
}

pub(crate) fn serialize_into<W, T>(writer: W, value: &T) -> wincode::WriteResult<()>
where
    W: Write,
    T: SchemaWrite<MaxStreamSizeConfig, Src = T>,
{
    wincode::config::serialize_into(WriteAdapter::new(writer), value, MaxStreamSizeConfig::new())
}

pub(crate) fn deserialize_wincode_from<R, T>(reader: R) -> wincode::ReadResult<T>
where
    R: Read,
    T: SchemaReadOwned<MaxStreamSizeConfig, Dst = T>,
{
    wincode::config::deserialize_from(io::BufReader::new(reader), MaxStreamSizeConfig::new())
}

pub(crate) fn deserialize_from<R, T>(reader: R) -> bincode::Result<T>
where
    R: Read,
    T: DeserializeOwned,
{
    bincode::options()
        .with_limit(MAX_STREAM_SIZE as u64)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from::<R, T>(reader)
}

fn deserialize_accounts_db_fields<R>(
    stream: &mut BufReader<R>,
) -> Result<AccountsDbFields<SerializableAccountStorageEntry>, Error>
where
    R: Read,
{
    deserialize_from::<_, _>(stream)
}

/// Extra fields that are deserialized from the end of snapshots.
///
/// Note that this struct's fields should stay synced with the fields in
/// ExtraFieldsToSerialize with the exception that new "extra fields" should be
/// added to this struct a minor release before they are added to the serialize
/// struct.
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Clone, Debug, Deserialize)]
struct ExtraFieldsToDeserialize {
    #[serde(deserialize_with = "default_on_eof")]
    lamports_per_signature: u64,
    #[serde(deserialize_with = "default_on_eof")]
    _unused_incremental_snapshot_persistence: Option<UnusedIncrementalSnapshotPersistence>,
    #[serde(deserialize_with = "default_on_eof")]
    _unused_epoch_accounts_hash: Option<Hash>,
    #[serde(deserialize_with = "default_on_eof")]
    versioned_epoch_stakes: Vec<(u64, DeserializableVersionedEpochStakes)>,
    #[serde(deserialize_with = "default_on_eof")]
    accounts_lt_hash: Option<SerdeAccountsLtHash>,
    #[serde(deserialize_with = "default_on_eof")]
    block_id: Option<Hash>,
}

/// Extra fields that are serialized at the end of snapshots.
///
/// Note that this struct's fields should stay synced with the fields in
/// ExtraFieldsToDeserialize with the exception that new "extra fields" should
/// be added to the deserialize struct a minor release before they are added to
/// this one.
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "dev-context-only-utils", derive(Default, PartialEq))]
#[derive(Debug, Serialize)]
pub struct ExtraFieldsToSerialize {
    pub lamports_per_signature: u64,
    pub unused_incremental_snapshot_persistence: Option<UnusedIncrementalSnapshotPersistence>,
    pub unused_epoch_accounts_hash: Option<Hash>,
    pub versioned_epoch_stakes: HashMap<u64, VersionedEpochStakes>,
    pub accounts_lt_hash: Option<SerdeAccountsLtHash>,
    pub block_id: Option<Hash>,
}

fn deserialize_bank_fields<R>(
    mut stream: &mut BufReader<R>,
) -> Result<BankFieldsToDeserialize, Error>
where
    R: Read,
{
    let deserializable_bank = deserialize_from::<_, DeserializableVersionedBank>(&mut stream)?;
    if !deserializable_bank.unused_epoch_stakes.is_empty() {
        return Err(Box::new(bincode::ErrorKind::Custom(
            "Expected deserialized bank's unused_epoch_stakes field to be empty".to_string(),
        )));
    }
    let mut bank_fields = BankFieldsToDeserialize::from(deserializable_bank);
    // Must deserialize to advance the stream past accounts db fields before reading extra fields.
    let _accounts_db_fields = deserialize_accounts_db_fields(stream)?;
    let extra_fields = deserialize_from(stream)?;

    let ExtraFieldsToDeserialize {
        lamports_per_signature,
        _unused_incremental_snapshot_persistence,
        _unused_epoch_accounts_hash,
        versioned_epoch_stakes,
        accounts_lt_hash,
        block_id,
    } = extra_fields;

    bank_fields.fee_rate_governor = bank_fields
        .fee_rate_governor
        .clone_with_lamports_per_signature(lamports_per_signature);
    bank_fields.versioned_epoch_stakes = versioned_epoch_stakes;
    bank_fields.accounts_lt_hash = accounts_lt_hash
        .expect("snapshot must have accounts_lt_hash")
        .into();
    bank_fields.block_id = block_id;

    Ok(bank_fields)
}

pub(crate) fn fields_from_stream<R: Read>(
    snapshot_stream: &mut BufReader<R>,
) -> std::result::Result<BankFieldsToDeserialize, Error> {
    deserialize_bank_fields(snapshot_stream)
}

#[cfg(feature = "dev-context-only-utils")]
pub(crate) fn fields_from_streams(
    snapshot_streams: &mut SnapshotStreams<impl Read>,
) -> std::result::Result<SnapshotBankFields, Error> {
    let full_snapshot_bank_fields = fields_from_stream(snapshot_streams.full_snapshot_stream)?;
    let incremental_snapshot_bank_fields = snapshot_streams
        .incremental_snapshot_stream
        .as_mut()
        .map(|stream| fields_from_stream(stream))
        .transpose()?;

    Ok(SnapshotBankFields {
        full: full_snapshot_bank_fields,
        incremental: incremental_snapshot_bank_fields,
    })
}

/// This struct contains side-info while reconstructing the bank from streams
#[derive(Debug)]
pub struct BankFromStreamsInfo {
    /// The accounts lt hash calculated during index generation.
    /// Will be used when verifying accounts, after rebuilding a Bank.
    pub calculated_accounts_lt_hash: AccountsLtHash,
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) fn bank_from_streams<R>(
    snapshot_streams: &mut SnapshotStreams<R>,
    account_paths: &[PathBuf],
    genesis_config: &GenesisConfig,
    runtime_config: &RuntimeConfig,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    limit_load_slot_count_from_snapshot: Option<usize>,
    verify_index: bool,
    accounts_db_config: AccountsDbConfig,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    exit: Arc<AtomicBool>,
) -> std::result::Result<(Bank, BankFromStreamsInfo), Error>
where
    R: Read,
{
    let bank_fields = fields_from_streams(snapshot_streams)?;
    let (bank, info) = reconstruct_bank_from_fields(
        bank_fields,
        genesis_config,
        runtime_config,
        account_paths,
        None, // obsolete_accounts — archive path has none
        debug_keys,
        None, // leader_for_tests
        limit_load_slot_count_from_snapshot,
        verify_index,
        accounts_db_config,
        accounts_update_notifier,
        exit,
    )?;
    Ok((
        bank,
        BankFromStreamsInfo {
            calculated_accounts_lt_hash: info.calculated_accounts_lt_hash,
        },
    ))
}

#[cfg(test)]
pub(crate) fn bank_to_stream<W>(stream: &mut io::BufWriter<W>, bank: &Bank) -> Result<(), Error>
where
    W: Write,
{
    bincode::serialize_into(stream, &SerializableBankAndStorage { bank })
}

/// Serializes bank snapshot into `stream` with bincode
pub fn serialize_bank_snapshot_into(
    stream: &mut dyn Write,
    bank_fields: BankFieldsToSerialize,
    bank_hash_stats: BankHashStats,
    extra_fields: ExtraFieldsToSerialize,
) -> Result<(), Error> {
    let mut serializer = bincode::Serializer::new(
        stream,
        bincode::DefaultOptions::new().with_fixint_encoding(),
    );
    serialize_bank_snapshot_with(&mut serializer, bank_fields, bank_hash_stats, extra_fields)
}

/// Serializes bank snapshot with `serializer`
pub fn serialize_bank_snapshot_with<S>(
    serializer: S,
    bank_fields: BankFieldsToSerialize,
    bank_hash_stats: BankHashStats,
    extra_fields: ExtraFieldsToSerialize,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let slot = bank_fields.slot;
    let serializable_bank = SerializableVersionedBank::from(bank_fields);
    let serializable_accounts_db = SerializableAccountsDb {
        slot,
        bank_hash_stats,
    };
    (serializable_bank, serializable_accounts_db, extra_fields).serialize(serializer)
}

#[cfg(test)]
struct SerializableBankAndStorage<'a> {
    bank: &'a Bank,
}

#[cfg(test)]
impl Serialize for SerializableBankAndStorage<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let slot = self.bank.slot();
        let mut bank_fields = self.bank.get_fields_to_serialize();
        let bank_hash_stats = self.bank.get_bank_hash_stats();
        let lamports_per_signature = bank_fields.fee_rate_governor.lamports_per_signature;
        let versioned_epoch_stakes = std::mem::take(&mut bank_fields.versioned_epoch_stakes);
        let accounts_lt_hash = Some(bank_fields.accounts_lt_hash.clone().into());
        let block_id = Some(bank_fields.block_id);
        (
            SerializableVersionedBank::from(bank_fields),
            SerializableAccountsDb {
                slot,
                bank_hash_stats,
            },
            ExtraFieldsToSerialize {
                lamports_per_signature,
                unused_incremental_snapshot_persistence: None,
                unused_epoch_accounts_hash: None,
                versioned_epoch_stakes,
                accounts_lt_hash,
                block_id,
            },
        )
            .serialize(serializer)
    }
}

#[cfg(test)]
struct SerializableBankAndStorageNoExtra<'a> {
    bank: &'a Bank,
}

#[cfg(test)]
impl Serialize for SerializableBankAndStorageNoExtra<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let slot = self.bank.slot();
        let bank_fields = self.bank.get_fields_to_serialize();
        let bank_hash_stats = self.bank.get_bank_hash_stats();
        (
            SerializableVersionedBank::from(bank_fields),
            SerializableAccountsDb {
                slot,
                bank_hash_stats,
            },
        )
            .serialize(serializer)
    }
}

#[cfg(test)]
impl<'a> From<SerializableBankAndStorageNoExtra<'a>> for SerializableBankAndStorage<'a> {
    fn from(s: SerializableBankAndStorageNoExtra<'a>) -> SerializableBankAndStorage<'a> {
        SerializableBankAndStorage { bank: s.bank }
    }
}

struct SerializableAccountsDb {
    slot: Slot,
    bank_hash_stats: BankHashStats,
}

impl Serialize for SerializableAccountsDb {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        // Always serialize an empty storage map — the load path scans account_paths directly.
        let entries = utils::serialize_iter_as_map(std::iter::empty::<(Slot, ())>());
        let bank_hash_info = BankHashInfo {
            _unused_accounts_delta_hash: [0; 32],
            _unused_accounts_hash: [0; 32],
            stats: self.bank_hash_stats.clone(),
        };
        let historical_roots = Vec::<Slot>::default();
        let historical_roots_with_hash = Vec::<(Slot, Hash)>::default();
        (
            entries,
            0u64, // unused, formerly write_version
            self.slot,
            bank_hash_info,
            historical_roots,
            historical_roots_with_hash,
        )
            .serialize(serializer)
    }
}

#[cfg(feature = "frozen-abi")]
impl solana_frozen_abi::abi_example::TransparentAsHelper for SerializableAccountsDb {}

/// This struct contains side-info while reconstructing the bank from fields
#[derive(Debug)]
pub(crate) struct ReconstructedBankInfo {
    /// The accounts lt hash calculated during index generation.
    /// Will be used when verifying accounts, after rebuilding a Bank.
    pub(crate) calculated_accounts_lt_hash: AccountsLtHash,
    /// The capitalization, in lamports, calculated during index generation.
    pub(crate) calculated_capitalization: u64,
}

#[expect(clippy::too_many_arguments)]
pub(crate) fn reconstruct_bank_from_fields(
    bank_fields: SnapshotBankFields,
    genesis_config: &GenesisConfig,
    runtime_config: &RuntimeConfig,
    account_paths: &[PathBuf],
    obsolete_accounts: Option<DashMap<Slot, (ObsoleteAccounts, AccountsFileId)>>,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    leader_for_tests: Option<SlotLeader>,
    limit_load_slot_count_from_snapshot: Option<usize>,
    verify_index: bool,
    accounts_db_config: AccountsDbConfig,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    exit: Arc<AtomicBool>,
) -> Result<(Bank, ReconstructedBankInfo), Error> {
    let mut bank_fields = bank_fields.collapse_into();
    // Epoch stakes take several seconds to reconstruct, do it in parallel with loading accountsdb
    let deserializable_epoch_stakes = std::mem::take(&mut bank_fields.versioned_epoch_stakes);
    let epoch_stakes_handle = thread::Builder::new()
        .name("solRctEpochStk".into())
        .spawn(|| {
            deserializable_epoch_stakes
                .into_iter()
                .map(|(epoch, stakes)| (epoch, stakes.into()))
                .collect()
        })?;
    let (accounts_db, reconstructed_accounts_db_info) = reconstruct_accountsdb_from_fields(
        account_paths,
        obsolete_accounts,
        limit_load_slot_count_from_snapshot,
        verify_index,
        accounts_db_config,
        accounts_update_notifier,
        exit,
    )?;

    let bank_rc = BankRc::new(Accounts::new(Arc::new(accounts_db)));
    let runtime_config = Arc::new(runtime_config.clone());
    let epoch_stakes = epoch_stakes_handle.join().expect("calculate epoch stakes");

    let bank = Bank::new_from_snapshot(
        bank_rc,
        genesis_config,
        runtime_config,
        bank_fields,
        leader_for_tests,
        debug_keys,
        reconstructed_accounts_db_info.accounts_data_len,
        epoch_stakes,
    );

    Ok((
        bank,
        ReconstructedBankInfo {
            calculated_accounts_lt_hash: reconstructed_accounts_db_info.calculated_accounts_lt_hash,
            calculated_capitalization: reconstructed_accounts_db_info.calculated_capitalization,
        },
    ))
}

pub(crate) fn reconstruct_single_storage_for_snapshot(
    slot: Slot,
    append_vec_file_info: FileInfo,
    id: AccountsFileId,
    storage_access: StorageAccess,
    obsolete_accounts: Option<(ObsoleteAccounts, AccountsFileId)>,
) -> Result<Arc<AccountStorageEntry>, SnapshotError> {
    Ok(reconstruct_single_storage(
        slot,
        append_vec_file_info,
        id,
        storage_access,
        obsolete_accounts,
    )?)
}

// Remap the AppendVec ID to handle any duplicate IDs that may previously existed
// due to full snapshots and incremental snapshots generated from different
// nodes
pub(crate) fn remap_append_vec_file(
    slot: Slot,
    old_append_vec_id: SerializedAccountsFileId,
    append_vec_file_info: FileInfo,
    next_append_vec_id: &AtomicAccountsFileId,
    num_collisions: &AtomicUsize,
) -> io::Result<(AccountsFileId, FileInfo)> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    let append_vec_path_cstr = cstring_from_path(&append_vec_file_info.path)?;

    let mut remapped_append_vec_path = append_vec_file_info.path.clone();

    // Break out of the loop in the following situations:
    // 1. The new ID is the same as the original ID.  This means we do not need to
    //    rename the file, since the ID is the "correct" one already.
    // 2. There is not a file already at the new path.  This means it is safe to
    //    rename the file to this new path.
    let (remapped_append_vec_id, remapped_append_vec_path) = loop {
        let remapped_append_vec_id = next_append_vec_id.fetch_add(1, Ordering::AcqRel);

        // this can only happen in the first iteration of the loop
        if old_append_vec_id == remapped_append_vec_id as SerializedAccountsFileId {
            break (remapped_append_vec_id, remapped_append_vec_path);
        }

        let remapped_file_name = AccountsFile::file_name(slot, remapped_append_vec_id);
        remapped_append_vec_path = remapped_append_vec_path
            .parent()
            .unwrap()
            .join(remapped_file_name);

        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        {
            let remapped_append_vec_path_cstr = cstring_from_path(&remapped_append_vec_path)?;

            // On linux we use renameat2(NO_REPLACE) instead of IF metadata(path).is_err() THEN
            // rename() in order to save a statx() syscall.
            match rename_no_replace(&append_vec_path_cstr, &remapped_append_vec_path_cstr) {
                // If the file was successfully renamed, break out of the loop
                Ok(_) => break (remapped_append_vec_id, remapped_append_vec_path),
                // If there's already a file at the new path, continue so we try
                // the next ID
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e),
            }
        }

        #[cfg(any(
            not(target_os = "linux"),
            all(target_os = "linux", not(target_env = "gnu"))
        ))]
        if std::fs::metadata(&remapped_append_vec_path).is_err() {
            break (remapped_append_vec_id, remapped_append_vec_path);
        }

        // If we made it this far, a file exists at the new path.  Record the collision
        // and try again.
        num_collisions.fetch_add(1, Ordering::Relaxed);
    };

    // Only rename the file if the new ID is actually different from the original. In the target_os
    // = linux case, we have already renamed if necessary.
    #[cfg(any(
        not(target_os = "linux"),
        all(target_os = "linux", not(target_env = "gnu"))
    ))]
    if old_append_vec_id != remapped_append_vec_id as SerializedAccountsFileId {
        std::fs::rename(&append_vec_file_info.path, &remapped_append_vec_path)?;
    }

    Ok((
        remapped_append_vec_id,
        FileInfo {
            path: remapped_append_vec_path,
            ..append_vec_file_info
        },
    ))
}

pub(crate) fn remap_and_reconstruct_single_storage(
    slot: Slot,
    old_append_vec_id: SerializedAccountsFileId,
    append_vec_file_info: FileInfo,
    next_append_vec_id: &AtomicAccountsFileId,
    num_collisions: &AtomicUsize,
    storage_access: StorageAccess,
) -> Result<Arc<AccountStorageEntry>, SnapshotError> {
    let (remapped_append_vec_id, remapped_append_vec_file_info) = remap_append_vec_file(
        slot,
        old_append_vec_id,
        append_vec_file_info,
        next_append_vec_id,
        num_collisions,
    )?;
    let storage = reconstruct_single_storage_for_snapshot(
        slot,
        remapped_append_vec_file_info,
        remapped_append_vec_id,
        storage_access,
        None,
    )?;
    Ok(storage)
}

/// This struct contains side-info while reconstructing the accounts DB from fields.
#[derive(Debug)]
pub struct ReconstructedAccountsDbInfo {
    pub accounts_data_len: u64,
    /// The accounts lt hash calculated during index generation.
    /// Will be used when verifying accounts, after rebuilding a Bank.
    pub calculated_accounts_lt_hash: AccountsLtHash,
    /// The capitalization, in lamports, calculated during index generation.
    pub calculated_capitalization: u64,
}

fn reconstruct_accountsdb_from_fields(
    account_paths: &[PathBuf],
    obsolete_accounts: Option<DashMap<Slot, (ObsoleteAccounts, AccountsFileId)>>,
    limit_load_slot_count_from_snapshot: Option<usize>,
    verify_index: bool,
    accounts_db_config: AccountsDbConfig,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    exit: Arc<AtomicBool>,
) -> Result<(AccountsDb, ReconstructedAccountsDbInfo), Error> {
    info!("Building accounts index...");
    let start = Instant::now();
    let (
        accounts_db,
        IndexGenerationInfo {
            accounts_data_len,
            calculated_accounts_lt_hash,
            calculated_capitalization,
        },
    ) = AccountsDb::from_snapshot(
        account_paths.to_vec(),
        accounts_db_config,
        accounts_update_notifier,
        exit,
        obsolete_accounts,
        limit_load_slot_count_from_snapshot,
        verify_index,
    )
    .map_err(|e| Box::new(bincode::ErrorKind::Custom(e.to_string())))?;
    info!("Building accounts index... Done in {:?}", start.elapsed());

    Ok((
        accounts_db,
        ReconstructedAccountsDbInfo {
            accounts_data_len,
            calculated_accounts_lt_hash,
            calculated_capitalization,
        },
    ))
}

// Rename `src` to `dest` only if `dest` doesn't already exist.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn rename_no_replace(src: &CStr, dest: &CStr) -> io::Result<()> {
    let ret = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            src.as_ptr() as *const _,
            libc::AT_FDCWD,
            dest.as_ptr() as *const _,
            libc::RENAME_NOREPLACE,
        )
    };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn cstring_from_path(path: &Path) -> io::Result<CString> {
    // It is better to allocate here than use the stack. Jemalloc is going to give us a chunk of a
    // preallocated small arena anyway. Instead if we used the stack since PATH_MAX=4096 it would
    // result in LLVM inserting a stack probe, see
    // https://docs.rs/compiler_builtins/latest/compiler_builtins/probestack/index.html.
    CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}
