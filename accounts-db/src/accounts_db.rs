//! `AccountsDb` — the top-level, backend-agnostic type that callers use.
//!
//! This owns all generic (non-AppendVec-specific) state and delegates to
//! a `backend: AppendVecBackend` for the AppendVec-specific storage layer.

mod accounts_db_config;
pub(crate) mod backend;
mod geyser_plugin_utils;
pub(crate) mod stats;

#[cfg(test)]
pub(crate) use backend::shrink::ShrinkCollectRefs;
pub(crate) use backend::{
    shrink::{AliveAccounts, ShrinkCollect, ShrinkCollectAliveSeparatedByRefs},
    *,
};
#[cfg(feature = "dev-context-only-utils")]
use {
    crate::accounts_file::AccountsFileProvider,
    rand::{Rng, rng},
    solana_vote_program,
};
use {
    crate::{
        account_storage::AccountStorageMap,
        accounts_cache::{AccountsCache, CachedAccount, SlotCache},
        accounts_hash::{AccountLtHash, AccountsLtHash},
        accounts_index::{
            AccountIndex, AccountSecondaryIndexes, IndexKey,
            secondary::{RwLockSecondaryIndexEntry, SecondaryIndex, SecondaryIndexEntry},
        },
        accounts_scan::{ScanConfig, ScanError, ScanGuard, ScanResult, ScanTracker},
        accounts_update_notifier_interface::{AccountForGeyser, AccountsUpdateNotifier},
        active_stats::{ActiveStatItem, ActiveStats},
        ancestors::Ancestors,
        is_zero_lamport::IsZeroLamport,
        partitioned_rewards::PartitionedEpochRewardsConfig,
        storable_accounts::StorableAccounts,
    },
    bv::BitVec,
    log::{info, warn},
    rayon::{ThreadPool, prelude::*},
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::{BankId, Slot},
    solana_measure::measure::Measure,
    solana_pubkey::{Pubkey, PubkeyHasherBuilder},
    spl_generic_token,
    stats::{
        AccountsStats, FlushStats, LoadAccountsStats, StoreAccountsUnfrozenStats,
        WriteAccountsToCacheStats,
    },
    std::{
        borrow::Cow,
        collections::{BTreeSet, HashMap, HashSet},
        path::{Path, PathBuf},
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    },
};
pub use {
    accounts_db_config::{
        ACCOUNTS_DB_CONFIG_FOR_BENCHMARKS, ACCOUNTS_DB_CONFIG_FOR_TESTING, AccountsDbConfig,
    },
    backend::{
        AccountShrinkThreshold, AccountsFileId, AppendVecBackend, AtomicAccountsFileId,
        DEFAULT_ACCOUNTS_SHRINK_OPTIMIZE_TOTAL_SPACE, DEFAULT_ACCOUNTS_SHRINK_RATIO,
        IndexGenerationInfo, TOTAL_IO_URING_BUFFERS_SIZE_LIMIT, UpdateIndexThreadSelection,
        default_num_foreground_threads, get_temp_accounts_paths, quarter_thread_count,
        shrink::GetUniqueAccountsResult, storable_accounts_by_slot::StorableAccountsBySlot,
    },
    stats::PurgeStats,
};

// when the accounts write cache exceeds this many bytes, we will flush it
// this can be specified on the command line, too (--accounts-db-cache-limit-mb)
const WRITE_CACHE_LIMIT_BYTES_DEFAULT: u64 = 15_000_000_000;
const SCAN_SLOT_PAR_ITER_THRESHOLD: usize = 4000;

// Some hints for applicability of additional sanity checks for the do_load fast-path;
// Slower fallback code path will be taken if the fast path has failed over the retry
// threshold, regardless of these hints. Also, load cannot fail not-deterministically
// even under very rare circumstances, unlike previously did allow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadHint {
    // Caller hints that it's loading transactions for a block which is
    // descended from the current root, and at the tip of its fork.
    // Thereby, further this assumes AccountIndex::max_root should not increase
    // during this load, meaning there should be no squash.
    // Overall, this enables us to assert!() strictly while running the fast-path for
    // account loading, while maintaining the determinism of account loading and resultant
    // transaction execution thereof.
    FixedMaxRoot,
    // Caller can't hint the above safety assumption. Generally RPC and miscellaneous
    // other call-site falls into this category. The likelihood of slower path is slightly
    // increased as well.
    Unspecified,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PopulateReadCache {
    /// If the account is found in storage, populate the read cache with the loaded account
    True,
    /// Do not populate the read cache with loaded accounts
    False,
}

pub struct AccountsAddRootTiming {
    pub index_us: u64,
    pub cache_us: u64,
}

/// The storage backend used by [`AccountsDb`].
#[derive(Debug)]
pub enum AccountsBackend {
    AppendVec(AppendVecBackend),
    // RocksDb(RocksDbBackend),  // future
}

impl AccountsBackend {
    pub fn store(
        &self,
        slot: Slot,
        accounts_to_flush: Vec<(&Pubkey, &AccountSharedData)>,
    ) -> FlushStats {
        match self {
            AccountsBackend::AppendVec(b) => b.store(slot, accounts_to_flush),
        }
    }

    /// Returns `true` if `pubkey` has a live (non-zero-lamport) entry in
    /// the persistent state. Returns `false` if the pubkey is unknown or
    /// is recorded with zero lamports.
    pub fn is_pubkey_alive(&self, pubkey: &Pubkey) -> bool {
        match self {
            AccountsBackend::AppendVec(b) => match b.accounts_index.get(pubkey) {
                None => false,
                Some((_, info)) => !info.is_zero_lamport(),
            },
        }
    }

    /// Returns `true` if `pubkey` has any entry in the persistent state,
    /// including zero-lamport entries that haven't been cleaned yet.
    pub fn contains_pubkey(&self, pubkey: &Pubkey) -> bool {
        match self {
            AccountsBackend::AppendVec(b) => b.accounts_index.get(pubkey).is_some(),
        }
    }


    /// Periodically log backend statistics (read cache + storage timings).
    pub fn report_stats(&self) {
        match self {
            AccountsBackend::AppendVec(b) => {
                b.report_read_cache_stats();
                b.report_storage_stats();
            }
        }
    }

    /// Load `pubkey` from the persistent state. Returns `None` if absent
    /// (or, for AppendVec, if the load was retried out).
    pub fn load(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        load_hint: LoadHint,
        populate_read_cache: PopulateReadCache,
    ) -> Option<(AccountSharedData, Slot)> {
        match self {
            AccountsBackend::AppendVec(b) => {
                b.get(ancestors, pubkey, load_hint, populate_read_cache)
            }
        }
    }

    /// Walk the index summing per-pubkey lt-hashes for the startup verification.
    pub fn calculate_accounts_lt_hash_at_startup_from_index(
        &self,
        ancestors: &Ancestors,
    ) -> AccountsLtHash {
        match self {
            AccountsBackend::AppendVec(b) => {
                b.calculate_accounts_lt_hash_at_startup_from_index(ancestors)
            }
        }
    }

    /// Walk the index summing per-pubkey balances for startup capitalization.
    pub fn calculate_capitalization_at_startup_from_index(&self, ancestors: &Ancestors) -> u64 {
        match self {
            AccountsBackend::AppendVec(b) => {
                b.calculate_capitalization_at_startup_from_index(ancestors)
            }
        }
    }

    /// Build the in-memory accounts index from the persisted storage.
    pub fn generate_index(
        &self,
        limit_load_slot_count_from_snapshot: Option<usize>,
        verify: bool,
    ) -> IndexGenerationInfo {
        match self {
            AccountsBackend::AppendVec(b) => {
                b.generate_index(limit_load_slot_count_from_snapshot, verify)
            }
        }
    }

    /// Streams snapshot archive entries via callback. Each callback invocation
    /// receives `(filename, reader, length)` for one entry to be added to the
    /// archive. For incremental snapshots, only entries past `base_slot` are
    /// emitted.
    pub fn write_snapshot_archive_entries(
        &self,
        base_slot: Option<Slot>,
        snapshot_slot: Slot,
        callback: impl FnMut(&str, &mut dyn std::io::Read, u64) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        match self {
            AccountsBackend::AppendVec(b) => {
                b.write_snapshot_archive_entries(base_slot, snapshot_slot, callback)
            }
        }
    }

    /// Persist a fastboot-compatible snapshot of the backend into `snapshot_dir`.
    pub fn save_to_snapshot_dir(
        &self,
        snapshot_dir: &std::path::Path,
        slot: Slot,
    ) -> std::io::Result<()> {
        match self {
            AccountsBackend::AppendVec(b) => b.save_to_snapshot_dir(snapshot_dir, slot),
        }
    }

    /// Periodic shrink pass. When `include_ancient` is true, ancient slots
    /// are squashed first (callers typically pair this with `clean_accounts`).
    /// Returns the number of candidate slots selected for shrinking.
    pub fn shrink(
        &self,
        epoch_schedule: &solana_epoch_schedule::EpochSchedule,
        include_ancient: bool,
    ) -> usize {
        match self {
            AccountsBackend::AppendVec(b) => b.shrink(epoch_schedule, include_ancient),
        }
    }

    /// Run a clean pass on the backend. Caller is responsible for resolving
    /// `max_clean_root_inclusive` against any in-flight scans.
    pub fn clean_accounts(&self, max_clean_root_inclusive: Option<Slot>, is_startup: bool) {
        match self {
            AccountsBackend::AppendVec(b) => b.clean_accounts(max_clean_root_inclusive, is_startup),
        }
    }

    /// Iterate every (pubkey, account, slot) triple persisted in the backend.
    /// The callback may return `ControlFlow::Break(())` to abort early.
    /// Entries whose accounts cannot be loaded (eg. mid-flight cache races)
    /// are silently skipped.
    pub fn for_each_account<F>(&self, mut callback: F)
    where
        F: FnMut(&Pubkey, AccountSharedData, Slot) -> std::ops::ControlFlow<()>,
    {
        match self {
            AccountsBackend::AppendVec(b) => {
                'outer: for pubkeys in b.accounts_index.iter() {
                    for pubkey in pubkeys {
                        let Some((slot, info)) = b.accounts_index.get(&pubkey) else {
                            continue;
                        };
                        let mut accessor =
                            b.get_account_accessor(slot, &pubkey, &info.storage_location());
                        let account = match accessor {
                            LoadedAccountAccessor::Cached(None) => None,
                            _ => accessor.get_loaded_account(|loaded| loaded.take_account()),
                        };
                        let Some(account) = account else { continue };
                        if callback(&pubkey, account, slot).is_break() {
                            break 'outer;
                        }
                    }
                }
            }
        }
    }

    pub fn as_append_vec(&self) -> &AppendVecBackend {
        match self {
            AccountsBackend::AppendVec(b) => b,
        }
    }

    #[cfg(any(test, feature = "dev-context-only-utils"))]
    pub fn as_append_vec_mut(&mut self) -> &mut AppendVecBackend {
        match self {
            AccountsBackend::AppendVec(b) => b,
        }
    }
}

/// The top-level accounts-db type used by callers.
///
/// Owns backend-agnostic (generic) state and contains a `backend: AccountsBackend`
/// for the storage-layer implementation.
#[derive(Debug)]
pub struct AccountsDb {
    /// The storage backend.
    pub backend: AccountsBackend,

    /// Committed slots still in the write cache (not yet flushed to storage).
    /// Used to distinguish rooted vs. unrooted cache slots.
    pub(crate) cache_roots: RwLock<HashSet<Slot>>,

    /// Monotonically increasing max committed root. Used for scan boundaries.
    pub max_root: AtomicU64,

    /// Monotonically increasing write counter used to tag Geyser plugin
    /// notifications. Incremented per-account stored.
    pub write_version: AtomicU64,

    /// If true, skip the initial accounts hash calculation that runs after a
    /// snapshot load (configured via `--accounts-db-skip-initial-hash-calculation`).
    pub(crate) skip_initial_hash_calc: bool,

    /// Stats for purges called outside of `clean_accounts()` — i.e.
    /// coordinator-initiated `purge_slots` operations.
    pub(crate) external_purge_slots_stats: PurgeStats,

    /// Optional set of `TempDir`s that own the on-disk paths the backend uses.
    /// Held here purely for lifetime — dropping these deletes the directories.
    /// Set when callers construct an `AccountsDb` with empty paths (typical
    /// for tests/benches), or via `push_temp_path` from test setup.
    temp_paths: Option<Vec<tempfile::TempDir>>,

    /// Write-back / accounts cache.
    pub accounts_cache: AccountsCache,

    /// Maximum write cache size in bytes before forcing a flush.
    pub(crate) write_cache_limit_bytes: Option<u64>,

    /// Thread pool for foreground tasks, e.g. transaction processing.
    pub thread_pool_foreground: ThreadPool,

    /// General-purpose account stats.
    pub stats: AccountsStats,

    /// Counters reflecting which background phases are currently active
    /// (clean / shrink / flush / squash-ancient + sub-phases).
    pub(crate) active_stats: ActiveStats,

    /// Stats for loading accounts during transaction processing.
    pub(crate) load_account_stats: LoadAccountsStats,

    /// Stats from storing accounts unfrozen.
    pub(crate) store_accounts_unfrozen_stats: StoreAccountsUnfrozenStats,

    /// Tracks ongoing index scans.
    pub scan_tracker: ScanTracker,

    /// Secondary index configuration.
    pub account_indexes: AccountSecondaryIndexes,
    pub(crate) program_id_index: SecondaryIndex<RwLockSecondaryIndexEntry>,
    pub(crate) spl_token_mint_index: SecondaryIndex<RwLockSecondaryIndexEntry>,
    pub(crate) spl_token_owner_index: SecondaryIndex<RwLockSecondaryIndexEntry>,

    /// GeyserPlugin accounts update notifier.
    pub(crate) accounts_update_notifier: Option<AccountsUpdateNotifier>,

    /// True if the bank drop callback is registered.
    pub(crate) is_bank_drop_callback_enabled: AtomicBool,

    /// Partitioned epoch rewards configuration.
    pub partitioned_epoch_rewards_config: PartitionedEpochRewardsConfig,

    /// Directory for bank hash details files.
    pub(crate) bank_hash_details_dir: PathBuf,

    /// Slot of the latest full snapshot. Set by the snapshot lifecycle
    /// (background service, archive write, restore). Mirrored on the AppendVec
    /// backend, which reads it during clean/shrink to gate purging of older
    /// account state.
    pub(crate) latest_full_snapshot_slot: seqlock::SeqLock<Option<Slot>>,

    /// Artificial delay for account loads (tests only).
    #[cfg(test)]
    pub(crate) load_delay: u64,
}

impl AccountsDb {
    pub fn new_with_config(
        paths: Vec<PathBuf>,
        accounts_db_config: AccountsDbConfig,
        accounts_update_notifier: Option<AccountsUpdateNotifier>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        Self::new_with_config_and_storage(
            paths,
            accounts_db_config,
            accounts_update_notifier,
            exit,
            AccountStorageMap::default(),
            0,
        )
    }

    pub fn new_with_config_and_storage(
        paths: Vec<PathBuf>,
        accounts_db_config: AccountsDbConfig,
        accounts_update_notifier: Option<AccountsUpdateNotifier>,
        exit: Arc<AtomicBool>,
        storage: AccountStorageMap,
        next_id: AccountsFileId,
    ) -> Self {
        // Increase the stack for foreground threads; rayon needs a lot of stack.
        const ACCOUNTS_STACK_SIZE: usize = 8 * 1024 * 1024;
        let num_foreground_threads = accounts_db_config
            .num_foreground_threads
            .map(Into::into)
            .unwrap_or_else(default_num_foreground_threads);
        let thread_pool_foreground = rayon::ThreadPoolBuilder::new()
            .num_threads(num_foreground_threads)
            .thread_name(|i| format!("solAcctsDbFg{i:02}"))
            .stack_size(ACCOUNTS_STACK_SIZE)
            .build()
            .expect("new rayon threadpool");

        let bank_hash_details_dir = accounts_db_config.bank_hash_details_dir.clone();
        let account_indexes = accounts_db_config
            .account_indexes
            .clone()
            .unwrap_or_default();
        let partitioned_epoch_rewards_config = accounts_db_config.partitioned_epoch_rewards_config;
        let write_cache_limit_bytes = accounts_db_config.write_cache_limit_bytes;
        let skip_initial_hash_calc = accounts_db_config.skip_initial_hash_calc;

        // If no paths were supplied (typical in tests/benches), spin up a
        // temporary directory set whose lifetime is bound to this AccountsDb.
        let (paths, temp_paths) = if paths.is_empty() {
            let (temp_dirs, temp_paths) = crate::accounts_db::backend::get_temp_accounts_paths(
                crate::accounts_db::backend::DEFAULT_NUM_DIRS,
            )
            .unwrap();
            (temp_paths, Some(temp_dirs))
        } else {
            (paths, None)
        };

        let backend = AccountsBackend::AppendVec(AppendVecBackend::new_backend(
            paths,
            accounts_db_config,
            exit,
            storage,
            next_id,
        ));

        Self {
            backend,

            cache_roots: RwLock::new(HashSet::default()),
            max_root: AtomicU64::new(0),
            write_version: AtomicU64::new(0),
            skip_initial_hash_calc,
            external_purge_slots_stats: PurgeStats::default(),
            temp_paths,
            accounts_cache: AccountsCache::default(),
            write_cache_limit_bytes,
            thread_pool_foreground,
            stats: AccountsStats::default(),
            active_stats: ActiveStats::default(),
            load_account_stats: LoadAccountsStats::default(),
            store_accounts_unfrozen_stats: StoreAccountsUnfrozenStats::default(),
            scan_tracker: ScanTracker::default(),
            account_indexes,
            program_id_index: SecondaryIndex::<RwLockSecondaryIndexEntry>::new(
                "program_id_index_stats",
            ),
            spl_token_mint_index: SecondaryIndex::<RwLockSecondaryIndexEntry>::new(
                "spl_token_mint_index_stats",
            ),
            spl_token_owner_index: SecondaryIndex::<RwLockSecondaryIndexEntry>::new(
                "spl_token_owner_index_stats",
            ),
            accounts_update_notifier,
            is_bank_drop_callback_enabled: AtomicBool::default(),
            partitioned_epoch_rewards_config,
            bank_hash_details_dir,
            latest_full_snapshot_slot: seqlock::SeqLock::new(None),
            #[cfg(test)]
            load_delay: u64::default(),
        }
    }

    fn update_spl_token_secondary_indexes<G: spl_generic_token::token::GenericTokenAccount>(
        &self,
        token_id: &Pubkey,
        pubkey: &Pubkey,
        account_owner: &Pubkey,
        account_data: &[u8],
    ) {
        if *account_owner == *token_id {
            if self.account_indexes.contains(&AccountIndex::SplTokenOwner) {
                if let Some(owner_key) = G::unpack_account_owner(account_data) {
                    if self.account_indexes.include_key(owner_key) {
                        self.spl_token_owner_index.insert(owner_key, pubkey);
                    }
                }
            }
            if self.account_indexes.contains(&AccountIndex::SplTokenMint) {
                if let Some(mint_key) = G::unpack_account_mint(account_data) {
                    if self.account_indexes.include_key(mint_key) {
                        self.spl_token_mint_index.insert(mint_key, pubkey);
                    }
                }
            }
        }
    }

    pub(crate) fn update_secondary_indexes(&self, pubkey: &Pubkey, account: &impl ReadableAccount) {
        if self.account_indexes.is_empty() {
            return;
        }
        let account_owner = account.owner();
        let account_data = account.data();
        if self.account_indexes.contains(&AccountIndex::ProgramId)
            && self.account_indexes.include_key(account_owner)
        {
            self.program_id_index.insert(account_owner, pubkey);
        }
        self.update_spl_token_secondary_indexes::<spl_generic_token::token::Account>(
            &spl_generic_token::token::id(),
            pubkey,
            account_owner,
            account_data,
        );
        self.update_spl_token_secondary_indexes::<spl_generic_token::token_2022::Account>(
            &spl_generic_token::token_2022::id(),
            pubkey,
            account_owner,
            account_data,
        );
    }

    pub(crate) fn get_index_key_pubkeys(&self, index_key: &IndexKey) -> Vec<Pubkey> {
        match index_key {
            IndexKey::ProgramId(key) => self.program_id_index.get(key),
            IndexKey::SplTokenMint(key) => self.spl_token_mint_index.get(key),
            IndexKey::SplTokenOwner(key) => self.spl_token_owner_index.get(key),
        }
    }

    pub fn get_index_key_size(&self, index: &AccountIndex, index_key: &Pubkey) -> Option<usize> {
        match index {
            AccountIndex::ProgramId => self.program_id_index.index.get(index_key).map(|x| x.len()),
            AccountIndex::SplTokenOwner => self
                .spl_token_owner_index
                .index
                .get(index_key)
                .map(|x| x.len()),
            AccountIndex::SplTokenMint => self
                .spl_token_mint_index
                .index
                .get(index_key)
                .map(|x| x.len()),
        }
    }

    pub(crate) fn log_secondary_indexes(&self) {
        if !self.program_id_index.index.is_empty() {
            info!("secondary index: {:?}", AccountIndex::ProgramId);
            self.program_id_index.log_contents();
        }
        if !self.spl_token_mint_index.index.is_empty() {
            info!("secondary index: {:?}", AccountIndex::SplTokenMint);
            self.spl_token_mint_index.log_contents();
        }
        if !self.spl_token_owner_index.index.is_empty() {
            info!("secondary index: {:?}", AccountIndex::SplTokenOwner);
            self.spl_token_owner_index.log_contents();
        }
    }

    pub(crate) fn purge_secondary_indexes_by_inner_key(&self, inner_key: &Pubkey) {
        if self.account_indexes.contains(&AccountIndex::ProgramId) {
            self.program_id_index.remove_by_inner_key(inner_key);
        }
        if self.account_indexes.contains(&AccountIndex::SplTokenOwner) {
            self.spl_token_owner_index.remove_by_inner_key(inner_key);
        }
        if self.account_indexes.contains(&AccountIndex::SplTokenMint) {
            self.spl_token_mint_index.remove_by_inner_key(inner_key);
        }
    }

    pub fn bank_hash_details_dir(&self) -> &Path {
        &self.bank_hash_details_dir
    }

    pub fn skip_initial_hash_calc(&self) -> bool {
        self.skip_initial_hash_calc
    }

    /// Initialize storage and the next-ID counter from a snapshot.
    /// Called once during snapshot restore, before `generate_index`.
    /// Reconstruct an `AccountsDb` from an AppendVec snapshot already on disk.
    ///
    /// `account_paths` must contain the AppendVec files named `"<slot>.<id>"`.
    /// Callers are responsible for ensuring files are in place before calling
    /// (archive extraction + ID remapping for archive snapshots; hardlinking for
    /// directory snapshots).
    pub fn from_snapshot(
        account_paths: Vec<PathBuf>,
        accounts_db_config: AccountsDbConfig,
        accounts_update_notifier: Option<AccountsUpdateNotifier>,
        exit: Arc<AtomicBool>,
        obsolete_accounts: Option<
            dashmap::DashMap<Slot, (crate::ObsoleteAccounts, AccountsFileId)>,
        >,
        limit_load_slot_count_from_snapshot: Option<usize>,
        verify_index: bool,
    ) -> Result<(Self, IndexGenerationInfo), crate::snapshot_storage::StorageRestoreError> {
        let storage_access = accounts_db_config.storage_access;

        for path in &account_paths {
            std::fs::create_dir_all(path).unwrap_or_else(|err| {
                panic!("Failed to create directory {}: {err}", path.display())
            });
        }

        let (storage, next_id) = crate::accounts_db::backend::AppendVecBackend::from_account_paths(
            &account_paths,
            storage_access,
            obsolete_accounts.as_ref(),
        )?;

        let accounts_db = Self::new_with_config_and_storage(
            account_paths,
            accounts_db_config,
            accounts_update_notifier,
            exit,
            storage,
            next_id,
        );
        let info = accounts_db.generate_index(limit_load_slot_count_from_snapshot, verify_index);
        Ok((accounts_db, info))
    }

    pub fn write_version(&self) -> u64 {
        self.write_version.load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "dev-context-only-utils"))]
    pub fn push_temp_path(&mut self, path: tempfile::TempDir) {
        self.temp_paths.get_or_insert_with(Vec::new).push(path);
    }

    /// Register the secondary-index eviction callback on the write cache.
    /// Must be called once, right after the `Arc<AccountsDb>` is created.
    pub fn init_eviction_callback(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        self.accounts_cache.set_on_pubkey_evicted(move |pubkey| {
            let Some(db) = weak.upgrade() else { return };
            if !db.backend.is_pubkey_alive(pubkey) {
                db.purge_secondary_indexes_by_inner_key(pubkey);
            }
        });
    }

    /// Returns true if there is an accounts update notifier.
    pub fn has_accounts_update_notifier(&self) -> bool {
        self.accounts_update_notifier.is_some()
    }

    // `force_flush` flushes all the cached roots `<= requested_flush_root`. It also then
    // flushes excess remaining rooted slots while 'should_aggressively_flush_cache' is true
    pub fn flush_accounts_cache(&self, force_flush: bool, requested_flush_root: Option<Slot>) {
        #[cfg(not(test))]
        assert!(requested_flush_root.is_some());

        if !force_flush && !self.should_aggressively_flush_cache() {
            return;
        }

        let mut flush_roots_elapsed = Measure::start("flush_roots_elapsed");
        let _guard = self.active_stats.activate(ActiveStatItem::Flush);

        let (total_new_cleaned_roots, num_cleaned_roots_flushed, mut flush_stats) =
            self.flush_rooted_accounts_cache_with_clean(requested_flush_root);
        flush_roots_elapsed.stop();

        let (total_new_excess_roots, num_excess_roots_flushed, flush_stats_aggressively) =
            /*if self.should_aggressively_flush_cache() {
                self.flush_rooted_accounts_cache_without_clean()
            } else*/ {
                (0, 0, FlushStats::default())
            };
        flush_stats.accumulate(&flush_stats_aggressively);

        datapoint_info!(
            "accounts_db-flush_accounts_cache",
            ("total_new_cleaned_roots", total_new_cleaned_roots, i64),
            ("num_cleaned_roots_flushed", num_cleaned_roots_flushed, i64),
            ("total_new_excess_roots", total_new_excess_roots, i64),
            ("num_excess_roots_flushed", num_excess_roots_flushed, i64),
            ("flush_roots_elapsed", flush_roots_elapsed.as_us(), i64),
            (
                "account_bytes_flushed",
                flush_stats.num_bytes_flushed.0,
                i64
            ),
            (
                "num_accounts_flushed",
                flush_stats.num_accounts_flushed.0,
                i64
            ),
            ("num_accounts_saved", flush_stats.num_accounts_purged.0, i64),
            (
                "store_accounts_total_us",
                flush_stats.store_accounts_total_us.0,
                i64
            ),
            (
                "update_index_us",
                flush_stats.store_accounts_timing.update_index_elapsed,
                i64
            ),
            (
                "store_accounts_elapsed_us",
                flush_stats.store_accounts_timing.store_accounts_elapsed,
                i64
            ),
            (
                "handle_reclaims_elapsed_us",
                flush_stats.store_accounts_timing.handle_reclaims_elapsed,
                i64
            ),
        );
    }

    /// Flush all rooted slots up to `requested_flush_root` with cleaning.
    fn flush_rooted_accounts_cache_with_clean(
        &self,
        requested_flush_root: Option<Slot>,
    ) -> (usize, usize, FlushStats) {
        let max_clean_root = self.max_clean_root(requested_flush_root);

        let flush_root = if let Some(max_clean_root) = max_clean_root {
            Some(max_clean_root.min(requested_flush_root.unwrap_or(Slot::MAX)))
        } else {
            requested_flush_root
        };
        let mut written_accounts = HashSet::new();

        let mut should_flush_f = Some(move |&pubkey: &Pubkey| written_accounts.insert(pubkey));

        self.flush_rooted_accounts_cache(flush_root, should_flush_f.as_mut())
    }

    /// Flush all rooted slots up to `requested_flush_root`.
    fn flush_rooted_accounts_cache(
        &self,
        requested_flush_root: Option<Slot>,
        mut should_flush_f: Option<&mut impl FnMut(&Pubkey) -> bool>,
    ) -> (usize, usize, FlushStats) {
        let flushed_roots: BTreeSet<Slot> =
            self.accounts_cache.begin_flush_roots(requested_flush_root);
        let max_flush_root = flushed_roots.last().copied();
        let num_new_roots = flushed_roots.len();

        let mut num_roots_flushed = 0;
        let mut flush_stats = FlushStats::default();
        // Iterate from highest to lowest so we don't flush earlier outdated updates first.
        for root in flushed_roots.into_iter().rev() {
            if let Some(stats) = self.flush_slot_cache(root, should_flush_f.as_mut()) {
                num_roots_flushed += 1;
                flush_stats.accumulate(&stats);
            }
        }

        max_flush_root.inspect(|&root| self.accounts_cache.set_max_flush_root(root));
        self.accounts_cache.end_flush_roots();

        (num_new_roots, num_roots_flushed, flush_stats)
    }

    /// `should_flush_f` is an optional closure that determines whether a given
    /// account should be flushed. Passing `None` will by default flush all accounts.
    fn flush_slot_cache(
        &self,
        slot: Slot,
        should_flush_f: Option<&mut impl FnMut(&Pubkey) -> bool>,
    ) -> Option<FlushStats> {
        self.accounts_cache
            .slot_cache(slot)
            .map(|slot_cache| self.do_flush_slot_cache(slot, &slot_cache, should_flush_f))
    }

    fn do_flush_slot_cache(
        &self,
        slot: Slot,
        slot_cache: &SlotCache,
        mut should_flush_f: Option<&mut impl FnMut(&Pubkey) -> bool>,
    ) -> FlushStats {
        debug_assert!(self.cache_roots.read().unwrap().contains(&slot));

        let iter_items: Vec<_> = slot_cache.iter().collect();
        let mut accounts_to_flush = Vec::new();
        let mut num_accounts_purged = 0usize;
        for iter_item in &iter_items {
            let key = iter_item.key();
            let account = &iter_item.value().account;
            if account.lamports() == 0 {
                self.purge_secondary_indexes_by_inner_key(key);
            }
            if should_flush_f.as_mut().map(|f| f(key)).unwrap_or(true) {
                accounts_to_flush.push((key, account));
            } else {
                num_accounts_purged += 1;
            }
        }

        // Transition slot from cache phase to storage phase.
        self.cache_roots.write().unwrap().remove(&slot);

        let mut flush_stats = self.backend.store(slot, accounts_to_flush);
        flush_stats.num_accounts_purged += num_accounts_purged;

        // Remove the slot from the cache last so it looks like an atomic switch from
        // cache to storage for new readers. Existing racing readers are handled by
        // retry_to_get_account_accessor().
        assert!(self.accounts_cache.remove_slot(slot).is_some());

        flush_stats
    }

    pub fn flush_accounts_cache_slot_for_tests(&self, slot: Slot) {
        assert!(
            self.cache_roots.read().unwrap().contains(&slot),
            "slot: {slot}"
        );
        self.flush_slot_cache(slot, None::<&mut fn(&_) -> bool>);
    }

    /// useful to adapt tests written prior to introduction of the write cache
    /// to use the write cache
    pub fn add_root_and_flush_write_cache(&self, slot: Slot) {
        self.add_root(slot);
        self.flush_root_write_cache(slot);
    }

    pub fn flush_root_write_cache(&self, root: Slot) {
        assert!(
            self.cache_roots.read().unwrap().contains(&root),
            "slot: {root}"
        );
        self.flush_accounts_cache(true, Some(root));
    }

    pub fn mark_slot_frozen(&self, slot: Slot) {
        if let Some(slot_cache) = self.accounts_cache.slot_cache(slot) {
            slot_cache.mark_slot_frozen();
            slot_cache.report_slot_store_metrics();
        }
        self.accounts_cache.report_size();
    }

    /// true if write cache is too big and there are unflushed roots available to flush.
    /// If there are no unflushed roots, we cannot reduce cache size because unrooted
    /// slots are not flushed.
    fn should_aggressively_flush_cache(&self) -> bool {
        self.write_cache_limit_bytes
            .unwrap_or(WRITE_CACHE_LIMIT_BYTES_DEFAULT)
            < self.accounts_cache.size()
            && self.accounts_cache.num_unflushed_roots() > 0
    }

    /// Updates the accounts index with the given accounts. If store_account is false, skip storing
    /// the account in the index as the account was not stored in the cache.
    /// Used for cached accounts only.
    fn update_index_cached_accounts<'a>(
        &self,
        accounts: &impl StorableAccounts<'a>,
        store_account: &BitVec,
        update_index_thread_selection: UpdateIndexThreadSelection,
    ) {
        let len = accounts.len();
        assert_eq!(accounts.len() as u64, store_account.len());

        let update = |start, end| {
            (start..end).for_each(|i| {
                if store_account[i as u64] {
                    accounts.account(i, |account| {
                        self.update_secondary_indexes(account.pubkey(), &account);
                    });
                }
            });
        };

        let threshold = 1;
        if matches!(
            update_index_thread_selection,
            UpdateIndexThreadSelection::PoolWithThreshold,
        ) && len > threshold
        {
            let chunk_size = std::cmp::max(1, len / quarter_thread_count());
            let batches = 1 + len / chunk_size;
            self.thread_pool_foreground.install(|| {
                (0..batches).into_par_iter().for_each(|batch| {
                    let start = batch * chunk_size;
                    let end = std::cmp::min(start + chunk_size, len);
                    update(start, end)
                })
            });
        } else {
            update(0, len);
        }
    }

    // Stores accounts in the write cache. If an account is zero-lamport and not present in the
    // index, there is no need to store it in the write cache as it will not affect the accounts
    // hash. The function returns a BitVec indicating whether each account was stored in the cache.
    // Ordering of accounts is important as duplicate pubkeys are possible. The last account in
    // accounts_and_meta_to_store for each pubkey is stored in the write cache.
    fn write_accounts_to_cache<'a, 'b>(
        &self,
        slot: Slot,
        accounts_and_meta_to_store: &impl StorableAccounts<'b>,
        ancestors: Option<&Ancestors>,
    ) -> (BitVec, WriteAccountsToCacheStats) {
        let len = accounts_and_meta_to_store.len();
        let mut pubkey_set = HashSet::with_capacity_and_hasher(len, PubkeyHasherBuilder::default());
        let mut stats = WriteAccountsToCacheStats {
            num_initial_accounts_to_store: len as u64,
            ..Default::default()
        };
        let mut store_account = BitVec::new_fill(false, len as u64);

        (0..len).rev().for_each(|index| {
            accounts_and_meta_to_store.account_default_if_zero_lamport(index, |account| {
                let pubkey = account.pubkey();
                let is_duplicate_account = !pubkey_set.insert(*pubkey);
                if is_duplicate_account {
                    stats.num_duplicate_accounts_skipped += 1;
                    return;
                }
                if account.is_zero_lamport() {
                    if let Some(ancestors) = ancestors {
                        if let Some((account, _)) = self.do_load(
                            ancestors,
                            pubkey,
                            LoadHint::FixedMaxRoot,
                            PopulateReadCache::True,
                        ) {
                            if account.is_zero_lamport() {
                                stats.num_ancestors_zero_lamport_skipped += 1;
                                return;
                            }
                        } else {
                            stats.num_ephemeral_accounts_skipped += 1;
                            return;
                        }
                    } else if !self.accounts_cache.contains_pubkey(pubkey)
                        && !self.backend.is_pubkey_alive(pubkey)
                    {
                        stats.num_ephemeral_accounts_skipped += 1;
                        return;
                    }
                }

                let account_shared_data = account.take_account();
                let account_data_len = account_shared_data.data().len();
                self.accounts_cache.store(slot, pubkey, account_shared_data);
                store_account.set(index as u64, true);
                stats.num_accounts_stored += 1;
                stats.account_data_bytes_stored += account_data_len as u64;
            })
        });

        (store_account, stats)
    }

    pub(crate) fn store_accounts_unfrozen<'a>(
        &self,
        accounts: impl StorableAccounts<'a>,
        update_index_thread_selection: UpdateIndexThreadSelection,
        ancestors: Option<&Ancestors>,
    ) {
        // If all transactions in a batch are errored,
        // it's possible to get a store with no accounts.
        if accounts.is_empty() {
            return;
        }

        // Store the accounts in the write cache
        let write_accounts_time = Measure::start("write_accounts");
        let (store_account, write_stats) =
            self.write_accounts_to_cache(accounts.target_slot(), &accounts, ancestors);
        let write_accounts_us = write_accounts_time.end_as_us();

        // Update the index
        let update_index_time = Measure::start("update_index");
        self.update_index_cached_accounts(&accounts, &store_account, update_index_thread_selection);
        let update_index_us = update_index_time.end_as_us();

        let stats = &self.store_accounts_unfrozen_stats;
        stats
            .write_to_cache_us
            .fetch_add(write_accounts_us, Ordering::Relaxed);
        stats
            .update_index_us
            .fetch_add(update_index_us, Ordering::Relaxed);
        stats
            .num_initial_accounts_to_store
            .fetch_add(write_stats.num_initial_accounts_to_store, Ordering::Relaxed);
        stats
            .num_accounts_stored
            .fetch_add(write_stats.num_accounts_stored, Ordering::Relaxed);
        stats.num_duplicate_accounts_skipped.fetch_add(
            write_stats.num_duplicate_accounts_skipped,
            Ordering::Relaxed,
        );
        stats.num_ephemeral_accounts_skipped.fetch_add(
            write_stats.num_ephemeral_accounts_skipped,
            Ordering::Relaxed,
        );
        stats.num_ancestors_zero_lamport_skipped.fetch_add(
            write_stats.num_ancestors_zero_lamport_skipped,
            Ordering::Relaxed,
        );
        stats
            .account_data_bytes_stored
            .fetch_add(write_stats.account_data_bytes_stored, Ordering::Relaxed);
        stats.report();
        self.report_store_timings();
    }

    fn report_store_timings(&self) {
        if self.stats.last_store_report.should_update(1000) {
            datapoint_info!(
                "accounts_db_store_timings",
                (
                    "stakes_cache_check_and_store_us",
                    self.stats
                        .stakes_cache_check_and_store_us
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
            );
            self.backend.report_stats();
            self.load_account_stats.report();
        }
    }

    /// note this returns None for accounts with zero lamports
    pub fn load(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        load_hint: LoadHint,
        populate_read_cache: PopulateReadCache,
    ) -> Option<(AccountSharedData, Slot)> {
        self.do_load(ancestors, pubkey, load_hint, populate_read_cache)
            .filter(|(account, _)| !account.is_zero_lamport())
    }

    #[cfg(any(test, feature = "dev-context-only-utils"))]
    pub fn load_without_fixed_root(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
    ) -> Option<(AccountSharedData, Slot)> {
        self.do_load(
            ancestors,
            pubkey,
            LoadHint::Unspecified,
            PopulateReadCache::True,
        )
    }

    fn do_load(
        &self,
        ancestors: &Ancestors,
        pubkey: &Pubkey,
        load_hint: LoadHint,
        populate_read_cache: PopulateReadCache,
    ) -> Option<(AccountSharedData, Slot)> {
        let starting_max_root = self.max_root.load(Ordering::Acquire);

        if let Some((cached_account, cached_slot, _)) =
            self.accounts_cache.load_latest(pubkey, ancestors)
        {
            self.load_account_stats
                .num_loaded_from_write_cache
                .fetch_add(1, Ordering::Relaxed);
            return Some((cached_account.account.clone(), cached_slot));
        }

        #[cfg(test)]
        {
            std::thread::sleep(std::time::Duration::from_millis(self.load_delay));
        }

        let result = self
            .backend
            .load(ancestors, pubkey, load_hint, populate_read_cache)?;

        if load_hint == LoadHint::FixedMaxRoot {
            let ending_max_root = self.max_root.load(Ordering::Acquire);
            if starting_max_root != ending_max_root {
                warn!(
                    "do_load_with_populate_read_cache() scanning pubkey {pubkey} called with \
                     fixed max root, but max root changed from {starting_max_root} to \
                     {ending_max_root} during function call"
                );
            }
        }
        Some(result)
    }

    /// Scans all accounts visible from `ancestors`, invoking `scan_func` for each.
    /// Pre-scans the write cache to capture entries not yet flushed to the accounts index, then
    /// deduplicates against the index scan, calling `scan_func` with the newest version of each
    /// account
    pub(crate) fn scan_accounts<F>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        mut scan_func: F,
        config: &ScanConfig,
    ) -> ScanResult<()>
    where
        F: FnMut(Option<(&Pubkey, AccountSharedData, Slot)>),
    {
        // Register this scan so that slots needed by the scan are not cleaned out from under us.
        let scan_guard = ScanGuard::try_new(&self.scan_tracker, bank_id, || {
            self.max_root.load(Ordering::Acquire)
        })
        .ok_or(ScanError::SlotRemoved {
            slot: ancestors.max_slot(),
            bank_id,
        })?;

        // If the scan's ancestors are all rooted, drop them and scan roots only
        // Scan Guard max root must be used as the scan guard guarantees that
        // the account state as of max root is persisted in the database
        let max_root_ancestors = Ancestors::from(vec![scan_guard.max_root()]);
        let ancestors = if scan_guard.should_use_ancestors(ancestors) {
            ancestors
        } else {
            &max_root_ancestors
        };

        // Step 1: Pre-scan the cache index to find the newest visible cached version of each
        // pubkey. Hold the Arc<CachedAccount> to keep the data alive even if the cache flushes
        // between now and step 3 (Arc clone is just a refcount bump).
        let cached_pubkeys = self.accounts_cache.cached_pubkeys();
        let mut cache_versions: HashMap<Pubkey, (Arc<CachedAccount>, Slot), PubkeyHasherBuilder> =
            HashMap::with_capacity_and_hasher(cached_pubkeys.len(), PubkeyHasherBuilder::default());
        for pubkey in cached_pubkeys {
            if config.is_aborted() {
                break;
            }

            if let Some((cached_account, slot, _)) =
                self.accounts_cache.load_latest(&pubkey, ancestors)
            {
                cache_versions.insert(pubkey, (cached_account, slot));
            }
        }

        // Step 2: Scan the persistent backend. For each pubkey, return the newest
        // version found in either storage or the cache. If both versions are the
        // same, use the cached version to avoid a redundant load from storage.
        self.backend.for_each_account(|pubkey, account, slot| {
            if config.is_aborted() {
                return std::ops::ControlFlow::Break(());
            }
            if let Some((cached_account, cache_slot)) = cache_versions.remove(pubkey) {
                if cache_slot >= slot {
                    scan_func(Some((pubkey, cached_account.account.clone(), cache_slot)));
                    return std::ops::ControlFlow::Continue(());
                }
            }
            scan_func(Some((pubkey, account, slot)));
            std::ops::ControlFlow::Continue(())
        });

        // Step 3: Call scan_func on cache-only entries — pubkeys that exist in the cache but not
        // in the accounts index at all.
        for (pubkey, (cached_account, slot)) in cache_versions {
            if config.is_aborted() {
                break;
            }
            scan_func(Some((&pubkey, cached_account.account.clone(), slot)));
        }

        // Check whether the bank was removed while the scan was in progress.
        if scan_guard.was_scan_corrupted() {
            return Err(ScanError::SlotRemoved {
                slot: ancestors.max_slot(),
                bank_id,
            });
        }
        Ok(())
    }

    pub(crate) fn index_scan_accounts<F>(
        &self,
        ancestors: &Ancestors,
        bank_id: BankId,
        index_key: IndexKey,
        mut scan_func: F,
        config: &ScanConfig,
    ) -> ScanResult<bool>
    where
        F: FnMut(Option<(&Pubkey, AccountSharedData, Slot)>),
    {
        let key = match &index_key {
            IndexKey::ProgramId(key) => key,
            IndexKey::SplTokenMint(key) => key,
            IndexKey::SplTokenOwner(key) => key,
        };
        if !self.account_indexes.include_key(key) {
            // the requested key was not indexed in the secondary index, so do a normal scan
            let used_index = false;
            self.scan_accounts(ancestors, bank_id, scan_func, config)?;
            return Ok(used_index);
        }

        // Register this scan so that slots needed by the scan are not cleaned out from under us.
        let scan_guard = ScanGuard::try_new(&self.scan_tracker, bank_id, || {
            self.max_root.load(Ordering::Acquire)
        })
        .ok_or(ScanError::SlotRemoved {
            slot: ancestors.max_slot(),
            bank_id,
        })?;

        // If the scan's ancestors are all rooted, drop them and scan roots only
        // Scan Guard max root must be used as the scan guard guarantees that
        // the account state as of max root is persisted in the database
        let max_root_ancestors = Ancestors::from(vec![scan_guard.max_root()]);
        let ancestors = if scan_guard.should_use_ancestors(ancestors) {
            ancestors
        } else {
            &max_root_ancestors
        };

        for pubkey in self.get_index_key_pubkeys(&index_key) {
            if config.is_aborted() {
                break;
            }
            let account = self.do_load(
                ancestors,
                &pubkey,
                LoadHint::Unspecified,
                PopulateReadCache::False,
            );

            if let Some((account, slot)) = account {
                scan_func(Some((&pubkey, account, slot)));
            }
        }

        // Check whether the bank was removed while the scan was in progress.
        if scan_guard.was_scan_corrupted() {
            return Err(ScanError::SlotRemoved {
                slot: ancestors.max_slot(),
                bank_id,
            });
        }
        let used_index = true;
        Ok(used_index)
    }

    /// Scan all accounts in the write cache for `slot`. Panics if the slot is not in the cache.
    pub fn scan_slot_cache<R>(
        &self,
        slot: Slot,
        func: impl Fn(&LoadedAccount) -> Option<R> + Sync,
    ) -> Vec<R>
    where
        R: Send,
    {
        let slot_cache = self
            .accounts_cache
            .slot_cache(slot)
            .expect("slot must be in the write cache");
        if slot_cache.len() > SCAN_SLOT_PAR_ITER_THRESHOLD {
            self.thread_pool_foreground.install(|| {
                slot_cache
                    .par_iter()
                    .filter_map(|cached_account| {
                        func(&LoadedAccount::Cached(Cow::Borrowed(
                            cached_account.value(),
                        )))
                    })
                    .collect()
            })
        } else {
            slot_cache
                .iter()
                .filter_map(|cached_account| {
                    func(&LoadedAccount::Cached(Cow::Borrowed(
                        cached_account.value(),
                    )))
                })
                .collect()
        }
    }

    pub fn enable_bank_drop_callback(&self) {
        self.is_bank_drop_callback_enabled
            .store(true, Ordering::Release);
    }

    /// This should only be called after the `Bank::drop()` runs in bank.rs, See BANK_DROP_SAFETY
    /// comment below for more explanation.
    /// * `is_serialized_with_abs` - indicates whether this call runs sequentially
    ///   with all other accounts_db relevant calls, such as shrinking, purging etc.,
    ///   in accounts background service.
    pub fn purge_slot(&self, slot: Slot, bank_id: BankId, is_serialized_with_abs: bool) {
        if self.is_bank_drop_callback_enabled.load(Ordering::Acquire) && !is_serialized_with_abs {
            panic!(
                "bad drop callpath detected; Bank::drop() must run serially with other logic in \
                 ABS like clean_accounts()"
            )
        }

        // BANK_DROP_SAFETY: Because this function only runs once the bank is dropped,
        // we know that there are no longer any ongoing scans on this bank, because scans require
        // and hold a reference to the bank at the tip of the fork they're scanning. Hence it's
        // safe to remove this bank_id from the `removed_bank_ids` list at this point.
        if self
            .scan_tracker
            .removed_bank_ids
            .lock()
            .unwrap()
            .remove(&bank_id)
        {
            // If this slot was already cleaned up, no need to do any further cleans
            return;
        }

        self.purge_slots(std::iter::once(&slot));
    }

    /// Purges every slot in `removed_slots` from both the cache and storage. This includes
    /// entries in the accounts index, cache entries, and any backing storage entries.
    fn purge_slots_from_cache_and_store<'a>(
        &self,
        removed_slots: impl Iterator<Item = &'a Slot> + Clone,
        purge_stats: &PurgeStats,
    ) {
        let mut remove_cache_elapsed_across_slots = 0;
        let mut num_cached_slots_removed = 0;
        let mut total_removed_cached_bytes = 0;
        for remove_slot in removed_slots {
            // This function is only currently safe with respect to `flush_slot_cache()` because
            // both functions run serially in AccountsBackgroundService.
            let mut remove_cache_elapsed = Measure::start("remove_cache_elapsed");
            // Note: we cannot remove this slot from the slot cache until we've removed its
            // entries from the accounts index first. This is because `scan_accounts()` relies on
            // holding the index lock, finding the index entry, and then looking up the entry
            // in the cache. If it fails to find that entry, it will panic in `get_loaded_account()`
            if let Some(slot_cache) = self.accounts_cache.slot_cache(*remove_slot) {
                // If the slot is still in the cache, remove the backing storages for
                // the slot and from the Accounts Index
                num_cached_slots_removed += 1;
                total_removed_cached_bytes += slot_cache.total_bytes();
                remove_cache_elapsed.stop();
                remove_cache_elapsed_across_slots += remove_cache_elapsed.as_us();
                // Nobody else should have removed the slot cache entry yet
                assert!(self.accounts_cache.remove_slot(*remove_slot).is_some());
            }
            // Unrooted slots are never flushed to storage, so if not in cache there is nothing to do.
        }

        purge_stats
            .remove_cache_elapsed
            .fetch_add(remove_cache_elapsed_across_slots, Ordering::Relaxed);
        purge_stats
            .num_cached_slots_removed
            .fetch_add(num_cached_slots_removed, Ordering::Relaxed);
        purge_stats
            .total_removed_cached_bytes
            .fetch_add(total_removed_cached_bytes, Ordering::Relaxed);
    }

    fn purge_slots<'a>(&self, slots: impl Iterator<Item = &'a Slot> + Clone) {
        // `add_root()` should be called first
        let mut safety_checks_elapsed = Measure::start("safety_checks_elapsed");
        let non_roots = slots
            // Only safe to check when there are duplicate versions of a slot
            // because ReplayStage will not make new roots before dumping the
            // duplicate slots first. Thus we will not be in a case where we
            // root slot `S`, then try to dump some other version of slot `S`, the
            // dumping has to finish first
            //
            // Also note roots are never removed via `remove_unrooted_slot()`, so
            // it's safe to filter them out here as they won't need deletion from
            // self.scan_tracker.removed_bank_ids in
            // `purge_slots_from_cache_and_store()`.
            .filter(|slot| !self.cache_roots.read().unwrap().contains(slot));
        safety_checks_elapsed.stop();
        self.external_purge_slots_stats
            .safety_checks_elapsed
            .fetch_add(safety_checks_elapsed.as_us(), Ordering::Relaxed);
        self.purge_slots_from_cache_and_store(non_roots, &self.external_purge_slots_stats);
        self.external_purge_slots_stats
            .report("external_purge_slots_stats", Some(1000));
    }

    pub fn remove_unrooted_slots(&self, remove_slots: &[(Slot, BankId)]) {
        let cache_roots = self.cache_roots.read().unwrap();
        let rooted_slots: Vec<Slot> = remove_slots
            .iter()
            .map(|(slot, _)| slot)
            .filter(|slot| cache_roots.contains(slot))
            .copied()
            .collect();
        drop(cache_roots);
        assert!(
            rooted_slots.is_empty(),
            "Trying to remove accounts for rooted slots {rooted_slots:?}"
        );

        // Mark down these slots are about to be purged so that new attempts to scan these
        // banks fail, and any ongoing scans over these slots will detect that they should abort
        // their results
        {
            let mut locked_removed_bank_ids = self.scan_tracker.removed_bank_ids.lock().unwrap();
            for (_slot, remove_bank_id) in remove_slots.iter() {
                locked_removed_bank_ids.insert(*remove_bank_id);
            }
        }

        let remove_unrooted_purge_stats = PurgeStats::default();
        self.purge_slots_from_cache_and_store(
            remove_slots.iter().map(|(slot, _)| slot),
            &remove_unrooted_purge_stats,
        );
        remove_unrooted_purge_stats.report("remove_unrooted_slots_purge_slots_stats", None);
    }

    pub fn add_root(&self, slot: Slot) -> AccountsAddRootTiming {
        let mut index_time = Measure::start("index_add_root");
        self.cache_roots.write().unwrap().insert(slot);
        self.max_root.fetch_max(slot, Ordering::Release);
        index_time.stop();
        let mut cache_time = Measure::start("cache_add_root");
        self.accounts_cache.add_root(slot);
        cache_time.stop();

        AccountsAddRootTiming {
            index_us: index_time.as_us(),
            cache_us: cache_time.as_us(),
        }
    }

    /// Resolve a proposed clean-root against any in-flight scans.
    /// Cleaning may not advance past the oldest live scan.
    pub(crate) fn max_clean_root(&self, proposed_clean_root: Option<Slot>) -> Option<Slot> {
        match (
            self.scan_tracker.min_ongoing_scan_root(),
            proposed_clean_root,
        ) {
            (None, None) => None,
            (Some(min_scan_root), None) => Some(min_scan_root),
            (None, Some(proposed_clean_root)) => Some(proposed_clean_root),
            (Some(min_scan_root), Some(proposed_clean_root)) => {
                Some(std::cmp::min(min_scan_root, proposed_clean_root))
            }
        }
    }

    // Purge zero lamport accounts and older rooted account states as garbage
    // collection
    // Only remove those accounts where the entire rooted history of the account
    // can be purged because there are no live append vecs in the ancestors
    pub fn clean_accounts(&self, max_clean_root_inclusive: Option<Slot>, is_startup: bool) {
        let _guard = self.active_stats.activate(ActiveStatItem::Clean);
        let resolved_max_clean_root = self.max_clean_root(max_clean_root_inclusive);
        self.backend
            .clean_accounts(resolved_max_clean_root, is_startup);
    }

    /// Call clean_accounts() with the common parameters that tests/benches use.
    pub fn clean_accounts_for_tests(&self) {
        self.clean_accounts(None, false)
    }

    /// Calculates the `AccountLtHash` of `account`
    pub fn lt_hash_account(account: &impl ReadableAccount, pubkey: &Pubkey) -> AccountLtHash {
        AppendVecBackend::lt_hash_account(account, pubkey)
    }

    /// Calculates the accounts lt hash
    ///
    /// Only intended to be called at startup (or by tests).
    /// Only intended to be used while testing the experimental accumulator hash.
    pub fn calculate_accounts_lt_hash_at_startup_from_index(
        &self,
        ancestors: &Ancestors,
    ) -> AccountsLtHash {
        self.backend
            .calculate_accounts_lt_hash_at_startup_from_index(ancestors)
    }

    /// Calculates the capitalization
    ///
    /// Panics if capitalization overflows a u64.
    ///
    /// Note, this is *very* expensive!  It walks the whole accounts index,
    /// account-by-account, summing each account's balance.
    ///
    /// Only intended to be called at startup by ledger-tool or tests.
    pub fn calculate_capitalization_at_startup_from_index(&self, ancestors: &Ancestors) -> u64 {
        self.backend
            .calculate_capitalization_at_startup_from_index(ancestors)
    }

    /// Returns all of the accounts' pubkeys for a given slot
    pub fn get_pubkeys_for_slot(&self, slot: Slot) -> Vec<Pubkey> {
        self.scan_slot_cache(slot, |loaded_account| Some(*loaded_account.pubkey()))
    }

    /// Return all of the accounts for a given slot
    pub fn get_pubkey_account_for_slot(&self, slot: Slot) -> Vec<(Pubkey, AccountSharedData)> {
        self.scan_slot_cache(slot, |loaded_account| {
            Some((*loaded_account.pubkey(), loaded_account.take_account()))
        })
    }

    /// Returns the latest full snapshot slot
    pub fn latest_full_snapshot_slot(&self) -> Option<Slot> {
        self.latest_full_snapshot_slot.read()
    }

    /// Sets the latest full snapshot slot to `slot`. Mirrored to the backend
    /// where clean/shrink consult the value to gate purging of older state.
    pub fn set_latest_full_snapshot_slot(&self, slot: Slot) {
        *self.latest_full_snapshot_slot.lock_write() = Some(slot);
        self.backend
            .as_append_vec()
            .set_latest_full_snapshot_slot(slot);
    }

    pub fn write_snapshot_archive_entries(
        &self,
        base_slot: Option<Slot>,
        snapshot_slot: Slot,
        callback: impl FnMut(&str, &mut dyn std::io::Read, u64) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        self.backend
            .write_snapshot_archive_entries(base_slot, snapshot_slot, callback)
    }

    pub fn save_to_snapshot_dir(
        &self,
        snapshot_dir: &std::path::Path,
        slot: Slot,
    ) -> std::io::Result<()> {
        self.backend.save_to_snapshot_dir(snapshot_dir, slot)
    }

    pub fn generate_index(
        &self,
        limit_load_slot_count_from_snapshot: Option<usize>,
        verify: bool,
    ) -> IndexGenerationInfo {
        let info = self
            .backend
            .generate_index(limit_load_slot_count_from_snapshot, verify);

        if !self.account_indexes.is_empty() || self.accounts_update_notifier.is_some() {
            let geyser_notifier = self
                .accounts_update_notifier
                .as_ref()
                .filter(|n| n.snapshot_notifications_enabled());
            self.backend.for_each_account(|pubkey, account, slot| {
                self.update_secondary_indexes(pubkey, &account);
                if let Some(notifier) = geyser_notifier {
                    let account_for_geyser = AccountForGeyser {
                        pubkey,
                        lamports: account.lamports(),
                        owner: account.owner(),
                        executable: account.executable(),
                        rent_epoch: account.rent_epoch(),
                        data: account.data(),
                    };
                    notifier.notify_account_restore_from_snapshot(
                        slot,
                        0,
                        &account_for_geyser,
                    );
                }
                std::ops::ControlFlow::Continue(())
            });
            if let Some(geyser_notifier) = &self.accounts_update_notifier {
                geyser_notifier.notify_end_of_restore_from_snapshot();
            }
        }

        self.log_secondary_indexes();

        info
    }
}

// These functions/fields are only usable from a dev context (i.e. tests and benches)
#[cfg(feature = "dev-context-only-utils")]
impl AccountsDb {
    pub fn default_for_tests() -> Self {
        Self::new_single_for_tests()
    }

    pub fn new_single_for_tests() -> Self {
        AccountsDb::new_for_tests(Vec::new())
    }

    pub fn new_single_for_tests_with_provider_and_config(
        file_provider: AccountsFileProvider,
        accounts_db_config: AccountsDbConfig,
    ) -> Self {
        AccountsDb::new_for_tests_with_provider_and_config(
            Vec::new(),
            file_provider,
            accounts_db_config,
        )
    }

    pub fn new_for_tests(paths: Vec<PathBuf>) -> Self {
        Self::new_for_tests_with_provider_and_config(
            paths,
            AccountsFileProvider::default(),
            ACCOUNTS_DB_CONFIG_FOR_TESTING,
        )
    }

    fn new_for_tests_with_provider_and_config(
        paths: Vec<PathBuf>,
        accounts_file_provider: AccountsFileProvider,
        accounts_db_config: AccountsDbConfig,
    ) -> Self {
        AccountsDb::new_with_config(
            paths,
            AccountsDbConfig {
                accounts_file_provider,
                ..accounts_db_config
            },
            None,
            Arc::default(),
        )
    }

    /// note this returns Some for accounts with zero lamports
    /// Note that this is non-deterministic if clean is running asynchronously.
    /// If a zero lamport account exists in the index, then Some is returned.
    /// Once it is cleaned from the index, None is returned.
    pub fn assert_load_account(&self, slot: Slot, pubkey: Pubkey, expected_lamports: u64) {
        let ancestors = Ancestors::from(vec![slot]);
        let (account, slot) = self.load_without_fixed_root(&ancestors, &pubkey).unwrap();
        assert_eq!((account.lamports(), slot), (expected_lamports, slot));
    }

    pub fn assert_not_load_account(&self, slot: Slot, pubkey: Pubkey) {
        let ancestors = Ancestors::from(vec![slot]);
        let load = self.load_without_fixed_root(&ancestors, &pubkey);
        assert!(load.is_none(), "{load:?}");
    }

    /// Is `pubkey` in the db?
    pub fn contains(&self, pubkey: &Pubkey) -> bool {
        self.accounts_cache.contains_pubkey(pubkey) || self.backend.contains_pubkey(pubkey)
    }

    pub fn check_accounts(&self, pubkeys: &[Pubkey], slot: Slot, num: usize, count: usize) {
        let ancestors = Ancestors::from(vec![slot]);
        for _ in 0..num {
            let idx = rng().random_range(0..num);
            let account = self.load_without_fixed_root(&ancestors, &pubkeys[idx]);
            let account1 = Some((
                AccountSharedData::new(
                    (idx + count) as u64,
                    0,
                    AccountSharedData::default().owner(),
                ),
                slot,
            ));
            assert_eq!(account, account1);
        }
    }

    // Store accounts for tests. For zero-lamport accounts, first store a single-lamport
    // placeholder, then store the actual account. This is to ensure that an index entry is created
    // for zero-lamport accounts.
    pub fn store_for_tests<'a>(&self, accounts: impl StorableAccounts<'a>) {
        let slot = accounts.target_slot();

        let placeholder = AccountSharedData::new(1, 0, &Pubkey::default());

        // Build a list of zero-lamport accounts not present in the index
        let mut pre_populate_zero_lamport = Vec::new();
        for i in 0..accounts.len() {
            if accounts.is_zero_lamport(i) {
                let key = *accounts.pubkey(i);
                if !self.backend.contains_pubkey(&key) {
                    pre_populate_zero_lamport.push((key, placeholder.clone()));
                }
            }
        }

        self.store_accounts_unfrozen(
            (slot, pre_populate_zero_lamport.as_slice()),
            UpdateIndexThreadSelection::PoolWithThreshold,
            None,
        );

        self.store_accounts_unfrozen(
            accounts,
            UpdateIndexThreadSelection::PoolWithThreshold,
            None,
        );
    }

    #[allow(clippy::needless_range_loop)]
    pub fn modify_accounts(&self, pubkeys: &[Pubkey], slot: Slot, num: usize, count: usize) {
        for idx in 0..num {
            let account = AccountSharedData::new(
                (idx + count) as u64,
                0,
                AccountSharedData::default().owner(),
            );
            self.store_for_tests((slot, [(&pubkeys[idx], &account)].as_slice()));
        }
    }

    pub fn create_account(
        &self,
        pubkeys: &mut Vec<Pubkey>,
        slot: Slot,
        num: usize,
        space: usize,
        num_vote: usize,
    ) {
        let ancestors = Ancestors::from(vec![slot]);
        for t in 0..num {
            let pubkey = solana_pubkey::new_rand();
            let account =
                AccountSharedData::new((t + 1) as u64, space, AccountSharedData::default().owner());
            pubkeys.push(pubkey);
            assert!(self.load_without_fixed_root(&ancestors, &pubkey).is_none());
            self.store_for_tests((slot, [(&pubkey, &account)].as_slice()));
        }
        for t in 0..num_vote {
            let pubkey = solana_pubkey::new_rand();
            let account =
                AccountSharedData::new((num + t + 1) as u64, space, &solana_vote_program::id());
            pubkeys.push(pubkey);
            let ancestors = Ancestors::from(vec![slot]);
            assert!(self.load_without_fixed_root(&ancestors, &pubkey).is_none());
            self.store_for_tests((slot, [(&pubkey, &account)].as_slice()));
        }
    }
}
