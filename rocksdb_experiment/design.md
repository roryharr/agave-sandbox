# RocksDB Experiment — Trait Design

## Context / Invariants

On this branch (`rocks_db_experiment_baseline`), AccountsIndex entries are always "normal":
- Slot list always has exactly one entry
- Ref count always 1
- Entries are never cached

These are facts about the branch, not encoded in the traits — the traits are general.

The **AccountsCache is preserved** as a layer above the backend. All traits below only
ever see frozen data. `store` is called on cache flush, not on every write.

---

## Level 1 — Bin Store Trait

Per-bin disk backing store (replaces `BucketMap`/`BucketApi`). No reclaim return value —
the bin doesn't know about AppendVec and RocksDB has no native atomic put-and-return-old.

```rust
trait AccountsBinStore {
    fn get(&self, pubkey: &Pubkey) -> Option<(Slot, AccountInfo)>;
    fn insert(&self, pubkey: &Pubkey, slot: Slot, info: AccountInfo);
    fn insert_batch(&self, entries: impl Iterator<Item = (Pubkey, Slot, AccountInfo)>);
    fn remove(&self, pubkey: &Pubkey);
    fn iter<F>(&self, callback: F) where F: FnMut(&Pubkey, Slot, AccountInfo);
}
```

`insert_batch` exists for startup/snapshot restore performance (write batches in RocksDB).

---

## Level 2 — Full Index Trait

Full index across all pubkeys (replaces `AccountsIndex`). `insert` returns the displaced entry
so the caller can reclaim the old AppendVec slot. `Option` (not `Vec`) is correct because the
branch invariant guarantees at most one existing entry per pubkey.

```rust
trait AccountsIndexTrait {
    fn get(&self, pubkey: &Pubkey) -> Option<(Slot, AccountInfo)>;
    fn insert(&self, pubkey: &Pubkey, slot: Slot, info: AccountInfo) -> Option<(Slot, AccountInfo)>;
    fn insert_batch(&self, entries: impl Iterator<Item = (Pubkey, Slot, AccountInfo)>);
    fn remove(&self, pubkey: &Pubkey);
    fn iter<F>(&self, callback: F) where F: FnMut(&Pubkey, Slot, AccountInfo);
}
```

Root tracking and scan (ancestor-filtered) are Level 3 concerns, not here.

---

## Level 3 — Full Backend Trait

Replaces the entire AppendVec storage + AccountsIndex together. The backend owns all
frozen account data. The AccountsCache sits above this and handles unfrozen slots.

```rust
trait AccountsBackend {
    fn load(&self, pubkey: &Pubkey) -> Option<(AccountSharedData, Slot)>;
    fn store(&self, slot: Slot, accounts: &[(&Pubkey, &AccountSharedData)]);
    fn snapshot(&self, path: &Path) -> Result<()>;
    fn for_each_account(&self, f: impl FnMut(&Pubkey, AccountSharedData, Slot));
}
```

No `ancestors` in `load` — unfrozen accounts are handled by the cache before reaching
the backend. Slot lifecycle is a higher-level concern managed above this trait.
No shrink/compaction methods — for RocksDB that's handled internally.

`for_each_account` yields every persisted account (latest version per pubkey). Used by the
coordinator's `scan_accounts` to merge backend accounts with cache contents — the coordinator
owns scan registration, cache merging, ancestor visibility filtering, and abort checking; the
backend only provides raw iteration. AppendVec implements it by walking `accounts_index` +
loading from storage. RocksDB implements it with a snapshot iterator — no separate index needed.

### Constructors (not on the trait)

Two restore paths exist as constructors on the concrete backend type:

```rust
// Path 1: restore from a backend-native snapshot (e.g. RocksDB checkpoint)
fn from_checkpoint(path: &Path) -> Result<Self>;

// Path 2: restore from AppendVec snapshot — needed for as long as the
// network produces AppendVec snapshots
fn from_accounts(accounts: impl Iterator<Item = (Pubkey, Slot, AccountSharedData)>) -> Result<Self>;
```

The iterator may contain duplicate pubkeys from different slots (the same account written
across multiple rooted slots). The backend resolves duplicates internally via read-before-write,
keeping the entry with the higher slot. The iterator is unordered to preserve parallel I/O
across storage files during snapshot loading.

---

## Pre-work Status

### Level 1

The BucketApi method calls map cleanly to `AccountsBinStore` (`read_value` → `get`,
`delete_key` → `remove`, `batch_insert_non_duplicates` → `insert_batch`).

The real pre-work is the **eviction/aging infrastructure** in `InMemAccountsIndex` —
`try_make_entry_for_flush`, the flush loop, and age tracking all exist to manage what lives
in memory vs on the BucketApi disk layer. For a RocksDB-backed bin store, RocksDB manages
its own block cache internally and eviction is unnecessary. This machinery needs to be
removable or bypassable rather than unconditionally baked into `InMemAccountsIndex`.

**Status: COMPLETE — `disable_eviction: bool` added to `AccountsIndexConfig` and `BucketMapHolder`. When true, `should_flush()` returns false, making `flush_internal` a no-op. Default is false; no behavior change on existing paths.**

---

### Level 2

All pre-work complete. The external surface of `AccountsIndex` as called from `accounts_db.rs`
has been reduced to the 5 trait methods plus internal implementation details.

**DONE:**
- Root tracking split into two pieces along the cache/storage seam:
  - `cache_roots: HashSet<Slot>` lives on `AccountsDb` — slots that are rooted but still
    in the write cache (not yet flushed). Inserted on root, removed at flush.
  - `StorageRoots` (in `accounts-db/src/storage_roots.rs`) lives on `AppendVecBackend` —
    slots whose append vecs are alive on disk. Backend-internal; per-backend.
  - `AccountsDb::is_alive_root(slot)` composes them: `cache_roots` ∪ backend's storage roots.
  Both pieces collectively replace the original monolithic `RootsTracker`.
- Multi-version cleaning deleted (`clean_rooted_entries`, `purge_older_root_entries`, etc.).
- `upsert` → `insert` (returns displaced `Option<(Slot, AccountInfo)>` instead of callback/reclaims).
- `get_and_then` / `get_with_and_then` / `contains` → `get`.
- `scan` per-pubkey calls → `get()`; moved to `#[cfg(test)]`.
- Secondary indexes (`program_id_index`, `spl_token_mint_index`, `spl_token_owner_index`)
  moved from `AccountsIndex` to `AccountsDb`. `handle_dead_keys` no longer takes
  `account_indexes`; caller handles secondary purge using the returned removed-key set.
- `ref_count_from_storage` → `#[cfg(test)]`.
- `bins()` → `pub(crate)`.

**Next: Step 6 — introduce the trait.**

---

### Level 3

Inherits everything from Level 2 (Level 3 also replaces AccountsIndex), plus the entire
storage layer:

**Approach: extract into modules with thin traits, not deletion.**

Shrink and clean are AppendVec-specific but must stay intact for the mainnet path. Rather than
deleting them, each is extracted into its own submodule of `accounts_db` and a thin trait
is exposed. The RocksDB implementation will be no-ops. Further abstraction within the modules
is feasible later if needed.

**DONE:**
- `accounts-db/src/accounts_db/clean.rs` — all clean logic + `AccountsClean` trait (1 method)
- `accounts-db/src/accounts_db/shrink.rs` — all shrink logic + `AccountsShrink` trait (3 methods)
- Both use `use super::*;` and contain `impl AppendVecBackend` blocks; trait impls delegate to inherent methods
- `accounts_db.rs` re-exports moved types (`AliveAccounts`, `ShrinkCollect`, etc.) for `ancient_append_vecs.rs`

**Shrink — `accounts-db/src/accounts_db/shrink.rs`**

```rust
pub(crate) trait AccountsShrink {
    fn shrink_candidate_slots(&self, epoch_schedule: &EpochSchedule) -> usize;
    fn shrink_ancient_slots(&self, epoch_schedule: &EpochSchedule);
    fn shrink_all_slots(&self, is_startup: bool, newest_slot_skip_shrink_inclusive: Option<Slot>);
}
```

AppendVec implementation: existing code in `shrink.rs`. RocksDB implementation: three no-ops.

**Clean — `accounts-db/src/accounts_db/clean.rs`**

```rust
pub(crate) trait AccountsClean {
    fn clean_accounts(&self, max_clean_root_inclusive: Option<Slot>, is_startup: bool);
}
```

AppendVec implementation: existing code in `clean.rs`. RocksDB implementation: no-op (compaction filter
handles zero-lamport removal asynchronously).

**Storage layer and direct AccountsFile access**

The remaining direct `store.accounts` accesses and `self.storage` API calls in `accounts_db.rs`
are in core load/store paths. These stay as-is — they become backend-internal when a Level 3
backend is actually written.

**Status: COMPLETE — modules created, traits defined, tests pass.**

---

### Coordinator split (`AccountsDb` wrapper)

**Status: STRUCT SPLIT COMPLETE. `impl` BLOCK SORTING COMPLETE. NARROW METHOD SURFACE EMERGING ON `AccountsBackend` ENUM.**

`AccountsDb` is split into two types:

- **`AppendVecBackend`** (`accounts-db/src/accounts_db/backend.rs`) — the AppendVec-specific backend.
  Owns: `accounts_index`, `storage`, `storage_roots` (the `StorageRoots` of slots with live
  append vecs), `paths`, shrink/clean machinery, `thread_pool_background`,
  `active_stats`, `store_accounts_frozen_stats`, `read_only_accounts_cache` (AppendVec-specific;
  RocksDB uses its own internal block cache), and all other AppendVec-specific state.
  Backend submodules under `accounts_db/backend/` (clean, shrink, ancient_append_vecs,
  storable_accounts_by_slot, geyser_plugin_utils, tests) host most AppendVec-only logic.

- **`AccountsDb`** (`accounts-db/src/accounts_db.rs`) — the top-level coordinator type used by
  all external callers (runtime, core, etc.). Owns backend-agnostic state: `accounts_cache`,
  `cache_roots` (rooted-but-not-yet-flushed slots), `max_root` (monotonic max committed root for
  scan boundaries), `write_version` (Geyser notification counter), `skip_initial_hash_calc`
  (startup-clean gate), `thread_pool_foreground`, secondary indexes, `scan_tracker`, `stats`,
  `load_account_stats`, `store_accounts_unfrozen_stats`, `accounts_update_notifier`,
  `is_bank_drop_callback_enabled`, `partitioned_epoch_rewards_config`, `bank_hash_details_dir`.
  Contains `pub backend: AccountsBackend` (an enum — see below).

The `backend` field holds an `AccountsBackend` enum:

```rust
pub enum AccountsBackend {
    AppendVec(AppendVecBackend),
    // RocksDb(RocksDbBackend),  // future
}
```

This enum is the seam for the future trait. Replacing `AccountsBackend` with a generic
`B: AccountsBackendTrait` is the remaining structural step for Level 3.

#### `AccountsBackend` API surface — COMPLETE

All coordinator-side reaches into the backend (`self.backend.as_append_vec().X`) in
`accounts_db.rs` have been replaced with narrow polymorphic methods on the `AccountsBackend`
enum. Each method is a future trait method, named for the operation rather than for AppendVec
mechanics. Final surface:

**Core data ops:**
- `store(slot, accounts, cleaning_enabled) -> FlushStats` — flush a slot's accounts.
- `load(ancestors, pubkey, load_hint, populate_read_cache) -> Option<(AccountSharedData, Slot)>`
  — load by pubkey. (For RocksDB the AppendVec-specific params will collapse.)
- `for_each_account(F: FnMut(&Pubkey, AccountSharedData, Slot) -> ControlFlow<()>)` — raw
  iteration over every persisted account. Used by the coordinator's `scan_accounts` to merge
  with cache contents. Race-skipped entries are silently elided.

**Narrow inspection:**
- `is_pubkey_alive(pubkey) -> bool` — true iff a non-zero-lamport entry exists.
- `contains_pubkey(pubkey) -> bool` — true iff any entry exists (zero-lamport or not).
- `account_count_in_slot(slot) -> usize` — number of accounts persisted for a slot.

**Maintenance:**
- `clean_accounts(max_clean_root, is_startup, active_scans, max_distance_to_min_scan_slot)` —
  caller resolves scan-tracker state first.
- `shrink(epoch_schedule, include_ancient: bool) -> usize` — periodic shrink pass.
- `purge_keys_exact(pubkey_to_slot_set) -> (Reclaims, HashSet<Pubkey>)`.

**Snapshot ops (storage-agnostic):**
- `write_snapshot_archive_entries(base_slot, snapshot_slot, callback)` — callback receives
  `(filename, &mut dyn Read, len)`; backend chooses what entries to emit.
- `save_to_snapshot_dir(snapshot_dir, slot, obsolete_accounts_path)` — fastboot snapshot.
- `generate_index(storages, verify) -> IndexGenerationInfo` — startup index build.
- `get_storages(slots)` / `all_storages()` / `latest_full_snapshot_slot()` /
  `set_latest_full_snapshot_slot(slot)` — startup helpers (some are AppendVec-shaped today).

**Telemetry:**
- `activate(item) -> ActiveStatGuard<'_>` — mark an in-progress operation.
- `report_stats()` — periodic backend-stats logging.

**Escape hatches:**
- `as_append_vec(&self) -> &AppendVecBackend` (production-visible) and
  `as_append_vec_mut(&mut self)` (`#[cfg(test/dev)]`-only). Used by AppendVec-specific tools
  (`snapshot_minimizer`) and tests; not used by the production coordinator.

`accounts_db.rs` itself contains zero `as_append_vec()` reaches outside the two method
definitions on `AccountsBackend`. The remaining `as_append_vec` calls in the codebase
are inside `#[cfg(test)]`/`#[cfg(feature = "dev-context-only-utils")]` test scaffolding, or
inside `snapshot_minimizer.rs` (a deliberate AppendVec-only dev tool).

#### Other major cleanup deltas (this branch)

- **Snapshot save path** is fully decoupled from `Arc<AccountStorageEntry>`; runtime no longer
  holds storage entries. Backend exposes `write_snapshot_archive_entries(base_slot, snapshot_slot, callback)`
  (callback receives `(filename, &mut dyn Read, len)` — storage-agnostic) and
  `save_to_snapshot_dir(snapshot_dir, slot, obsolete_accounts_path)` for fastboot.
- **Shrink API consolidation:** four public methods (`shrink_storage`, `shrink_slot_forced`,
  `shrink_candidate_slots`, `shrink_ancient_slots`, plus `shrink_all_slots`) collapsed to a single
  `shrink(epoch_schedule, include_ancient: bool) -> usize` on `AccountsDb`. `shrink_all_slots`
  was deleted entirely (validator hardcoded its skip flag and the snapshot-path replacement
  was unnecessary). The clean callback for periodic interleaving is plumbed through a
  `Fn(Option<Slot>)` parameter.
- **Dead-code chain deleted:** `AccountsDb::is_alive_root`, `AppendVecBackend::contains_root`,
  `VerifyAccountsHashConfig` struct, and `require_rooted_bank` field — all unreachable in
  production, removed along with the parent-recursion fallback in `verify_accounts`.
- **`StorableAccountsBySlot` relocation:** moved to `accounts_db/backend/storable_accounts_by_slot.rs`;
  it now holds `storage: &'a AccountStorage` instead of `db: &'a AccountsDb`, eliminating the
  dishonest dependency that blocked further leaf conversions in ancient_append_vecs.
- **Misnamed flag renamed:** `accounts_db_skip_shrink` → `accounts_db_skip_initial_clean`
  (CLI flag, ProcessOptions field, all plumbing). The flag now exclusively gates the startup
  `clean_accounts` call in `verify_snapshot_bank`.
- **Two-step `initialize_storage_and_next_id` collapsed** into the constructor:
  `AccountsDb::new_with_config_and_storage(...)` now takes pre-scanned storage + next_id.
  The `&mut self` mutate-after-construct pattern is gone.
- **`as_append_vec_mut()` is now `#[cfg(any(test, feature = "dev-context-only-utils"))]`** —
  production binaries no longer expose mutable backend access.
- **`StorageRoots` rename:** the per-backend root tracker (formerly `RootsTracker`) lives at
  `accounts-db/src/storage_roots.rs`. The split surfaces in `is_alive_root` semantics:
  `cache_roots` ∪ `storage_roots` = "rooted somewhere"; each side owns its own tracker.

---

## Open Questions — Level 3

### 1. Slot abandonment — RESOLVED

Unrooted slots are **never flushed** from the write cache to any backend (AppendVec or
RocksDB). Therefore slot abandonment requires no backend operation at all.

The full bank-drop chain — `purge_slot` → `purge_slots` →
`purge_slots_from_cache_and_store` → `purge_slot_cache_pubkeys` →
`remove_dead_slots_metadata` — only ever touches coordinator state: the write cache,
`scan_tracker.removed_bank_ids`, and backend `storage_roots`. The one backend touch is an assertion
in `purge_slot_cache_pubkeys` that no AppendVec storage exists for the slot, which just
confirms the invariant. That assertion has no RocksDB equivalent and can be dropped on that
path.

No trait method needed for slot abandonment.

### 2. lt_hash maintenance

`AccountsDb` currently maintains a running `AccountsLtHash` updated on every store. The
coordinator already holds account data at the point of calling `backend.store(...)`, so
lt_hash can be updated before the call. No change to the trait needed — this is a coordinator
concern.

### 3. Scan paths beyond snapshots — RESOLVED

`for_each_account` added to the trait (see Level 3 above). The coordinator's `scan_accounts`
owns all the orchestration logic (scan_tracker registration, cache pre-scan, merge, ancestor
filtering, abort); the backend only provides raw iteration via `for_each_account`.

- Accounts hash: replaced by lt_hash, maintained incrementally — no full scan needed.
- Secondary index population: `from_accounts` constructor iterates accounts anyway; secondary
  indexes can be populated during that pass.
- `get_filtered_program_accounts`: goes through coordinator-owned secondary indexes
  (`AccountsDb.program_id_index` etc., moved in Level 2 pre-work).

### 4. Snapshot format interop

`from_accounts` takes an iterator of `(Pubkey, Slot, AccountSharedData)` reconstructed
from AppendVec snapshot files. The coordinator needs to provide this iterator. The existing
`generate_index` path already walks all storage files in this way — it can be repurposed
as the iterator source for `from_accounts`. No new infrastructure needed.

---

## How to Build / Verify

```
cargo test -p solana-accounts-db --lib
```

---

## Next Steps

### Sort the misplaced `impl AccountsDb` blocks in `backend.rs` — DONE

All `impl AccountsDb` blocks previously in `backend.rs` and submodules have been sorted.
Methods that only touch `AppendVecBackend` fields are now `impl AppendVecBackend`. Methods
that touch coordinator fields are `impl AccountsDb` in `accounts_db.rs`.

The list below is the per-item walkthrough that was used to do this work. Legend: ⬜ pending · ✅ done.

**Reclassify as `impl AppendVecBackend`** (stay in `backend.rs`, change impl header):

- ✅ `lt_hash_account` — moved to `impl AccountsDb` in `accounts_db.rs` (no field access; external callers use `AccountsDb::lt_hash_account`)
- ✅ `hash_account_helper` — moved to `impl AccountsDb` in `accounts_db.rs` (private static helper for `lt_hash_account`)
- ✅ `read_index_for_accessor_or_load_slow` — reclassified to `impl AppendVecBackend`; depends only on `accounts_index` + `get_account_accessor`
- ✅ `remove_dead_accounts` — reclassified to `impl AppendVecBackend` (uses `storage`, `accounts_index`, `shrink_candidate_slots`); helpers `should_not_shrink`, `is_shrinking_productive`, `is_candidate_for_shrink` also moved to `impl AppendVecBackend` in `shrink.rs`
- ✅ `write_accounts_to_storage` — reclassified to `impl AppendVecBackend`; writes directly to `AccountStorageEntry`/AppendVec, calls `create_and_insert_store`
- ✅ `mark_zero_lamport_single_ref_accounts` — reclassified to `impl AppendVecBackend`; called only by `_store_accounts_frozen`
- ✅ `flush_slot_to_storage` — split: cache coordination became `flush_slot_cache` in `accounts_db.rs`; storage write became `AppendVecBackend::store()` in `backend.rs`
- ✅ `report_store_stats` — reclassified to `impl AppendVecBackend`
- ✅ `report_read_cache_stats` — reclassified to `impl AppendVecBackend`
- ✅ `update_index_stored_accounts` — reclassified to `impl AppendVecBackend`; called only from `_store_accounts_frozen`
- ✅ `_store_accounts_frozen` — reclassified to `impl AppendVecBackend`; `accounts_db` param removed after `handle_reclaims` chain moved
- ✅ `store_accounts_frozen` — reclassified to `impl AppendVecBackend`; thin wrapper around `_store_accounts_frozen` with `IgnoreReclaims`
- ✅ `handle_reclaims` — reclassified to `impl AppendVecBackend`; entire reclaim chain (`process_dead_slots`, `clean_stored_dead_slots`) moved with it
- ✅ `process_dead_slots` — reclassified to `impl AppendVecBackend` (pulled along with `handle_reclaims`)
- ✅ `remove_dead_slots_metadata` — reclassified to `impl AppendVecBackend`; updates backend `storage_roots` + `clean_accounts_stats`
- ✅ `calculate_accounts_lt_hash_at_startup_from_index` — reclassified to `impl AppendVecBackend`; `AccountsDb` keeps a forwarding wrapper; calls `AccountsDb::lt_hash_account` (static)
- ✅ `calculate_capitalization_at_startup_from_index` — reclassified to `impl AppendVecBackend`; `AccountsDb` keeps a forwarding wrapper
- ✅ `get_storages` — reclassified to `impl AppendVecBackend`; accesses only `storage`
- ✅ `latest_full_snapshot_slot` — reclassified to `impl AppendVecBackend`; reads `latest_full_snapshot_slot` field
- ✅ `set_latest_full_snapshot_slot` — reclassified to `impl AppendVecBackend`; writes `latest_full_snapshot_slot` field
- ✅ `set_storage_count_and_alive_bytes` — reclassified to `impl AppendVecBackend`; accesses only `storage`; private helper called from `generate_index`
- ✅ `print_count_and_status` — deleted entirely; callers in serde_snapshot tests removed
- ✅ `remove_dead_slot_storages_for_snapshot_minimizer` — reclassified to `impl AppendVecBackend`; accesses only `storage`
- ✅ `get_len_of_slots_with_uncleaned_pubkeys` — moved to `impl AppendVecBackend` in `clean.rs` under `#[cfg(any(test, ...))]`; callers updated to go through `backend.as_append_vec()`
- ✅ `storage_access` — moved to `impl AppendVecBackend` in `clean.rs` under `#[cfg(test)]`; caller in `storable_accounts.rs` updated to go through `backend.as_append_vec()`
- ✅ `check_storage` — moved to `impl AppendVecBackend` in `clean.rs` under `#[cfg(test)]`; serde_snapshot test calls removed; backend test callers updated
- ✅ `all_account_count_in_accounts_file` — moved to `impl AppendVecBackend` in `clean.rs` under `#[cfg(test)]`; serde_snapshot test calls removed; backend test callers updated

**Move to `impl AccountsDb` in `accounts_db.rs`** (file move + update field accesses):

- ✅ `get_account_accessor` — reclassified to `impl AppendVecBackend`; `Cached` arm stripped (`unreachable!`) since index never holds cached entries on this branch
- ✅ `purge_keys_exact` — moved to `impl AppendVecBackend`; secondary index purge relocated to `do_flush_slot_cache` in `accounts_db.rs`; `AccountsDb` keeps forwarding wrapper (runtime caller in `snapshot_minimizer.rs`)
- ✅ `scan_account_storage` — production callers (`scan_slot`, `get_pubkeys_for_slot`, `get_pubkey_account_for_slot`) replaced with `scan_slot_cache` on `AccountsDb` (asserts slot is in write cache); `scan_account_storage` / `scan_cache_storage_fallback` retained for tests that scan flushed storage
- ✅ `retry_to_get_account_accessor` — moved to `impl AppendVecBackend`; `load_limit` moved to `AppendVecBackend`, `load_delay` duplicated; made private (`fn`)
- ✅ `get` — moved to `impl AppendVecBackend`; `accounts_db.rs` caller updated to `self.backend.as_append_vec().get(...)`
- ✅ `generate_index_for_slot` — moved to `impl AppendVecBackend`; `update_secondary_indexes` and Geyser notifications stripped out; `hash_account_helper` + `lt_hash_account` also moved to `AppendVecBackend` (AccountsDb keeps forwarding wrapper); post-step added to `generate_index`: iterates all storages after index build and populates secondary indexes + fires Geyser notifications if either is enabled
- ✅ `generate_index` — core moved to `impl AppendVecBackend` taking `&[Arc<AccountStorageEntry>]`; `AccountsDb::generate_index` is now a thin wrapper that builds/sorts storages, calls `b.generate_index(...)`, runs the secondary-index + Geyser post-step, and calls `log_secondary_indexes`
- ✅ `print_index` / `print_accounts_stats` — deleted entirely; removed from `AccountsDb`, `Bank`, ledger-tool CLI flag, and all test call sites
- ✅ `get_pubkeys_for_slot` / `get_pubkey_account_for_slot` — stays on `impl AccountsDb`; both delegate to `scan_slot_cache` which uses the write cache and thread pool (coordinator concerns)
- ✅ dev-context constructors + test helpers — all stay on `impl AccountsDb`; constructors call `AccountsDb::new_with_config`, test helpers use coordinator methods (`load_without_fixed_root`, `accounts_cache`)

### `as_append_vec()` cleanup in `accounts_db.rs` — DONE

Started with ~40 `self.backend.as_append_vec().X` reaches in `accounts_db.rs`, ending with
zero (only the two method definitions for `as_append_vec` / `as_append_vec_mut` remain on
the `AccountsBackend` enum). Each site was resolved by one of:

(a) become a narrow polymorphic method on `AccountsBackend` (future trait method), or
(b) move underlying state to `AccountsDb` if it's actually a coordinator concern, or
(c) be deleted if dead, or
(d) be moved to a test-only module if it was test scaffolding reaching into AppendVec.

Selected highlights of the per-site work:

- `background_thread_pool` — caller went sequential; accessor + thread-pool exposure
  deleted entirely. Backend keeps its private pool for clean/shrink.
- `paths` — 4 tests refactored to set up explicit `accounts_dir` upfront via
  `create_tmp_accounts_dir_for_tests()` + `Bank::new_with_paths_for_tests(...)`; accessor deleted.
- `skip_initial_hash_calc`, `temp_paths`, `external_purge_slots_stats`, `write_version` —
  moved from `AppendVecBackend` to `AccountsDb` (they were coordinator concerns misplaced
  on the backend).
- `accounts_file_provider` — moved into `AccountsDbConfig`; eliminated the
  mutate-after-construct test helper. `as_append_vec_mut()` is now used only by tests.
- `is_pubkey_alive`, `contains_pubkey`, `account_count_in_slot` — added as narrow inspection
  methods on `AccountsBackend`.
- `activate`, `report_stats` — telemetry consolidated to one method each.
- `load`, `for_each_account` — the design doc's core trait operations, added directly.
  `scan_accounts` was refactored to use `for_each_account`, with the coordinator owning
  cache-merging / ancestor filtering / abort signaling.
- `clean_accounts`, `shrink`, `purge_keys_exact`,
  `calculate_accounts_lt_hash_at_startup_from_index`,
  `calculate_capitalization_at_startup_from_index`, `get_storages`, `all_storages`,
  `latest_full_snapshot_slot`, `set_latest_full_snapshot_slot`, `generate_index`,
  `write_snapshot_archive_entries`, `save_to_snapshot_dir`, `report_read_cache_stats` /
  `report_storage_stats` — added as backend delegation methods, replacing wrapper-style
  reaches.
- `is_alive_root`, `contains_root`, `VerifyAccountsHashConfig`, `require_rooted_bank`,
  `shrink_all_slots`, `alive_account_count_in_slot` — deleted as dead code or
  redundant test helpers.
- `scan_account_storage` / `scan_cache_storage_fallback` — moved from `accounts_db.rs`
  to `backend/tests.rs` as free functions (test-only with AppendVec-shaped callbacks).
- `geyser_plugin_utils.rs` — moved out of `backend/` into `accounts_db/`, since the only
  function there (`notify_account_at_accounts_update`) is a coordinator concern.
  Test `test_notify_account_restore_from_snapshot` rewritten to use the public store +
  flush API; `AppendVecBackend::generate_index` learned to reset per-storage counters
  defensively (no-op in production where storages are fresh from disk; makes the
  flush-then-generate_index test path correct).

Remaining `as_append_vec` calls in the codebase are entirely inside test scaffolding
(`backend/tests.rs`, the trailing test mods of `ancient_append_vecs.rs` and
`storable_accounts_by_slot.rs`) or inside `snapshot_minimizer.rs` (a deliberate
AppendVec-only dev tool).

### Level 2 — introduce the trait (Step 6)

The external surface is clean. Remaining work is mechanical:

1. Define `AccountsIndexTrait` in a new file `accounts-db/src/accounts_index/trait.rs` (or
   inline in `accounts_index.rs`).
2. `impl AccountsIndexTrait for AccountsIndex<AccountInfo, AccountInfo>`.
3. Change `AppendVecBackend.accounts_index` field type from `AccountsIndex<...>` to
   `Box<dyn AccountsIndexTrait>` (or a generic parameter `I: AccountsIndexTrait`).
4. Update all call sites to go through the trait methods only.

Generic parameter (`I: AccountsIndexTrait`) is preferable to `Box<dyn>` — no vtable overhead
and the compiler can still monomorphize. The concrete type is fixed per binary anyway.

### Level 3 — longer horizon

Blocked on Level 2 trait. Once `AccountsIndexTrait` exists, a RocksDB-backed implementation
can be written that implements both `AccountsIndexTrait` and `AccountsBackend` as a single
struct. The `backend: AppendVecBackend` field in `AccountsDb` becomes `backend: Box<dyn AccountsBackend>`
(or a generic). No separate Level 1 implementation is needed — Level 1 is only useful if
keeping the existing `InMemAccountsIndex` structure with a swappable disk backend.
