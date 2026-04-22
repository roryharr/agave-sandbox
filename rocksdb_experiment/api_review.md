# `AccountsBackend` API review

Tracking the remaining API surface issues to address before the enum can be cleanly
swapped to a trait. Each issue is a candidate cleanup; completing them all should
leave only storage-agnostic methods on the seam.

## Status legend

- [ ] open
- [x] done

## Issues

- [x] **`purge_keys_exact`** — only used by `snapshot_minimizer.rs` (AppendVec-only)
  + internal AppendVec callers. Doesn't need to be on the enum; minimizer reaches
  in via `as_append_vec()`.

- [x] **`load` signature is AppendVec-shaped** — *not actually an issue.*
  `LoadHint::FixedMaxRoot` ↔ `RocksDB ReadOptions::snapshot`,
  `PopulateReadCache::{True,False}` ↔ `RocksDB ReadOptions::fill_cache`.
  Both flags map cleanly onto a non-AppendVec backend. The names are already
  generic (no "AppendVec" in them). What's AppendVec-specific is the
  *implementation* (retry loop, `LoadedAccountAccessor`, etc.) — but those
  are hidden inside `AppendVecBackend::get`. The public signature is portable.
  No cleanup required.

- [x] **`get_storages` / `all_storages` leak `Arc<AccountStorageEntry>`** —
  both removed from the `AccountsBackend` enum and the `AccountsDb` wrapper.
  `all_storages` had no remaining callers (dead code). External callers of
  `get_storages` (snapshot_minimizer + snapshot tests) now reach in via
  `accounts_db.backend.as_append_vec().get_storages(..)` — same pattern used
  for other AppendVec-only paths. Internal AppendVec uses (snapshot archive
  emission, save_to_snapshot_dir, clean) stay unchanged. Unused imports
  `AccountStorageEntry` and `RangeBounds` removed from accounts_db.rs.

- [x] **`generate_index(storages, verify)`** — backend now owns storage
  collection: signature changed to `generate_index(limit_load_slot_count_from_snapshot, verify)`.
  AppendVec backend internally collects/sorts/truncates its own storages.
  The coordinator's post-pass for secondary-index population + Geyser
  notification was rewritten to walk the index via `for_each_account` instead
  of re-scanning storages — eliminating a duplicate per-storage scan and
  removing `Arc<AccountStorageEntry>` from this path entirely.

  **Contract change**: Geyser snapshot-restore notifications now emit one
  notification per *live* pubkey at its latest slot (previously: one per
  storage entry, including superseded historical versions still on disk).
  Test `test_notify_account_restore_from_snapshot` updated to reflect this.

- [x] **`clean_accounts` parameter shape** — backend signature reduced from
  5 params to 2: `(max_clean_root_inclusive, is_startup)`. Dropped:
  - `active_scans` and `max_distance_to_min_scan_slot` were pure telemetry
    (only emitted in datapoint at the end). Both removed; their datapoint
    fields removed; the `max_distance_to_min_scan_slot` field on `ScanTracker`
    removed (no remaining readers).
  - `active_stats` removed because clean is fast enough now that the
    sub-phase measurements aren't useful. Sub-phase enum variants
    (`CleanConstructCandidates` / `CleanScanCandidates` /
    `CleanCollectStoreCounts` / `CleanCalcDeleteDeps` / `CleanFilterZeroLamport`
    / `CleanReclaims`) deleted from `ActiveStatItem`. Top-level `Clean` guard
    in `AccountsDb::clean_accounts` is preserved.

- [x] **`store(cleaning_enabled)`** — flag deleted entirely. Production was
  always passing `true` (the `cleaning_enabled = false` path was reachable
  only via test helpers and via a flip-to-None branch that was unreachable
  given upstream clamping; the without-clean alternative was already
  commented out in the call site). Backend now always uses
  `UpsertReclaim::ReclaimOldSlots` and tracks only zero-lamport pubkeys in
  `uncleaned_pubkeys`. Test `test_shrink_candidate_slots_cached` updated
  by the user to reflect the new behavior (intermediate per-slot count
  asserts removed).

- [x] **`save_to_snapshot_dir(obsolete_accounts_path)`** — parameter dropped.
  Both production callers were computing `bank_snapshot_dir.join(SNAPSHOT_OBSOLETE_ACCOUNTS_FILENAME)`
  and passing it in; the value is fully determined by `snapshot_dir`. Backend
  now derives the path internally. The filename constant was added to
  `accounts-db/src/snapshot_storage.rs` (next to the existing
  `SNAPSHOT_ACCOUNTS_HARDLINKS` which was already duplicated this way) so the
  AppendVec backend can reach it without a circular dep on agave-snapshots.

- [x] **`latest_full_snapshot_slot` / `set_latest_full_snapshot_slot`** —
  enum methods deleted. `AccountsDb` now owns its own `latest_full_snapshot_slot`
  field (lifecycle state on the coordinator). `AccountsDb::set_latest_full_snapshot_slot`
  writes both the local field and the AppendVec backend's mirror (which clean
  and shrink internals continue to read pervasively without parameter
  threading). `AccountsDb::latest_full_snapshot_slot` reads from the local
  field. Future trait can decide whether the backend needs its own copy or
  takes the value as a parameter.

- [x] **`activate` returns AppendVec-internal type** — moved `ActiveStats` from
  `AppendVecBackend` onto `AccountsDb`. Top-level guards (`Clean` / `Shrink` /
  `SquashAncient` / `Flush`) now sit at coordinator scope (slightly wider area).
  Sub-phase guards inside `AppendVecBackend::clean_accounts` are preserved by
  threading `&ActiveStats` through as a parameter on `clean_accounts`.

- [x] **`account_count_in_slot`** — deleted from the `AccountsBackend` enum.
  All external callers (in runtime tests) deleted. Method body retained on
  `AppendVecBackend` gated `#[cfg(test)]` `pub(super)` for the AppendVec
  backend's own tests.
