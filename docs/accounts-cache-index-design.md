# Design: Accounts cache index (authoritative cached-location index)

## Context / goal

The system currently tracks two conceptually different kinds of account data:

- **Stored**: persisted in an `AppendVec` (via an `AccountStorageEntry`), addressed by `(store_id, offset)`.
- **Cached**: in-memory write-cache (`AccountsCache` / `SlotCache`), addressed by `(slot, pubkey)` in the cache’s per-slot map.

Today both are represented inside the *primary accounts index* (the “accounts index”). The proposal is to introduce a new data structure, the **cache index**, which stores cached-location information separately and becomes the authoritative index for cached data over time.

Primary motivations:
- Improve **load/store throughput**, primarily by reducing work and contention in primary index operations and lookups.
- Enable future simplification where the primary index no longer needs **slot lists** at all (eventually), by moving cached-location resolution and cached-presence tracking into a purpose-built structure.

Non-goals (for this doc):
- Startup considerations (per requirement: during startup no data is stored in the cache).
- Changing secondary index semantics (secondary index semantics remain as today).

---

## Existing design (today)

### Data structures

1. **AccountsCache**
   - `AccountsCache` is keyed by `Slot -> SlotCache`.
   - `SlotCache` is keyed by `Pubkey -> Arc<CachedAccount>`.
   - The cache is populated by `store_accounts_unfrozen()` and drained/transitioned to storage by `flush_slot_cache_with_clean()`.

2. **Primary accounts index**
   - Per-pubkey entry includes a **slot list** of versions (roots + ancestor updates).
   - Each slot-list item includes enough information to locate the account data:
     - stored: `StorageLocation::AppendVec(store_id, offset)` (inside `AccountInfo`)
     - cached: `StorageLocation::Cached`
   - Fork selection is done by scanning the slot list for the **latest visible** slot under `(ancestors, max_root)` rules.

### Load path (high level)

- Load reads primary index to select `(slot, location)` for a pubkey given `ancestors` and `max_root`.
- If location is cached, it reads `(slot, pubkey)` from `AccountsCache`.
- If stored, it reads from `AccountStorageEntry` by `(store_id, offset)`.

### Correctness under races: retry loop design

`AccountsDb::retry_to_get_account_accessor()` implements a deterministic, retry-based guarantee:
- If a reader sees a location in the index but cannot fetch it from the indicated backend, it retries by re-reading the index.
- This works because flush/shrink first write to the new backend, then update the index, then remove old backend entries.

Invariant today:
- **Index update is the serialization point** for “what data is visible” and “where to fetch it”.

---

## Proposed design: cache index

### Summary

Introduce a new in-memory index, the **cache index**, which tracks where cached account data lives. Over time, it becomes authoritative for cached location.

Key decisions (resolved):
- A pubkey **can exist in multiple cached slots** concurrently (multi-fork).
- The cache index stores cached locations via **(slot, pubkey) indirection only** (no direct payload handle).
- The cache index maintains a **per-pubkey cached-slot refcount** to support immediate clearing of entries when the pubkey no longer exists in any cached slot.
- Stage 1 performs a **double-check**: read both primary index and cache index and assert equivalence (with deterministic fallback).

### Cache index responsibilities

For each `pubkey`, cache index must answer:
1. Does this pubkey currently exist **anywhere** in the write cache?
2. What is the **newest cached slot** for this pubkey (fast-path candidate)?
3. Provide a cached location using `(slot, pubkey)` so loads can call `accounts_cache.load(slot, pubkey)`.

### Cache index data model (logical)

- `cache_index[pubkey] => CachedSlotsEntry`

Where `CachedSlotsEntry` contains:
- `newest_slot: Slot`
  Best-effort newest cached slot where this pubkey exists.
- `slot_refcount: u32`
  Number of cached slots that currently contain this pubkey.

Notes:
- `newest_slot` is an optimization hint; it is not sufficient by itself to guarantee visibility under `(ancestors, max_root)` because the newest cached slot may be on a different fork.
- `slot_refcount` is required so that when a specific `(slot, pubkey)` is removed (flush/purge), the cache index can delete `cache_index[pubkey]` immediately once it reaches 0.

### Cached location representation

Cached location is represented as **(slot, pubkey)** indirection:
- cache index stores `slot` (pubkey is implied by the key)
- load uses `accounts_cache.load(slot, pubkey)`

This keeps lifetime/eviction simple and aligns with the existing retry-on-miss correctness model.

---

## Operations

### Store to cache (unrooted writes)

On storing `(pubkey, slot)` into `AccountsCache`:

Required update ordering (resolved):
1. Write payload to `AccountsCache` (payload exists first).
2. Update cache index:
   - increment `slot_refcount`
   - update `newest_slot = max(newest_slot, slot)` (or equivalent)
3. Update primary index (existing Stage-dependent behavior).
4. Update secondary indexes (same semantics as today; ordering relative to index updates stays consistent with current behavior).

Rationale:
- This ordering ensures that if a reader observes the primary index indicating cached data, the cache index is already able to resolve it (or deterministically miss and retry).

### Flush from cache to storage (rooted transition)

Flush remains “write storage, update index, then remove cache”, with immediate cache-index clearing:

1. Write alive accounts to AppendVec (storage).
2. Update primary index stored-location fields.
3. Remove cached payload from `AccountsCache`.
4. Update cache index by decrementing `slot_refcount` for each pubkey removed from that slot; delete `cache_index[pubkey]` when refcount reaches 0 (“clear immediately”).

Correctness requirement:
- Any reader failing to fetch from cache due to a race must be able to retry and observe the stored location via the primary index, as today.

### Purge / remove unrooted slots

When a cached slot is purged:
- Remove entries from the primary index (existing behavior).
- Remove cached payload from AccountsCache.
- Decrement cache-index refcounts for each removed `(slot, pubkey)`; delete when refcount reaches 0.

---

## Load behavior (migration)

### Stage 1 (fast-path verifier): double-check and assert equivalence

Stage 1 does not change correctness behavior; it verifies cache-index maintenance.

When the primary index selects a cached version at slot `S`:
1. Baseline resolution: `accounts_cache.load(S, pubkey)`
2. Cache-index resolution: `cache_index.get(pubkey) => newest_slot S'`, then `accounts_cache.load(S', pubkey)`
3. Validation behavior:
   - If `S == S'`: both should succeed and yield identical account data (modulo expected retry-on-race).
   - If `S != S'`: record/diagnose (metrics + debug assert) and **fall back deterministically** to baseline `(S, pubkey)` for correctness.

This yields the Stage-1 measurements needed to understand how often “newest cached slot” aligns with “visible cached slot”.

### Stage 2 (authoritative cached-location resolution)

Once Stage 1 shows cache index is consistent:
- For cached loads, cached-location resolution becomes authoritative via cache index (still using the primary index for visibility/slot selection until later stages).
- Primary index may still record cached-ness/slot list, but it no longer needs to serve as the detailed cached location source.

### Stage 3 (reduce slot-list reliance for cached updates)

- Stop inserting cached versions into primary index slot lists (or keep minimal metadata).
- Primary index focuses on rooted stored versions and required correctness metadata.

### Stage 4 (future): remove slot lists entirely

- Out of scope for this doc beyond acknowledging it requires additional structural work for visibility without per-pubkey slot lists.

---

## Why do this? Benefits

### 1) Performance: reduce primary-index hot-path work

- Cached updates are high-churn; representing them in primary index slot lists drives frequent writes, scans, and cleanups.
- Cache index concentrates cached-churn into a cheaper structure and simplifies cached lookup.

### 2) Separation of concerns

- Primary index: long-lived persisted location + rooted visibility.
- Cache index: short-lived cached presence + cached location resolution.

### 3) Enables future simplifications

- Separating cached location and presence tracking is a prerequisite to eventual slot-list removal.

---

## Correctness / invariants (no new race tolerance)

1. **Deterministic retry on miss remains valid**
   - If a cached load fails due to flush/purge, the reader retries from the index read and resolves to the new location (as today).

2. **Immediate clearing**
   - Cache index entries are removed immediately when `slot_refcount` reaches 0.

3. **No secondary-index semantic change**
   - Secondary indexes must be updated with the same semantics as today; cache-index introduction must not alter RPC/scan-visible results.

4. **Update ordering is explicit**
   - Store-to-cache updates cache index before primary index updates.
   - Flush/purge clears cache index immediately when cache payload is removed.

---

## Metrics / validation (kept after Stage 1)

Even after Stage 1, keep low-cost counters to detect regressions:
- primary index selects cached slot `S` but cache index missing for pubkey
- cache index newest slot `S'` but `AccountsCache` missing `(S', pubkey)`
- mismatch rate `S != S'` and fallback frequency

---

## Risks / mitigations

- **New “newest_slot” mismatch frequency is high** (multi-fork):
  Mitigation: Stage 1 falls back deterministically to baseline; if mismatch is common, Stage 2 may require augmenting cache index with additional per-pubkey cached-slot metadata (beyond newest+refcount).
- **Extra overhead in Stage 1** (double reads):
  Mitigation: gate Stage 1 behind a feature flag and/or sampling, and remove the double-check once validated.
- **Refcount correctness** (must decrement on every cache removal path):
  Mitigation: centralize cache-slot removal hooks and add debug assertions on underflow/missing entries; keep mismatch metrics.

---

## Phase 1: Implementation plan (Stage 1 verifier)

### Objectives
- Introduce a cache-resident index (`AccountsCacheIndex`) that tracks cached presence for each pubkey.
- Maintain it on all cache write/removal paths.
- Add a **load-path verifier** that compares:
  - Primary index selected cached slot `S`
  - Cache-index hinted newest cached slot `S'`
- Preserve correctness via deterministic fallback: **always trust `S`** in Phase 1.

### Deliverables
1. `AccountsCacheIndex` data structure with:
   - `DashMap<Pubkey, CachedSlotsEntry { newest_slot: Slot, slot_refcount: u32, newest_slot_stale: bool }>`
2. Wiring:
   - store-to-cache: payload → cache_index → primary index
   - flush/purge: remove payload → decrement cache_index refcounts (delete on 0)
3. Load verifier:
   - only when `StorageLocation::Cached` is selected by primary index
   - metrics + (optional) sampled double-read
4. Telemetry:
   - mismatch and missing counters + datapoints
5. Tests:
   - unit tests for refcount correctness
   - integration tests that store the same pubkey in multiple unrooted slots and confirm no correctness change

### Detailed steps

#### 1) Add cache index type
- Location: `accounts-db/src/accounts_index/cache_index.rs` (new module)
- API:
  - `on_store(slot, pubkey)`
  - `on_remove(slot, pubkey)` (debug assert on underflow)
  - `get_newest(pubkey) -> Option<Slot>` (may return stale; Phase 1 safe)
  - `stats()` counters (atomic)

#### 2) Add it to `AccountsDb`
- Add field: `cache_index: AccountsCacheIndex`
- Construct in `AccountsDb::new_with_config()`

#### 3) Store-to-cache wiring (`store_accounts_unfrozen`)
- After `write_accounts_to_cache()` returns infos, update cache index for each pubkey stored:
  - `cache_index.on_store(slot, pubkey)`
- Keep ordering: payload first, cache index second, primary index third.

#### 4) Flush wiring (`do_flush_slot_cache`)
- Collect pubkeys as you already do for purge/uncleaned management.
- After payload removal (or during the same critical section), call:
  - `cache_index.on_remove(slot, pubkey)` for every pubkey removed from that slot.
- This should run for both:
  - full slot flush
  - clean-driven partial flushes (if any pubkeys remain cached, they must *not* be decremented)

#### 5) Purge wiring (`purge_slot_cache_pubkeys`)
- When purging a cached slot, we already iterate pubkeys.
- Extend to also decrement cache_index for every `(slot,pubkey)` purged.

#### 6) Load-path verifier (Phase 1)
- In the cached-load path inside `AccountsDb::retry_to_get_account_accessor()` (or the narrowest helper that runs when primary index selected cached):
  - Let `S` be the slot selected by primary index.
  - Read `S' = cache_index.get_newest(pubkey)`.
  - Metrics:
    - missing if `S' == None`
    - mismatch if `S' != Some(S)`
    - match otherwise
  - Optional sampled double-read:
    - if `S' != Some(S)` and `S'.is_some()`: attempt `accounts_cache.load(S', pubkey)` and record hit/miss
  - Correctness: still read from `(S,pubkey)` only.

### Metrics (Phase 1)
- `cache_index_primary_cached_selected_total`
- `cache_index_missing_for_pubkey_total`
- `cache_index_newest_slot_mismatch_total`
- `cache_index_hint_cache_miss_total` (when `load(S',pubkey)` is None)
- `cache_index_refcount_underflow_total`

### Definition of done
- All existing tests pass.
- New tests validate refcount delete-on-zero and multi-fork mismatch scenarios.
- Metrics appear in logs; mismatch rate can be evaluated without correctness changes.
