#![cfg_attr(feature = "frozen-abi", feature(min_specialization))]
#![allow(clippy::arithmetic_side_effects)]

pub mod account_info;
pub mod account_locks;
pub mod account_storage;
pub mod account_storage_reader;
pub mod accounts;
mod accounts_cache;
pub mod accounts_db;
pub mod accounts_file;
pub mod accounts_hash;
pub mod accounts_index;
pub mod accounts_index_storage;
pub mod accounts_update_notifier_interface;
mod active_stats;
pub mod ancestors;
mod ancient_append_vecs;
#[cfg(feature = "dev-context-only-utils")]
pub mod append_vec;
#[cfg(not(feature = "dev-context-only-utils"))]
mod append_vec;
pub mod blockhash_queue;
mod bucket_map_holder;
mod bucket_map_holder_stats;
mod buffered_reader;
pub mod contains;
mod file_io;
pub mod hardened_unpack;
mod io_uring;
pub mod is_loadable;
mod is_zero_lamport;
pub mod partitioned_rewards;
pub mod pubkey_bins;
#[cfg(feature = "dev-context-only-utils")]
pub mod read_only_accounts_cache;
#[cfg(not(feature = "dev-context-only-utils"))]
mod read_only_accounts_cache;
mod rolling_bit_field;
pub mod sorted_storages;
pub mod stake_rewards;
pub mod storable_accounts;
pub mod tiered_storage;
pub mod utils;
mod verify_accounts_hash_in_background;
pub mod waitable_condvar;

pub use buffered_reader::large_file_buf_reader;

#[macro_use]
extern crate solana_metrics;
#[macro_use]
extern crate serde_derive;

#[cfg_attr(feature = "frozen-abi", macro_use)]
#[cfg(feature = "frozen-abi")]
extern crate solana_frozen_abi_macro;
