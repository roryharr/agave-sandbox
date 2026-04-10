use {
    super::{DiskIndexValue, RefCount, SlotList},
    rocksdb::{DB, IteratorMode, Options, WriteBatch},
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{collections::HashMap, path::PathBuf},
    tempfile::TempDir,
};

/// RocksDB-backed disk index for the accounts index.
///
/// Stores `Pubkey → (ref_count, [(Slot, AccountInfo)]...)` as raw bytes.
///
/// Encoding (little-endian):
///   bytes[0..8]         = ref_count: u64
///   bytes[8..24]        = (slot0: u64, info0: [u8; 8])
///   bytes[24..40]       = (slot1: u64, info1: [u8; 8])
///   ...
pub struct RocksDbDiskIndex {
    db: DB,
    /// Kept alive so the directory is not deleted while the DB is open.
    _temp_dir: Option<TempDir>,
}

impl RocksDbDiskIndex {
    pub fn new(drives: Option<&[PathBuf]>) -> Self {
        let (path, temp_dir) = match drives.and_then(|d| d.first()) {
            Some(drive) => (drive.join("accounts_index_rocksdb"), None),
            None => {
                let dir = TempDir::new().expect("failed to create temp dir for accounts index");
                let path = dir.path().to_path_buf();
                (path, Some(dir))
            }
        };

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let db = DB::open(&opts, &path).expect("failed to open accounts index RocksDB");

        Self {
            db,
            _temp_dir: temp_dir,
        }
    }

    pub fn read_value<U: DiskIndexValue>(&self, pubkey: &Pubkey) -> Option<(SlotList<U>, RefCount)> {
        self.db
            .get(pubkey.as_ref())
            .expect("RocksDB read failed")
            .map(|bytes| decode(&bytes))
    }

    pub fn write<U: DiskIndexValue>(
        &self,
        pubkey: &Pubkey,
        slot_list: &[(Slot, U)],
        ref_count: RefCount,
    ) {
        self.db
            .put(pubkey.as_ref(), encode(slot_list, ref_count))
            .expect("RocksDB write failed");
    }

    pub fn delete_key(&self, pubkey: &Pubkey) {
        self.db
            .delete(pubkey.as_ref())
            .expect("RocksDB delete failed");
    }

    pub fn keys(&self) -> Vec<Pubkey> {
        self.db
            .iterator(IteratorMode::Start)
            .filter_map(|r| r.ok())
            .filter_map(|(k, _v)| {
                <[u8; 32]>::try_from(k.as_ref())
                    .ok()
                    .map(Pubkey::new_from_array)
            })
            .collect()
    }

    /// Insert items that don't already exist on disk.
    ///
    /// Returns `(index, existing_value)` for each item whose pubkey was already
    /// present — either on disk before this call, or earlier in this same batch.
    /// Non-duplicate items are written to disk.
    pub fn batch_insert_non_duplicates<U: DiskIndexValue>(
        &self,
        items: &[(Pubkey, (Slot, U))],
    ) -> Vec<(usize, (Slot, U))> {
        let keys: Vec<_> = items.iter().map(|(k, _)| k.as_ref()).collect();
        let existing = self.db.multi_get(keys);

        let mut batch = WriteBatch::default();
        let mut duplicates = Vec::new();
        // Tracks the first (Slot, U) written for each pubkey within this batch,
        // so that later occurrences of the same key can be returned as duplicates.
        let mut first_in_batch: HashMap<Pubkey, (Slot, U)> = HashMap::new();

        for (i, (pubkey, entry)) in items.iter().enumerate() {
            if let Ok(Some(bytes)) = &existing[i] {
                // Key already on disk before this call.
                let (slot_list, _ref_count) = decode::<U>(bytes);
                if let Some(first) = slot_list.into_iter().next() {
                    duplicates.push((i, first));
                    continue;
                }
            }
            if let Some(&first_entry) = first_in_batch.get(pubkey) {
                // Key appears earlier in this same batch.
                duplicates.push((i, first_entry));
                continue;
            }
            batch.put(pubkey.as_ref(), encode(std::slice::from_ref(entry), 1));
            first_in_batch.insert(*pubkey, *entry);
        }

        self.db.write(batch).expect("RocksDB batch write failed");
        duplicates
    }
}

fn encode<U: DiskIndexValue>(slot_list: &[(Slot, U)], ref_count: RefCount) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + 16 * slot_list.len());
    bytes.extend_from_slice(&(ref_count as u64).to_le_bytes());
    for (slot, info) in slot_list {
        bytes.extend_from_slice(&slot.to_le_bytes());
        bytes.extend_from_slice(&info.to_bytes());
    }
    bytes
}

fn decode<U: DiskIndexValue>(bytes: &[u8]) -> (SlotList<U>, RefCount) {
    debug_assert!(bytes.len() >= 8, "corrupt disk entry: too short");
    let ref_count = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as RefCount;
    let slot_list = bytes[8..]
        .chunks_exact(16)
        .map(|chunk| {
            let slot = u64::from_le_bytes(chunk[..8].try_into().unwrap());
            let info = U::from_bytes(chunk[8..].try_into().unwrap());
            (slot, info)
        })
        .collect();
    (slot_list, ref_count)
}
