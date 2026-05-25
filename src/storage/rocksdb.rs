// src/storage/rocksdb.rs — thin RocksDB wrapper.
//
// We use two column families:
//   "log"   — Raft log entries, keyed by u64 index (big-endian bytes)
//   "meta"  — Persistent Raft metadata (current_term, voted_for)
//   "state" — Applied key-value state machine data
//
// WHY separate column families?
//   Column families in RocksDB are like separate key spaces with independent
//   compaction. Keeping the log and state machine separate lets us delete old
//   log entries (after snapshotting) without affecting application data.

use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options, WriteBatch,
};
use serde::{de::DeserializeOwned, Serialize};
use anyhow::{Context, Result};
use std::path::Path;

pub const CF_LOG:   &str = "log";
pub const CF_META:  &str = "meta";
pub const CF_STATE: &str = "state";

// Key constants for the "meta" column family.
pub const META_CURRENT_TERM: &[u8] = b"current_term";
pub const META_VOTED_FOR:    &[u8] = b"voted_for";

pub type Db = DBWithThreadMode<MultiThreaded>;

// ─── StorageEngine ───────────────────────────────────────────────────────────

pub struct StorageEngine {
    db: Db,
}

impl StorageEngine {
    /// Open (or create) the RocksDB database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_LOG,   Options::default()),
            ColumnFamilyDescriptor::new(CF_META,  Options::default()),
            ColumnFamilyDescriptor::new(CF_STATE, Options::default()),
        ];

        let db = Db::open_cf_descriptors(&opts, path, cfs)
            .context("opening RocksDB")?;

        Ok(Self { db })
    }

    // ── Helper: column family handle ─────────────────────────────────────────

    fn cf(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).expect("column family must exist")
    }

    // ── Low-level raw get/put/delete ─────────────────────────────────────────

    pub fn raw_get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf(self.cf(cf), key)?)
    }

    pub fn raw_put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.db.put_cf(self.cf(cf), key, value)?)
    }

    pub fn raw_delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        Ok(self.db.delete_cf(self.cf(cf), key)?)
    }

    // ── JSON-serialised helpers ───────────────────────────────────────────────

    pub fn get_json<T: DeserializeOwned>(&self, cf: &str, key: &[u8]) -> Result<Option<T>> {
        match self.raw_get(cf, key)? {
            None => Ok(None),
            Some(bytes) => {
                let val = serde_json::from_slice(&bytes)
                    .context("deserialising JSON from RocksDB")?;
                Ok(Some(val))
            }
        }
    }

    pub fn put_json<T: Serialize>(&self, cf: &str, key: &[u8], value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.raw_put(cf, key, &bytes)
    }

    // ── Raft metadata helpers ─────────────────────────────────────────────────

    /// Atomically persist current_term and voted_for together.
    ///
    /// IMPORTANT: Raft requires these to be durable before responding to any
    /// RPC. A crash between updating term and voted_for would violate safety.
    /// Using a WriteBatch makes both updates atomic.
    pub fn save_hard_state(&self, current_term: u64, voted_for: Option<&str>) -> Result<()> {
        let mut batch = WriteBatch::default();

        let term_cf = self.cf(CF_META);
        batch.put_cf(term_cf, META_CURRENT_TERM, current_term.to_be_bytes());

        let vote_bytes = match voted_for {
            Some(id) => id.as_bytes().to_vec(),
            None     => b"".to_vec(),
        };
        batch.put_cf(term_cf, META_VOTED_FOR, vote_bytes);

        Ok(self.db.write(batch)?)
    }

    pub fn load_current_term(&self) -> Result<u64> {
        match self.raw_get(CF_META, META_CURRENT_TERM)? {
            None => Ok(0),
            Some(b) => {
                let arr: [u8; 8] = b.try_into().expect("term is 8 bytes");
                Ok(u64::from_be_bytes(arr))
            }
        }
    }

    pub fn load_voted_for(&self) -> Result<Option<String>> {
        match self.raw_get(CF_META, META_VOTED_FOR)? {
            None => Ok(None),
            Some(b) if b.is_empty() => Ok(None),
            Some(b) => Ok(Some(String::from_utf8(b)?)),
        }
    }

    // ── Log helpers ───────────────────────────────────────────────────────────

    /// Convert a log index to big-endian bytes for use as a RocksDB key.
    /// Big-endian ensures lexicographic order == numeric order.
    pub fn log_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Delete all log entries with index in [from, to] inclusive.
    /// Used after snapshotting to reclaim disk space.
    pub fn truncate_log_prefix(&self, from: u64, to: u64) -> Result<()> {
        // TODO: implement using delete_range_cf for efficiency.
        // For now, iterate and delete one by one.
        for idx in from..=to {
            self.raw_delete(CF_LOG, &Self::log_key(idx))?;
        }
        Ok(())
    }

    // ── State machine helpers ─────────────────────────────────────────────────

    pub fn state_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.raw_get(CF_STATE, key)
    }

    pub fn state_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.raw_put(CF_STATE, key, value)
    }

    pub fn state_delete(&self, key: &[u8]) -> Result<()> {
        self.raw_delete(CF_STATE, key)
    }

    /// Export the entire state machine as raw key-value pairs.
    /// Used when taking a snapshot.
    pub fn state_snapshot(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // TODO: iterate over CF_STATE and collect all entries.
        // Hint: self.db.iterator_cf(self.cf(CF_STATE), rocksdb::IteratorMode::Start)
        todo!("iterate CF_STATE and collect (key, value) pairs")
    }

    /// Atomically replace the entire state machine with snapshot data.
    /// Used when installing a snapshot received from the leader.
    pub fn state_restore(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        // TODO:
        // 1. Delete all existing keys in CF_STATE.
        // 2. Write all entries from the snapshot in a single WriteBatch.
        todo!("clear CF_STATE then bulk-insert snapshot entries")
    }
}
