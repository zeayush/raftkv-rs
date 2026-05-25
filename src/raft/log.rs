// src/raft/log.rs — Raft log management.
//
// THE LOG IS THE HEART OF RAFT.
//
// Key invariants you must never violate:
//   1. Log indices are 1-based. Index 0 is a sentinel "empty" state.
//   2. A committed entry is NEVER overwritten.
//   3. Entries are only committed once a majority of nodes have them.
//   4. The log is durable — written to RocksDB before any RPC response.
//
// Log entry lifecycle:
//   append (leader) → replicate → majority ack → commit → apply to state machine

use crate::storage::StorageEngine;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ─── Types ───────────────────────────────────────────────────────────────────

/// A command that can be applied to the key-value state machine.
/// This is what gets serialised into LogEntry::command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvCommand {
    Set { key: String, value: Vec<u8> },
    Delete { key: String },
    /// A no-op entry. Leaders append this on election to commit previous entries.
    /// (Raft §5.4.2 — leader completeness property)
    Noop,
}

/// A single entry in the replicated log.
/// Mirrors the protobuf LogEntry but owned by the Raft layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The term in which this entry was created.
    pub term: u64,
    /// 1-based position in the log.
    pub index: u64,
    /// The command to apply when this entry is committed.
    pub command: KvCommand,
}

// ─── RaftLog ─────────────────────────────────────────────────────────────────

/// Manages the persistent Raft log on top of RocksDB.
///
/// In-memory: we keep entries since the last snapshot in a Vec for fast access.
/// On disk:   every entry is written to CF_LOG keyed by index (big-endian u64).
pub struct RaftLog {
    storage: Arc<StorageEngine>,

    /// In-memory cache of log entries since the last snapshot.
    /// entries[0] corresponds to log index (snapshot_index + 1).
    entries: Vec<LogEntry>,

    /// The index of the last entry included in the most recent snapshot.
    /// 0 if no snapshot has been taken yet.
    pub snapshot_index: u64,

    /// The term of the entry at snapshot_index.
    pub snapshot_term: u64,
}

impl RaftLog {
    /// Create a new RaftLog, loading existing entries from storage.
    pub fn new(storage: Arc<StorageEngine>) -> Result<Self> {
        // TODO:
        // 1. Load snapshot_index and snapshot_term from CF_META.
        // 2. Load all log entries with index > snapshot_index from CF_LOG.
        //    Hint: iterate over CF_LOG from snapshot_index+1 upward.
        // 3. Populate self.entries.
        //
        // For now, start fresh:
        Ok(Self {
            storage,
            entries: Vec::new(),
            snapshot_index: 0,
            snapshot_term: 0,
        })
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    /// The index of the last entry in the log (or snapshot_index if empty).
    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|e| e.index).unwrap_or(self.snapshot_index)
    }

    /// The term of the last entry (or snapshot_term if empty).
    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|e| e.term).unwrap_or(self.snapshot_term)
    }

    /// Get the term of the entry at `index`, or None if not in our log.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Get a reference to the entry at `index`, or None.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.snapshot_index || self.entries.is_empty() {
            return None;
        }
        let offset = (index - self.snapshot_index - 1) as usize;
        self.entries.get(offset)
    }

    /// Return all entries with index > `after_index`.
    /// Returns a slice for zero-copy efficiency.
    pub fn entries_from(&self, after_index: u64) -> &[LogEntry] {
        if after_index >= self.last_index() {
            return &[];
        }
        let start = (after_index.saturating_sub(self.snapshot_index)) as usize;
        &self.entries[start.min(self.entries.len())..]
    }

    // ── Mutations ─────────────────────────────────────────────────────────────

    /// Append a new entry to the log (leader path).
    ///
    /// Assigns the next index automatically.
    /// Persists to RocksDB synchronously — this must happen before the leader
    /// sends AppendEntries RPCs.
    pub fn append(&mut self, term: u64, command: KvCommand) -> Result<LogEntry> {
        let index = self.last_index() + 1;
        let entry = LogEntry { term, index, command };
        self.persist_entry(&entry)?;
        self.entries.push(entry.clone());
        Ok(entry)
    }

    /// Append a batch of entries received from the leader (follower path).
    ///
    /// Implements the AppendEntries consistency check:
    ///   - Entries that match the existing log are skipped (idempotent).
    ///   - On first conflict (same index, different term), truncate from there.
    ///
    /// Returns the index of the last new entry appended.
    pub fn append_entries(&mut self, entries: Vec<LogEntry>) -> Result<u64> {
        // TODO:
        // For each entry in `entries`:
        //   1. If index <= snapshot_index, skip (already snapshotted).
        //   2. If we have an entry at that index with the SAME term, skip.
        //   3. If we have an entry at that index with a DIFFERENT term:
        //      a. Truncate our log from that index onward (delete from storage too!).
        //      b. Append the new entries from this point forward.
        //   4. If the index is beyond our last_index, just append.
        //
        // After all entries are processed, return self.last_index().
        //
        // Hint: don't forget to persist each appended entry to RocksDB!
        todo!("implement AppendEntries log reconciliation")
    }

    /// Truncate all entries with index >= `from`.
    /// Called when a follower detects a conflict.
    fn truncate_from(&mut self, from: u64) -> Result<()> {
        // TODO:
        // 1. Remove entries from self.entries where entry.index >= from.
        // 2. Delete those entries from RocksDB (CF_LOG).
        todo!("truncate in-memory vec and RocksDB log entries")
    }

    /// Persist a single entry to RocksDB.
    fn persist_entry(&self, entry: &LogEntry) -> Result<()> {
        let key = StorageEngine::log_key(entry.index);
        self.storage.put_json(
            crate::storage::rocksdb::CF_LOG,
            &key,
            entry,
        )
    }

    // ── Snapshot integration ──────────────────────────────────────────────────

    /// Discard log entries up to and including `up_to_index` after a snapshot.
    ///
    /// Called by the snapshot module after a snapshot is durably saved.
    /// Frees memory and disk space.
    pub fn compact_to(&mut self, up_to_index: u64, up_to_term: u64) -> Result<()> {
        // TODO:
        // 1. Delete log entries [snapshot_index+1 .. up_to_index] from RocksDB.
        // 2. Drop those entries from self.entries.
        // 3. Update self.snapshot_index and self.snapshot_term.
        todo!("compact log up to snapshot index")
    }
}
