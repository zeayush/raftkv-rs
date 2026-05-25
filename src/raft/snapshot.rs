// src/raft/snapshot.rs — log compaction via snapshotting.
//
// WHY SNAPSHOT?
// ─────────────
// The Raft log grows unboundedly. Snapshotting:
//   1. Captures the full state machine at a commit index.
//   2. Lets us discard old log entries (saves disk + memory).
//   3. Lets lagging nodes catch up faster (one RPC vs many AppendEntries).
//
// SNAPSHOT PROTOCOL
// ──────────────────
// Leader decides to snapshot (log size > threshold).
//   → State machine serialised to bytes.
//   → Metadata (last_included_index, last_included_term) saved to disk.
//   → Log compacted up to last_included_index.
//
// When a follower is too far behind (next_index <= snapshot_index):
//   Leader sends InstallSnapshot RPC.
//   Follower:
//     1. Saves snapshot to disk.
//     2. Resets state machine from snapshot.
//     3. Discards conflicting log entries.
//     4. Updates snapshot_index / snapshot_term.

use crate::raft::node::RaftNode;
use anyhow::Result;
use serde::{Deserialize, Serialize};

// ─── Snapshot metadata ───────────────────────────────────────────────────────

const META_SNAPSHOT_INDEX: &[u8] = b"snapshot_index";
const META_SNAPSHOT_TERM:  &[u8] = b"snapshot_term";
const META_SNAPSHOT_DATA:  &[u8] = b"snapshot_data";

/// The on-disk representation of a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// The log index this snapshot replaces up to (inclusive).
    pub last_included_index: u64,

    /// The term of last_included_index.
    pub last_included_term: u64,

    /// The full serialised state machine: a list of (key, value) pairs.
    pub state: Vec<(Vec<u8>, Vec<u8>)>,
}

impl RaftNode {
    // ── Taking a snapshot (leader / any node) ────────────────────────────────

    /// Take a snapshot of the current state machine and compact the log.
    ///
    /// Called when committed log length exceeds `config.raft.snapshot_threshold`.
    ///
    /// Steps:
    ///   1. Read commit_index and last_applied from state (snapshot up to last_applied).
    ///   2. Export state machine via storage.state_snapshot().
    ///   3. Build Snapshot struct and persist it to CF_META.
    ///   4. Call log.compact_to(snapshot.last_included_index, snapshot.last_included_term).
    pub async fn take_snapshot(&self) -> Result<()> {
        // TODO: implement snapshot creation
        todo!("serialise state machine and compact log")
    }

    /// Persist a Snapshot to durable storage.
    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        // TODO:
        // 1. Persist snapshot.last_included_index to CF_META / META_SNAPSHOT_INDEX.
        // 2. Persist snapshot.last_included_term  to CF_META / META_SNAPSHOT_TERM.
        // 3. Serialise snapshot.state to JSON and persist to META_SNAPSHOT_DATA.
        //
        // Use a WriteBatch so all three writes are atomic.
        todo!("persist snapshot metadata and data atomically")
    }

    /// Load the most recently saved snapshot from storage, if any.
    pub fn load_snapshot(&self) -> Result<Option<Snapshot>> {
        // TODO:
        // 1. Load META_SNAPSHOT_INDEX from CF_META. If None → return Ok(None).
        // 2. Load META_SNAPSHOT_TERM.
        // 3. Load and deserialise META_SNAPSHOT_DATA.
        // 4. Return Some(Snapshot { ... }).
        todo!("load snapshot from RocksDB CF_META")
    }

    // ── Installing a snapshot (follower receiving InstallSnapshot RPC) ────────

    /// Handle an InstallSnapshot RPC from the leader.
    ///
    /// Steps:
    ///   1. If snapshot.last_included_index <= our commit_index: ignore (already have it).
    ///   2. Save the snapshot via save_snapshot().
    ///   3. Restore state machine via storage.state_restore(snapshot.state).
    ///   4. Discard log entries up to last_included_index via log.compact_to().
    ///   5. Update state.commit_index and state.last_applied.
    ///   6. Reset election timer (we heard from the leader).
    ///
    /// Returns our current term (so the leader can detect stale snapshot sends).
    pub async fn handle_install_snapshot(
        &self,
        leader_term:          u64,
        leader_id:            String,
        last_included_index:  u64,
        last_included_term:   u64,
        data:                 Vec<u8>,
    ) -> Result<u64> {
        // TODO: implement InstallSnapshot receiver
        todo!("install snapshot: restore state, compact log, update indices")
    }

    // ── Snapshot trigger check ────────────────────────────────────────────────

    /// Returns true if it's time to take a snapshot.
    ///
    /// Heuristic: number of log entries since last snapshot > threshold.
    pub async fn should_snapshot(&self) -> bool {
        // TODO:
        // let log = self.log.lock().await;
        // let entries_since_snapshot = log.last_index() - log.snapshot_index;
        // entries_since_snapshot > self.config.raft.snapshot_threshold
        todo!("compare entries since snapshot against threshold")
    }
}
