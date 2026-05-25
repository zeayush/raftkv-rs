// src/raft/replication.rs — log replication and heartbeat logic (leader side).
//
// REPLICATION OVERVIEW
// ─────────────────────
// After winning election the leader:
//   1. Appends a Noop entry (to commit previous term's entries safely).
//   2. Starts a heartbeat loop: every heartbeat_interval_ms, send
//      AppendEntries to all peers (empty if nothing new = heartbeat).
//   3. For each new client write: append to log, trigger replication.
//   4. When a peer responds success: update match_index, maybe advance
//      commit_index.
//   5. When commit_index advances: signal the apply loop.
//
// BACKPRESSURE
// ─────────────
// Each peer has its own next_index. A lagging peer receives a batch of entries
// starting from next_index. If a peer is too far behind (pre-snapshot), we
// send InstallSnapshot instead.

use crate::raft::node::RaftNode;
use crate::raft::log::KvCommand;
use std::time::Duration;
use tokio::time;

impl RaftNode {
    // ── Leader main loop ─────────────────────────────────────────────────────

    /// Main loop run by the leader. Sends heartbeats and drives replication.
    ///
    /// Runs until the node steps down (term change detected).
    ///
    /// Implementation hint:
    ///   let mut interval = time::interval(Duration::from_millis(cfg.heartbeat_interval_ms));
    ///   loop {
    ///       interval.tick().await;
    ///       self.replicate_to_all_peers().await;
    ///   }
    pub async fn run_leader_loop(&self) {
        // TODO:
        // 1. Append a Noop entry to establish leadership.
        // 2. Enter heartbeat loop.
        // 3. Break when we're no longer leader (check role in state lock).
        todo!("implement leader heartbeat + replication loop")
    }

    // ── Replicate to a single peer ────────────────────────────────────────────

    /// Send AppendEntries (or InstallSnapshot) to one peer.
    ///
    /// Steps:
    ///   1. Read next_index for this peer from LeaderState.
    ///   2. If next_index <= snapshot_index: send InstallSnapshot RPC instead.
    ///   3. Otherwise: build AppendEntriesRequest with entries [next_index..].
    ///   4. Send RPC, handle response.
    pub async fn replicate_to_peer(&self, peer_id: &str) {
        // TODO: implement single-peer replication
        todo!("build and send AppendEntries or InstallSnapshot to peer")
    }

    /// Send AppendEntries to ALL peers concurrently.
    pub async fn replicate_to_all_peers(&self) {
        let peers: Vec<String> = {
            // Scope the lock to avoid holding it across awaits.
            self.state.lock().await.peer_ids.clone()
        };
        // TODO:
        // Spawn a tokio task per peer calling self.replicate_to_peer(peer_id).
        // Use tokio::join! or FuturesUnordered for concurrency.
        todo!("fan-out replication to all peers concurrently")
    }

    // ── Handling AppendEntries response ──────────────────────────────────────

    /// Process a successful AppendEntries response from a peer.
    ///
    /// Updates match_index / next_index, then tries to advance commit_index.
    /// If commit_index advances, signals the apply loop.
    async fn on_append_entries_success(&self, peer_id: &str, match_index: u64) {
        // TODO:
        // 1. state.leader_state.record_replication(peer_id, match_index).
        // 2. current_term = storage.load_current_term().
        // 3. new_commit = state.maybe_advance_commit(current_term).
        // 4. If new_commit > state.commit_index:
        //    a. state.commit_index = new_commit.
        //    b. self.apply_tx.send(()).await (wake up apply loop).
        todo!("update replication progress and advance commit index")
    }

    /// Process a failed AppendEntries response.
    ///
    /// If peer_term > current_term: step down.
    /// Otherwise: back off next_index using conflict hint.
    async fn on_append_entries_failure(
        &self,
        peer_id: &str,
        peer_term: u64,
        conflict_index: u64,
        conflict_term: u64,
    ) {
        // TODO:
        // 1. If peer_term > current_term → save_hard_state, become_follower.
        // 2. Else → leader_state.record_rejection(peer_id, conflict_index, conflict_term).
        todo!("handle AppendEntries rejection")
    }

    // ── Client write entry point ──────────────────────────────────────────────

    /// Append a client command to the log and replicate to peers.
    ///
    /// Called by the KV gRPC handler after verifying we are the leader.
    /// Returns the log index assigned to this command so the caller can
    /// wait for it to be committed (linearisable reads).
    pub async fn propose(&self, command: KvCommand) -> anyhow::Result<u64> {
        // TODO:
        // 1. Verify we're still the leader (return Err if not).
        // 2. current_term = storage.load_current_term().
        // 3. entry = log.append(current_term, command).
        // 4. Trigger replicate_to_all_peers().await (or via channel).
        // 5. Return entry.index.
        todo!("append to log and trigger replication")
    }

    // ── Apply loop ────────────────────────────────────────────────────────────

    /// Background task: apply committed log entries to the state machine.
    ///
    /// Woken up whenever commit_index advances (via apply_rx channel).
    /// Applies entries [last_applied+1 .. commit_index] in order.
    pub async fn run_apply_loop(&self) {
        // TODO:
        // loop {
        //     self.apply_rx.recv().await;
        //     let (last_applied, commit_index) = { lock state, read both };
        //     for index in (last_applied + 1)..=commit_index {
        //         let entry = log.get(index);
        //         self.apply_entry(entry).await;
        //         state.last_applied = index;
        //     }
        // }
        todo!("implement apply loop: last_applied → commit_index")
    }

    /// Apply a single committed log entry to the RocksDB state machine.
    async fn apply_entry(&self, entry: &crate::raft::log::LogEntry) -> anyhow::Result<()> {
        use crate::raft::log::KvCommand;
        match &entry.command {
            KvCommand::Set { key, value } => {
                self.storage.state_put(key.as_bytes(), value)?;
            }
            KvCommand::Delete { key } => {
                self.storage.state_delete(key.as_bytes())?;
            }
            KvCommand::Noop => {
                // Nothing to apply; just advances last_applied.
            }
        }
        Ok(())
    }
}
