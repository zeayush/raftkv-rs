// src/membership.rs — dynamic cluster membership changes.
//
// JOINT CONSENSUS (Raft §6)
// ──────────────────────────
// Adding/removing nodes while the cluster is live is tricky because:
//   - You can't switch all nodes atomically.
//   - During the transition, two different majorities could elect two leaders.
//
// Raft solves this with a two-phase "joint consensus" approach:
//   Phase 1 — C_old,new: entries must be committed by BOTH the old and new majority.
//   Phase 2 — C_new:     once C_old,new is committed, switch to the new config.
//
// A simpler alternative (single-server changes) adds/removes ONE node at a time.
// That's safe because adding/removing one node can't create two disjoint majorities.
// We implement the single-server approach here as it's easier to get right.
//
// IMPLEMENTATION NOTE:
// Membership changes are themselves log entries (MembershipChange command).
// They take effect when applied, not when proposed.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

// ─── Types ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberConfig {
    /// Set of node IDs currently in the cluster.
    pub members: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MembershipChange {
    /// Add a new node with the given ID and gRPC address.
    AddNode  { id: String, address: String },
    /// Remove a node by ID. The node should step down gracefully first.
    RemoveNode { id: String },
}

// ─── MembershipManager ───────────────────────────────────────────────────────

pub struct MembershipManager {
    /// The committed cluster configuration.
    pub committed: MemberConfig,

    /// A pending (proposed but not yet committed) configuration change, if any.
    /// Only one change may be in-flight at a time.
    pub pending: Option<MemberConfig>,
}

impl MembershipManager {
    pub fn new(initial_members: HashSet<String>) -> Self {
        Self {
            committed: MemberConfig { members: initial_members },
            pending: None,
        }
    }

    /// Propose adding a new node.
    ///
    /// Returns Err if:
    ///   - Another change is already pending.
    ///   - The node is already a member.
    pub fn propose_add(&mut self, id: &str) -> anyhow::Result<MemberConfig> {
        // TODO:
        // 1. Check self.pending.is_none().
        // 2. Check that id is NOT in self.committed.members.
        // 3. Build a new MemberConfig with the id added.
        // 4. Store it in self.pending.
        // 5. Return a clone of the pending config (it will be serialised
        //    into a log entry by the caller).
        todo!("validate and stage an AddNode membership change")
    }

    /// Propose removing a node.
    pub fn propose_remove(&mut self, id: &str) -> anyhow::Result<MemberConfig> {
        // TODO:
        // 1. Check self.pending.is_none().
        // 2. Check that id IS in self.committed.members.
        // 3. Ensure removing it still leaves a majority (>= 3 nodes, or at
        //    least 1 node for single-node testing).
        // 4. Build new config, store as pending, return it.
        todo!("validate and stage a RemoveNode membership change")
    }

    /// Called when the membership change log entry is committed.
    /// Promotes pending → committed.
    pub fn commit_pending(&mut self) {
        if let Some(pending) = self.pending.take() {
            tracing::info!(
                new_members = ?pending.members,
                "membership change committed"
            );
            self.committed = pending;
        }
    }

    /// The active config for quorum calculation.
    ///
    /// During a transition, quorum requires approval from BOTH old and new
    /// configs (joint consensus). For single-server changes, pending is only
    /// one node different so we simply use the pending config once committed.
    pub fn active_config(&self) -> &MemberConfig {
        self.pending.as_ref().unwrap_or(&self.committed)
    }

    /// Quorum size for the active config.
    pub fn quorum(&self) -> usize {
        let n = self.active_config().members.len();
        n / 2 + 1
    }
}
