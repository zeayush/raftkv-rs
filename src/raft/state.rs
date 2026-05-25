// src/raft/state.rs — Raft node state machine (roles + volatile state).
//
// This module owns the "volatile" state that does NOT need to survive crashes
// (it is re-derived on startup by replaying the log or loading a snapshot).
//
// Volatile state on ALL servers:
//   commit_index — highest log entry known to be committed
//   last_applied — highest log entry applied to the state machine
//
// Volatile state on LEADER only (reinitialized after election):
//   next_index  — for each peer, next log index to send
//   match_index — for each peer, highest log index known to be replicated

use std::collections::HashMap;

// ─── Role ────────────────────────────────────────────────────────────────────

/// The current role of this Raft node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Role::Follower  => write!(f, "Follower"),
            Role::Candidate => write!(f, "Candidate"),
            Role::Leader    => write!(f, "Leader"),
        }
    }
}

// ─── LeaderState ─────────────────────────────────────────────────────────────

/// Per-peer tracking maintained only by the leader.
/// Resets every time a new leader is elected.
#[derive(Debug, Clone)]
pub struct LeaderState {
    /// For each peer: the next log index to send to that peer.
    /// Initialised to last_log_index + 1 after winning election.
    pub next_index: HashMap<String, u64>,

    /// For each peer: highest index known to be replicated on that peer.
    /// Initialised to 0 after winning election.
    pub match_index: HashMap<String, u64>,
}

impl LeaderState {
    /// Create fresh leader state after winning an election.
    ///
    /// `peers`          — IDs of all other cluster members.
    /// `last_log_index` — our own last log index at the time of election.
    pub fn new(peers: &[String], last_log_index: u64) -> Self {
        let mut next_index  = HashMap::new();
        let mut match_index = HashMap::new();
        for peer in peers {
            next_index.insert(peer.clone(), last_log_index + 1);
            match_index.insert(peer.clone(), 0);
        }
        Self { next_index, match_index }
    }

    /// Update match_index and next_index for `peer` after a successful
    /// AppendEntries response confirming replication up to `replicated_index`.
    pub fn record_replication(&mut self, peer: &str, replicated_index: u64) {
        // TODO:
        // 1. Update match_index[peer] = max(current, replicated_index).
        // 2. Update next_index[peer]  = replicated_index + 1.
        todo!("update match_index and next_index for the peer")
    }

    /// Decrease next_index for `peer` on rejection.
    ///
    /// `conflict_index` is the hint from the follower's response (0 = no hint).
    /// When 0, simply decrement by 1.
    pub fn record_rejection(&mut self, peer: &str, conflict_index: u64, conflict_term: u64) {
        // TODO (optional optimisation — basic decrement is fine to start):
        // Use conflict_index and conflict_term to skip over an entire term's
        // worth of entries at once instead of probing one-by-one.
        //
        // Simple version: next_index[peer] = max(1, next_index[peer] - 1)
        todo!("handle AppendEntries rejection, rewind next_index")
    }
}

// ─── RaftState ───────────────────────────────────────────────────────────────

/// All volatile (in-memory, non-persistent) Raft state for one node.
///
/// The persistent state (current_term, voted_for, log) lives in StorageEngine.
pub struct RaftState {
    // ── Identity ──────────────────────────────────────────────────────────────
    pub node_id:  String,
    pub peer_ids: Vec<String>,

    // ── Role ──────────────────────────────────────────────────────────────────
    pub role: Role,

    // ── Volatile state (all servers) ──────────────────────────────────────────

    /// Highest log index known to be committed.
    /// Monotonically increasing. Advances when leader confirms quorum.
    pub commit_index: u64,

    /// Highest log index applied to the state machine.
    /// Always <= commit_index. Advances in the apply loop.
    pub last_applied: u64,

    // ── Leader-only state ─────────────────────────────────────────────────────
    pub leader_state: Option<LeaderState>,

    // ── Election bookkeeping ──────────────────────────────────────────────────

    /// ID of the node we believe is the current leader (used for redirects).
    pub current_leader: Option<String>,

    /// Number of votes received in the current election (Candidate only).
    pub votes_received: usize,
}

impl RaftState {
    pub fn new(node_id: String, peer_ids: Vec<String>) -> Self {
        Self {
            node_id,
            peer_ids,
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            leader_state: None,
            current_leader: None,
            votes_received: 0,
        }
    }

    /// Quorum size for this cluster (majority of all nodes including self).
    pub fn quorum(&self) -> usize {
        (self.peer_ids.len() + 1) / 2 + 1
    }

    // ── Role transitions ─────────────────────────────────────────────────────

    /// Transition to Follower. Called when we see a higher term.
    /// Clears leader state and vote tracking.
    pub fn become_follower(&mut self, new_leader: Option<String>) {
        self.role           = Role::Follower;
        self.leader_state   = None;
        self.votes_received = 0;
        self.current_leader = new_leader;
        tracing::info!(node_id = %self.node_id, "became Follower");
    }

    /// Transition to Candidate. Called when election timeout fires.
    pub fn become_candidate(&mut self) {
        // TODO:
        // 1. Set role = Candidate.
        // 2. Increment current_term (caller must persist it via StorageEngine).
        // 3. Vote for ourselves (votes_received = 1).
        // 4. Clear current_leader.
        todo!("transition to Candidate role")
    }

    /// Transition to Leader after winning election.
    /// Initialises LeaderState for all peers.
    pub fn become_leader(&mut self, last_log_index: u64) {
        // TODO:
        // 1. Set role = Leader.
        // 2. Set current_leader = Some(self.node_id.clone()).
        // 3. Create LeaderState::new(&self.peer_ids, last_log_index).
        todo!("transition to Leader role, initialise LeaderState")
    }

    // ── Commit index advancement ──────────────────────────────────────────────

    /// Recalculate commit_index based on match_index values (leader only).
    ///
    /// The highest N such that match_index[majority] >= N AND
    /// log[N].term == current_term is the new commit_index.
    /// (§5.4.2 — only commit entries from the current term directly)
    pub fn maybe_advance_commit(&mut self, current_term: u64) -> u64 {
        // TODO:
        // 1. Collect all match_index values (including our own last_log_index).
        // 2. Sort descending.
        // 3. The quorum()-th value is the highest index replicated on a majority.
        // 4. Only advance commit_index if log[N].term == current_term.
        // 5. Return the new commit_index.
        todo!("calculate new commit_index from match_index quorum")
    }
}
