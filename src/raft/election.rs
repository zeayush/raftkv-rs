// src/raft/election.rs — leader election logic.
//
// ELECTION OVERVIEW
// ─────────────────
// 1. Follower's election timer fires (randomised 150–300ms by default).
// 2. Node becomes Candidate: increments term, votes for itself.
// 3. Sends RequestVote RPC to all peers concurrently.
// 4. If majority responds vote_granted=true → become Leader.
// 5. If sees term > current_term in any response → step down to Follower.
// 6. If election times out without majority → restart election (new term).
//
// RANDOMISED TIMEOUT (why?)
// ──────────────────────────
// If all nodes had the same timeout they'd all start elections simultaneously,
// causing split votes indefinitely. Randomness breaks the tie.

use crate::{
    config::RaftConfig,
    raft::node::RaftNode,
};
use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;

/// Draw a random election timeout between min and max.
pub fn random_election_timeout(cfg: &RaftConfig) -> Duration {
    let ms = rand::thread_rng()
        .gen_range(cfg.election_timeout_min_ms..=cfg.election_timeout_max_ms);
    Duration::from_millis(ms)
}

impl RaftNode {
    // ── Timer loop ────────────────────────────────────────────────────────────

    /// Runs the election timer loop for a Follower/Candidate.
    ///
    /// Must be cancelled (dropped) when the node becomes Leader.
    /// Returns only when the node starts an election.
    ///
    /// Implementation hint:
    ///   Use `tokio::select!` to race between:
    ///     a) a `sleep(random_timeout)` future
    ///     b) a `reset_rx` channel that the heartbeat handler sends to
    ///        whenever a valid AppendEntries or vote is received.
    ///
    ///   On sleep completing: call `self.start_election().await`.
    ///   On reset signal:     restart the timer with a new random duration.
    pub async fn run_election_timer(&self) {
        // TODO:
        // let mut reset_rx = self.reset_election_timer_rx.lock().await;
        // loop {
        //     let timeout = random_election_timeout(&self.config.raft);
        //     tokio::select! {
        //         _ = sleep(timeout) => {
        //             self.start_election().await;
        //             break; // or loop again depending on outcome
        //         }
        //         _ = reset_rx.recv() => {
        //             // received heartbeat — restart timer
        //         }
        //     }
        // }
        todo!("implement election timer loop with tokio::select!")
    }

    // ── Starting an election ──────────────────────────────────────────────────

    /// Transition to Candidate and broadcast RequestVote to all peers.
    ///
    /// Steps:
    ///   1. Lock state, call state.become_candidate().
    ///   2. Persist new term + voted_for=self via storage.save_hard_state().
    ///   3. Build RequestVoteRequest with our last_log_index / last_log_term.
    ///   4. Spawn a task per peer to send the RPC concurrently.
    ///   5. Collect responses via a channel; call handle_vote_response().
    pub async fn start_election(&self) {
        // TODO: implement election initiation
        todo!("start election: become candidate, persist state, send RequestVote RPCs")
    }

    // ── Handling incoming RequestVote (receiver side) ─────────────────────────

    /// Handle a RequestVote RPC sent BY another candidate TO us.
    ///
    /// Grant the vote if ALL of the following are true:
    ///   a) candidate's term >= our current_term.
    ///   b) We haven't voted for anyone else in this term
    ///      (voted_for is None OR voted_for == candidate_id).
    ///   c) Candidate's log is at least as up-to-date as ours:
    ///      - candidate's last_log_term > our last_log_term, OR
    ///      - terms equal AND candidate's last_log_index >= our last_log_index.
    ///
    /// If candidate's term > current_term, update our term and step down first.
    ///
    /// Returns (our_term, vote_granted).
    pub async fn handle_request_vote(
        &self,
        candidate_term:    u64,
        candidate_id:      String,
        last_log_index:    u64,
        last_log_term:     u64,
    ) -> (u64, bool) {
        // TODO: implement vote grant logic described above.
        // Remember to reset the election timer if vote is granted!
        todo!("implement RequestVote receiver logic")
    }

    // ── Handling a vote response (collector side) ─────────────────────────────

    /// Process a single RequestVote response from a peer.
    ///
    /// If vote_granted: increment votes_received.
    ///   If votes_received >= quorum: call state.become_leader() and
    ///   spawn run_leader_loop().
    ///
    /// If response term > current_term: step down to Follower.
    async fn handle_vote_response(&self, peer_id: &str, peer_term: u64, vote_granted: bool) {
        // TODO: implement vote tally and leader promotion
        todo!("tally votes, promote to leader if quorum reached")
    }
}
