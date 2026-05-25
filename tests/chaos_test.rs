// tests/chaos_test.rs — kill the leader, verify a new one is elected < 500ms.
//
// HOW TO RUN:
//   cargo test --test chaos_test -- --nocapture
//
// WHAT THIS TESTS:
//   1. Start a 3-node cluster in-process.
//   2. Wait for a leader to be elected.
//   3. Kill the leader node (drop it / close its listener).
//   4. Start a timer.
//   5. Poll remaining nodes until one reports Role::Leader.
//   6. Assert elapsed < 500ms.
//
// WHY IN-PROCESS?
//   Easier to control timing precisely. Docker-based chaos is in the script.

use std::time::{Duration, Instant};

/// The maximum time allowed for a new leader to emerge after the old one dies.
const MAX_ELECTION_TIME: Duration = Duration::from_millis(500);

/// Poll interval when waiting for leader election.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[tokio::test]
async fn test_leader_reelection_within_500ms() {
    // TODO:
    // 1. Spin up 3 in-process RaftNodes with test configs (low timeouts).
    //    - heartbeat_interval_ms:   10
    //    - election_timeout_min_ms: 50
    //    - election_timeout_max_ms: 100
    //    Use in-memory storage or a temp dir via tempfile::tempdir().
    //
    // 2. Wait for initial leader election:
    //    let leader_id = wait_for_leader(&nodes, Duration::from_secs(2)).await
    //        .expect("cluster did not elect a leader in 2s");
    //
    // 3. "Kill" the leader — drop its Arc<RaftNode> and close its gRPC listener.
    //    nodes.remove(&leader_id);
    //
    // 4. Start timer, poll for new leader on remaining nodes.
    //    let start = Instant::now();
    //    let new_leader = wait_for_leader(&remaining_nodes, MAX_ELECTION_TIME).await
    //        .expect("no new leader elected within 500ms");
    //    let elapsed = start.elapsed();
    //
    // 5. Assert new_leader != leader_id (different node won).
    // 6. Assert elapsed < MAX_ELECTION_TIME.
    //
    // Helper: wait_for_leader polls node.state.lock().role == Leader.

    todo!("implement 3-node chaos test")
}

/// Poll nodes until one is a Leader. Returns the leader's node ID, or None on timeout.
async fn wait_for_leader(
    _nodes: &[/* Arc<RaftNode> */()],
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        // TODO: iterate nodes, check state.role == Role::Leader.
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    None
}
