#!/usr/bin/env bash
# scripts/chaos.sh — Docker-based chaos test.
#
# Kills the leader container, verifies a new leader is elected in < 500ms.
# Requires: docker compose up -d (cluster must already be running).
#
# Usage:
#   ./scripts/chaos.sh

set -euo pipefail

NODES=("raftkv-node1" "raftkv-node2" "raftkv-node3")
MAX_ELECTION_MS=500

# ── Helpers ───────────────────────────────────────────────────────────────────

log() { echo "[chaos] $*"; }

# Find which node is the current leader by querying each node's Redis proxy.
# Uses redis-cli PING + a custom GET __leader__ key (set by the leader loop).
# Simpler: query the GetLeader gRPC endpoint via grpcurl.
#
# TODO: implement leader detection.
# Hint: use `docker exec <node> redis-cli -p 6379 GET __raft_leader__`
# or use grpcurl to call kv.KvService/GetLeader.
find_leader() {
    for container in "${NODES[@]}"; do
        # TODO: query the container and return its id if it's the leader.
        echo "TODO: detect leader from $container"
    done
}

# ── Main ──────────────────────────────────────────────────────────────────────

log "Finding current leader..."
# TODO: leader=$(find_leader)
# log "Current leader: $leader"

log "Killing leader container..."
# TODO: docker stop "$leader"

log "Starting election timer..."
START_MS=$(date +%s%3N)

log "Polling for new leader (max ${MAX_ELECTION_MS}ms)..."
# TODO:
# while true; do
#     new_leader=$(find_leader)
#     if [[ -n "$new_leader" && "$new_leader" != "$leader" ]]; then
#         ELAPSED=$(($(date +%s%3N) - START_MS))
#         log "New leader elected: $new_leader (in ${ELAPSED}ms)"
#         if [[ $ELAPSED -lt $MAX_ELECTION_MS ]]; then
#             log "✓ PASS: election completed in ${ELAPSED}ms < ${MAX_ELECTION_MS}ms"
#             exit 0
#         else
#             log "✗ FAIL: election took ${ELAPSED}ms > ${MAX_ELECTION_MS}ms"
#             exit 1
#         fi
#     fi
#     sleep 0.01
# done

log "TODO: complete chaos test implementation"
exit 1
