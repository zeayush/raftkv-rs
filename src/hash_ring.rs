// src/hash_ring.rs — consistent hashing for key-to-node routing.
//
// Uses the `consistent-hash-rs` library (your Month 1 project).
//
// WHAT IS THIS FOR?
// ──────────────────
// In a single Raft cluster all data is replicated on ALL nodes (full replication).
// Consistent hashing is relevant when you have MULTIPLE Raft groups (shards),
// and you need to route each key to the correct shard.
//
// For this project: we use consistent hashing to route client requests to the
// right node (which knows the current leader for that shard). In a single-shard
// setup it's trivially the one cluster, but the infrastructure is here to extend.
//
// ConsistentHashRing API (from your lib):
//   ring.add(node_id: &str, weight: usize) -> bool
//   ring.remove(node_id: &str)             -> bool
//   ring.get(key: &str)                    -> Option<String>
//   ring.nodes()                           -> Vec<String>

use consistent_hash_rs::ConsistentHashRing;
use std::sync::Arc;

// ─── ClusterRouter ───────────────────────────────────────────────────────────

/// Routes keys to cluster nodes using consistent hashing.
///
/// Thread-safe: ConsistentHashRing uses an internal RwLock.
pub struct ClusterRouter {
    ring: Arc<ConsistentHashRing>,
}

impl ClusterRouter {
    /// Create a new router with the given initial nodes.
    ///
    /// `replicas` — number of virtual nodes per physical node (higher = more
    ///              uniform distribution, more memory). 150 is a good default.
    pub fn new(node_ids: &[String], replicas: usize) -> Self {
        let ring = ConsistentHashRing::new(replicas);
        for id in node_ids {
            ring.add(id, 1);
        }
        Self { ring: Arc::new(ring) }
    }

    /// Route a key to the responsible node.
    /// Returns None only if the ring is empty.
    pub fn route(&self, key: &str) -> Option<String> {
        self.ring.get(key)
    }

    /// Add a node to the ring when it joins the cluster.
    pub fn add_node(&self, node_id: &str) {
        self.ring.add(node_id, 1);
        tracing::info!(node_id, "added node to hash ring");
    }

    /// Remove a node from the ring when it leaves the cluster.
    pub fn remove_node(&self, node_id: &str) {
        self.ring.remove(node_id);
        tracing::info!(node_id, "removed node from hash ring");
    }

    /// List all nodes currently in the ring.
    pub fn all_nodes(&self) -> Vec<String> {
        self.ring.nodes()
    }

    /// Check if a key belongs to this node.
    ///
    /// Used by each node to decide whether to handle or forward a request.
    pub fn is_responsible(&self, node_id: &str, key: &str) -> bool {
        self.route(key).as_deref() == Some(node_id)
    }
}
