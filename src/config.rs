// src/config.rs — cluster configuration loaded at startup.
//
// The config file is a TOML file passed via --config <path>.
// Example:
//
//   [node]
//   id       = "node1"
//   address  = "0.0.0.0:7001"      # gRPC listen address
//   data_dir = "/data/node1"        # RocksDB + snapshot directory
//
//   [cluster]
//   peers = [
//     { id = "node2", address = "node2:7001" },
//     { id = "node3", address = "node3:7001" },
//   ]
//
//   [raft]
//   heartbeat_interval_ms   = 50
//   election_timeout_min_ms = 150
//   election_timeout_max_ms = 300
//   snapshot_threshold      = 1000   # snapshot after N committed entries
//
//   [redis_proxy]
//   listen = "0.0.0.0:6379"

use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use anyhow::Context;

// ─── Top-level config ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node:        NodeConfig,
    pub cluster:     ClusterConfig,
    pub raft:        RaftConfig,
    pub redis_proxy: RedisProxyConfig,
}

// ─── Sub-configs ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique string identifier for this node (e.g. "node1").
    pub id: String,

    /// gRPC listen address, e.g. "0.0.0.0:7001".
    pub address: String,

    /// Directory for RocksDB data and snapshots.
    pub data_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub id:      String,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// All OTHER nodes in the cluster (not including self).
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// How often the leader sends heartbeats (ms).
    pub heartbeat_interval_ms: u64,

    /// Lower bound of the randomised election timeout (ms).
    /// Must be > heartbeat_interval_ms.
    pub election_timeout_min_ms: u64,

    /// Upper bound of the randomised election timeout (ms).
    pub election_timeout_max_ms: u64,

    /// Take a snapshot after this many committed log entries accumulate.
    pub snapshot_threshold: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisProxyConfig {
    /// TCP address for the Redis RESP proxy (e.g. "0.0.0.0:6379").
    pub listen: String,
}

// ─── Loader ──────────────────────────────────────────────────────────────────

impl Config {
    /// Load and parse a TOML config file from `path`.
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let raw = fs::read_to_string(path.as_ref())
            .with_context(|| format!("reading config file {:?}", path.as_ref()))?;
        let cfg: Config = toml::from_str(&raw)
            .with_context(|| "parsing config TOML")?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Basic sanity checks on the loaded config.
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.raft.heartbeat_interval_ms < self.raft.election_timeout_min_ms,
            "heartbeat_interval_ms must be less than election_timeout_min_ms"
        );
        anyhow::ensure!(
            self.raft.election_timeout_min_ms < self.raft.election_timeout_max_ms,
            "election_timeout_min_ms must be less than election_timeout_max_ms"
        );
        anyhow::ensure!(
            !self.node.id.is_empty(),
            "node.id must not be empty"
        );
        Ok(())
    }
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms:   50,
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            snapshot_threshold:      1000,
        }
    }
}
