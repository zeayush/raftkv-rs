// src/raft/node.rs — the central RaftNode struct that ties everything together.
//
// RaftNode is the main actor. It holds:
//   - Persistent state (via StorageEngine)
//   - Volatile state (RaftState behind a Mutex)
//   - The log (RaftLog behind a Mutex)
//   - gRPC client connections to each peer
//   - Channels for internal coordination (heartbeat reset, apply loop, etc.)

use crate::{
    config::Config,
    raft::{log::RaftLog, state::RaftState},
    storage::StorageEngine,
};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

// Re-export generated gRPC types for use in the raft module.
pub mod raft_proto {
    tonic::include_proto!("raft");
}
pub mod kv_proto {
    tonic::include_proto!("kv");
}

// ─── RaftNode ────────────────────────────────────────────────────────────────

pub struct RaftNode {
    pub config:  Arc<Config>,
    pub storage: Arc<StorageEngine>,

    /// Volatile Raft state. Locked per operation — never held across awaits
    /// that involve network I/O (deadlock risk).
    pub state: Arc<Mutex<RaftState>>,

    /// The replicated log. Separate lock from state so replication and
    /// election don't block each other unnecessarily.
    pub log: Arc<Mutex<RaftLog>>,

    /// Send to this channel to reset the election timer
    /// (e.g., on receiving a heartbeat or granting a vote).
    pub reset_election_timer_tx: mpsc::Sender<()>,

    /// Receive side of the election timer reset channel.
    /// Owned by the election timer loop.
    pub reset_election_timer_rx: Arc<Mutex<mpsc::Receiver<()>>>,

    /// Signal the apply loop that commit_index has advanced.
    pub apply_tx: mpsc::Sender<()>,

    /// Receive side for the apply loop.
    pub apply_rx: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl RaftNode {
    /// Construct a new RaftNode from config, restoring durable state.
    pub async fn new(config: Arc<Config>) -> anyhow::Result<Arc<Self>> {
        let storage = Arc::new(StorageEngine::open(&config.node.data_dir)?);

        // Restore persistent state from storage.
        let current_term = storage.load_current_term()?;
        let _voted_for   = storage.load_voted_for()?;

        let peer_ids: Vec<String> = config.cluster.peers.iter()
            .map(|p| p.id.clone())
            .collect();

        let state = Arc::new(Mutex::new(
            RaftState::new(config.node.id.clone(), peer_ids)
        ));

        let log = Arc::new(Mutex::new(RaftLog::new(storage.clone())?));

        let (reset_tx, reset_rx) = mpsc::channel(32);
        let (apply_tx, apply_rx) = mpsc::channel(128);

        let node = Arc::new(Self {
            config,
            storage,
            state,
            log,
            reset_election_timer_tx: reset_tx,
            reset_election_timer_rx: Arc::new(Mutex::new(reset_rx)),
            apply_tx,
            apply_rx: Arc::new(Mutex::new(apply_rx)),
        });

        tracing::info!(
            node_id = %node.config.node.id,
            term    = current_term,
            "RaftNode initialized"
        );

        Ok(node)
    }

    /// Start all background tasks: election timer, apply loop.
    /// The leader loop is started by the election module upon winning.
    pub async fn run(self: Arc<Self>) {
        let apply_node  = self.clone();
        let elect_node  = self.clone();

        tokio::spawn(async move { apply_node.run_apply_loop().await });
        tokio::spawn(async move { elect_node.run_election_timer().await });
    }

    /// Helper: reset the election timer (call after receiving valid leader RPC).
    pub async fn reset_election_timer(&self) {
        // Best-effort send; if the channel is full the timer will fire soon anyway.
        let _ = self.reset_election_timer_tx.try_send(());
    }

    /// Helper: get a gRPC channel to a peer by ID.
    /// TODO: cache these connections rather than creating them each time.
    pub async fn peer_channel(
        &self,
        peer_id: &str,
    ) -> anyhow::Result<tonic::transport::Channel> {
        let addr = self.config.cluster.peers.iter()
            .find(|p| p.id == peer_id)
            .map(|p| format!("http://{}", p.address))
            .ok_or_else(|| anyhow::anyhow!("unknown peer: {}", peer_id))?;

        let channel = tonic::transport::Channel::from_shared(addr)?
            .connect()
            .await?;

        Ok(channel)
    }
}
