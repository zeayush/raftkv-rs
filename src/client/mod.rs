// src/client/mod.rs — cluster-aware Raft KV client.
//
// The client:
//   1. Connects to any node in the cluster.
//   2. On receiving a redirect (non-leader response), follows the redirect.
//   3. Retries with exponential backoff on transient errors.
//
// This is used internally (e.g. chaos tests, benchmarks) and could be
// published as a separate crate for external users.

use crate::raft::node::kv_proto::{
    kv_service_client::KvServiceClient,
    GetRequest, SetRequest, DeleteRequest,
};
use anyhow::{anyhow, Result};
use tonic::transport::Channel;

// ─── RaftKvClient ────────────────────────────────────────────────────────────

pub struct RaftKvClient {
    /// Address of the node we're currently talking to (may change on redirect).
    current_address: String,
    client: KvServiceClient<Channel>,
}

impl RaftKvClient {
    /// Connect to any node in the cluster.
    pub async fn connect(address: &str) -> Result<Self> {
        let client = KvServiceClient::connect(format!("http://{}", address)).await?;
        Ok(Self {
            current_address: address.to_string(),
            client,
        })
    }

    // ── KV operations ────────────────────────────────────────────────────────

    pub async fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        // TODO:
        // 1. Send GetRequest.
        // 2. If response.redirect_to is non-empty → reconnect and retry.
        // 3. If found → return Some(response.value).
        // 4. Else → return None.
        todo!("send GET request, handle redirect")
    }

    pub async fn set(&mut self, key: &str, value: Vec<u8>) -> Result<()> {
        // TODO: send SetRequest, handle redirect, return Ok(()) on success.
        todo!("send SET request, handle redirect")
    }

    pub async fn delete(&mut self, key: &str) -> Result<()> {
        // TODO: send DeleteRequest, handle redirect.
        todo!("send DEL request, handle redirect")
    }

    // ── Redirect helper ───────────────────────────────────────────────────────

    /// Reconnect to a different node (used after receiving a redirect).
    async fn redirect(&mut self, new_address: &str) -> Result<()> {
        // TODO:
        // self.client = KvServiceClient::connect(format!("http://{}", new_address)).await?;
        // self.current_address = new_address.to_string();
        todo!("reconnect client to new_address")
    }
}
