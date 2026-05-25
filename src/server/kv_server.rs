// src/server/kv_server.rs — gRPC KV service (client-facing).
//
// All writes must go through the leader.
// Non-leader nodes return a redirect_to address so the client can retry.
//
// For linearisable reads (strong consistency), reads also go through the
// leader. For stale reads you could serve them locally — a good extension.

use crate::raft::{
    log::KvCommand,
    node::{RaftNode, kv_proto},
    state::Role,
};
use kv_proto::{
    kv_service_server::KvService,
    DeleteRequest, DeleteResponse,
    GetLeaderRequest, GetLeaderResponse,
    GetRequest, GetResponse,
    SetRequest, SetResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct KvGrpcServer {
    pub node: Arc<RaftNode>,
}

#[tonic::async_trait]
impl KvService for KvGrpcServer {
    async fn get(
        &self,
        request: Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let key = request.into_inner().key;

        // For linearisable reads the leader must confirm it's still the leader.
        // Simple approach: only serve reads from the leader.
        let redirect = self.redirect_if_not_leader().await;
        if let Some(addr) = redirect {
            return Ok(Response::new(GetResponse {
                value:       vec![],
                found:       false,
                redirect_to: addr,
            }));
        }

        // TODO: perform the read from the state machine.
        // Hint: self.node.storage.state_get(key.as_bytes())
        todo!("read key from state machine and return GetResponse")
    }

    async fn set(
        &self,
        request: Request<SetRequest>,
    ) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();

        let redirect = self.redirect_if_not_leader().await;
        if let Some(addr) = redirect {
            return Ok(Response::new(SetResponse { success: false, redirect_to: addr }));
        }

        // TODO:
        // 1. Build KvCommand::Set { key, value }.
        // 2. Call self.node.propose(command).await to get the log index.
        // 3. Wait for that index to be committed (poll state.last_applied, or
        //    use a oneshot channel registered in a commit_notifier map).
        // 4. Return SetResponse { success: true, redirect_to: "".into() }.
        todo!("propose Set command and wait for commit")
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        let redirect = self.redirect_if_not_leader().await;
        if let Some(addr) = redirect {
            return Ok(Response::new(DeleteResponse { success: false, redirect_to: addr }));
        }

        // TODO: similar to set — propose KvCommand::Delete and wait for commit.
        todo!("propose Delete command and wait for commit")
    }

    async fn get_leader(
        &self,
        _request: Request<GetLeaderRequest>,
    ) -> Result<Response<GetLeaderResponse>, Status> {
        let state = self.node.state.lock().await;
        let leader_id = state.current_leader.clone().unwrap_or_default();

        // Look up the leader's address from config.
        let leader_address = self.node.config.cluster.peers.iter()
            .find(|p| p.id == leader_id)
            .map(|p| p.address.clone())
            .unwrap_or_default();

        Ok(Response::new(GetLeaderResponse { leader_id, leader_address }))
    }
}

impl KvGrpcServer {
    /// Returns Some(redirect_address) if this node is not the leader.
    async fn redirect_if_not_leader(&self) -> Option<String> {
        let state = self.node.state.lock().await;
        if state.role != Role::Leader {
            let addr = state.current_leader.as_ref().and_then(|id| {
                self.node.config.cluster.peers.iter()
                    .find(|p| &p.id == id)
                    .map(|p| p.address.clone())
            }).unwrap_or_default();
            Some(addr)
        } else {
            None
        }
    }
}
