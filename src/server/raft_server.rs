// src/server/raft_server.rs — gRPC service implementation for the Raft protocol.
//
// This is what OTHER NODES call when doing Raft RPCs.
// The three RPCs we must implement:
//   RequestVote    — called by Candidates seeking our vote
//   AppendEntries  — called by the Leader (also serves as heartbeat)
//   InstallSnapshot — called by the Leader when we're too far behind

use crate::raft::node::{RaftNode, raft_proto};
use raft_proto::{
    raft_service_server::RaftService,
    AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct RaftGrpcServer {
    pub node: Arc<RaftNode>,
}

#[tonic::async_trait]
impl RaftService for RaftGrpcServer {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let (term, vote_granted) = self.node.handle_request_vote(
            req.term,
            req.candidate_id,
            req.last_log_index,
            req.last_log_term,
        ).await;

        Ok(Response::new(RequestVoteResponse { term, vote_granted }))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        let req = request.into_inner();

        // TODO:
        // 1. If req.term < current_term → reject (return success=false, our term).
        // 2. If req.term >= current_term → step down to follower, update term.
        // 3. Reset election timer (we have a valid leader).
        // 4. Perform the log consistency check:
        //    - If prev_log_index > 0 and we don't have entry at prev_log_index
        //      with term == prev_log_term → reject with conflict hint.
        // 5. Call log.append_entries(req.entries).
        // 6. Update commit_index = min(req.leader_commit, our last log index).
        // 7. Signal apply loop if commit_index advanced.
        // 8. Return success=true.
        todo!("implement AppendEntries RPC handler")
    }

    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequest>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let current_term = self.node.handle_install_snapshot(
            req.term,
            req.leader_id,
            req.last_included_index,
            req.last_included_term,
            req.data,
        ).await.map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(InstallSnapshotResponse { term: current_term }))
    }
}
