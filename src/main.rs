// src/main.rs — entry point.
//
// Parses CLI args, loads config, starts gRPC servers and background tasks.

mod config;
mod storage;
mod raft;
mod server;
mod client;
mod membership;
mod hash_ring;
mod redis_proxy;

use clap::Parser;
use config::Config;
use raft::RaftNode;
use server::{
    raft_server::RaftGrpcServer,
    kv_server::KvGrpcServer,
};
use raft::node::{raft_proto, kv_proto};
use raft_proto::raft_service_server::RaftServiceServer;
use kv_proto::kv_service_server::KvServiceServer;
use std::sync::Arc;
use anyhow::Result;

// ─── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "raftkv", about = "A Raft-based distributed key-value store")]
struct Cli {
    /// Path to the TOML config file.
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Initialise structured logging. RUST_LOG env var controls the level.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let config = Arc::new(Config::from_file(&cli.config)?);

    tracing::info!(
        node_id = %config.node.id,
        address = %config.node.address,
        "Starting raftkv node"
    );

    // ── Initialise Raft node ─────────────────────────────────────────────────
    let node = RaftNode::new(config.clone()).await?;

    // ── Start background tasks (election timer, apply loop) ──────────────────
    node.clone().run().await;

    // ── Start gRPC server ────────────────────────────────────────────────────
    let raft_svc = RaftServiceServer::new(RaftGrpcServer { node: node.clone() });
    let kv_svc   = KvServiceServer::new(KvGrpcServer   { node: node.clone() });

    let addr = config.node.address.parse()?;
    tracing::info!(%addr, "gRPC server listening");

    let grpc_handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(raft_svc)
            .add_service(kv_svc)
            .serve(addr),
    );

    // ── Start Redis proxy ─────────────────────────────────────────────────────
    let redis_addr = config.redis_proxy.listen.clone();
    let redis_node = node.clone();
    let redis_handle = tokio::spawn(async move {
        redis_proxy::run_redis_proxy(&redis_addr, redis_node).await
    });

    // Wait for either server to exit (they shouldn't unless there's an error).
    tokio::select! {
        result = grpc_handle  => { result??; }
        result = redis_handle => { result??; }
    }

    Ok(())
}
