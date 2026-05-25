// src/redis_proxy.rs — Redis RESP protocol proxy.
//
// WHAT IS RESP?
// ──────────────
// Redis Serialization Protocol (RESP) is a simple line-based protocol.
// A command like `SET foo bar` looks like this on the wire:
//
//   *3\r\n          ← array of 3 elements
//   $3\r\nSET\r\n   ← bulk string "SET"
//   $3\r\nfoo\r\n   ← bulk string "foo"
//   $3\r\nbar\r\n   ← bulk string "bar"
//
// A simple string reply ("+OK\r\n") or error ("-ERR ...\r\n").
// A bulk string reply: "$3\r\nbar\r\n" or "$-1\r\n" (nil).
//
// We support: GET, SET, DEL (plus PING for health checks).
//
// HOW IT CONNECTS TO RAFT:
//   GET  → read from state machine (via KV gRPC or directly from RaftNode)
//   SET  → propose KvCommand::Set via RaftNode::propose()
//   DEL  → propose KvCommand::Delete via RaftNode::propose()

use crate::raft::{node::RaftNode, log::KvCommand};
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

// ─── Server ──────────────────────────────────────────────────────────────────

/// Listen for Redis clients on `addr` and proxy commands to the Raft node.
pub async fn run_redis_proxy(addr: &str, node: Arc<RaftNode>) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Redis proxy listening");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        tracing::debug!(%peer_addr, "Redis client connected");
        let node = node.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, node).await {
                tracing::warn!(%peer_addr, error = %e, "Redis client error");
            }
        });
    }
}

// ─── Per-connection handler ───────────────────────────────────────────────────

async fn handle_client(stream: TcpStream, node: Arc<RaftNode>) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    loop {
        // Parse one RESP command from the connection.
        let args = match parse_resp_command(&mut reader).await? {
            None => break, // client disconnected
            Some(a) => a,
        };

        if args.is_empty() {
            continue;
        }

        let response = dispatch_command(args, &node).await;
        writer.write_all(response.as_bytes()).await?;
    }

    Ok(())
}

// ─── Command dispatch ─────────────────────────────────────────────────────────

async fn dispatch_command(args: Vec<String>, node: &RaftNode) -> String {
    match args[0].to_uppercase().as_str() {
        "PING" => "+PONG\r\n".to_string(),

        "GET" => {
            if args.len() < 2 {
                return "-ERR wrong number of arguments for GET\r\n".to_string();
            }
            // TODO:
            // 1. node.storage.state_get(args[1].as_bytes())
            // 2. If Some(val) → format!("${}\r\n{}\r\n", val.len(), String::from_utf8_lossy(&val))
            // 3. If None      → "$-1\r\n" (nil bulk string)
            todo!("implement GET: read from state machine")
        }

        "SET" => {
            if args.len() < 3 {
                return "-ERR wrong number of arguments for SET\r\n".to_string();
            }
            let cmd = KvCommand::Set {
                key:   args[1].clone(),
                value: args[2].as_bytes().to_vec(),
            };
            // TODO:
            // 1. node.propose(cmd).await → log_index
            // 2. Wait for log_index to be applied (poll state.last_applied).
            //    Hint: use a small sleep-poll loop or a commit notifier map.
            // 3. Return "+OK\r\n" on success, "-ERR ...\r\n" on failure.
            todo!("implement SET: propose and wait for commit")
        }

        "DEL" => {
            if args.len() < 2 {
                return "-ERR wrong number of arguments for DEL\r\n".to_string();
            }
            let cmd = KvCommand::Delete { key: args[1].clone() };
            // TODO: similar to SET, but return ":1\r\n" (integer reply) on success.
            todo!("implement DEL: propose and wait for commit")
        }

        _ => format!("-ERR unknown command '{}'\r\n", args[0]),
    }
}

// ─── RESP parser ─────────────────────────────────────────────────────────────

/// Parse one RESP command from the buffered reader.
///
/// Returns None on EOF, Some(args) on a complete command.
///
/// RESP array format:
///   *<count>\r\n
///   (<$<len>\r\n<data>\r\n>) × count
async fn parse_resp_command(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<Option<Vec<String>>> {
    // Read the array header line (e.g. "*3\r\n").
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None); // EOF
    }

    let line = line.trim_end_matches("\r\n");
    if !line.starts_with('*') {
        return Err(anyhow::anyhow!("expected RESP array, got: {:?}", line));
    }

    let count: usize = line[1..].parse()?;
    let mut args = Vec::with_capacity(count);

    for _ in 0..count {
        // Read bulk string header (e.g. "$3\r\n").
        let mut header = String::new();
        reader.read_line(&mut header).await?;
        let header = header.trim_end_matches("\r\n");
        if !header.starts_with('$') {
            return Err(anyhow::anyhow!("expected bulk string, got: {:?}", header));
        }
        let len: usize = header[1..].parse()?;

        // Read `len` bytes + the trailing \r\n.
        let mut data = vec![0u8; len + 2];
        // TODO: use read_exact for the data bytes.
        // Hint: tokio::io::AsyncReadExt::read_exact
        todo!("read bulk string data bytes using read_exact");
        // args.push(String::from_utf8(data[..len].to_vec())?);
    }

    Ok(Some(args))
}
