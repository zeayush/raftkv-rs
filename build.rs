// build.rs — compile .proto files into Rust code via tonic-build.
//
// This runs automatically before `cargo build`. The generated code lands in
// OUT_DIR and is pulled in with tonic::include_proto!() in src/server/*.rs.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        // Derive serde::{Serialize, Deserialize} on generated types so we can
        // persist LogEntry to RocksDB as JSON during development.
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &["proto/raft.proto", "proto/kv.proto"],
            &["proto"],
        )?;
    Ok(())
}
