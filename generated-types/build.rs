//! Compiles Protocol Buffers into native Rust types.
//!

// see https://timvw.be/2022/04/28/notes-on-using-grpc-with-rust-and-tonic/

use std::env;
use std::path::{Path, PathBuf};

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protos");

    generate_grpc_types(&root)?;

    Ok(())
}

fn generate_grpc_types(root: &Path) -> Result<()> {
    let ingester_path = root.join("celestica/ingester/v1");
    let namespace_path = root.join("celestica/namespace/v1");
    let querier_path = root.join("celestica/querier/v1");
    let table_path = root.join("celestica/table/v1");
    let wal_path = root.join("celestica/wal/v1");

    let proto_files = vec![
        ingester_path.join("persist.proto"),
        ingester_path.join("write.proto"),
        namespace_path.join("service.proto"),
        querier_path.join("flight.proto"),
        table_path.join("service.proto"),
        wal_path.join("wal.proto"),
    ];

    // Tell cargo to recompile if any of these proto files are changed
    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let mut config = prost_build::Config::new();

    config.compile_well_known_types();

    let descriptor_path =
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        // protoc in ubuntu builder needs this option
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_with_config(config, &proto_files, &[root])?;

    Ok(())
}
