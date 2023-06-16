#![warn(unused_crate_dependencies)]

/// This module imports the generated protobuf code into a Rust module
/// hierarchy that matches the namespace hierarchy of the protobuf
/// definitions
pub mod celestica {

    pub mod ingester {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/celestica.ingester.v1.rs"));
            // include!(concat!(env!("OUT_DIR"), "/celestica.ingester.v1.serde.rs"));
        }
    }

    pub mod namespace {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/celestica.namespace.v1.rs"));
            // include!(concat!(env!("OUT_DIR"), "/celestica.namespace.v1.serde.rs"));
        }
    }

    pub mod querier {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/celestica.querier.v1.rs"));
            // include!(concat!(env!("OUT_DIR"), "/celestica.querier.v1.serde.rs"));
        }
    }

    pub mod table {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/celestica.table.v1.rs"));
            // include!(concat!(env!("OUT_DIR"), "/celestica.table.v1.serde.rs"));
        }
    }

    pub mod wal {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/celestica.wal.v1.rs"));
            // include!(concat!(env!("OUT_DIR"), "/celestica.wal.v1.serde.rs"));
        }
    }

    pub mod pbdata {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/celestica.pbdata.v1.rs"));
            // include!(concat!(env!("OUT_DIR"), "/celestica.pbdata.v1.serde.rs"));
        }
    }
}

/// Protobuf file descriptor containing all generated types.
/// Useful in gRPC reflection. see https://timvw.be/2022/04/28/notes-on-using-grpc-with-rust-and-tonic/
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("proto_descriptor");
