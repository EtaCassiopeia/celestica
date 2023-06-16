#[macro_use]
extern crate tracing;

use std::net::SocketAddr;

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use celestica_eventual_consistency::test_utils::MemStore;
use celestica_eventual_consistency::{
    EventuallyConsistentStoreExtension,
    ReplicatedStoreHandle,
    Storage,
};
use celestica_node::{
    CelesticaNodeBuilder,
    ConnectionConfig,
    Consistency,
    DCAwareSelector,
};
use clap::Parser;
use generated_types::celestica::namespace::v1::namespace_service_server::{
    NamespaceService,
    NamespaceServiceServer,
};
use generated_types::celestica::namespace::v1::{
    CreateNamespaceRequest,
    CreateNamespaceResponse,
    DeleteNamespaceRequest,
    DeleteNamespaceResponse,
    GetNamespacesRequest,
    GetNamespacesResponse,
    Namespace,
};
use generated_types::FILE_DESCRIPTOR_SET;
use serde_json::json;
use tonic::transport::Server as TonicServer;
use tonic::{Request, Response, Status};

// #[derive(Default)]
struct MyNamespaceService<S>
where
    S: Storage,
{
    inner_state: ReplicatedStoreHandle<S>,
}

impl<S> MyNamespaceService<S>
where
    S: Storage,
{
    pub fn new(inner_state: ReplicatedStoreHandle<S>) -> Self {
        Self { inner_state }
    }

    pub fn inner_state(&self) -> &ReplicatedStoreHandle<S> {
        &self.inner_state
    }
}

#[tonic::async_trait]
impl<S> NamespaceService for MyNamespaceService<S>
where
    S: Storage,
{
    async fn get_namespaces(
        &self,
        request: Request<GetNamespacesRequest>,
    ) -> std::result::Result<Response<GetNamespacesResponse>, Status> {
        info!("get_namespaces: {:?}", request);

        let namespace = Namespace {
            id: 1,
            name: "default".to_string(),
        };

        let response = GetNamespacesResponse {
            namespaces: vec![namespace],
        };

        Ok(Response::new(response))
    }

    async fn create_namespace(
        &self,
        request: Request<CreateNamespaceRequest>,
    ) -> std::result::Result<Response<CreateNamespaceResponse>, Status> {
        info!("create_namespace: {:?}", request);

        let namespace = Namespace {
            id: 1,
            name: "default".to_string(),
        };

        let response = CreateNamespaceResponse {
            namespace: Some(namespace),
        };

        Ok(Response::new(response))
    }

    async fn delete_namespace(
        &self,
        request: Request<DeleteNamespaceRequest>,
    ) -> std::result::Result<Response<DeleteNamespaceResponse>, Status> {
        info!("delete_namespace: {:?}", request);
        let response = DeleteNamespaceResponse {};

        Ok(Response::new(response))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Starting node...");

    tracing_subscriber::fmt::init();
    let args: Args = Args::parse();

    let storage = MemStore::default();
    let connection_cfg = ConnectionConfig::new(
        args.cluster_listen_addr,
        args.public_addr.unwrap_or(args.cluster_listen_addr),
        args.seeds.into_iter(),
    );

    let node = CelesticaNodeBuilder::<DCAwareSelector>::new(1, connection_cfg)
        .connect()
        .await?;
    let store = node
        .add_extension(EventuallyConsistentStoreExtension::new(storage))
        .await?;

    let handle = store.handle();

    let namespace_server = MyNamespaceService::new(handle);

    let reflection_server = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()?;

    info!("NamespaceService listening on {}", args.grpc_listen_addr);

    TonicServer::builder()
        .add_service(NamespaceServiceServer::new(namespace_server))
        .add_service(reflection_server)
        .serve(args.grpc_listen_addr)
        .await?;

    node.shutdown().await;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[arg(long)]
    /// The unique ID of the node.
    node_id: String,

    #[arg(long = "seed")]
    /// The set of seed nodes.
    ///
    /// This is used to kick start the auto-discovery of nodes within the cluster.
    seeds: Vec<String>,

    #[arg(long, default_value = "127.0.0.1:9000")]
    /// The address for the GRPC server to listen on.
    ///
    /// This is what will serve the API.
    grpc_listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:8001")]
    /// The address for the cluster RPC system to listen on.
    cluster_listen_addr: SocketAddr,

    #[arg(long)]
    /// The public address for the node to broadcast to other nodes.
    ///
    /// If not provided the `cluster_listen_addr` is used which will only
    /// work when running a cluster on the same local network.
    public_addr: Option<SocketAddr>,

    #[arg(long)]
    /// The path to store the data.
    data_dir: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Params {
    keyspace: String,
    key: u64,
}

async fn get_value(
    Path(params): Path<Params>,
    State(handle): State<ReplicatedStoreHandle<MemStore>>,
) -> Result<Bytes, StatusCode> {
    info!(
        doc_id = params.key,
        keyspace = params.keyspace,
        "Getting document!"
    );

    let doc = handle
        .get(&params.keyspace, params.key)
        .await
        .map_err(|e| {
            error!(error = ?e, doc_id = params.key, "Failed to fetch doc.");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    match doc {
        None => Err(StatusCode::NOT_FOUND),
        Some(doc) => Ok(Bytes::copy_from_slice(doc.data())),
    }
}

async fn set_value(
    Path(params): Path<Params>,
    State(handle): State<ReplicatedStoreHandle<MemStore>>,
    data: Bytes,
) -> Result<Json<serde_json::Value>, StatusCode> {
    info!(
        doc_id = params.key,
        keyspace = params.keyspace,
        "Storing document!"
    );

    handle
        .put(&params.keyspace, params.key, data, Consistency::EachQuorum)
        .await
        .map_err(|e| {
            error!(error = ?e, doc_id = params.key, "Failed to fetch doc.");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(json!({
        "key": params.key,
        "keyspace": params.keyspace,
    })))
}
