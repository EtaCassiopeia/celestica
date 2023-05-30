use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use celestica_rpc::RpcClient;
use chitchat::serialize::Serializable;
use chitchat::transport::{Socket, Transport};
use chitchat::ChitchatMessage;
use tracing::trace;

use crate::rpc::network::RpcNetwork;
use crate::rpc::services::chitchat_impl::{ChitchatRpcMessage, ChitchatService};
use crate::Clock;

#[derive(Clone)]
/// Chitchat compatible transport built on top of an existing RPC connection.
///
/// This allows us to maintain a single connection rather than both a UDP and TCP connection.
pub struct ChitchatTransport(Arc<ChitchatTransportInner>);

impl ChitchatTransport {
    /// Creates a new GRPC transport instances.
    pub fn new(
        rpc_listen_addr: SocketAddr,
        clock: Clock,
        network: RpcNetwork,
        messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
    ) -> Self {
        Self(Arc::new(ChitchatTransportInner {
            rpc_listen_addr,
            clock,
            network,
            messages,
        }))
    }
}

impl Deref for ChitchatTransport {
    type Target = ChitchatTransportInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl Transport for ChitchatTransport {
    async fn open(
        &self,
        listen_addr: SocketAddr,
    ) -> Result<Box<dyn Socket>, anyhow::Error> {
        if listen_addr != self.rpc_listen_addr {
            bail!(
                "Listen addr does not match RPC server address. {listen_addr} != {}",
                self.rpc_listen_addr
            );
        }

        Ok(Box::new(GrpcConnection {
            clock: self.clock.clone(),
            self_addr: self.rpc_listen_addr,
            network: self.network.clone(),
            messages: self.messages.clone(),
        }))
    }
}

pub struct ChitchatTransportInner {
    /// The socket address the RPC server is listening on.
    rpc_listen_addr: SocketAddr,

    /// The node clock.
    clock: Clock,

    /// The RPC network of clients.
    network: RpcNetwork,

    /// Received messages to be sent to the Chitchat cluster.
    messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
}

pub struct GrpcConnection {
    clock: Clock,
    self_addr: SocketAddr,
    network: RpcNetwork,
    messages: flume::Receiver<(SocketAddr, ChitchatMessage)>,
}

#[async_trait]
impl Socket for GrpcConnection {
    async fn send(
        &mut self,
        to: SocketAddr,
        msg: ChitchatMessage,
    ) -> Result<(), anyhow::Error> {
        trace!(to = %to, msg = ?msg, "Gossip send");
        let data = msg.serialize_to_vec();

        let channel = self.network.get_or_connect(to);

        let timestamp = self.clock.get_time().await;
        let msg = ChitchatRpcMessage {
            data,
            source: self.self_addr,
            timestamp,
        };

        let client = RpcClient::<ChitchatService>::new(channel);
        client.send(&msg).await?;

        Ok(())
    }

    async fn recv(&mut self) -> Result<(SocketAddr, ChitchatMessage), anyhow::Error> {
        let msg = self.messages.recv_async().await?;
        Ok(msg)
    }
}
