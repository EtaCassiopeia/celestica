use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::{AlignedVec, Archive, Serialize};

use crate::net::Status;
use crate::request::{Request, RequestContents};
use crate::{Body, SCRATCH_SPACE};

/// A specific handler key.
///
/// This is in the format of (service_name, handler_path).
pub type HandlerKey = u64;

/// A registry system used for linking a service's message handlers
/// with the RPC system at runtime.
///
/// Since the RPC system cannot determine what message payload matches
/// with which handler at compile time, it must dynamically link them
/// at runtime.
///
/// Not registering a handler will cause the handler to not be triggered
/// even if a valid message comes through.
///
pub struct ServiceRegistry<Svc> {
    handlers: BTreeMap<HandlerKey, Arc<dyn OpaqueMessageHandler>>,
    service: Arc<Svc>,
}

impl<Svc> ServiceRegistry<Svc>
where
    Svc: RpcService + Send + Sync + 'static,
{
    pub(crate) fn new(service: Svc) -> Self {
        Self {
            handlers: BTreeMap::new(),
            service: Arc::new(service),
        }
    }

    /// Consumes the registry into the produced handlers.
    pub(crate) fn into_handlers(
        self,
    ) -> BTreeMap<HandlerKey, Arc<dyn OpaqueMessageHandler>> {
        self.handlers
    }

    /// Adds a new handler to the registry.
    ///
    /// This is done in the form of specifying what message types are handled
    /// by the service via the generic.
    pub fn add_handler<Msg>(&mut self)
    where
        Msg: RequestContents + Sync + Send + 'static,
        Svc: Handler<Msg>,
    {
        let phantom = PhantomHandler {
            handler: self.service.clone(),
            _msg: PhantomData::<Msg>::default(),
        };

        let uri = crate::to_uri_path(Svc::service_name(), <Svc as Handler<Msg>>::path());
        self.handlers.insert(crate::hash(&uri), Arc::new(phantom));
    }
}

/// A standard RPC server that handles messages.
pub trait RpcService: Sized {
    /// An optional name of the service.
    ///
    /// This can be used to prevent overlaps or clashes
    /// in handlers as two services may handle the same
    /// message but behave differently, to distinguish between
    /// these services, the message paths also use the service name
    /// to create a unique key.
    fn service_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Register all message handlers for this server with the registry.
    ///
    /// See [ServiceRegistry] for more information.
    fn register_handlers(registry: &mut ServiceRegistry<Self>);
}

#[async_trait]
/// A generic RPC message handler.
///
pub trait Handler<Msg>: RpcService
where
    Msg: RequestContents,
{
    /// Our reply can be any type that implements [Archive] and [Serialize] as part
    /// of the [rkyv] package. Here we're just echoing the message back.
    type Reply: TryIntoBody;

    /// The path of the message, this is similar to the service name which can
    /// be used to avoid conflicts, by default this uses the name of the message type.
    fn path() -> &'static str {
        std::any::type_name::<Msg>()
    }

    /// Process a message.
    /// We get passed a [Request] which is a thin wrapper around the inner content of
    /// the specified type as defined by [RequestContents::Content]
    ///
    /// This means we are simply being given a zero-copy view of the message rather
    /// than a owned value. If you need a owned version which is not tied ot the
    /// request buffer, you can use the `to_owned` method which will attempt to
    /// deserialize the inner message/view.
    async fn on_message(&self, msg: Request<Msg>) -> Result<Self::Reply, Status>;
}

#[async_trait]
pub(crate) trait OpaqueMessageHandler: Send + Sync {
    async fn try_handle(
        &self,
        remote_addr: SocketAddr,
        data: Body,
    ) -> Result<Body, AlignedVec>;
}

struct PhantomHandler<H, Msg>
where
    H: Send + Sync + 'static,
    Msg: Send + 'static,
{
    handler: Arc<H>,
    _msg: PhantomData<Msg>,
}

#[async_trait]
impl<H, Msg> OpaqueMessageHandler for PhantomHandler<H, Msg>
where
    Msg: RequestContents + Send + Sync + 'static,
    H: Handler<Msg> + Send + Sync + 'static,
{
    async fn try_handle(
        &self,
        remote_addr: SocketAddr,
        data: Body,
    ) -> Result<Body, AlignedVec> {
        let view = match Msg::from_body(data).await {
            Ok(view) => view,
            Err(status) => {
                let error = rkyv::to_bytes::<_, SCRATCH_SPACE>(&status)
                    .unwrap_or_else(|_| AlignedVec::new());
                return Err(error);
            },
        };

        let msg = Request::new(remote_addr, view);

        self.handler
            .on_message(msg)
            .await
            .and_then(|reply| reply.try_into_body())
            .map_err(|status| {
                rkyv::to_bytes::<_, SCRATCH_SPACE>(&status)
                    .unwrap_or_else(|_| AlignedVec::new())
            })
    }
}

/// The serializer trait converting replies into hyper bodies.
///
/// This is a light abstraction to allow users to be able to
/// stream data across the RPC system which may not fit in memory.
///
/// Any types which implement [TryAsBody] will implement this type.
pub trait TryIntoBody {
    /// Try convert the reply into a body or return an error
    /// status.
    fn try_into_body(self) -> Result<Body, Status>;
}

/// The serializer trait for converting replies into hyper bodies
/// using a reference to self.
///
/// This will work for most implementations but if you want to stream
/// hyper bodies for example, you cannot implement this trait.
pub trait TryAsBody {
    /// Try convert the reply into a body or return an error
    /// status.
    fn try_as_body(&self) -> Result<Body, Status>;
}

impl<T> TryAsBody for T
where
    T: Archive + Serialize<AllocSerializer<SCRATCH_SPACE>>,
{
    fn try_as_body(&self) -> Result<Body, Status> {
        rkyv::to_bytes::<_, SCRATCH_SPACE>(self)
            .map(|v| Body::from(v.to_vec()))
            .map_err(|e| Status::internal(e.to_string()))
    }
}

impl<T> TryIntoBody for T
where
    T: TryAsBody,
{
    fn try_into_body(self) -> Result<Body, Status> {
        <Self as TryAsBody>::try_as_body(&self)
    }
}

impl TryIntoBody for Body {
    fn try_into_body(self) -> Result<Body, Status> {
        Ok(self)
    }
}
