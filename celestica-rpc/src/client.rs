use std::borrow::Cow;
use std::marker::PhantomData;
use std::time::Duration;

use crate::handler::{Handler, RpcService, TryAsBody, TryIntoBody};
use crate::net::{Channel, Status};
use crate::request::{MessageMetadata, RequestContents};
use crate::Body;

/// A type alias for the returned data view of the RPC message reply.
pub type MessageReply<Svc, Msg> =
    <<Svc as Handler<Msg>>::Reply as RequestContents>::Content;

pub struct RpcClient<Svc>
where
    Svc: RpcService,
{
    channel: Channel,
    timeout: Option<Duration>,
    _p: PhantomData<Svc>,
}

impl<Svc> Clone for RpcClient<Svc>
where
    Svc: RpcService,
{
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            timeout: self.timeout,
            _p: PhantomData,
        }
    }
}
impl<Svc> RpcClient<Svc>
where
    Svc: RpcService,
{
    /// Creates a new RPC client which can handle a new service type.
    ///
    /// [RpcClient]'s are cheap to create and should be preferred over
    /// locking or other synchronization primitives.
    pub fn new(channel: Channel) -> Self {
        Self {
            channel,
            timeout: None,
            _p: PhantomData,
        }
    }

    /// Sets a timeout of a given amount of time.
    ///
    /// If any requests exceed this amount of time `Status::timeout` is returned.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = Some(timeout);
    }

    /// Creates a new RPC client which can handle a new service type.
    ///
    /// [RpcClient]'s are cheap to create and should be preferred over
    /// locking or other synchronization primitives.
    pub fn new_client<Svc2>(&self) -> RpcClient<Svc2>
    where
        Svc2: RpcService,
    {
        RpcClient {
            channel: self.channel.clone(),
            timeout: None,
            _p: PhantomData,
        }
    }

    /// Sends a message to the server and wait for a reply.
    ///
    /// This lets you send messages behind a reference which can help
    /// avoid excess copies when it isn't needed.
    ///
    /// In the event you need to send a [Body] or type which must consume `self`
    /// you can use [Self::send_owned]
    pub async fn send<Msg>(&self, msg: &Msg) -> Result<MessageReply<Svc, Msg>, Status>
    where
        Msg: RequestContents + TryAsBody,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody,
    {
        let metadata = MessageMetadata {
            service_name: Cow::Borrowed(<Svc as RpcService>::service_name()),
            path: Cow::Borrowed(<Svc as Handler<Msg>>::path()),
        };

        let body = msg.try_as_body()?;
        self.send_body(body, metadata).await
    }

    /// Sends a message to the server and wait for a reply using an owned
    /// message value.
    ///
    /// This allows you to send types implementing [TryIntoBody] like [Body].
    pub async fn send_owned<Msg>(
        &self,
        msg: Msg,
    ) -> Result<MessageReply<Svc, Msg>, Status>
    where
        Msg: RequestContents + TryIntoBody,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody,
    {
        let metadata = MessageMetadata {
            service_name: Cow::Borrowed(<Svc as RpcService>::service_name()),
            path: Cow::Borrowed(<Svc as Handler<Msg>>::path()),
        };

        let body = msg.try_into_body()?;
        self.send_body(body, metadata).await
    }

    async fn send_body<Msg>(
        &self,
        body: Body,
        metadata: MessageMetadata,
    ) -> Result<MessageReply<Svc, Msg>, Status>
    where
        Msg: RequestContents,
        Svc: Handler<Msg>,
        // Due to some interesting compiler errors, we couldn't use GATs here to enforce
        // this on the trait side, which is a shame.
        <Svc as Handler<Msg>>::Reply: RequestContents + TryIntoBody,
    {
        let future = self.channel.send_msg(metadata, body);

        let result = match self.timeout {
            Some(duration) => tokio::time::timeout(duration, future)
                .await
                .map_err(|_| Status::timeout())?
                .map_err(Status::connection)?,
            None => future.await.map_err(Status::connection)?,
        };

        match result {
            Ok(body) => <<Svc as Handler<Msg>>::Reply>::from_body(body).await,
            Err(buffer) => {
                let status = rkyv::from_bytes(&buffer).map_err(|_| Status::invalid())?;
                Err(status)
            },
        }
    }
}
