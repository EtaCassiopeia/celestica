#[macro_use]
extern crate tracing;

mod body;
mod client;
mod handler;
mod net;
mod request;
mod server;
mod utils;
mod view;

pub(crate) const SCRATCH_SPACE: usize = 4096;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A re-export of the async-trait macro.
pub use async_trait::async_trait;
pub use body::Body;
pub use client::{MessageReply, RpcClient};
pub use handler::{Handler, RpcService, ServiceRegistry, TryAsBody, TryIntoBody};
pub use net::{ArchivedErrorCode, ArchivedStatus, Channel, Error, ErrorCode, Status};
pub use request::{Request, RequestContents};
pub use server::Server;
pub use view::{DataView, InvalidView};

pub(crate) fn hash<H: Hash + ?Sized>(v: &H) -> u64 {
    let mut hasher = DefaultHasher::new();
    v.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn to_uri_path(service: &str, path: &str) -> String {
    format!("/{}/{}", sanitise(service), sanitise(path))
}

fn sanitise(parameter: &str) -> String {
    parameter.replace(['<', '>'], "-")
}
