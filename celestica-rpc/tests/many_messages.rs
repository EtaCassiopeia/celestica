use std::collections::BTreeMap;

use bytecheck::CheckBytes;
use celestica_rpc::{
    Channel,
    Handler,
    Request,
    RpcClient,
    RpcService,
    Server,
    ServiceRegistry,
    Status,
};
use parking_lot::Mutex;
use rkyv::{Archive, Deserialize, Serialize};

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct IncCounter {
    name: String,
    value: u64,
}

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct DecCounter {
    name: String,
    value: u64,
}

#[derive(Default)]
pub struct CountingService {
    counters: Mutex<BTreeMap<String, u64>>,
}

impl RpcService for CountingService {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<IncCounter>();
        registry.add_handler::<DecCounter>();
    }
}

#[celestica_rpc::async_trait]
impl Handler<IncCounter> for CountingService {
    type Reply = u64;

    async fn on_message(&self, msg: Request<IncCounter>) -> Result<Self::Reply, Status> {
        let counter = msg.to_owned().expect("Get owned value.");

        let mut lock = self.counters.lock();
        let value = lock.entry(counter.name).or_default();
        (*value) += counter.value;

        Ok(*value)
    }
}

#[celestica_rpc::async_trait]
impl Handler<DecCounter> for CountingService {
    type Reply = u64;

    async fn on_message(&self, msg: Request<DecCounter>) -> Result<Self::Reply, Status> {
        let counter = msg.to_owned().expect("Get owned value.");

        let mut lock = self.counters.lock();
        let value = lock.entry(counter.name).or_default();
        (*value) -= counter.value;

        Ok(*value)
    }
}

#[tokio::test]
async fn test_multiple_msgs() {
    let addr = test_helper::get_unused_addr();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(CountingService::default());
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr);
    println!("Connected to address {}!", addr);

    let rpc_client = RpcClient::<CountingService>::new(client);

    let msg = IncCounter {
        name: "Bobby".to_string(),
        value: 5,
    };

    let resp = rpc_client.send(&msg).await.unwrap();
    assert_eq!(resp, 5);

    let msg = DecCounter {
        name: "Bobby".to_string(),
        value: 3,
    };
    let resp = rpc_client.send(&msg).await.unwrap();
    assert_eq!(resp, 2);

    server.shutdown();
}
