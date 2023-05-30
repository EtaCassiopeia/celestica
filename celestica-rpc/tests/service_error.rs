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
use rkyv::{Archive, Deserialize, Serialize};

#[repr(C)]
#[derive(Serialize, Deserialize, Archive, Debug)]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct MyMessage {
    name: String,
    age: u32,
    buffer: Vec<u8>,
}

pub struct MyService;

impl RpcService for MyService {
    fn register_handlers(registry: &mut ServiceRegistry<Self>) {
        registry.add_handler::<MyMessage>();
    }
}

#[celestica_rpc::async_trait]
impl Handler<MyMessage> for MyService {
    type Reply = String;

    async fn on_message(&self, _msg: Request<MyMessage>) -> Result<Self::Reply, Status> {
        Err(Status::internal("Oops! Something went wrong!"))
    }
}

#[tokio::test]
async fn test_service_error() {
    let addr = test_helper::get_unused_addr();

    let server = Server::listen(addr).await.unwrap();
    server.add_service(MyService);
    println!("Listening to address {}!", addr);

    let client = Channel::connect(addr);
    println!("Connected to address {}!", addr);

    let rpc_client = RpcClient::<MyService>::new(client);

    let msg1 = MyMessage {
        name: "Bobby".to_string(),
        age: 12,
        buffer: vec![0u8; 32 << 10],
    };

    let resp = rpc_client.send(&msg1).await;
    assert_eq!(
        resp,
        Err(Status::internal("Oops! Something went wrong!")),
        "Results should match."
    );

    server.shutdown();
}
