pub mod proto {
    tonic::include_proto!("queue");
}
pub mod broker;
pub mod job;

use std::{error::Error, net::SocketAddr};
use tonic::transport::Server;
use crate::proto::queue_server::{QueueServer};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;
    let service = broker::QueueService::default();

    Server::builder()
        .add_service(QueueServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
