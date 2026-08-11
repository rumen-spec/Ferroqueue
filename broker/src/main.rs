pub mod proto {
    tonic::include_proto!("queue");
}
pub mod broker;
pub mod job;

use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};
use tonic::transport::Server;
use crate::broker::QueueService;
use crate::proto::queue_server::{QueueServer};

const VISIBILITY_SWEEP_INTERVAL: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;
    let service = Arc::new(QueueService::default());

    tokio::spawn(broker::sweep_invisible_queue(service.clone(), VISIBILITY_SWEEP_INTERVAL));

    Server::builder()
        .add_service(QueueServer::from_arc(service))
        .serve(addr)
        .await?;

    Ok(())
}
