pub mod proto {
    tonic::include_proto!("queue");
}
pub mod broker;
pub mod job;
pub mod log;

use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};
use tonic::transport::Server;
use crate::broker::QueueService;
use crate::proto::queue_server::{QueueServer};

const VISIBILITY_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const WAL_PATH: &str = "ferroqueue.wal";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let addr: SocketAddr = "127.0.0.1:5000".parse()?;

    let (wal, records) = log::Wal::open(WAL_PATH)?;
    let wal = Arc::new(wal);
    let service = Arc::new(QueueService::new(wal.clone()));

    println!("Replaying {} record(s) from {WAL_PATH}", records.len());
    log::replay(&service, records).await;

    tokio::spawn(broker::sweep_invisible_queue(service.clone(), VISIBILITY_SWEEP_INTERVAL));

    Server::builder()
        .add_service(QueueServer::from_arc(service))
        .serve_with_shutdown(addr, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    // Flush anything still queued before the process exits.
    wal.shutdown().await;

    Ok(())
}
