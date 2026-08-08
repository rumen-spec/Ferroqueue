use tonic::{Request, Response, Status};

use crate::proto::{AckRequest, AckResponse, DequeueRequest, DequeueResponse, EnqueueRequest, EnqueueResponse, queue_server::Queue};

#[derive(Debug, Default)]
pub struct QueueService {
    queue: Vec<String>
}

#[tonic::async_trait]
impl Queue for QueueService {
    async fn enqueue(&self, req: Request<EnqueueRequest>) -> Result<Response<EnqueueResponse>, Status> {
        Err(Status::aborted("message"))
    }
    async fn dequeue(&self, req: Request<DequeueRequest>) -> Result<Response<DequeueResponse>, Status> {
        Err(Status::aborted("message"))
    }
    async fn ack(&self, req: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        Err(Status::aborted("message"))
    }
}