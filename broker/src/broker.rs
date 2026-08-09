use std::collections::{HashMap, VecDeque};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use crate::job::{Job, JobState, New};
use crate::proto::{AckRequest, AckResponse, DequeueRequest, DequeueResponse, EnqueueRequest, EnqueueResponse, queue_server::Queue};

const VISIBILITY_S: i32 = 100;
const MAX_RETRIES: i32 = 10;

#[derive(Debug, Default)]
pub struct QueueService {
    ready_queue: Mutex<VecDeque<Job>>,
    invisible_queue: Mutex<HashMap<Uuid, Job>>,
    DLQueue: Mutex<VecDeque<Job>>
}

#[tonic::async_trait]
impl Queue for QueueService {
    async fn enqueue(&self, req: Request<EnqueueRequest>) -> Result<Response<EnqueueResponse>, Status> {
        let job = Job::new(req.into_inner().payload);
        let id = job.id();
        
        self.ready_queue.lock().await.push_back(job);
        Ok(Response::new(EnqueueResponse { job_id: id.to_string() }))
    }
    async fn dequeue(&self, _req: Request<DequeueRequest>) -> Result<Response<DequeueResponse>, Status> {
        let job = self.ready_queue.lock().await.pop_front();
        match job {
            Some(mut _job) => {
                let id = _job.id().clone();
                let payload = _job.payload().clone();
                let created_at = _job.created_at().clone();
                _job.set_state(JobState::INFLIGHT);

                self.invisible_queue.lock().await.insert(_job.id(), _job);
                let created_at = prost_types::Timestamp {
                    seconds: created_at.timestamp(),
                    nanos: created_at.timestamp_subsec_nanos() as i32,
                };
                
                Ok(Response::new(DequeueResponse { job_id: Some(id.to_string()), payload: Some(payload), created_at: Some(created_at)}))
            },
            None => Ok(Response::new(DequeueResponse { job_id: None, payload: None, created_at: None}))

        }
    
    }

    async fn ack(&self, req: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let id: Uuid = req
            .into_inner()
            .job_id
            .parse()
            .map_err(|_| Status::invalid_argument("Invalid job ID"))?;

        let job = self.invisible_queue
            .lock()
            .await
            .remove(&id);

        match job{
            Some(_) => {
                Ok(Response::new(AckResponse::default()))
            },
            None => Err(Status::not_found("Job not found"))
        }
    }
}