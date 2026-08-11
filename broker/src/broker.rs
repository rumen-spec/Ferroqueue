use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use chrono::{DateTime, Duration, Utc};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use crate::job::{Job, JobState, New, VisibilityEntry};
use crate::proto::{AckRequest, AckResponse, DequeueRequest, DequeueResponse, EnqueueRequest, EnqueueResponse, queue_server::Queue};

const VISIBILITY_S: i64 = 100;
const MAX_RETRIES: i64 = 10;

#[derive(Debug, Default)]
pub struct QueueService {
    pub jobs: Arc<Mutex<HashMap<Uuid, Job>>>,
    pub ready_queue: Arc<Mutex<VecDeque<Uuid>>>,
    pub invisible_queue: Arc<Mutex<VecDeque<VisibilityEntry>>>,
    DLQueue: Mutex<VecDeque<Uuid>>
}

#[tonic::async_trait]
impl Queue for QueueService {
    async fn enqueue(&self, req: Request<EnqueueRequest>) -> Result<Response<EnqueueResponse>, Status> {
        let job = Job::new(req.into_inner().payload);
        let id = job.id();

        {
            self.ready_queue.lock().await.push_back(id);
        }
        self.jobs.lock().await.insert(id, job);
        Ok(Response::new(EnqueueResponse { job_id: id.to_string() }))
    }

    async fn dequeue(&self, _req: Request<DequeueRequest>) -> Result<Response<DequeueResponse>, Status> {
        let id = self.ready_queue.lock().await.pop_front();
        match id {
            Some(_id) => {
                let payload: String;
                let created_at: DateTime<Utc>;
                let visible_at: DateTime<Utc>;

                {
                    let mut guard = self.jobs.lock().await;
                    let job = guard.get_mut(&_id).unwrap();
                    
                    visible_at = Utc::now() + Duration::seconds(VISIBILITY_S);
                    job.set_state(JobState::INFLIGHT);
                    job.set_visible_at(Some(visible_at.clone()));

                    created_at = job.created_at();
                    payload = job.payload().clone();
                }
                

                self.invisible_queue.lock().await.push_back(
                    VisibilityEntry {
                        id: _id,
                        visible_at: visible_at
                    }
                );

                let created_at = prost_types::Timestamp {
                    seconds: created_at.timestamp(),
                    nanos: created_at.timestamp_subsec_nanos() as i32,
                };
                
                Ok(Response::new(DequeueResponse { job_id: Some(_id.to_string()), payload: Some(payload), created_at: Some(created_at)}))
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

        {
            let mut guard = self.invisible_queue.lock().await;
            let Some(job_idx) = guard.iter().position(|entry| entry.id == id) else {
                return Err(Status::not_found("Job not found"));
            };
            guard.remove(job_idx);
        }
        let job = self.jobs.lock().await.remove(&id);

        match job{
            Some(_) => {
                Ok(Response::new(AckResponse::default()))
            },
            None => Err(Status::not_found("Job not found"))
        }
    }
}

pub async fn sweep_invisible_queue(service: Arc<QueueService>, interval: StdDuration) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let now = Utc::now();

        loop {
            let expired_id = {
                let mut invisible = service.invisible_queue.lock().await;
                match invisible.front() {
                    Some(entry) if entry.visible_at <= now => {
                        invisible.pop_front().map(|entry| entry.id)
                    }
                    _ => None,
                }
            };

            let Some(id) = expired_id else { break };

            let mut jobs = service.jobs.lock().await;
            let Some(job) = jobs.get_mut(&id) else { continue };

            job.set_retries(job.retries() + 1);
            let dead = job.retries() >= MAX_RETRIES as i32;
            job.set_state(if dead { JobState::DLQ } else { JobState::READY });
            if !dead {
                job.set_visible_at(None);
            }
            drop(jobs);

            if dead {
                service.DLQueue.lock().await.push_back(id);
            } else {
                service.ready_queue.lock().await.push_back(id);
            }
        }
    }
}