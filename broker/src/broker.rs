use std::cmp::max;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use chrono::{DateTime, Duration, Utc};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use crate::job::{Job, JobState, New};
use crate::log::{Record, Wal};
use crate::proto::{AckRequest, AckResponse, DequeueRequest, DequeueResponse, EnqueueRequest, EnqueueResponse, queue_server::Queue};

const VISIBILITY_S: i64 = 5;
const MAX_RETRIES: i64 = 10;

#[derive(Debug, Default)]
pub struct InvisibleQueue {
    timers: BTreeMap<(DateTime<Utc>, Uuid), Uuid>,
    leases: HashMap<Uuid, (Uuid, DateTime<Utc>)>,
}

impl InvisibleQueue {
    pub fn insert(&mut self, job_id: Uuid, delivery_id: Uuid, visible_at: DateTime<Utc>) {
        self.timers.insert((visible_at, delivery_id), job_id);
        self.leases.insert(delivery_id, (job_id, visible_at));
    }

    pub fn remove(&mut self, delivery_id: Uuid) -> Option<Uuid> {
        let (job_id, visible_at) = self.leases.remove(&delivery_id)?;
        self.timers.remove(&(visible_at, delivery_id));
        Some(job_id)
    }

    pub fn pop_expired(&mut self, now: DateTime<Utc>) -> Option<Uuid> {
        let (&key, &job_id) = self.timers.first_key_value()?;
        if key.0 > now {
            return None;
        }
        self.timers.remove(&key);
        self.leases.remove(&key.1);
        Some(job_id)
    }
}

#[derive(Debug)]
pub struct QueueService {
    pub jobs: Arc<Mutex<HashMap<Uuid, Job>>>,
    pub ready_queue: Arc<Mutex<VecDeque<Uuid>>>,
    pub invisible_queue: Arc<Mutex<InvisibleQueue>>,
    pub dl_queue: Mutex<VecDeque<Uuid>>,
    pub wal: Arc<Wal>
}

impl QueueService {
    pub fn new(wal: Arc<Wal>) -> Self {
        Self {
            jobs: Arc::default(),
            ready_queue: Arc::default(),
            invisible_queue: Arc::default(),
            dl_queue: Mutex::default(),
            wal,
        }
    }
}

#[tonic::async_trait]
impl Queue for QueueService {
    async fn enqueue(&self, req: Request<EnqueueRequest>) -> Result<Response<EnqueueResponse>, Status> {
        let job = Job::new(req.into_inner().payload);
        let id = job.id();

        self.wal
            .append(Record::Enqueued {
                job_id: id.as_u128(),
                payload: job.payload().clone(),
                created_at: job.created_at().timestamp_micros(),
            })
            .await
            .map_err(|e| Status::internal(format!("Failed to persist job: {e}")))?;

        self.jobs.lock().await.insert(id, job);
        {
            self.ready_queue.lock().await.push_back(id);
        }
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
                    
                    visible_at = Utc::now() + Duration::seconds(max(VISIBILITY_S, job.retry_time_s()));
                    job.set_state(JobState::INFLIGHT);

                    created_at = job.created_at();
                    payload = job.payload().clone();
                }
                let delivery_id = Uuid::new_v4();

                self.invisible_queue.lock().await.insert(_id, delivery_id, visible_at);

                let created_at = prost_types::Timestamp {
                    seconds: created_at.timestamp(),
                    nanos: created_at.timestamp_subsec_nanos() as i32,
                };
                
                Ok(Response::new(DequeueResponse { job_id: Some(_id.to_string()), delivery_id: Some(delivery_id.to_string()), payload: Some(payload), created_at: Some(created_at)}))
            },
            None => Ok(Response::new(DequeueResponse { job_id: None, delivery_id: None, payload: None, created_at: None}))

        }
    
    }

    async fn ack(&self, req: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let delivery_id: Uuid = req
            .into_inner()
            .delivery_id
            .parse()
            .map_err(|_| Status::invalid_argument("Invalid delivery ID"))?;

        let job_id = self.invisible_queue.lock().await.remove(delivery_id);
        let Some(job_id) = job_id else {
            return Err(Status::not_found("Job not found"));
        };

        self.wal
            .append(Record::Acked { job_id: job_id.as_u128() })
            .await
            .map_err(|e| Status::internal(format!("Failed to persist ack: {e}")))?;

        let job = self.jobs.lock().await.remove(&job_id);

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
            let expired_id = service.invisible_queue.lock().await.pop_expired(now);
            let Some(id) = expired_id else { break };

            let retries = {
                let jobs = service.jobs.lock().await;
                let Some(job) = jobs.get(&id) else { continue };
                job.retries() + 1
            };
            let dead = retries >= MAX_RETRIES as i32;

            let record = if dead {
                Record::DeadLettered { job_id: id.as_u128() }
            } else {
                Record::Retried { job_id: id.as_u128(), retries }
            };
            if let Err(e) = service.wal.append(record).await {
                eprintln!("sweeper: failed to persist expiry for {id}: {e}");
                continue;
            }

            let mut jobs = service.jobs.lock().await;
            let Some(job) = jobs.get_mut(&id) else { continue };
            job.set_retries(retries);
            job.set_state(if dead { JobState::DLQ } else { JobState::READY });
            drop(jobs);

            if dead {
                service.dl_queue.lock().await.push_back(id);
            } else {
                service.ready_queue.lock().await.push_back(id);
            }
        }
    }
}