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
use crate::proto::{AckRequest, AckResponse, DeadJob, DequeueRequest, DequeueResponse, EnqueueRequest, EnqueueResponse, ListDlqRequest, ListDlqResponse, RedriveDlqRequest, RedriveDlqResponse, queue_server::Queue};

pub const VISIBILITY_S: i64 = 5;
const MAX_RETRIES: i64 = 10;

/// Ceiling on the retry backoff, so a job that keeps failing does not drift
/// out to an unusable redelivery interval before it reaches the DLQ.
const MAX_BACKOFF_S: i64 = 300;

/// Unacknowledged jobs the broker will hold before rejecting producers. Counts
/// dead-lettered jobs too, since those also occupy memory until redriven.
const MAX_QUEUE_DEPTH: usize = 10_000;

/// Visibility timeout for a job's next delivery, doubling per retry.
///
/// With no nack RPC, lease expiry is the only failure signal, so the lease
/// length *is* the interval between redelivery attempts: a consumer that keeps
/// dying sees gaps of 5s, 10s, 20s, 40s… rather than a hot loop.
pub fn backoff_seconds(retries: i32) -> i64 {
    let doublings = retries.clamp(0, 16) as u32;
    VISIBILITY_S
        .saturating_mul(1i64 << doublings)
        .min(MAX_BACKOFF_S)
}

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
        // Backpressure: reject before paying for an fsync, so a saturated
        // broker sheds load cheaply instead of queueing work it cannot hold.
        let depth = self.jobs.lock().await.len();
        if depth >= MAX_QUEUE_DEPTH {
            return Err(Status::resource_exhausted(format!(
                "Queue is full: {depth} unacknowledged jobs (limit {MAX_QUEUE_DEPTH})"
            )));
        }

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

    async fn list_dlq(&self, _req: Request<ListDlqRequest>) -> Result<Response<ListDlqResponse>, Status> {
        let dead: Vec<Uuid> = self.dl_queue.lock().await.iter().copied().collect();

        let guard = self.jobs.lock().await;
        let jobs = dead
            .into_iter()
            .filter_map(|id| {
                let job = guard.get(&id)?;
                let created_at = job.created_at();
                Some(DeadJob {
                    job_id: id.to_string(),
                    payload: job.payload().clone(),
                    retries: job.retries(),
                    created_at: Some(prost_types::Timestamp {
                        seconds: created_at.timestamp(),
                        nanos: created_at.timestamp_subsec_nanos() as i32,
                    }),
                })
            })
            .collect();

        Ok(Response::new(ListDlqResponse { jobs }))
    }

    async fn redrive_dlq(&self, req: Request<RedriveDlqRequest>) -> Result<Response<RedriveDlqResponse>, Status> {
        let requested: Vec<Uuid> = req
            .into_inner()
            .job_ids
            .iter()
            .map(|id| id.parse())
            .collect::<Result<_, _>>()
            .map_err(|_| Status::invalid_argument("Invalid job ID"))?;

        let targets: Vec<Uuid> = {
            let dead = self.dl_queue.lock().await;
            dead.iter()
                .copied()
                .filter(|id| requested.is_empty() || requested.contains(id))
                .collect()
        };

        // Each job is logged before it moves, so a failure part-way through
        // leaves the log and memory agreeing on exactly what was redriven.
        let mut redriven = Vec::with_capacity(targets.len());
        for id in targets {
            if let Err(e) = self.wal.append(Record::Redriven { job_id: id.as_u128() }).await {
                eprintln!("redrive: failed to persist {id}, stopping: {e}");
                break;
            }

            self.dl_queue.lock().await.retain(|queued| *queued != id);
            {
                let mut guard = self.jobs.lock().await;
                let Some(job) = guard.get_mut(&id) else { continue };
                job.set_retries(0);
                job.set_retry_time_s(VISIBILITY_S);
                job.set_state(JobState::READY);
            }
            self.ready_queue.lock().await.push_back(id);
            redriven.push(id.to_string());
        }

        Ok(Response::new(RedriveDlqResponse { job_ids: redriven }))
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
            job.set_retry_time_s(backoff_seconds(retries));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::Wal;

    async fn service(name: &str) -> QueueService {
        let path = std::env::temp_dir().join(format!("ferroqueue-broker-{name}.wal"));
        let _ = std::fs::remove_file(&path);
        let (wal, _) = Wal::open(&path).unwrap();
        QueueService::new(Arc::new(wal))
    }

    #[test]
    fn backoff_doubles_then_caps() {
        assert_eq!(backoff_seconds(0), 5);
        assert_eq!(backoff_seconds(1), 10);
        assert_eq!(backoff_seconds(2), 20);
        assert_eq!(backoff_seconds(3), 40);
        assert_eq!(backoff_seconds(6), 300, "capped at MAX_BACKOFF_S");
        assert_eq!(backoff_seconds(1000), 300, "no overflow at absurd counts");
    }

    #[tokio::test]
    async fn enqueue_rejects_once_the_queue_is_full() {
        let service = service("backpressure").await;

        {
            let mut jobs = service.jobs.lock().await;
            for i in 0..MAX_QUEUE_DEPTH {
                let job = Job::new(format!("filler-{i}"));
                jobs.insert(job.id(), job);
            }
        }

        let err = service
            .enqueue(Request::new(EnqueueRequest { payload: "one too many".into() }))
            .await
            .expect_err("a full queue must reject the producer");
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);

        service.wal.shutdown().await;
    }

    #[tokio::test]
    async fn dlq_is_listable_and_redrivable() {
        let service = service("dlq").await;

        let dead = Job::new("poisoned".to_string());
        let id = dead.id();
        {
            let mut jobs = service.jobs.lock().await;
            let mut dead = dead;
            dead.set_retries(MAX_RETRIES as i32);
            dead.set_state(JobState::DLQ);
            jobs.insert(id, dead);
        }
        service.dl_queue.lock().await.push_back(id);

        let listed = service
            .list_dlq(Request::new(ListDlqRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(listed.jobs.len(), 1);
        assert_eq!(listed.jobs[0].job_id, id.to_string());
        assert_eq!(listed.jobs[0].payload, "poisoned");
        assert_eq!(listed.jobs[0].retries, MAX_RETRIES as i32);

        let redriven = service
            .redrive_dlq(Request::new(RedriveDlqRequest { job_ids: vec![] }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(redriven.job_ids, vec![id.to_string()]);

        assert!(service.dl_queue.lock().await.is_empty());
        assert_eq!(service.ready_queue.lock().await.front(), Some(&id));
        assert_eq!(
            service.jobs.lock().await.get(&id).unwrap().retries(),
            0,
            "redrive must reset the retry count or it dead-letters again immediately"
        );

        service.wal.shutdown().await;
    }

    #[tokio::test]
    async fn redrive_survives_a_restart() {
        let path = std::env::temp_dir().join("ferroqueue-broker-redrive-replay.wal");
        let _ = std::fs::remove_file(&path);

        let id = {
            let (wal, _) = Wal::open(&path).unwrap();
            let service = QueueService::new(Arc::new(wal));

            service
                .enqueue(Request::new(EnqueueRequest { payload: "poisoned".into() }))
                .await
                .unwrap();

            // Dead-letter the job, then redrive it.
            let id = service.ready_queue.lock().await.front().copied().unwrap();
            service.wal.append(Record::DeadLettered { job_id: id.as_u128() }).await.unwrap();
            service.ready_queue.lock().await.clear();
            service.dl_queue.lock().await.push_back(id);

            service
                .redrive_dlq(Request::new(RedriveDlqRequest { job_ids: vec![] }))
                .await
                .unwrap();
            service.wal.shutdown().await;
            id
        };

        let (wal, records) = Wal::open(&path).unwrap();
        let service = QueueService::new(Arc::new(wal));
        crate::log::replay(&service, records).await;
        service.wal.shutdown().await;

        assert!(service.dl_queue.lock().await.is_empty(), "redrive must not replay back into the DLQ");
        assert_eq!(service.ready_queue.lock().await.front(), Some(&id));
    }
}