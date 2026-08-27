use std::collections::{BTreeMap, HashMap, VecDeque};

use bincode_next::{Decode, Encode};
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::job::{Job, JobState, New};

pub const VISIBILITY_S: i64 = 5;
pub const MAX_RETRIES: i32 = 10;

/// Ceiling on the retry backoff, so a job that keeps failing does not drift
/// out to an unusable redelivery interval before it reaches the DLQ.
const MAX_BACKOFF_S: i64 = 300;

/// Unacknowledged jobs the broker will hold before rejecting producers. Counts
/// dead-lettered jobs too, since those also occupy memory until redriven.
pub const MAX_QUEUE_DEPTH: usize = 10_000;

/// A state transition.
///
/// Every field is something `apply` could not have worked out for itself: a
/// value from the clock, from a random generator, or from the client. Anything
/// derivable from existing state is deliberately absent and recomputed.
#[derive(Encode, Decode, Debug, Clone, PartialEq)]
pub enum Record {
    Enqueued { job_id: u128, payload: String, created_at: i64 },
    Leased { job_id: u128, delivery_id: u128, visible_at: i64 },
    Acked { job_id: u128, delivery_id: u128 },
    Retried { job_id: u128, delivery_id: u128 },
    DeadLettered { job_id: u128, delivery_id: u128 },
    /// An operator returned a dead-lettered job to the ready queue.
    Redriven { job_id: u128 },
}

/// Visibility timeout for a job's next delivery, doubling per retry.
///
/// With no nack RPC, lease expiry is the only failure signal, so the lease
/// length *is* the interval between redelivery attempts: a consumer that keeps
/// dying sees gaps of 5s, 10s, 20s, 40s... rather than a hot loop.
pub fn backoff_seconds(retries: i32) -> i64 {
    let doublings = retries.clamp(0, 16) as u32;
    VISIBILITY_S
        .saturating_mul(1i64 << doublings)
        .min(MAX_BACKOFF_S)
}

/// Outstanding deliveries, indexed two ways so neither the expiry sweep nor ack
/// has to scan. Both maps always describe the same set of deliveries.
#[derive(Debug, Default)]
pub struct InvisibleQueue {
    /// (visible_at, delivery_id) -> job_id, ordered by expiry.
    timers: BTreeMap<(DateTime<Utc>, Uuid), Uuid>,
    /// delivery_id -> (job_id, visible_at), so ack can rebuild the timer key.
    leases: HashMap<Uuid, (Uuid, DateTime<Utc>)>,
}

impl InvisibleQueue {
    fn insert(&mut self, job_id: Uuid, delivery_id: Uuid, visible_at: DateTime<Utc>) {
        self.timers.insert((visible_at, delivery_id), job_id);
        self.leases.insert(delivery_id, (job_id, visible_at));
    }

    fn remove(&mut self, delivery_id: Uuid) -> Option<Uuid> {
        let (job_id, visible_at) = self.leases.remove(&delivery_id)?;
        self.timers.remove(&(visible_at, delivery_id));
        Some(job_id)
    }

    fn job_of(&self, delivery_id: Uuid) -> Option<Uuid> {
        self.leases.get(&delivery_id).map(|(job_id, _)| *job_id)
    }

    /// The earliest lease if it has expired by `now`, left in place. The caller
    /// records the expiry and `apply` performs the removal.
    fn peek_expired(&self, now: DateTime<Utc>) -> Option<(Uuid, Uuid)> {
        let (&(visible_at, delivery_id), &job_id) = self.timers.first_key_value()?;
        (visible_at <= now).then_some((job_id, delivery_id))
    }
}

#[derive(Debug, Default)]
struct Inner {
    jobs: HashMap<Uuid, Job>,
    ready: VecDeque<Uuid>,
    invisible: InvisibleQueue,
    dead: VecDeque<Uuid>,
}

/// The replicated state machine: the queue and nothing else.
///
/// Knows nothing about gRPC and nothing about Raft. `apply` is the only way to
/// change it, and is a pure function of (record, current state) so that every
/// replica fed the same records lands in the same place.
#[derive(Debug, Default)]
pub struct QueueState {
    inner: Mutex<Inner>,
}

/// A dead-lettered job, flattened for the API layer.
pub struct DeadJobView {
    pub job_id: Uuid,
    pub payload: String,
    pub retries: i32,
    pub created_at: DateTime<Utc>,
}

impl QueueState {
    pub async fn apply(&self, record: &Record) {
        let mut inner = self.inner.lock().await;
        match *record {
            Record::Enqueued { job_id, ref payload, created_at } => {
                let id = Uuid::from_u128(job_id);
                let mut job = Job::new(payload.clone());
                job.set_id(id);
                if let Some(created_at) = DateTime::from_timestamp_micros(created_at) {
                    job.set_created_at(created_at);
                }
                inner.jobs.insert(id, job);
                inner.ready.push_back(id);
            }
            Record::Leased { job_id, delivery_id, visible_at } => {
                let id = Uuid::from_u128(job_id);
                let delivery_id = Uuid::from_u128(delivery_id);
                let Some(visible_at) = DateTime::from_timestamp_micros(visible_at) else { return };
                inner.ready.retain(|queued| *queued != id);
                if let Some(job) = inner.jobs.get_mut(&id) {
                    job.set_state(JobState::INFLIGHT);
                }
                inner.invisible.insert(id, delivery_id, visible_at);
            }
            Record::Acked { job_id, delivery_id } => {
                let id = Uuid::from_u128(job_id);
                inner.invisible.remove(Uuid::from_u128(delivery_id));
                inner.jobs.remove(&id);
                inner.ready.retain(|queued| *queued != id);
            }
            Record::Retried { job_id, delivery_id } => {
                let id = Uuid::from_u128(job_id);
                inner.invisible.remove(Uuid::from_u128(delivery_id));
                if let Some(job) = inner.jobs.get_mut(&id) {
                    job.set_retries(job.retries() + 1);
                    job.set_state(JobState::READY);
                }
                inner.ready.push_back(id);
            }
            Record::DeadLettered { job_id, delivery_id } => {
                let id = Uuid::from_u128(job_id);
                inner.invisible.remove(Uuid::from_u128(delivery_id));
                if let Some(job) = inner.jobs.get_mut(&id) {
                    job.set_retries(job.retries() + 1);
                    job.set_state(JobState::DLQ);
                }
                inner.dead.push_back(id);
            }
            Record::Redriven { job_id } => {
                let id = Uuid::from_u128(job_id);
                inner.dead.retain(|queued| *queued != id);
                if let Some(job) = inner.jobs.get_mut(&id) {
                    job.set_retries(0);
                    job.set_state(JobState::READY);
                }
                inner.ready.push_back(id);
            }
        }
    }

    /// Unacknowledged jobs held, for the backpressure check.
    pub async fn depth(&self) -> usize {
        self.inner.lock().await.jobs.len()
    }

    /// The next job a consumer would receive, with the retry count that decides
    /// how long its lease should last. Left in the ready queue; `apply` removes
    /// it when the lease record commits.
    pub async fn peek_ready(&self) -> Option<(Uuid, i32)> {
        let inner = self.inner.lock().await;
        let id = *inner.ready.front()?;
        let retries = inner.jobs.get(&id).map(Job::retries).unwrap_or(0);
        Some((id, retries))
    }

    pub async fn job_view(&self, id: Uuid) -> Option<(String, DateTime<Utc>)> {
        let inner = self.inner.lock().await;
        let job = inner.jobs.get(&id)?;
        Some((job.payload().clone(), job.created_at()))
    }

    pub async fn job_of_delivery(&self, delivery_id: Uuid) -> Option<Uuid> {
        self.inner.lock().await.invisible.job_of(delivery_id)
    }

    pub async fn peek_expired(&self, now: DateTime<Utc>) -> Option<(Uuid, Uuid)> {
        self.inner.lock().await.invisible.peek_expired(now)
    }

    pub async fn retries_of(&self, id: Uuid) -> Option<i32> {
        self.inner.lock().await.jobs.get(&id).map(Job::retries)
    }

    pub async fn dead_ids(&self) -> Vec<Uuid> {
        self.inner.lock().await.dead.iter().copied().collect()
    }

    pub async fn dead_jobs(&self) -> Vec<DeadJobView> {
        let inner = self.inner.lock().await;
        inner
            .dead
            .iter()
            .filter_map(|id| {
                let job = inner.jobs.get(id)?;
                Some(DeadJobView {
                    job_id: *id,
                    payload: job.payload().clone(),
                    retries: job.retries(),
                    created_at: job.created_at(),
                })
            })
            .collect()
    }

    #[cfg(test)]
    pub async fn ready_ids(&self) -> Vec<Uuid> {
        self.inner.lock().await.ready.iter().copied().collect()
    }

    #[cfg(test)]
    pub async fn insert_dead_for_test(&self, job: Job) {
        let mut inner = self.inner.lock().await;
        let id = job.id();
        inner.jobs.insert(id, job);
        inner.dead.push_back(id);
    }

    #[cfg(test)]
    pub async fn fill_for_test(&self, count: usize) {
        let mut inner = self.inner.lock().await;
        for i in 0..count {
            let job = Job::new(format!("filler-{i}"));
            inner.jobs.insert(job.id(), job);
        }
    }
}
