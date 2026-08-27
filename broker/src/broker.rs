use std::sync::Arc;
use std::time::Duration as StdDuration;

use chrono::{Duration, Utc};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::queue::queue_server::Queue;
use crate::proto::queue::{
    AckRequest, AckResponse, DeadJob, DequeueRequest, DequeueResponse, EnqueueRequest,
    EnqueueResponse, ListDlqRequest, ListDlqResponse, RedriveDlqRequest, RedriveDlqResponse,
};
use crate::raft::{Node, ProposeError};
use crate::state::{MAX_QUEUE_DEPTH, MAX_RETRIES, Record, backoff_seconds};

pub struct QueueService {
    node: Arc<Node>,
    dequeue: Mutex<()>,
}

impl QueueService {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node, dequeue: Mutex::new(()) }
    }
}

fn propose_error(e: ProposeError) -> Status {
    match e {
        ProposeError::NotLeader(Some(leader)) => {
            Status::failed_precondition(format!("Not the leader; try {leader}"))
        }
        ProposeError::NotLeader(None) => {
            Status::unavailable("No leader elected; retry shortly")
        }
        ProposeError::LostLeadership => {
            Status::unavailable("Leadership changed before the write committed; retry")
        }
        ProposeError::Storage(e) => Status::internal(format!("Log write failed: {e}")),
    }
}

#[tonic::async_trait]
impl Queue for QueueService {
    async fn enqueue(
        &self,
        req: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let depth = self.node.state().depth().await;
        if depth >= MAX_QUEUE_DEPTH {
            return Err(Status::resource_exhausted(format!(
                "Queue is full: {depth} unacknowledged jobs (limit {MAX_QUEUE_DEPTH})"
            )));
        }

        let job_id = Uuid::new_v4();
        self.node
            .propose(Record::Enqueued {
                job_id: job_id.as_u128(),
                payload: req.into_inner().payload,
                created_at: Utc::now().timestamp_micros(),
            })
            .await
            .map_err(propose_error)?;

        Ok(Response::new(EnqueueResponse { job_id: job_id.to_string() }))
    }

    async fn dequeue(
        &self,
        _req: Request<DequeueRequest>,
    ) -> Result<Response<DequeueResponse>, Status> {
        let _serialise = self.dequeue.lock().await;

        let Some((job_id, retries)) = self.node.state().peek_ready().await else {
            return Ok(Response::new(DequeueResponse::default()));
        };

        let delivery_id = Uuid::new_v4();
        let visible_at = Utc::now() + Duration::seconds(backoff_seconds(retries));

        self.node
            .propose(Record::Leased {
                job_id: job_id.as_u128(),
                delivery_id: delivery_id.as_u128(),
                visible_at: visible_at.timestamp_micros(),
            })
            .await
            .map_err(propose_error)?;

        let Some((payload, created_at)) = self.node.state().job_view(job_id).await else {
            return Ok(Response::new(DequeueResponse::default()));
        };

        Ok(Response::new(DequeueResponse {
            job_id: Some(job_id.to_string()),
            delivery_id: Some(delivery_id.to_string()),
            payload: Some(payload),
            created_at: Some(prost_types::Timestamp {
                seconds: created_at.timestamp(),
                nanos: created_at.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    async fn ack(&self, req: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let delivery_id: Uuid = req
            .into_inner()
            .delivery_id
            .parse()
            .map_err(|_| Status::invalid_argument("Invalid delivery ID"))?;

        let Some(job_id) = self.node.state().job_of_delivery(delivery_id).await else {
            return Err(Status::not_found("No such delivery; it may have expired"));
        };

        self.node
            .propose(Record::Acked { job_id: job_id.as_u128(), delivery_id: delivery_id.as_u128() })
            .await
            .map_err(propose_error)?;

        Ok(Response::new(AckResponse::default()))
    }

    async fn list_dlq(
        &self,
        _req: Request<ListDlqRequest>,
    ) -> Result<Response<ListDlqResponse>, Status> {
        let jobs = self
            .node
            .state()
            .dead_jobs()
            .await
            .into_iter()
            .map(|job| DeadJob {
                job_id: job.job_id.to_string(),
                payload: job.payload,
                retries: job.retries,
                created_at: Some(prost_types::Timestamp {
                    seconds: job.created_at.timestamp(),
                    nanos: job.created_at.timestamp_subsec_nanos() as i32,
                }),
            })
            .collect();

        Ok(Response::new(ListDlqResponse { jobs }))
    }

    async fn redrive_dlq(
        &self,
        req: Request<RedriveDlqRequest>,
    ) -> Result<Response<RedriveDlqResponse>, Status> {
        let requested: Vec<Uuid> = req
            .into_inner()
            .job_ids
            .iter()
            .map(|id| id.parse())
            .collect::<Result<_, _>>()
            .map_err(|_| Status::invalid_argument("Invalid job ID"))?;

        let targets: Vec<Uuid> = self
            .node
            .state()
            .dead_ids()
            .await
            .into_iter()
            .filter(|id| requested.is_empty() || requested.contains(id))
            .collect();

        let mut redriven = Vec::with_capacity(targets.len());
        for id in targets {
            if let Err(e) = self.node.propose(Record::Redriven { job_id: id.as_u128() }).await {
                eprintln!("redrive: stopping at {id}: {e:?}");
                break;
            }
            redriven.push(id.to_string());
        }

        Ok(Response::new(RedriveDlqResponse { job_ids: redriven }))
    }
}

pub async fn sweep_expired_leases(node: Arc<Node>, interval: StdDuration) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;

        if !node.is_leader().await {
            continue;
        }

        loop {
            let now = Utc::now();
            let Some((job_id, delivery_id)) = node.state().peek_expired(now).await else { break };

            let retries = node.state().retries_of(job_id).await.unwrap_or(0);
            let record = if retries + 1 >= MAX_RETRIES {
                Record::DeadLettered {
                    job_id: job_id.as_u128(),
                    delivery_id: delivery_id.as_u128(),
                }
            } else {
                Record::Retried { job_id: job_id.as_u128(), delivery_id: delivery_id.as_u128() }
            };

            if let Err(e) = node.propose(record).await {
                eprintln!("sweeper: could not record expiry for {job_id}: {e:?}");
                break;
            }
        }
    }
}
