use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::raft::raft_client::RaftClient;
use crate::proto::raft::raft_server::Raft;
use crate::proto::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::raft::Node;

pub type NodeId = Uuid;

#[tonic::async_trait]
pub trait RaftTransport: Send + Sync {
    async fn append_entries(
        &self,
        to: NodeId,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, String>;

    async fn request_vote(
        &self,
        to: NodeId,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, String>;
}

pub struct GrpcTransport {
    peers: HashMap<NodeId, RaftClient<Channel>>,
}

impl GrpcTransport {
    pub fn new(peers: &HashMap<NodeId, String>) -> Result<Self, tonic::transport::Error> {
        let mut clients = HashMap::new();
        for (id, addr) in peers {
            let endpoint = Endpoint::from_shared(addr.clone())?
                .connect_timeout(Duration::from_millis(100))
                .timeout(Duration::from_millis(120));
            clients.insert(*id, RaftClient::new(endpoint.connect_lazy()));
        }
        Ok(Self { peers: clients })
    }
}

#[tonic::async_trait]
impl RaftTransport for GrpcTransport {
    async fn append_entries(
        &self,
        to: NodeId,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, String> {
        let Some(client) = self.peers.get(&to) else {
            return Err(format!("unknown peer {to}"));
        };
        let mut client = client.clone();
        client
            .append_entries(req)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|e| e.to_string())
    }

    async fn request_vote(
        &self,
        to: NodeId,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, String> {
        let Some(client) = self.peers.get(&to) else {
            return Err(format!("unknown peer {to}"));
        };
        let mut client = client.clone();
        client
            .request_vote(req)
            .await
            .map(|resp| resp.into_inner())
            .map_err(|e| e.to_string())
    }
}

pub struct RaftService {
    node: Arc<Node>,
}

impl RaftService {
    pub fn new(node: Arc<Node>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl Raft for RaftService {
    async fn append_entries(
        &self,
        req: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        Ok(Response::new(self.node.handle_append_entries(req.into_inner()).await))
    }

    async fn request_vote(
        &self,
        req: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        Ok(Response::new(self.node.handle_request_vote(req.into_inner()).await))
    }
}

#[derive(Clone, Default)]
pub struct MemoryCluster {
    inner: Arc<Mutex<MemoryClusterInner>>,
}

#[derive(Default)]
struct MemoryClusterInner {
    nodes: HashMap<NodeId, Arc<Node>>,
    isolated: HashSet<NodeId>,
}

impl MemoryCluster {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn transport(&self, from: NodeId) -> Arc<dyn RaftTransport> {
        Arc::new(MemoryTransport { from, cluster: self.clone() })
    }

    pub async fn register(&self, node: Arc<Node>) {
        self.inner.lock().await.nodes.insert(node.id(), node);
    }

    pub async fn isolate(&self, id: NodeId) {
        self.inner.lock().await.isolated.insert(id);
    }

    pub async fn heal(&self, id: NodeId) {
        self.inner.lock().await.isolated.remove(&id);
    }

    async fn route(&self, from: NodeId, to: NodeId) -> Option<Arc<Node>> {
        let inner = self.inner.lock().await;
        if inner.isolated.contains(&from) || inner.isolated.contains(&to) {
            return None;
        }
        inner.nodes.get(&to).cloned()
    }
}

struct MemoryTransport {
    from: NodeId,
    cluster: MemoryCluster,
}

#[tonic::async_trait]
impl RaftTransport for MemoryTransport {
    async fn append_entries(
        &self,
        to: NodeId,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, String> {
        let Some(node) = self.cluster.route(self.from, to).await else {
            return Err("unreachable".to_string());
        };
        Ok(node.handle_append_entries(req).await)
    }

    async fn request_vote(
        &self,
        to: NodeId,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, String> {
        let Some(node) = self.cluster.route(self.from, to).await else {
            return Err("unreachable".to_string());
        };
        Ok(node.handle_request_vote(req).await)
    }
}
