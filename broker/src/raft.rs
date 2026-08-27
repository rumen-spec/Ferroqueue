use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bincode_next::{Decode, Encode, config};
use tokio::sync::{Mutex, Notify, oneshot};
use uuid::Uuid;

use crate::log::{EntryPayload, LogEntry, RaftLog, decode_entry, encode_entry};
use crate::proto::raft::{
    AppendEntriesRequest, AppendEntriesResponse, RequestVoteRequest, RequestVoteResponse,
};
use crate::state::{QueueState, Record};
use crate::transport::{NodeId, RaftTransport};

const TICK: Duration = Duration::from_millis(20);
const HEARTBEAT: Duration = Duration::from_millis(50);
const ELECTION_TIMEOUT_MIN_MS: u64 = 150;
const ELECTION_TIMEOUT_MAX_MS: u64 = 300;
const MAX_ENTRIES_PER_RPC: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub enum ProposeError {
    NotLeader(Option<NodeId>),
    LostLeadership,
    Storage(String),
}

#[derive(Encode, Decode, Debug, Clone, Default)]
struct HardState {
    current_term: u64,
    voted_for: Option<u128>,
}

#[derive(Encode, Decode, Debug, Clone)]
pub struct Identity {
    pub node_id: u128,
    pub cluster_id: u128,
}

struct Core {
    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    log: RaftLog,
    commit_index: u64,
    last_applied: u64,
    leader: Option<NodeId>,
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    votes: HashSet<NodeId>,
    last_response: HashMap<NodeId, Instant>,
    last_heard: Instant,
    last_heartbeat_sent: Instant,
    election_timeout: Duration,
}

pub struct Node {
    id: NodeId,
    cluster_id: Uuid,
    peers: Vec<NodeId>,
    quorum: usize,
    core: Mutex<Core>,
    state: Arc<QueueState>,
    transport: Arc<dyn RaftTransport>,
    hard_state_path: PathBuf,
    waiters: Mutex<Vec<(u64, oneshot::Sender<Result<(), ProposeError>>)>>,
    inflight: Mutex<HashSet<NodeId>>,
    replicate: Notify,
}

fn random_election_timeout() -> Duration {
    Duration::from_millis(rand::random_range(ELECTION_TIMEOUT_MIN_MS..ELECTION_TIMEOUT_MAX_MS))
}

impl Node {
    pub async fn open(
        dir: impl AsRef<Path>,
        node_id: NodeId,
        cluster_id: Uuid,
        peers: Vec<NodeId>,
        state: Arc<QueueState>,
        transport: Arc<dyn RaftTransport>,
    ) -> io::Result<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let identity_path = dir.join("identity");
        let hard_state_path = dir.join("hard-state");
        let log_path = dir.join("raft.log");

        let identity = match read_bincode::<Identity>(&identity_path)? {
            Some(identity) => {
                if identity.cluster_id != cluster_id.as_u128() {
                    return Err(io::Error::other(format!(
                        "data directory belongs to cluster {}, not {cluster_id}",
                        Uuid::from_u128(identity.cluster_id)
                    )));
                }
                if identity.node_id != node_id.as_u128() {
                    return Err(io::Error::other(format!(
                        "data directory belongs to node {}, not {node_id}",
                        Uuid::from_u128(identity.node_id)
                    )));
                }
                identity
            }
            None => {
                if log_path.exists() {
                    return Err(io::Error::other(
                        "found a raft log but no identity file; refusing to bootstrap over it",
                    ));
                }
                let identity =
                    Identity { node_id: node_id.as_u128(), cluster_id: cluster_id.as_u128() };
                write_bincode(&identity_path, &identity)?;
                identity
            }
        };

        let hard: HardState = read_bincode(&hard_state_path)?.unwrap_or_default();
        let log = RaftLog::open(&log_path)?;
        let id = Uuid::from_u128(identity.node_id);
        let peers: Vec<NodeId> = peers.into_iter().filter(|p| *p != id).collect();
        let quorum = (peers.len() + 1) / 2 + 1;

        let core = Core {
            role: Role::Follower,
            current_term: hard.current_term,
            voted_for: hard.voted_for.map(Uuid::from_u128),
            log,
            commit_index: 0,
            last_applied: 0,
            leader: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
            last_response: HashMap::new(),
            last_heard: Instant::now(),
            last_heartbeat_sent: Instant::now(),
            election_timeout: random_election_timeout(),
        };

        Ok(Arc::new(Node {
            id,
            cluster_id,
            peers,
            quorum,
            core: Mutex::new(core),
            state,
            transport,
            hard_state_path,
            waiters: Mutex::new(Vec::new()),
            inflight: Mutex::new(HashSet::new()),
            replicate: Notify::new(),
        }))
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    pub fn state(&self) -> &Arc<QueueState> {
        &self.state
    }

    pub async fn role(&self) -> Role {
        self.core.lock().await.role
    }

    pub async fn term(&self) -> u64 {
        self.core.lock().await.current_term
    }

    pub async fn leader(&self) -> Option<NodeId> {
        self.core.lock().await.leader
    }

    pub async fn is_leader(&self) -> bool {
        self.core.lock().await.role == Role::Leader
    }

    pub async fn last_index(&self) -> u64 {
        self.core.lock().await.log.last_index()
    }

    pub async fn commit_index(&self) -> u64 {
        self.core.lock().await.commit_index
    }

    pub async fn propose(self: &Arc<Self>, record: Record) -> Result<(), ProposeError> {
        let index = {
            let mut core = self.core.lock().await;
            if core.role != Role::Leader {
                return Err(ProposeError::NotLeader(core.leader));
            }
            let index = core.log.last_index() + 1;
            let entry =
                LogEntry { term: core.current_term, index, payload: EntryPayload::Queue(record) };
            core.log.append(&[entry]).map_err(|e| ProposeError::Storage(e.to_string()))?;
            index
        };

        let (tx, rx) = oneshot::channel();
        self.waiters.lock().await.push((index, tx));

        self.advance_commit().await;
        self.apply_committed().await;
        self.replicate.notify_one();

        match rx.await {
            Ok(result) => result,
            Err(_) => Err(ProposeError::LostLeadership),
        }
    }

    pub async fn handle_request_vote(&self, req: RequestVoteRequest) -> RequestVoteResponse {
        let mut core = self.core.lock().await;

        if req.term < core.current_term {
            return RequestVoteResponse { term: core.current_term, vote_granted: false };
        }
        if req.term > core.current_term {
            self.step_down(&mut core, req.term);
        }

        let Ok(candidate) = req.candidate_id.parse::<Uuid>() else {
            return RequestVoteResponse { term: core.current_term, vote_granted: false };
        };

        let free_to_vote = core.voted_for.is_none() || core.voted_for == Some(candidate);
        let log_current = (req.last_log_term, req.last_log_index)
            >= (core.log.last_term(), core.log.last_index());

        let granted = free_to_vote && log_current;
        if granted {
            core.voted_for = Some(candidate);
            if let Err(e) = self.persist_hard_state(&core) {
                eprintln!("raft: refusing to vote, could not persist state: {e}");
                return RequestVoteResponse { term: core.current_term, vote_granted: false };
            }
            core.last_heard = Instant::now();
        }

        RequestVoteResponse { term: core.current_term, vote_granted: granted }
    }

    pub async fn handle_append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        let mut core = self.core.lock().await;

        if req.term < core.current_term {
            return AppendEntriesResponse {
                term: core.current_term,
                success: false,
                match_index: 0,
            };
        }
        if req.term > core.current_term {
            self.step_down(&mut core, req.term);
        }

        core.role = Role::Follower;
        core.leader = req.leader_id.parse::<Uuid>().ok();
        core.last_heard = Instant::now();

        if core.log.term_at(req.prev_log_index) != Some(req.prev_log_term) {
            return AppendEntriesResponse {
                term: core.current_term,
                success: false,
                match_index: 0,
            };
        }

        let mut entries = Vec::with_capacity(req.entries.len());
        for bytes in &req.entries {
            match decode_entry(bytes) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    eprintln!("raft: undecodable entry from leader: {e}");
                    return AppendEntriesResponse {
                        term: core.current_term,
                        success: false,
                        match_index: 0,
                    };
                }
            }
        }

        let mut fresh_from = entries.len();
        for (i, entry) in entries.iter().enumerate() {
            match core.log.term_at(entry.index) {
                Some(term) if term == entry.term => continue,
                Some(_) => {
                    if let Err(e) = core.log.truncate_after(entry.index - 1) {
                        eprintln!("raft: truncation failed: {e}");
                        return AppendEntriesResponse {
                            term: core.current_term,
                            success: false,
                            match_index: 0,
                        };
                    }
                    fresh_from = i;
                    break;
                }
                None => {
                    fresh_from = i;
                    break;
                }
            }
        }
        if fresh_from < entries.len() {
            if let Err(e) = core.log.append(&entries[fresh_from..]) {
                eprintln!("raft: append failed: {e}");
                return AppendEntriesResponse {
                    term: core.current_term,
                    success: false,
                    match_index: 0,
                };
            }
        }

        if req.leader_commit > core.commit_index {
            core.commit_index = req.leader_commit.min(core.log.last_index());
        }
        let match_index = core.log.last_index();
        drop(core);

        self.apply_committed().await;
        AppendEntriesResponse {
            term: self.core.lock().await.current_term,
            success: true,
            match_index,
        }
    }

    pub async fn run(self: Arc<Self>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(TICK) => {}
                _ = self.replicate.notified() => {}
            }

            self.step_down_if_quorum_lost().await;

            let (role, needs_election, needs_heartbeat) = {
                let core = self.core.lock().await;
                (
                    core.role,
                    core.role != Role::Leader && core.last_heard.elapsed() > core.election_timeout,
                    core.role == Role::Leader
                        && core.last_heartbeat_sent.elapsed() >= HEARTBEAT,
                )
            };

            if role != Role::Leader {
                self.fail_pending_proposals().await;
            }

            if needs_election {
                self.start_election().await;
            } else if role == Role::Leader {
                if needs_heartbeat {
                    self.core.lock().await.last_heartbeat_sent = Instant::now();
                }
                self.replicate_to_peers().await;
            }
        }
    }

    async fn start_election(self: &Arc<Self>) {
        let (term, last_index, last_term) = {
            let mut core = self.core.lock().await;
            core.current_term += 1;
            core.role = Role::Candidate;
            core.voted_for = Some(self.id);
            core.leader = None;
            core.votes.clear();
            core.votes.insert(self.id);
            core.last_heard = Instant::now();
            core.election_timeout = random_election_timeout();

            if let Err(e) = self.persist_hard_state(&core) {
                eprintln!("raft: abandoning election, could not persist state: {e}");
                return;
            }
            (core.current_term, core.log.last_index(), core.log.last_term())
        };

        if self.peers.is_empty() {
            self.become_leader(term).await;
            return;
        }

        for peer in &self.peers {
            let node = Arc::clone(self);
            let peer = *peer;
            let req = RequestVoteRequest {
                term,
                candidate_id: self.id.to_string(),
                last_log_index: last_index,
                last_log_term: last_term,
            };
            tokio::spawn(async move {
                let Ok(resp) = node.transport.request_vote(peer, req).await else { return };
                node.on_vote(peer, term, resp).await;
            });
        }
    }

    async fn on_vote(self: &Arc<Self>, peer: NodeId, term: u64, resp: RequestVoteResponse) {
        let won = {
            let mut core = self.core.lock().await;
            if resp.term > core.current_term {
                self.step_down(&mut core, resp.term);
                let _ = self.persist_hard_state(&core);
                return;
            }
            if core.role != Role::Candidate || core.current_term != term {
                return;
            }
            if !resp.vote_granted {
                return;
            }
            core.votes.insert(peer);
            core.votes.len() >= self.quorum
        };

        if won {
            self.become_leader(term).await;
        }
    }

    async fn become_leader(self: &Arc<Self>, term: u64) {
        {
            let mut core = self.core.lock().await;
            if core.current_term != term || core.role == Role::Leader {
                return;
            }
            core.role = Role::Leader;
            core.leader = Some(self.id);
            core.last_heartbeat_sent = Instant::now() - HEARTBEAT;

            let next = core.log.last_index() + 1;
            let now = Instant::now();
            for peer in &self.peers {
                core.next_index.insert(*peer, next);
                core.match_index.insert(*peer, 0);
                core.last_response.insert(*peer, now);
            }

            let entry = LogEntry { term, index: next, payload: EntryPayload::Noop };
            if let Err(e) = core.log.append(&[entry]) {
                eprintln!("raft: could not append leader no-op: {e}");
            }
        }

        self.advance_commit().await;
        self.apply_committed().await;
        self.replicate.notify_one();
    }

    async fn replicate_to_peers(self: &Arc<Self>) {
        for peer in &self.peers {
            let peer = *peer;
            if !self.inflight.lock().await.insert(peer) {
                continue;
            }

            let (term, req) = {
                let core = self.core.lock().await;
                if core.role != Role::Leader {
                    self.inflight.lock().await.remove(&peer);
                    return;
                }
                let next = core.next_index.get(&peer).copied().unwrap_or(1).max(1);
                let prev_log_index = next - 1;
                let Some(prev_log_term) = core.log.term_at(prev_log_index) else {
                    self.inflight.lock().await.remove(&peer);
                    continue;
                };
                let entries = core
                    .log
                    .entries_from(next, MAX_ENTRIES_PER_RPC)
                    .iter()
                    .filter_map(|e| encode_entry(e).ok())
                    .collect();

                (
                    core.current_term,
                    AppendEntriesRequest {
                        term: core.current_term,
                        leader_id: self.id.to_string(),
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: core.commit_index,
                    },
                )
            };

            let node = Arc::clone(self);
            tokio::spawn(async move {
                let result = node.transport.append_entries(peer, req).await;
                node.inflight.lock().await.remove(&peer);
                let Ok(resp) = result else { return };
                node.on_append_response(peer, term, resp).await;
            });
        }
    }

    async fn on_append_response(
        self: &Arc<Self>,
        peer: NodeId,
        term: u64,
        resp: AppendEntriesResponse,
    ) {
        {
            let mut core = self.core.lock().await;
            core.last_response.insert(peer, Instant::now());

            if resp.term > core.current_term {
                self.step_down(&mut core, resp.term);
                let _ = self.persist_hard_state(&core);
                return;
            }
            if core.role != Role::Leader || core.current_term != term {
                return;
            }

            if resp.success {
                core.match_index.insert(peer, resp.match_index);
                core.next_index.insert(peer, resp.match_index + 1);
            } else {
                let next = core.next_index.get(&peer).copied().unwrap_or(1);
                core.next_index.insert(peer, next.saturating_sub(1).max(1));
            }
        }

        self.advance_commit().await;
        self.apply_committed().await;
    }

    async fn advance_commit(&self) {
        let mut core = self.core.lock().await;
        if core.role != Role::Leader {
            return;
        }

        let mut indices: Vec<u64> = self
            .peers
            .iter()
            .map(|p| core.match_index.get(p).copied().unwrap_or(0))
            .collect();
        indices.push(core.log.last_index());
        indices.sort_unstable_by(|a, b| b.cmp(a));

        let candidate = indices[self.quorum - 1];
        if candidate > core.commit_index && core.log.term_at(candidate) == Some(core.current_term) {
            core.commit_index = candidate;
        }
    }

    async fn apply_committed(&self) {
        loop {
            let entry = {
                let core = self.core.lock().await;
                if core.last_applied >= core.commit_index {
                    break;
                }
                core.log.entry(core.last_applied + 1).cloned()
            };

            let Some(entry) = entry else { break };
            if let EntryPayload::Queue(record) = &entry.payload {
                self.state.apply(record).await;
            }
            self.core.lock().await.last_applied = entry.index;
        }

        let applied = self.core.lock().await.last_applied;
        let mut waiters = self.waiters.lock().await;
        let mut still_waiting = Vec::with_capacity(waiters.len());
        for (index, tx) in waiters.drain(..) {
            if index <= applied {
                let _ = tx.send(Ok(()));
            } else {
                still_waiting.push((index, tx));
            }
        }
        *waiters = still_waiting;
    }

    async fn step_down_if_quorum_lost(&self) {
        if self.peers.is_empty() {
            return;
        }
        let mut core = self.core.lock().await;
        if core.role != Role::Leader {
            return;
        }

        let window = Duration::from_millis(ELECTION_TIMEOUT_MAX_MS);
        let reachable = self
            .peers
            .iter()
            .filter(|peer| {
                core.last_response.get(peer).is_some_and(|seen| seen.elapsed() < window)
            })
            .count();

        if reachable + 1 < self.quorum {
            let term = core.current_term;
            eprintln!("raft: standing down in term {term}, lost contact with the majority");
            self.step_down(&mut core, term);
        }
    }

    async fn fail_pending_proposals(&self) {
        let mut waiters = self.waiters.lock().await;
        for (_, tx) in waiters.drain(..) {
            let _ = tx.send(Err(ProposeError::LostLeadership));
        }
    }

    fn step_down(&self, core: &mut Core, term: u64) {
        core.current_term = term;
        core.role = Role::Follower;
        core.voted_for = None;
        core.leader = None;
        core.votes.clear();
        core.last_heard = Instant::now();
    }

    fn persist_hard_state(&self, core: &Core) -> io::Result<()> {
        write_bincode(
            &self.hard_state_path,
            &HardState {
                current_term: core.current_term,
                voted_for: core.voted_for.map(|id| id.as_u128()),
            },
        )
    }
}

fn read_bincode<T: Decode<()>>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => bincode_next::decode_from_slice::<T, _>(&bytes, config::standard())
            .map(|(value, _)| Some(value))
            .map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write_bincode<T: Encode>(path: &Path, value: &T) -> io::Result<()> {
    let bytes = bincode_next::encode_to_vec(value, config::standard()).map_err(io::Error::other)?;
    let tmp = path.with_extension("tmp");
    {
        let mut file = fs::File::create(&tmp)?;
        io::Write::write_all(&mut file, &bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)
}
