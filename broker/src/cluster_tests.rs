//! Cluster behaviour, exercised over the in-memory transport so partitions and
//! failures are deterministic rather than a matter of timing luck.

use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::raft::{Node, ProposeError, Role};
use crate::state::{QueueState, Record};
use crate::transport::{MemoryCluster, NodeId};

/// Generous relative to a 150-300ms election timeout, so a slow machine does
/// not turn a correctness test into a flaky one.
const SETTLE: Duration = Duration::from_secs(5);

fn id_for(name: &str, i: usize) -> NodeId {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("{name}-{i}").as_bytes())
}

async fn cluster(name: &str, size: usize) -> (MemoryCluster, Vec<Arc<Node>>) {
    let cluster = MemoryCluster::new();
    let cluster_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes());
    let ids: Vec<NodeId> = (0..size).map(|i| id_for(name, i)).collect();

    let mut nodes = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        let dir = std::env::temp_dir().join(format!("ferroqueue-cluster-{name}-{i}"));
        let _ = std::fs::remove_dir_all(&dir);

        let node = Node::open(
            &dir,
            *id,
            cluster_id,
            ids.clone(),
            Arc::new(QueueState::default()),
            cluster.transport(*id),
        )
        .await
        .expect("node should open");

        cluster.register(Arc::clone(&node)).await;
        nodes.push(node);
    }

    for node in &nodes {
        tokio::spawn(Arc::clone(node).run());
    }
    (cluster, nodes)
}

/// Waits until exactly one of `candidates` reports itself leader.
async fn wait_for_leader(candidates: &[Arc<Node>]) -> Arc<Node> {
    let deadline = Instant::now() + SETTLE;
    while Instant::now() < deadline {
        let mut leaders = Vec::new();
        for node in candidates {
            if node.is_leader().await {
                leaders.push(Arc::clone(node));
            }
        }
        if leaders.len() == 1 {
            return leaders.remove(0);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no single leader emerged within {SETTLE:?}");
}

async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + SETTLE;
    while Instant::now() < deadline {
        if condition().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

fn enqueued(n: u128) -> Record {
    Record::Enqueued { job_id: n, payload: format!("job-{n}"), created_at: n as i64 }
}

#[tokio::test]
async fn single_node_commits_on_its_own_fsync() {
    let (_cluster, nodes) = cluster("single", 1).await;
    let node = wait_for_leader(&nodes).await;

    node.propose(enqueued(1)).await.expect("a one-member cluster is its own majority");

    assert_eq!(node.state().ready_ids().await.len(), 1);
    assert_eq!(node.commit_index().await, node.last_index().await);
}

#[tokio::test]
async fn elects_exactly_one_leader() {
    let (_cluster, nodes) = cluster("elect", 3).await;
    let leader = wait_for_leader(&nodes).await;

    let term = leader.term().await;
    let mut leaders_in_term = 0;
    for node in &nodes {
        if node.role().await == Role::Leader && node.term().await == term {
            leaders_in_term += 1;
        }
        // Everyone should have converged on the same term.
        assert_eq!(node.term().await, term, "term disagreement across the cluster");
    }
    assert_eq!(leaders_in_term, 1, "two leaders in one term is a split brain");
}

#[tokio::test]
async fn replicates_committed_entries_to_every_follower() {
    let (_cluster, nodes) = cluster("replicate", 3).await;
    let leader = wait_for_leader(&nodes).await;

    for i in 1..=5 {
        leader.propose(enqueued(i)).await.expect("leader should commit");
    }

    let expected = leader.state().ready_ids().await;
    assert_eq!(expected.len(), 5);

    for node in &nodes {
        let node = Arc::clone(node);
        let expected = expected.clone();
        wait_until("followers to converge", || {
            let node = Arc::clone(&node);
            let expected = expected.clone();
            async move { node.state().ready_ids().await == expected }
        })
        .await;
    }
}

#[tokio::test]
async fn a_new_leader_takes_over_when_the_old_one_is_isolated() {
    let (cluster, nodes) = cluster("failover", 3).await;
    let old_leader = wait_for_leader(&nodes).await;
    let old_term = old_leader.term().await;

    cluster.isolate(old_leader.id()).await;

    let survivors: Vec<Arc<Node>> =
        nodes.iter().filter(|n| n.id() != old_leader.id()).cloned().collect();
    let new_leader = wait_for_leader(&survivors).await;

    assert_ne!(new_leader.id(), old_leader.id());
    assert!(
        new_leader.term().await > old_term,
        "a new leader must run in a later term than the one it replaced"
    );

    // The new majority can still make progress.
    new_leader.propose(enqueued(99)).await.expect("the remaining majority should commit");
}

#[tokio::test]
async fn an_isolated_leader_cannot_commit() {
    let (cluster, nodes) = cluster("noquorum", 3).await;
    let leader = wait_for_leader(&nodes).await;

    cluster.isolate(leader.id()).await;

    // Cut off from its followers it has no majority, so this must fail rather
    // than commit or hang.
    let result = tokio::time::timeout(SETTLE, leader.propose(enqueued(7))).await;
    match result {
        Ok(Err(ProposeError::LostLeadership)) | Ok(Err(ProposeError::NotLeader(_))) => {}
        Ok(Ok(())) => panic!("an isolated leader committed a write without a quorum"),
        Ok(Err(other)) => panic!("unexpected error: {other:?}"),
        Err(_) => panic!("propose hung instead of failing"),
    }
}

#[tokio::test]
async fn a_partitioned_follower_catches_up_after_healing() {
    let (cluster, nodes) = cluster("catchup", 3).await;
    let leader = wait_for_leader(&nodes).await;

    let follower = nodes
        .iter()
        .find(|n| n.id() != leader.id())
        .cloned()
        .expect("three nodes means there is a follower");

    cluster.isolate(follower.id()).await;

    for i in 1..=4 {
        leader.propose(enqueued(i)).await.expect("the other two are still a majority");
    }
    assert!(
        follower.state().ready_ids().await.len() < 4,
        "the isolated follower should have missed entries"
    );

    cluster.heal(follower.id()).await;

    let expected = leader.state().ready_ids().await;
    wait_until("the healed follower to catch up", || {
        let follower = Arc::clone(&follower);
        let expected = expected.clone();
        async move { follower.state().ready_ids().await == expected }
    })
    .await;
}

#[tokio::test]
async fn a_lone_node_cannot_elect_itself_in_a_three_node_cluster() {
    let (cluster, nodes) = cluster("minority", 3).await;
    wait_for_leader(&nodes).await;

    // Pick a node that is not the leader and cut it off. On its own it is a
    // minority of one and must never win an election, however long it tries.
    let leader_id = wait_for_leader(&nodes).await.id();
    let lone = nodes.iter().find(|n| n.id() != leader_id).cloned().unwrap();
    cluster.isolate(lone.id()).await;

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert_ne!(
        lone.role().await,
        Role::Leader,
        "one node out of three is not a majority"
    );
    assert!(
        lone.term().await > 0,
        "it should have kept campaigning, just never won"
    );
}
