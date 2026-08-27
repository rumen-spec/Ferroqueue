pub mod proto {
    pub mod queue {
        tonic::include_proto!("queue");
    }
    pub mod raft {
        tonic::include_proto!("raft");
    }
}
#[cfg(test)]
mod cluster_tests;

pub mod broker;
pub mod job;
pub mod log;
pub mod raft;
pub mod state;
pub mod transport;

use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tonic::transport::Server;
use uuid::Uuid;

use crate::broker::QueueService;
use crate::proto::queue::queue_server::QueueServer;
use crate::proto::raft::raft_server::RaftServer;
use crate::raft::Node;
use crate::state::QueueState;
use crate::transport::{GrpcTransport, NodeId, RaftService};

const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

struct Config {
    dir: String,
    client_addr: String,
    peer_addr: String,
    /// Every member's raft address, including this node's.
    members: Vec<String>,
}

impl Config {
    fn from_args() -> Self {
        let mut dir = "data".to_string();
        let mut client_addr = "127.0.0.1:5000".to_string();
        let mut peer_addr = "127.0.0.1:6000".to_string();
        let mut joins: Vec<String> = Vec::new();

        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--dir" => {
                    dir = args[i + 1].clone();
                    i += 2;
                }
                "--client" => {
                    client_addr = args[i + 1].clone();
                    i += 2;
                }
                "--peer" => {
                    peer_addr = args[i + 1].clone();
                    i += 2;
                }
                "--join" => {
                    joins.push(args[i + 1].clone());
                    i += 2;
                }
                other => {
                    eprintln!("unknown argument {other}");
                    i += 1;
                }
            }
        }

        let mut members = joins;
        members.push(peer_addr.clone());
        members.sort();
        members.dedup();

        Config { dir, client_addr, peer_addr, members }
    }
}

/// Node and cluster ids are derived from addresses rather than generated, so
/// every member computes the same values from the same config and there is no
/// chicken-and-egg at bootstrap. A consequence: changing a node's peer address
/// changes its identity, and pointing it at a different member set changes the
/// cluster id, which the data directory will then refuse to load.
fn derive_id(value: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, value.as_bytes())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args();

    let node_id = derive_id(&config.peer_addr);
    let cluster_id = derive_id(&config.members.join(","));

    let mut peer_addrs: HashMap<NodeId, String> = HashMap::new();
    for member in &config.members {
        if member == &config.peer_addr {
            continue;
        }
        peer_addrs.insert(derive_id(member), format!("http://{member}"));
    }
    let peer_ids: Vec<NodeId> = peer_addrs.keys().copied().collect();

    println!("node {node_id} (cluster {cluster_id})");
    println!("  data dir  {}", config.dir);
    println!("  clients   {}", config.client_addr);
    println!("  peers     {}", config.peer_addr);
    if peer_addrs.is_empty() {
        println!("  cluster   single node (commits on its own fsync)");
    } else {
        for (id, addr) in &peer_addrs {
            println!("  member    {id} at {addr}");
        }
    }

    let transport = Arc::new(GrpcTransport::new(&peer_addrs)?);
    let state = Arc::new(QueueState::default());
    let node = Node::open(&config.dir, node_id, cluster_id, peer_ids, state, transport).await?;

    println!("replayed to index {}", node.last_index().await);

    tokio::spawn(Arc::clone(&node).run());
    tokio::spawn(broker::sweep_expired_leases(Arc::clone(&node), SWEEP_INTERVAL));

    // Peer traffic gets its own listener so client load cannot starve
    // heartbeats, and so it can be firewalled separately.
    let peer_socket: SocketAddr = config.peer_addr.parse()?;
    let raft_node = Arc::clone(&node);
    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(RaftServer::new(RaftService::new(raft_node)))
            .serve(peer_socket)
            .await
        {
            eprintln!("raft listener stopped: {e}");
        }
    });

    let client_socket: SocketAddr = config.client_addr.parse()?;
    Server::builder()
        .add_service(QueueServer::new(QueueService::new(node)))
        .serve_with_shutdown(client_socket, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}
