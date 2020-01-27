//! all the network related actions and processes
//!
//! This module only provides and handle the different connections
//! and act as message passing between the other modules (blockchain,
//! transactions...);
//!

pub mod bootstrap;
mod client;
mod grpc;
mod inbound;
pub mod p2p;
mod service;
mod subscription;

use thiserror::Error;

// Constants

mod buffer_sizes {
    pub mod inbound {
        // Size of buffer for processing of header push/pull streams.
        pub const HEADERS: usize = 32;

        // The maximum number of blocks to buffer from an incoming stream
        // (GetBlocks response or an UploadBlocks request)
        // while waiting for the block task to become ready to process
        // the next block.
        pub const BLOCKS: usize = 8;

        // The maximum number of fragments to buffer from an incoming subscription
        // while waiting for the fragment task to become ready to process them.
        pub const FRAGMENTS: usize = 128;
    }
    pub mod outbound {
        // Size of buffer for outbound header streams.
        pub const HEADERS: usize = 32;

        // The maximum number of blocks to buffer for an outbound stream
        // (GetBlocks response or an UploadBlocks request)
        // before the client request task producing them gets preempted.
        pub const BLOCKS: usize = 8;
    }
}

use self::client::ConnectError;
use self::p2p::{
    comm::{PeerComms, Peers},
    P2pTopology,
};
use crate::blockcfg::{Block, HeaderHash};
use crate::blockchain::{Blockchain as NewBlockchain, Tip};
use crate::intercom::{BlockMsg, ClientMsg, NetworkMsg, PropagateMsg, TransactionMsg};
use crate::settings::start::network::{Configuration, Peer, Protocol, DEFAULT_PEER_GC_INTERVAL};
use crate::utils::{
    async_msg::{MessageBox, MessageQueue},
    task::TokioServiceInfo,
};
use futures::future;
use futures::future::Either::{A, B};
use futures::prelude::*;
use futures::stream;
use network_core::gossip::{Gossip, Node};
use poldercast::StrikeReason;
use rand::seq::SliceRandom;
use slog::Logger;
use tokio::timer::Interval;

use std::error;
use std::fmt;
use std::io;
use std::iter;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub use self::bootstrap::Error as BootstrapError;

#[derive(Debug)]
pub struct ListenError {
    cause: io::Error,
    sockaddr: SocketAddr,
}

impl fmt::Display for ListenError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "failed to listen for connections on {}", self.sockaddr)
    }
}

impl error::Error for ListenError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        Some(&self.cause)
    }
}

type Connection = SocketAddr;

pub enum BlockConfig {}

/// all the different channels the network may need to talk to
pub struct Channels {
    pub client_box: MessageBox<ClientMsg>,
    pub transaction_box: MessageBox<TransactionMsg>,
    pub block_box: MessageBox<BlockMsg>,
}

impl Clone for Channels {
    fn clone(&self) -> Self {
        Channels {
            client_box: self.client_box.clone(),
            transaction_box: self.transaction_box.clone(),
            block_box: self.block_box.clone(),
        }
    }
}

/// Global state shared between all network tasks.
pub struct GlobalState {
    pub block0_hash: HeaderHash,
    pub config: Configuration,
    pub topology: P2pTopology,
    pub peers: Peers,
    pub logger: Logger,
}

type GlobalStateR = Arc<GlobalState>;

impl GlobalState {
    /// the network global state
    pub fn new(
        block0_hash: HeaderHash,
        config: Configuration,
        topology: P2pTopology,
        logger: Logger,
    ) -> Self {
        let peers = Peers::new(
            config.max_connections,
            config.max_connections_threshold,
            logger.clone(),
        );

        GlobalState {
            block0_hash,
            config,
            topology,
            peers,
            logger,
        }
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        tokio::spawn(f);
    }
}

pub struct ConnectionState {
    /// The global state shared between all connections
    pub global: GlobalStateR,

    /// the timeout to wait for unbefore the connection replies
    pub timeout: Duration,

    /// the local (to the task) connection details
    pub connection: Connection,

    logger: Logger,
}

impl ConnectionState {
    fn new(global: GlobalStateR, peer: &Peer) -> Self {
        ConnectionState {
            timeout: peer.timeout,
            connection: peer.connection.clone(),
            logger: global.logger().new(o!("peer_addr" => peer.connection)),
            global,
        }
    }

    fn logger(&self) -> &Logger {
        &self.logger
    }
}

pub struct TaskParams {
    pub config: Configuration,
    pub block0_hash: HeaderHash,
    pub input: MessageQueue<NetworkMsg>,
    pub channels: Channels,
}

pub fn start(
    service_info: TokioServiceInfo,
    params: TaskParams,
    topology: P2pTopology,
) -> impl Future<Item = (), Error = ()> {
    // TODO: the node needs to be saved/loaded
    //
    // * the ID needs to be consistent between restart;
    let input = params.input;
    let channels = params.channels;
    let global_state = Arc::new(GlobalState::new(
        params.block0_hash,
        params.config,
        topology,
        service_info.logger().clone(),
    ));

    // open the port for listening/accepting other peers to connect too
    let listen = global_state.config.listen();
    use futures::future::Either;
    let listener = if let Some(listen) = listen {
        match listen.protocol {
            Protocol::Grpc => {
                match grpc::run_listen_socket(&listen, global_state.clone(), channels.clone()) {
                    Ok(future) => Either::A(future),
                    Err(e) => {
                        error!(
                            service_info.logger(),
                            "failed to listen for P2P connections at {}", listen.connection;
                            "reason" => %e);
                        Either::B(future::err(()))
                    }
                }
            }
            Protocol::Ntt => unimplemented!(),
        }
    } else {
        Either::B(future::ok(()))
    };

    global_state.spawn(start_gossiping(global_state.clone(), channels.clone()));

    let handle_cmds = handle_network_input(input, global_state.clone(), channels.clone());

    let gossip_err_logger = global_state.logger.clone();
    let reset_err_logger = global_state.logger.clone();
    let tp2p = global_state.topology.clone();

    if let Some(interval) = global_state.config.topology_force_reset_interval.clone() {
        global_state.spawn(
            Interval::new_interval(interval)
                .map_err(move |e| {
                    error!(reset_err_logger, "interval timer error: {:?}", e);
                })
                .for_each(move |_| tp2p.force_reset_layers()),
        );
    }

    let peers = global_state.peers.clone();
    let gc_err_logger = global_state.logger.clone();
    let gc_info_logger = global_state.logger.clone();
    global_state.spawn(
        Interval::new_interval(DEFAULT_PEER_GC_INTERVAL)
            .map_err(move |e| {
                error!(gc_err_logger, "interval timer error: {:?}", e);
            })
            .for_each(move |_| {
                let gc_info_logger = gc_info_logger.clone();
                peers.gc().map(move |maybe_gced| {
                    if let Some(number_gced) = maybe_gced {
                        warn!(
                            gc_info_logger,
                            "Peer GCed {number_gced} nodes",
                            number_gced = number_gced
                        );
                    } else {
                        debug!(gc_info_logger, "Peer GCed nothing");
                    }
                })
            }),
    );

    let gossip = Interval::new_interval(global_state.config.gossip_interval.clone())
        .map_err(move |e| {
            error!(gossip_err_logger, "interval timer error: {:?}", e);
        })
        .for_each(move |_| send_gossip(global_state.clone(), channels.clone()));

    listener.join3(handle_cmds, gossip).map(|_| ())
}

fn handle_network_input(
    input: MessageQueue<NetworkMsg>,
    state: GlobalStateR,
    channels: Channels,
) -> impl Future<Item = (), Error = ()> {
    input.for_each(move |msg| match msg {
        NetworkMsg::Propagate(msg) => A(A(handle_propagation_msg(
            msg,
            state.clone(),
            channels.clone(),
        ))),
        NetworkMsg::GetBlocks(block_ids) => A(B(state.peers.fetch_blocks(block_ids))),
        NetworkMsg::GetNextBlock(node_id, block_id) => {
            B(A(state.peers.solicit_blocks(node_id, vec![block_id])))
        }
        NetworkMsg::PullHeaders { node_id, from, to } => {
            B(B(A(state.peers.pull_headers(node_id, from.into(), to))))
        }
        NetworkMsg::PeerInfo(reply) => {
            B(B(B(state.peers.infos().map(|infos| reply.reply_ok(infos)))))
        }
    })
}

fn handle_propagation_msg(
    msg: PropagateMsg,
    state: GlobalStateR,
    channels: Channels,
) -> impl Future<Item = (), Error = ()> {
    let prop_state = state.clone();
    let send_to_peers = match msg {
        PropagateMsg::Block(ref header) => {
            let header = header.clone();
            let future = state
                .topology
                .view(poldercast::Selection::Topic {
                    topic: p2p::topic::BLOCKS,
                })
                .and_then(move |view| prop_state.peers.propagate_block(view.peers, header));
            A(future)
        }
        PropagateMsg::Fragment(ref fragment) => {
            let fragment = fragment.clone();
            let future = state
                .topology
                .view(poldercast::Selection::Topic {
                    topic: p2p::topic::MESSAGES,
                })
                .and_then(move |view| prop_state.peers.propagate_fragment(view.peers, fragment));
            B(future)
        }
    };
    // If any nodes selected for propagation are not in the
    // active subscriptions map, connect to them and deliver
    // the item.
    send_to_peers.then(move |res| {
        if let Err(unreached_nodes) = res {
            debug!(
                state.logger(),
                "{} of the peers selected for propagation have not been reached, will try to connect",
                unreached_nodes.len(),
            );
            for node in unreached_nodes {
                let msg = msg.clone();
                connect_and_propagate_with(node, state.clone(), channels.clone(), |comms| match msg {
                    PropagateMsg::Block(header) => comms.set_pending_block_announcement(header),
                    PropagateMsg::Fragment(fragment) => comms.set_pending_fragment(fragment),
                });
            }
        }
        Ok(())
    })
}

fn start_gossiping(state: GlobalStateR, channels: Channels) -> impl Future<Item = (), Error = ()> {
    let config = &state.config;
    let topology = state.topology.clone();
    let conn_state = state.clone();
    // inject the trusted peers as initial gossips, this will make the node
    // gossip with them at least at the beginning
    topology
        .accept_gossips(
            (*config.profile.id()).into(),
            config
                .trusted_peers
                .iter()
                .map(|tp| {
                    let mut builder = poldercast::NodeProfileBuilder::new();
                    builder.id(tp.id.clone().into());
                    builder.address(tp.address.clone().into());
                    builder.build()
                })
                .map(p2p::Gossip::from)
                .collect::<Vec<p2p::Gossip>>()
                .into(),
        )
        .and_then(move |()| topology.view(poldercast::Selection::Any))
        .and_then(move |view| {
            for node in view.peers {
                let self_node = view.self_node.clone();
                let gossip = Gossip::from_nodes(iter::once(self_node.into()));
                connect_and_propagate_with(
                    node,
                    conn_state.clone(),
                    channels.clone(),
                    move |comms| {
                        comms.set_pending_gossip(gossip);
                    },
                );
            }
            Ok(())
        })
}

fn send_gossip(state: GlobalStateR, channels: Channels) -> impl Future<Item = (), Error = ()> {
    let topology = state.topology.clone();
    topology
        .view(poldercast::Selection::Any)
        .and_then(move |view| {
            stream::iter_ok(view.peers).for_each(move |node| {
                let peer_id = node.id();
                let state_prop = state.clone();
                let state_err = state.clone();
                let channels_err = channels.clone();
                topology
                    .initiate_gossips(peer_id)
                    .and_then(move |gossips| {
                        state_prop
                            .peers
                            .propagate_gossip_to(peer_id, Gossip::from(gossips))
                    })
                    .then(move |res| {
                        if let Err(gossip) = res {
                            connect_and_propagate_with(node, state_err, channels_err, |comms| {
                                comms.set_pending_gossip(gossip)
                            })
                        }
                        Ok(())
                    })
            })
        })
}

fn connect_and_propagate_with<F>(
    node: p2p::Node,
    state: GlobalStateR,
    channels: Channels,
    modify_comms: F,
) where
    F: FnOnce(&mut PeerComms) + Send + 'static,
{
    let addr = match node.address() {
        Some(addr) => addr,
        None => {
            debug!(
                state.logger(),
                "ignoring P2P node without an IP address" ;
                "node" => %node.id()
            );
            return;
        }
    };
    let node_id = node.id();
    assert_ne!(
        node_id,
        state.topology.node_id(),
        "topology tells the node to connect to itself"
    );
    let peer = Peer::new(addr, Protocol::Grpc);
    let conn_state = ConnectionState::new(state.clone(), &peer);
    let conn_logger = conn_state
        .logger()
        .new(o!("node_id" => node_id.to_string()));
    info!(conn_logger, "connecting to peer");
    let (handle, connecting) = client::connect(conn_state, channels.clone());
    let spawn_state = state.clone();
    let conn_err_state = state.clone();
    let cf = state.peers.connecting_with(node_id, handle, modify_comms)
        .and_then(|()| connecting)
        .or_else(move |e| {
            let benign = match e {
                ConnectError::Connect(e) => {
                    if let Some(e) = e.connect_error() {
                        info!(conn_logger, "failed to connect to peer"; "reason" => %e);
                    } else if let Some(e) = e.http_error() {
                        info!(conn_logger, "failed to establish an HTTP connection with the peer"; "reason" => %e);
                    } else {
                        info!(conn_logger, "gRPC connection to peer failed"; "reason" => %e);
                    }
                    false
                }
                ConnectError::Canceled => {
                    debug!(conn_logger, "connection to peer has been canceled");
                    true
                }
                _ => {
                    info!(conn_logger, "connection to peer failed"; "reason" => %e);
                    false
                }
            };
            if !benign {
                let future = conn_err_state
                    .topology
                    .report_node(node_id, StrikeReason::CannotConnect)
                    .join(conn_err_state.peers.remove_peer(node_id))
                    .and_then(|_| future::err(()));
                A(future)
            } else {
                B(future::err(()))
            }
        })
        .and_then(move |client| {
            let connected_node_id = client.remote_node_id();
            if connected_node_id != node_id {
                info!(
                    client.logger(),
                    "peer node ID differs from the expected {}", node_id
                );
                let future = state
                    .topology
                    .report_node(node_id, StrikeReason::InvalidPublicId)
                    .join(state.peers.remove_peer(node_id))
                    .and_then(|_| future::err(()));
                A(future)
            } else {
                B(future::ok(client))
            }
        })
        .and_then(|client| client);
    spawn_state.spawn(cf);
}

fn trusted_peers_shuffled(config: &Configuration) -> Vec<SocketAddr> {
    let mut peers = config
        .trusted_peers
        .iter()
        .filter_map(|peer| peer.address.to_socketaddr())
        .collect::<Vec<_>>();
    let mut rng = rand::thread_rng();
    peers.shuffle(&mut rng);
    peers
}

pub async fn bootstrap(
    config: &Configuration,
    blockchain: NewBlockchain,
    branch: Tip,
    logger: &Logger,
) -> Result<bool, bootstrap::Error> {
    if config.protocol != Protocol::Grpc {
        unimplemented!()
    }

    if config.trusted_peers.is_empty() {
        warn!(logger, "No trusted peers joinable to bootstrap the network");
    }

    let mut bootstrapped = false;

    for address in trusted_peers_shuffled(&config) {
        let logger = logger.new(o!("peer_addr" => address.to_string()));
        let peer = Peer::new(address, Protocol::Grpc);
        let res = bootstrap::bootstrap_from_peer(
            peer,
            blockchain.clone(),
            branch.clone(),
            logger.clone(),
        )
        .await;

        match res {
            Err(bootstrap::Error::Connect { source: e }) => {
                warn!(logger, "unable to reach peer for initial bootstrap"; "reason" => %e);
            }
            Err(e) => {
                warn!(logger, "initial bootstrap failed"; "error" => ?e);
            }
            Ok(_) => {
                info!(logger, "initial bootstrap completed");
                bootstrapped = true;
                break;
            }
        }
    }

    Ok(bootstrapped)
}

/// Queries the trusted peers for a block identified with the hash.
/// The calling thread is blocked until the block is retrieved.
/// This function is called during blockchain initialization
/// to retrieve the genesis block.
pub async fn fetch_block(
    config: &Configuration,
    hash: HeaderHash,
    logger: &Logger,
) -> Result<Block, FetchBlockError> {
    if config.protocol != Protocol::Grpc {
        unimplemented!()
    }

    if config.trusted_peers.is_empty() {
        return Err(FetchBlockError::NoTrustedPeers);
    }

    let mut block = None;

    let logger = logger.new(o!("block" => hash.to_string()));

    for address in trusted_peers_shuffled(&config) {
        let logger = logger.new(o!("peer_address" => address.to_string()));
        let peer = Peer::new(address, Protocol::Grpc);
        match grpc::fetch_block(peer, hash, &logger).await {
            Err(grpc::FetchBlockError::Connect { source: e }) => {
                warn!(logger, "unable to reach peer for block download"; "reason" => %e);
            }
            Err(e) => {
                warn!(logger, "failed to download block"; "error" => ?e);
            }
            Ok(b) => {
                info!(logger, "genesis block fetched");
                block = Some(b);
                break;
            }
        }
    }

    if let Some(block) = block {
        Ok(block)
    } else {
        Err(FetchBlockError::CouldNotDownloadBlock {
            block: hash.to_owned(),
        })
    }
}

#[derive(Debug, Error)]
pub enum FetchBlockError {
    #[error("no trusted peers specified")]
    NoTrustedPeers,
    #[error("could not download block hash {block}")]
    CouldNotDownloadBlock { block: HeaderHash },
}
