use super::{
    candidate, chain,
    chain_selection::{self, ComparisonResult},
    Blockchain, Error, ErrorKind, PreCheckedHeader, Ref, Tip, MAIN_BRANCH_TAG,
};
use crate::{
    blockcfg::{Block, FragmentId, Header},
    blockchain::Checkpoints,
    intercom::{
        self, BlockMsg, ExplorerMsg, NetworkMsg, PropagateMsg, ReplyHandle, TransactionMsg,
    },
    log,
    network::p2p::Id as NodeId,
    stats_counter::StatsCounter,
    utils::{
        async_msg::{self, MessageBox, MessageQueue},
        fire_forget_scheduler::{
            FireForgetScheduler, FireForgetSchedulerConfig, FireForgetSchedulerFuture,
        },
        task::TokioServiceInfo,
    },
    HeaderHash,
};
use chain_core::property::{Block as _, Fragment as _, HasHeader as _, Header as _};
use jormungandr_lib::interfaces::FragmentStatus;

use futures::future::{Either, Loop};
use slog::Logger;
use tokio::{
    prelude::*,
    timer::{timeout, Interval, Timeout},
};
use tokio_compat::prelude::*;

use std::{sync::Arc, time::Duration};

type TimeoutError = timeout::Error<Error>;
type PullHeadersScheduler = FireForgetScheduler<HeaderHash, NodeId, Checkpoints>;
type GetNextBlockScheduler = FireForgetScheduler<HeaderHash, NodeId, ()>;

const DEFAULT_TIMEOUT_PROCESS_LEADERSHIP: u64 = 5;
const DEFAULT_TIMEOUT_PROCESS_ANNOUNCEMENT: u64 = 5;
const DEFAULT_TIMEOUT_PROCESS_BLOCKS: u64 = 60;
const DEFAULT_TIMEOUT_PROCESS_HEADERS: u64 = 60;

const PULL_HEADERS_SCHEDULER_CONFIG: FireForgetSchedulerConfig = FireForgetSchedulerConfig {
    max_running: 16,
    max_running_same_task: 2,
    command_channel_size: 1024,
    timeout: Duration::from_millis(500),
};

const GET_NEXT_BLOCK_SCHEDULER_CONFIG: FireForgetSchedulerConfig = FireForgetSchedulerConfig {
    max_running: 16,
    max_running_same_task: 2,
    command_channel_size: 1024,
    timeout: Duration::from_millis(500),
};

pub struct Process {
    pub blockchain: Blockchain,
    pub blockchain_tip: Tip,
    pub stats_counter: StatsCounter,
    pub network_msgbox: MessageBox<NetworkMsg>,
    pub fragment_msgbox: MessageBox<TransactionMsg>,
    pub explorer_msgbox: Option<MessageBox<ExplorerMsg>>,
    pub garbage_collection_interval: Duration,
}

impl Process {
    pub fn start(
        mut self,
        service_info: TokioServiceInfo,
        input: MessageQueue<BlockMsg>,
    ) -> impl Future<Item = (), Error = ()> {
        service_info.spawn(self.start_branch_reprocessing(service_info.logger().clone()));
        let pull_headers_scheduler = self.spawn_pull_headers_scheduler(&service_info);
        let get_next_block_scheduler = self.spawn_get_next_block_scheduler(&service_info);
        input.for_each(move |msg| {
            self.handle_input(
                &service_info,
                msg,
                &pull_headers_scheduler,
                &get_next_block_scheduler,
            );
            future::ok(())
        })
    }

    fn handle_input(
        &mut self,
        info: &TokioServiceInfo,
        input: BlockMsg,
        pull_headers_scheduler: &PullHeadersScheduler,
        get_next_block_scheduler: &GetNextBlockScheduler,
    ) {
        let blockchain = self.blockchain.clone();
        let blockchain_tip = self.blockchain_tip.clone();
        let network_msg_box = self.network_msgbox.clone();
        let explorer_msg_box = self.explorer_msgbox.clone();
        let mut tx_msg_box = self.fragment_msgbox.clone();
        let stats_counter = self.stats_counter.clone();

        match input {
            BlockMsg::LeadershipBlock(block) => {
                let logger = info.logger().new(o!(
                    "hash" => block.header.hash().to_string(),
                    "parent" => block.header.parent_id().to_string(),
                    "date" => block.header.block_date().to_string()));
                let logger2 = logger.clone();
                let logger3 = logger.clone();

                info!(logger, "receiving block from leadership service");

                let process_new_block =
                    process_leadership_block(logger.clone(), blockchain.clone(), block.clone());

                let fragments = block.fragments().map(|f| f.id()).collect();

                let update_mempool = process_new_block.and_then(move |new_block_ref| {
                    debug!(logger2, "updating fragment's log");
                    try_request_fragment_removal(&mut tx_msg_box, fragments, new_block_ref.header())
                        .map_err(|_| "cannot remove fragments from pool".into())
                        .map(|_| new_block_ref)
                });

                let process_new_ref = update_mempool.and_then(move |new_block_ref| {
                    debug!(logger3, "processing the new block and propagating");
                    process_and_propagate_new_ref(
                        logger3,
                        blockchain,
                        blockchain_tip,
                        Arc::clone(&new_block_ref),
                        network_msg_box,
                    )
                });

                let notify_explorer = process_new_ref.and_then(move |()| {
                    if let Some(msg_box) = explorer_msg_box {
                        Either::A(
                            msg_box
                                .send(ExplorerMsg::NewBlock(block))
                                .map_err(|_| "Cannot propagate block to explorer".into())
                                .map(|_| ()),
                        )
                    } else {
                        Either::B(future::ok(()))
                    }
                });

                info.spawn(
                    Timeout::new(notify_explorer, Duration::from_secs(DEFAULT_TIMEOUT_PROCESS_LEADERSHIP))
                        .map_err(move |err: TimeoutError| {
                            error!(logger, "cannot process leadership block" ; "reason" => ?err)
                        })
                )
            }
            BlockMsg::AnnouncedBlock(header, node_id) => {
                let logger = info.logger().new(o!(
                    "hash" => header.hash().to_string(),
                    "parent" => header.parent_id().to_string(),
                    "date" => header.block_date().to_string(),
                    "from_node_id" => node_id.to_string()));

                info!(logger, "received block announcement from network");

                let future = process_block_announcement(
                    blockchain.clone(),
                    blockchain_tip.clone(),
                    header,
                    node_id,
                    pull_headers_scheduler.clone(),
                    get_next_block_scheduler.clone(),
                    logger.clone(),
                );

                info.spawn(Timeout::new(future, Duration::from_secs(DEFAULT_TIMEOUT_PROCESS_ANNOUNCEMENT)).map_err(move |err: TimeoutError| {
                    error!(logger, "cannot process block announcement" ; "reason" => ?err)
                }))
            }
            BlockMsg::NetworkBlocks(handle) => {
                struct State<S> {
                    stream: S,
                    reply: ReplyHandle<()>,
                    candidate: Option<Arc<Ref>>,
                }

                info!(info.logger(), "receiving block stream from network");

                let logger = info.logger().clone();
                let logger_fold = logger.clone();
                let logger_err = logger.clone();
                let blockchain_fold = blockchain.clone();
                let (stream, reply) = handle.into_stream_and_reply();
                let stream =
                    stream.map_err(|()| Error::from("Error while processing block input stream"));
                let state = State {
                    stream,
                    reply,
                    candidate: None,
                };
                let get_next_block_scheduler = get_next_block_scheduler.clone();
                let future = future::loop_fn(state, move |state| {
                    let blockchain = blockchain_fold.clone();
                    let tx_msg_box = tx_msg_box.clone();
                    let explorer_msg_box = explorer_msg_box.clone();
                    let stats_counter = stats_counter.clone();
                    let logger = logger_fold.clone();
                    let get_next_block_scheduler = get_next_block_scheduler.clone();
                    let State {
                        stream,
                        reply,
                        candidate,
                    } = state;
                    stream.into_future().map_err(|(e, _)| e).and_then(
                        move |(maybe_block, stream)| match maybe_block {
                            Some(block) => Either::A(
                                process_network_block(
                                    blockchain,
                                    block,
                                    tx_msg_box,
                                    explorer_msg_box,
                                    get_next_block_scheduler,
                                    logger.clone(),
                                )
                                .then(move |res| match res {
                                    Ok(candidate) => {
                                        stats_counter.add_block_recv_cnt(1);
                                        Ok(Loop::Continue(State {
                                            stream,
                                            reply,
                                            candidate,
                                        }))
                                    }
                                    Err(e) => {
                                        info!(
                                            logger,
                                            "validation of an incoming block failed";
                                            "reason" => ?e,
                                        );
                                        reply.reply_error(network_block_error_into_reply(e));
                                        Ok(Loop::Break(candidate))
                                    }
                                }),
                            ),
                            None => {
                                reply.reply_ok(());
                                Either::B(future::ok(Loop::Break(candidate)))
                            }
                        },
                    )
                })
                .and_then(move |maybe_updated| match maybe_updated {
                    Some(new_block_ref) => {
                        let future = process_and_propagate_new_ref(
                            logger,
                            blockchain,
                            blockchain_tip,
                            Arc::clone(&new_block_ref),
                            network_msg_box,
                        );
                        Either::A(future)
                    }
                    None => Either::B(future::ok(())),
                });

                info.spawn(Timeout::new(future, Duration::from_secs(DEFAULT_TIMEOUT_PROCESS_BLOCKS)).map_err(move |err: TimeoutError| {
                    error!(logger_err, "cannot process network blocks" ; "reason" => ?err)
                }))
            }
            BlockMsg::ChainHeaders(handle) => {
                info!(info.logger(), "receiving header stream from network");

                let (stream, reply) = handle.into_stream_and_reply();
                let logger = info.logger().new(o!(log::KEY_SUB_TASK => "chain_pull"));
                let logger_err1 = logger.clone();
                let logger_err2 = logger.clone();
                let schedule_logger = logger.clone();
                let mut pull_headers_scheduler = pull_headers_scheduler.clone();

                let future = candidate::advance_branch(blockchain, stream, logger)
                    .inspect(move |(header_ids, _)|
                        header_ids.iter()
                        .try_for_each(|header_id| pull_headers_scheduler.declare_completed(*header_id))
                        .unwrap_or_else(|e| error!(schedule_logger, "get blocks schedule completion failed"; "reason" => ?e)))
                    .then(move |resp| match resp {
                        Err(e) => {
                            info!(
                                logger_err1,
                                "error processing an incoming header stream";
                                "reason" => %e,
                            );
                            reply.reply_error(chain_header_error_into_reply(e));
                            Either::A(future::ok(()))
                        }
                        Ok((hashes, maybe_remainder)) => {
                            if hashes.is_empty() {
                                Either::A(future::ok(()))
                            } else {
                                Either::B(
                                    network_msg_box
                                        .send(NetworkMsg::GetBlocks(hashes))
                                        .map_err(|_| "cannot request blocks from network".into())
                                        .map(|_| reply.reply_ok(())),
                                )
                                // TODO: if the stream is not ended, resume processing
                                // after more blocks arrive
                            }
                        }
                    })
                    .timeout(Duration::from_secs(DEFAULT_TIMEOUT_PROCESS_HEADERS))
                    .map_err(move |err: TimeoutError| {
                        error!(logger_err2, "cannot process network headers" ; "reason" => ?err)
                    });
                info.spawn(future);
            }
        }
    }

    fn start_branch_reprocessing(&self, logger: Logger) -> impl Future<Item = (), Error = ()> {
        let tip = self.blockchain_tip.clone();
        let blockchain = self.blockchain.clone();
        let error_logger = logger.clone();

        Interval::new_interval(Duration::from_secs(60))
            .map_err(move |e| {
                error!(error_logger, "cannot run branch reprocessing" ; "reason" => ?e);
            })
            .for_each(move |_instance| {
                let error_logger = logger.clone();
                reprocess_tip(logger.clone(), blockchain.clone(), tip.clone()).map_err(move |e| {
                    error!(error_logger, "cannot run branch reprocessing" ; "reason" => ?e);
                })
            })
    }

    fn spawn_pull_headers_scheduler(&self, info: &TokioServiceInfo) -> PullHeadersScheduler {
        let network_msgbox = self.network_msgbox.clone();
        let scheduler_logger = info.logger().clone();
        let scheduler_future = FireForgetSchedulerFuture::new(
            &PULL_HEADERS_SCHEDULER_CONFIG,
            move |to, node_id, from| {
                network_msgbox
                    .clone()
                    .try_send(NetworkMsg::PullHeaders { node_id, from, to })
                    .unwrap_or_else(|e| {
                        error!(scheduler_logger, "cannot send PullHeaders request to network";
                        "reason" => e.to_string())
                    })
            },
        );
        let scheduler = scheduler_future.scheduler();
        let logger = info.logger().clone();
        let future = scheduler_future
            .map(|never| match never {})
            .map_err(move |e| error!(logger, "get blocks scheduling failed"; "reason" => ?e));
        info.spawn(future);
        scheduler
    }

    fn spawn_get_next_block_scheduler(&self, info: &TokioServiceInfo) -> GetNextBlockScheduler {
        let network_msgbox = self.network_msgbox.clone();
        let scheduler_logger = info.logger().clone();
        let scheduler_future = FireForgetSchedulerFuture::new(
            &GET_NEXT_BLOCK_SCHEDULER_CONFIG,
            move |header_id, node_id, ()| {
                network_msgbox
                    .clone()
                    .try_send(NetworkMsg::GetNextBlock(node_id, header_id))
                    .unwrap_or_else(|e| {
                        error!(
                            scheduler_logger,
                            "cannot send GetNextBlock request to network"; "reason" => ?e
                        )
                    });
            },
        );
        let scheduler = scheduler_future.scheduler();
        let logger = info.logger().clone();
        let future = scheduler_future
            .map(|never| match never {})
            .map_err(move |e| error!(logger, "get next block scheduling failed"; "reason" => ?e));
        info.spawn(future);
        scheduler
    }
}

fn try_request_fragment_removal(
    tx_msg_box: &mut MessageBox<TransactionMsg>,
    fragment_ids: Vec<FragmentId>,
    header: &Header,
) -> Result<(), async_msg::TrySendError<TransactionMsg>> {
    let hash = header.hash().into();
    let date = header.block_date().clone().into();
    let status = FragmentStatus::InABlock { date, block: hash };
    tx_msg_box.try_send(TransactionMsg::RemoveTransactions(fragment_ids, status))
}

/// this function will re-process the tip against the different branches
/// this is because a branch may have become more interesting with time
/// moving forward and branches may have been dismissed
pub async fn reprocess_tip(logger: Logger, blockchain: Blockchain, tip: Tip) -> Result<(), Error> {
    let others = blockchain
        .branches()
        .branches::<Error>()
        .join(tip.get_ref::<Error>())
        .map(|(all, tip)| {
            all.into_iter()
                .filter(|r|
                    // remove our own tip so we don't apply it against itself
                    !Arc::ptr_eq(&r, &tip))
                .collect::<Vec<_>>()
        })
        .compat()
        .await?;

    for other in others.into_iter() {
        process_new_ref(logger.clone(), blockchain.clone(), tip.clone(), other).await?;
    }

    Ok(())
}

/// process a new candidate block on top of the blockchain, this function may:
///
/// * update the current tip if the candidate's parent is the current tip;
/// * update a branch if the candidate parent is that branch's tip;
/// * create a new branch if none of the above;
///
/// If the current tip is not the one being updated we will then trigger
/// chain selection after updating that other branch as it may be possible that
/// this branch just became more interesting for the current consensus algorithm.
pub async fn process_new_ref(
    logger: Logger,
    mut blockchain: Blockchain,
    mut tip: Tip,
    candidate: Arc<Ref>,
) -> Result<(), Error> {
    let candidate_hash = candidate.hash();
    let storage = blockchain.storage().clone();

    let tip_ref = tip.clone().get_ref::<Error>().compat().await?;

    let tip_updated = if tip_ref.hash() == candidate.block_parent_hash() {
        info!(logger, "update current branch tip");
        match tip.update_ref(candidate).compat().await {
            Ok(_) => true,
            Err(_) => unreachable!(),
        }
    } else {
        match chain_selection::compare_against(blockchain.storage(), &tip_ref, &candidate) {
            ComparisonResult::PreferCurrent => {
                info!(logger, "create new branch");
                false
            }
            ComparisonResult::PreferCandidate => {
                info!(logger, "switching to new candidate branch");
                let branch_res = blockchain
                    .branches_mut()
                    .apply_or_create(candidate)
                    .compat()
                    .await;

                match branch_res {
                    Ok(branch) => {
                        tip.swap(branch);
                        true
                    }
                    Err(_) => unreachable!(),
                }
            }
        }
    };

    if tip_updated {
        storage
            .put_tag(MAIN_BRANCH_TAG.to_owned(), candidate_hash)
            .await
            .map_err(|e| Error::with_chain(e, "Cannot update the main storage's tip"))
    } else {
        Ok(())
    }
}

async fn process_and_propagate_new_ref(
    logger: Logger,
    blockchain: Blockchain,
    tip: Tip,
    new_block_ref: Arc<Ref>,
    network_msg_box: MessageBox<NetworkMsg>,
) -> Result<(), Error> {
    process_new_ref(logger.clone(), blockchain, tip, new_block_ref.clone()).await?;

    debug!(logger, "propagating block to the network");
    let header = new_block_ref.header().clone();
    network_msg_box
        .send(NetworkMsg::Propagate(PropagateMsg::Block(header)))
        .compat()
        .await
        .map_err(|_| "Cannot propagate block to network".into())
        .map(|_| ())
}

pub async fn process_leadership_block(
    logger: Logger,
    blockchain: Blockchain,
    block: Block,
) -> Result<Arc<Ref>, Error> {
    let header = block.header();
    let parent_hash = block.parent_id();

    // This is a trusted block from the leadership task, so we can skip
    // pre-validation.
    let parent = blockchain.get_ref(parent_hash).await?;

    let post_checked = if let Some(parent_ref) = parent {
        debug!(logger, "processing block from leader event");
        blockchain
            .post_check_header(header, parent_ref)
            .compat()
            .await?
    } else {
        error!(
            logger,
            "block from leader event does not have parent block in storage"
        );
        return Err(ErrorKind::MissingParentBlock(parent_hash).into());
    };

    debug!(logger, "apply and store block");
    let new_ref = blockchain
        .apply_and_store_block(post_checked, block)
        .await
        .map_err(|err| Error::with_chain(err, "cannot process leadership block"))?;
    info!(logger, "block from leader event successfully stored");

    Ok(new_ref)
}

async fn process_block_announcement(
    blockchain: Blockchain,
    blockchain_tip: Tip,
    header: Header,
    node_id: NodeId,
    mut pull_headers_scheduler: PullHeadersScheduler,
    mut get_next_block_scheduler: GetNextBlockScheduler,
    logger: Logger,
) -> Result<(), Error> {
    let pre_checked = blockchain
        .pre_check_header(header, false)
        .await
        .map_err(|err| Error::with_chain(err, "cannot process block announcement"))?;
    match pre_checked {
        PreCheckedHeader::AlreadyPresent { .. } => {
            debug!(logger, "block is already present");
        }
        PreCheckedHeader::MissingParent { header, .. } => {
            debug!(logger, "block is missing a locally stored parent");
            let to = header.hash();
            let from = blockchain
                .get_checkpoints(blockchain_tip.branch().clone())
                .compat()
                .await
                .map_err(|err| Error::with_chain(err, "cannot process block announcement"))?;
            pull_headers_scheduler
                .schedule(to, node_id, from)
                .unwrap_or_else(move |err| {
                    error!(
                        logger,
                        "cannot schedule pulling headers"; "reason" => err.to_string()
                    )
                });
        }
        PreCheckedHeader::HeaderWithCache {
            header,
            parent_ref: _,
        } => {
            debug!(
                logger,
                "Announced block has a locally stored parent, fetch it"
            );
            get_next_block_scheduler
                .schedule(header.id(), node_id, ())
                .unwrap_or_else(move |err| {
                    error!(
                        logger,
                        "cannot schedule getting next block"; "reason" => err.to_string()
                    )
                });
        }
    }

    Ok(())
}

async fn process_network_block(
    blockchain: Blockchain,
    block: Block,
    tx_msg_box: MessageBox<TransactionMsg>,
    explorer_msg_box: Option<MessageBox<ExplorerMsg>>,
    mut get_next_block_scheduler: GetNextBlockScheduler,
    logger: Logger,
) -> Result<Option<Arc<Ref>>, chain::Error> {
    get_next_block_scheduler.declare_completed(block.id())
        .unwrap_or_else(|e| error!(logger, "get next block schedule completion failed"; "reason" => e.to_string()));
    let header = block.header();
    let pre_checked = blockchain.pre_check_header(header, false).await?;
    match pre_checked {
        PreCheckedHeader::AlreadyPresent { header, .. } => {
            debug!(
                logger,
                "block is already present";
                "hash" => %header.hash(),
                "parent" => %header.parent_id(),
                "date" => %header.block_date(),
            );
            Ok(None)
        }
        PreCheckedHeader::MissingParent { header, .. } => {
            let parent_hash = header.parent_id();
            debug!(
                logger,
                "block is missing a locally stored parent";
                "hash" => %header.hash(),
                "parent" => %parent_hash,
                "date" => %header.block_date(),
            );
            Err(ErrorKind::MissingParentBlock(parent_hash).into())
        }
        PreCheckedHeader::HeaderWithCache { parent_ref, .. } => check_and_apply_block(
            blockchain,
            parent_ref,
            block,
            tx_msg_box,
            explorer_msg_box,
            logger,
        )
        .await
        .map(Some),
    }
}

async fn check_and_apply_block(
    blockchain: Blockchain,
    parent_ref: Arc<Ref>,
    block: Block,
    tx_msg_box: MessageBox<TransactionMsg>,
    explorer_msg_box: Option<MessageBox<ExplorerMsg>>,
    logger: Logger,
) -> Result<Arc<Ref>, chain::Error> {
    let explorer_enabled = explorer_msg_box.is_some();
    let blockchain1 = blockchain.clone();
    let mut tx_msg_box = tx_msg_box.clone();
    let mut explorer_msg_box = explorer_msg_box.clone();
    let logger = logger.clone();
    let header = block.header();

    let post_checked = blockchain
        .post_check_header(header, parent_ref)
        .compat()
        .await?;
    let header = post_checked.header();
    debug!(
        logger,
        "applying block to storage";
        "hash" => %header.hash(),
        "parent" => %header.parent_id(),
        "date" => %header.block_date(),
    );

    let mut block_for_explorer = if explorer_enabled {
        Some(block.clone())
    } else {
        None
    };

    let fragment_ids = block.fragments().map(|f| f.id()).collect::<Vec<_>>();

    let block_ref = blockchain
        .apply_and_store_block(post_checked, block)
        .await?;

    try_request_fragment_removal(&mut tx_msg_box, fragment_ids, block_ref.header()).unwrap_or_else(
        |err| error!(logger, "cannot remove fragments from pool" ; "reason" => %err),
    );
    if let Some(msg_box) = explorer_msg_box.as_mut() {
        msg_box
            .try_send(ExplorerMsg::NewBlock(block_for_explorer.take().unwrap()))
            .unwrap_or_else(|err| error!(logger, "cannot add block to explorer: {}", err));
    }

    Ok(block_ref)
}

fn network_block_error_into_reply(err: chain::Error) -> intercom::Error {
    use super::chain::ErrorKind::*;

    match err.0 {
        Storage(e) => intercom::Error::failed(e),
        Ledger(e) => intercom::Error::failed_precondition(e),
        Block0(e) => intercom::Error::failed(e),
        MissingParentBlock(_) => intercom::Error::failed_precondition(err.to_string()),
        BlockHeaderVerificationFailed(_) => intercom::Error::invalid_argument(err.to_string()),
        _ => intercom::Error::failed(err.to_string()),
    }
}

fn chain_header_error_into_reply(err: candidate::Error) -> intercom::Error {
    use super::candidate::Error::*;

    // TODO: more detailed error case matching
    match err {
        Blockchain(e) => intercom::Error::failed(e.to_string()),
        EmptyHeaderStream => intercom::Error::invalid_argument(err.to_string()),
        MissingParentBlock(_) => intercom::Error::failed_precondition(err.to_string()),
        BrokenHeaderChain(_) => intercom::Error::invalid_argument(err.to_string()),
        HeaderChainVerificationFailed(e) => intercom::Error::invalid_argument(e),
        _ => intercom::Error::failed(err.to_string()),
    }
}
