use crate::blockcfg::{Block, Header, HeaderHash};
use crate::blockchain::{Storage, Tip};
use crate::intercom::{ClientMsg, Error, ReplySendError, ReplyStreamHandle};
use crate::utils::task::{Input, TokioServiceInfo};
use chain_core::property::HasHeader;

use futures_03::stream::{Stream as Stream03, StreamExt as _, TryStreamExt as _};
use tokio::prelude::*;
use tokio::timer::Timeout;
use tokio_compat::prelude::*;

use std::time::Duration;

const PROCESS_TIMEOUT_GET_BLOCK_TIP: u64 = 5;
const PROCESS_TIMEOUT_GET_HEADERS: u64 = 5 * 60;
const PROCESS_TIMEOUT_GET_HEADERS_RANGE: u64 = 5 * 60;
const PROCESS_TIMEOUT_GET_BLOCKS: u64 = 10 * 60;
const PROCESS_TIMEOUT_PULL_BLOCKS_TO_TIP: u64 = 60 * 60;

pub struct TaskData {
    pub storage: Storage,
    pub blockchain_tip: Tip,
}

pub fn handle_input(
    info: &TokioServiceInfo,
    task_data: &mut TaskData,
    input: Input<ClientMsg>,
) -> Result<(), ()> {
    let cquery = match input {
        Input::Shutdown => return Ok(()),
        Input::Input(msg) => msg,
    };

    match cquery {
        ClientMsg::GetBlockTip(handle) => {
            let fut = handle.async_reply(get_block_tip(&task_data.blockchain_tip));
            let logger = info.logger().new(o!("request" => "GetBlockTip"));
            info.spawn(
                Timeout::new(fut, Duration::from_secs(PROCESS_TIMEOUT_GET_BLOCK_TIP)).map_err(
                    move |e| {
                        error!(
                            logger,
                            "request timed out or failed unexpectedly";
                            "error" => ?e,
                        );
                    },
                ),
            );
        }
        ClientMsg::GetHeaders(ids, handle) => {
            let fut = handle.async_reply(get_headers(task_data.storage.clone(), ids));
            let logger = info.logger().new(o!("request" => "GetHeaders"));
            info.spawn(
                Timeout::new(fut, Duration::from_secs(PROCESS_TIMEOUT_GET_HEADERS)).map_err(
                    move |e| {
                        warn!(
                            logger,
                            "request timed out or failed unexpectedly";
                            "error" => ?e,
                        );
                    },
                ),
            );
        }
        ClientMsg::GetHeadersRange(checkpoints, to, handle) => {
            let fut = handle_get_headers_range(task_data, checkpoints, to, handle);
            let logger = info.logger().new(o!("request" => "GetHeadersRange"));
            info.spawn(
                Timeout::new(fut, Duration::from_secs(PROCESS_TIMEOUT_GET_HEADERS_RANGE)).map_err(
                    move |e| {
                        warn!(
                            logger,
                            "request timed out or failed unexpectedly";
                            "error" => ?e,
                        );
                    },
                ),
            );
        }
        ClientMsg::GetBlocks(ids, handle) => {
            let fut = handle.async_reply(get_blocks(task_data.storage.clone(), ids));
            let logger = info.logger().new(o!("request" => "GetBlocks"));
            info.spawn(
                Timeout::new(fut, Duration::from_secs(PROCESS_TIMEOUT_GET_BLOCKS)).map_err(
                    move |e| {
                        warn!(
                            logger,
                            "request timed out or failed unexpectedly";
                            "error" => ?e,
                        );
                    },
                ),
            );
        }
        ClientMsg::PullBlocksToTip(from, handle) => {
            let fut = handle_pull_blocks_to_tip(task_data, from, handle);
            let logger = info.logger().new(o!("request" => "PullBlocksToTip"));
            info.spawn(
                Timeout::new(fut, Duration::from_secs(PROCESS_TIMEOUT_PULL_BLOCKS_TO_TIP)).map_err(
                    move |e| {
                        warn!(
                            logger,
                            "request timed out or failed unexpectedly";
                            "error" => ?e,
                        );
                    },
                ),
            );
        }
    }
    Ok(())
}

fn get_block_tip(blockchain_tip: &Tip) -> impl Future<Item = Header, Error = Error> {
    blockchain_tip
        .get_ref()
        .and_then(|tip| Ok(tip.header().clone()))
}

async fn handle_get_headers_range(
    task_data: &TaskData,
    checkpoints: Vec<HeaderHash>,
    to: HeaderHash,
    handle: ReplyStreamHandle<Header>,
) -> Result<(), ()> {
    use futures_03::FutureExt;

    let storage = task_data.storage.clone();
    match storage.find_closest_ancestor(checkpoints, to).await {
        Ok(maybe_ancestor) => {
            let depth = maybe_ancestor.map(|ancestor| ancestor.distance);
            let sink = handle
                .with(|res: Result<Block, Error>| -> Result<_, ReplySendError> {
                    Ok(res.map(|block| block.header()))
                })
                .sink_compat();
            storage.send_branch(to, depth, Box::pin(sink)).await;
            Ok(())
        }
        Err(e) => handle.async_error(e.into()).compat().await,
    }
}

fn get_blocks(
    storage: Storage,
    ids: Vec<HeaderHash>,
) -> impl Stream03<Item = Result<Block, Error>> {
    futures_03::stream::iter(ids).then(|id| {
        async move {
            match storage.get(id).await? {
                Some(block) => Ok(block),
                None => Err(Error::not_found(format!(
                    "block {} is not known to this node",
                    id
                ))),
            }
        }
    })
}

fn get_headers(
    storage: Storage,
    ids: Vec<HeaderHash>,
) -> impl Stream03<Item = Result<Header, Error>> {
    get_blocks(storage, ids).map_ok(|block| block.header())
}

async fn handle_pull_blocks_to_tip(
    task_data: &TaskData,
    checkpoints: Vec<HeaderHash>,
    handle: ReplyStreamHandle<Block>,
) -> Result<(), ()> {
    use crate::blockchain::StorageError;

    let res = task_data
        .blockchain_tip
        .get_ref::<StorageError>()
        .compat()
        .await;

    match res {
        Ok(tip) => {
            let res = task_data
                .storage
                .find_closest_ancestor(checkpoints, tip.hash())
                .await;
            match res {
                Ok(maybe_ancestor) => {
                    let depth = maybe_ancestor.map(|ancestor| ancestor.distance);
                    task_data
                        .storage
                        .send_branch(tip.hash(), depth, Box::pin(handle.sink_compat()))
                        .await
                        .map_err(|_| ())
                }
                Err(e) => handle.async_error(e.into()).compat().await,
            }
        }
        Err(e) => handle.async_error(e.into()).compat().await,
    }
}
