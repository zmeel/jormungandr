/*

```text
          +------------+                     +------------+                    +------------+
          | Leadership |                     | Leadership |                    | Leadership |
          +-----+------+                     +-----+------+                    +-------+----+
                ^                                  ^                                   ^
                |                                  |                                   |
      +---------v-----^--------------+             +<------------+                +--->+--------+
      |               |              |             |             |                |             |
      |               |              |             |             |                |             |
   +--+--+         +--+--+        +--+--+       +--+--+       +--+--+          +--+--+       +--+--+
   | Ref +<--------+ Ref +<-------+ Ref +<--+---+ Ref +<------+ Ref +<---------+ Ref +<------+ Ref |
   +--+--+         +--+--+        +--+--+   ^   +--+--+       +--+--+          +---+-+       +---+-+
      |               |              |      |      |             |                 |             |
      v               v              v      |      v             v                 v             v
+-----+--+      +-----+--+       +---+----+ |   +--+-----+   +---+----+      +-----+--+       +--+-----+
| Ledger |      | Ledger |       | Ledger | |   | Ledger |   | Ledger |      | Ledger |       | Ledger |
+--------+      +--------+       +--------+ |   +--------+   +--------+      +--------+       +--------+
                                            |
                                            |
                                            |parent
                                            |hash
                                            |
                                            |         +----------+
                                            +---------+New header|
                                                      +----------+
```

When proposing a new header to the blockchain we are creating a new
potential fork on the blockchain. In the ideal case it will simply be
a new block on top of the _main_ current branch. We are adding blocks
after the other. In some cases it will also be a new branch, a fork.
We need to maintain some of them in order to be able to make an
informed choice when selecting the branch of consensus.

We are constructing a blockchain as we would on with git blocks:

* each block is represented by a [`Ref`];
* the [`Ref`] contains a reference to the associated `Ledger` state
  and associated `Leadership`.

A [`Branch`] contains a [`Ref`]. It allows us to follow and monitor
forks between different tasks of the blockchain module.

See Internal documentation for more details: doc/internal_design.md

[`Ref`]: ./struct.Ref.html
[`Branch`]: ./struct.Branch.html
*/

use super::{branch::Branches, reference_cache::RefCache};
use crate::{
    blockcfg::{
        Block, Block0Error, BlockDate, ChainLength, Epoch, EpochRewardsInfo, Header, HeaderHash,
        Leadership, Ledger, LedgerParameters, RewardsInfoParameters,
    },
    blockchain::{Branch, Checkpoints, Multiverse, Ref, Storage},
    start_up::NodeStorage,
};
use chain_impl_mockchain::{leadership::Verification, ledger};
use chain_storage::error::Error as StorageError;
use chain_time::TimeFrame;
use futures_03::stream::{Stream as Stream03, StreamExt as StreamExt03};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::prelude::*;
use tokio_compat::prelude::*;

// derive
use thiserror::Error;

error_chain! {
    foreign_links {
        Storage(StorageError);
        Ledger(ledger::Error);
        Block0(Block0Error);
    }

    errors {
        Block0InitialLedgerError {
            description("Error while creating the initial ledger out of the block0")
        }

        Block0AlreadyInStorage {
            description("Block0 already exists in the storage")
        }

        Block0NotAlreadyInStorage {
            description("Block0 is not yet in the storage")
        }

        MissingParentBlock(hash: HeaderHash) {
            description("missing a parent block from the storage"),
            display(
                "Missing a block from the storage. The node was recovering \
                 the blockchain and the parent block '{}' was not \
                 already stored",
                hash,
            ),
        }

        NoTag (tag: String) {
            description("Missing tag from the storage"),
            display("Tag '{}' not found in the storage", tag),
        }

        BlockHeaderVerificationFailed (reason: String) {
            description("Block header verification failed"),
            display("The block header verification failed: {}", reason),
        }

        BlockNotRequested (hash: HeaderHash) {
            description("Received an unknown block"),
            display("Received block {} is not known from previously received headers", hash)
        }

        CannotApplyBlock {
            description("Block cannot be applied on top of the previous block's ledger state"),
        }
    }
}

#[derive(Error, Debug)]
pub enum HeaderChainVerifyError {
    #[error("date is set before parent; new block: {child}, parent: {parent}")]
    BlockDateBeforeParent { child: BlockDate, parent: BlockDate },
    #[error("chain length is not incrementally increasing; new block: {child}, parent: {parent}")]
    ChainLengthNotIncremental {
        child: ChainLength,
        parent: ChainLength,
    },
}

pub const MAIN_BRANCH_TAG: &str = "HEAD";

/// Performs lightweight sanity checks on information fields of a block header
/// against those in the header of the block's parent.
/// The `parent` header must have been retrieved based on, or otherwise
/// matched to, the parent block hash of `header`.
///
/// # Panics
///
/// If the parent hash in the header does not match that of the parent,
/// this function may panic.
pub fn pre_verify_link(
    header: &Header,
    parent: &Header,
) -> ::std::result::Result<(), HeaderChainVerifyError> {
    use chain_core::property::ChainLength as _;

    debug_assert_eq!(header.block_parent_hash(), parent.hash());

    if header.block_date() <= parent.block_date() {
        return Err(HeaderChainVerifyError::BlockDateBeforeParent {
            child: header.block_date(),
            parent: parent.block_date(),
        });
    }
    if header.chain_length() != parent.chain_length().next() {
        return Err(HeaderChainVerifyError::ChainLengthNotIncremental {
            child: header.chain_length(),
            parent: parent.chain_length(),
        });
    }
    Ok(())
}

/// blockchain object, can be safely shared across multiple threads. However it is better not
/// to as some operations may require a mutex.
///
/// This objects holds a reference to the persistent storage. It also holds 2 different caching
/// of objects:
///
/// * `RefCache`: a cache of blocks headers and associated states;
/// * `Multiverse`: of ledger. It is a cache of different ledger states.
///
#[derive(Clone)]
pub struct Blockchain {
    branches: Branches,

    ref_cache: RefCache,

    ledgers: Multiverse<Arc<Ledger>>,

    storage: Storage,

    block0: HeaderHash,
}

pub enum PreCheckedHeader {
    /// result when the given header is already present in the
    /// local storage. The embedded `cached_reference` gives us
    /// the local cached reference is the header is already loaded
    /// in memory
    AlreadyPresent {
        /// return the Header so we can avoid doing clone
        /// of the data all the time
        header: Header,
        /// the cached reference if it was already cached.
        /// * `None` means the associated block is already in storage
        ///   but not already in the cache;
        /// * `Some(ref)` returns the local cached Ref of the block
        cached_reference: Option<Arc<Ref>>,
    },

    /// the parent is missing from the local storage
    MissingParent {
        /// return back the Header so we can avoid doing a clone
        /// of the data all the time
        header: Header,
    },

    /// The parent's is already present in the local storage and
    /// is loaded in the local cache
    HeaderWithCache {
        /// return back the Header so we can avoid doing a clone
        /// of the data all the time
        header: Header,

        /// return the locally stored parent's Ref. Already cached in memory
        /// for future processing
        parent_ref: Arc<Ref>,
    },
}

pub struct PostCheckedHeader {
    header: Header,
    epoch_leadership_schedule: Arc<Leadership>,
    epoch_ledger_parameters: Arc<LedgerParameters>,
    parent_ledger_state: Arc<Ledger>,
    time_frame: Arc<TimeFrame>,
    previous_epoch_state: Option<Arc<Ref>>,
}

impl PostCheckedHeader {
    pub fn header(&self) -> &Header {
        &self.header
    }
}

impl Blockchain {
    pub async fn new(block0: HeaderHash, storage: NodeStorage, ref_cache_ttl: Duration) -> Self {
        Blockchain {
            branches: Branches::new(),
            ref_cache: RefCache::new(ref_cache_ttl),
            ledgers: Multiverse::new(),
            storage: Storage::new(storage).await,
            block0,
        }
    }

    pub fn block0(&self) -> &HeaderHash {
        &self.block0
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub fn branches(&self) -> &Branches {
        &self.branches
    }

    pub fn branches_mut(&mut self) -> &mut Branches {
        &mut self.branches
    }

    /// create and store a reference of this leader to the new
    fn create_and_store_reference(
        &self,
        header_hash: HeaderHash,
        header: Header,
        ledger: Arc<Ledger>,
        time_frame: Arc<TimeFrame>,
        leadership: Arc<Leadership>,
        ledger_parameters: Arc<LedgerParameters>,
        previous_epoch_state: Option<Arc<Ref>>,
    ) -> impl Future<Item = Arc<Ref>, Error = Infallible> {
        let chain_length = header.chain_length();

        let multiverse = self.ledgers.clone();
        let ref_cache = self.ref_cache.clone();

        multiverse
            .insert(chain_length, header_hash, ledger.clone())
            .and_then(move |ledger_gcroot| {
                let reference = Ref::new(
                    ledger_gcroot,
                    ledger,
                    time_frame,
                    leadership,
                    ledger_parameters,
                    header,
                    previous_epoch_state,
                );
                let reference = Arc::new(reference);
                ref_cache
                    .insert(header_hash, Arc::clone(&reference))
                    .map(|()| reference)
            })
    }

    /// get `Ref` of the given header hash
    ///
    /// once the `Ref` is in hand, it means we have the Leadership schedule associated
    /// to this block and the `Ledger` state after this block.
    ///
    /// If the future returns `None` it means we don't know about this block locally
    /// and it might be necessary to contacts the network to retrieve a missing
    /// branch
    ///
    /// TODO: the case where the block is in storage but not yet in the cache
    ///       is not implemented
    pub async fn get_ref(&self, header_hash: HeaderHash) -> Result<Option<Arc<Ref>>> {
        let maybe_ref = self
            .ref_cache
            .get(header_hash.clone())
            .compat()
            .await
            .unwrap();

        if maybe_ref.is_none() {
            let block_exists =
                self.storage.block_exists(header_hash).await.map_err(|e| {
                    Error::with_chain(e, "cannot check if the block is in the storage")
                })?;
            // TODO: we have the block in the storage but it is missing from the
            // state management. Force the node to fall through reloading the
            // blocks from the storage to allow fast from storage reload.
            Ok(None)
        } else {
            Ok(maybe_ref)
        }
    }

    /// load the header's parent `Ref`.
    async fn load_header_parent(&self, header: Header, force: bool) -> Result<PreCheckedHeader> {
        let block_id = header.hash();
        let parent_block_id = header.block_parent_hash().clone();

        let maybe_self_ref = if force {
            None
        } else {
            self.get_ref(block_id.clone()).await?
        };

        let maybe_parent_ref = self.get_ref(parent_block_id).await?;

        if let Some(self_ref) = maybe_self_ref {
            Ok(PreCheckedHeader::AlreadyPresent {
                header,
                cached_reference: Some(self_ref),
            })
        } else {
            if let Some(parent_ref) = maybe_parent_ref {
                Ok(PreCheckedHeader::HeaderWithCache { header, parent_ref })
            } else {
                Ok(PreCheckedHeader::MissingParent { header })
            }
        }
    }

    /// load the header's parent and perform some simple verification:
    ///
    /// * check the block_date is increasing
    /// * check the chain_length is monotonically increasing
    ///
    /// At the end of this future we know either of:
    ///
    /// * the block is already present (nothing to do);
    /// * the block's parent is already present (we can then continue validation)
    /// * the block's parent is missing: we need to download it and call again
    ///   this function.
    ///
    pub async fn pre_check_header(&self, header: Header, force: bool) -> Result<PreCheckedHeader> {
        let pre_check = self.load_header_parent(header, force).await?;
        match &pre_check {
            PreCheckedHeader::HeaderWithCache {
                ref header,
                ref parent_ref,
            } => pre_verify_link(header, parent_ref.header())
                .map(|()| pre_check)
                .map_err(|e| ErrorKind::BlockHeaderVerificationFailed(e.to_string()).into()),
            _ => Ok(pre_check),
        }
    }

    /// check the header cryptographic properties and leadership's schedule
    ///
    /// on success returns the PostCheckedHeader:
    ///
    /// * the header,
    /// * the ledger state associated to the parent block
    /// * the leadership schedule associated to the header
    pub fn post_check_header(
        &self,
        header: Header,
        parent: Arc<Ref>,
    ) -> impl Future<Item = PostCheckedHeader, Error = Error> {
        let current_date = header.block_date();

        let (
            parent_ledger_state,
            epoch_leadership_schedule,
            epoch_ledger_parameters,
            time_frame,
            previous_epoch_state,
        ) = new_epoch_leadership_from(current_date.epoch, parent);

        match epoch_leadership_schedule.verify(&header) {
            Verification::Success => future::ok(PostCheckedHeader {
                header,
                epoch_leadership_schedule,
                epoch_ledger_parameters,
                parent_ledger_state,
                time_frame,
                previous_epoch_state,
            }),
            Verification::Failure(error) => {
                future::err(ErrorKind::BlockHeaderVerificationFailed(error.to_string()).into())
            }
        }
    }

    fn apply_block(
        &self,
        post_checked_header: PostCheckedHeader,
        block: &Block,
    ) -> impl Future<Item = Arc<Ref>, Error = Error> {
        let header = post_checked_header.header;
        let block_id = header.hash();
        let epoch_leadership_schedule = post_checked_header.epoch_leadership_schedule;
        let epoch_ledger_parameters = post_checked_header.epoch_ledger_parameters;
        let ledger = post_checked_header.parent_ledger_state;
        let time_frame = post_checked_header.time_frame;
        let previous_epoch_state = post_checked_header.previous_epoch_state;

        debug_assert!(block.header.hash() == block_id);

        let metadata = header.to_content_eval_context();

        let self1 = self.clone();

        future::result(
            ledger
                .apply_block(&epoch_ledger_parameters, &block.contents, &metadata)
                .chain_err(|| ErrorKind::CannotApplyBlock),
        )
        .and_then(move |new_ledger| {
            self1
                .create_and_store_reference(
                    block_id,
                    header,
                    Arc::new(new_ledger),
                    time_frame,
                    epoch_leadership_schedule,
                    epoch_ledger_parameters,
                    previous_epoch_state,
                )
                .map_err(|_: Infallible| unreachable!())
        })
    }

    /// Apply the block on the blockchain from a post checked header
    /// and add it to the storage.
    pub async fn apply_and_store_block(
        &self,
        post_checked_header: PostCheckedHeader,
        block: Block,
    ) -> Result<Arc<Ref>> {
        let block_ref = self
            .apply_block(post_checked_header, &block)
            .compat()
            .await?;
        match self.storage.put_block(block).await {
            Ok(()) => Ok(block_ref),
            Err(StorageError::BlockAlreadyPresent) => Ok(block_ref),
            Err(e) => Err(e.into()),
        }
    }

    /// Apply the given block0 in the blockchain (updating the RefCache and the other objects)
    ///
    /// This function returns the created block0 branch. Having it will
    /// avoid searching for it in the blockchain's `branches` and perform
    /// operations to update the branch as we move along already.
    ///
    /// # Errors
    ///
    /// The resulted future may fail if
    ///
    /// * the block0 does build an invalid `Ledger`: `ErrorKind::Block0InitialLedgerError`;
    ///
    fn apply_block0(&self, block0: Block) -> impl Future<Item = Branch, Error = Error> {
        let block0_header = block0.header.clone();
        let block0_id = block0_header.hash();
        let block0_id_1 = block0_header.hash();
        let block0_date = block0_header.block_date().clone();

        let self1 = self.clone();
        let mut branches = self.branches.clone();

        let time_frame = {
            use crate::blockcfg::Block0DataSource as _;

            let start_time = block0
                .start_time()
                .map_err(|err| Error::with_chain(err, ErrorKind::Block0InitialLedgerError));
            let slot_duration = block0
                .slot_duration()
                .map_err(|err| Error::with_chain(err, ErrorKind::Block0InitialLedgerError));

            future::result(start_time.and_then(|start_time| {
                slot_duration.map(|slot_duration| {
                    TimeFrame::new(
                        chain_time::Timeline::new(start_time),
                        chain_time::SlotDuration::from_secs(slot_duration.as_secs() as u32),
                    )
                })
            }))
        };

        // we lift the creation of the ledger in the future type
        // this allow chaining of the operation and lifting the error handling
        // in the same place
        Ledger::new(block0_id_1, block0.contents.iter())
            .map(future::ok)
            .map_err(|err| Error::with_chain(err, ErrorKind::Block0InitialLedgerError))
            .unwrap_or_else(future::err)
            .map(move |block0_ledger| {
                let block0_leadership = Leadership::new(block0_date.epoch, &block0_ledger);
                (block0_ledger, block0_leadership)
            })
            .and_then(move |(block0_ledger, block0_leadership)| {
                time_frame.map(|time_frame| (block0_ledger, block0_leadership, time_frame))
            })
            .and_then(move |(block0_ledger, block0_leadership, time_frame)| {
                let ledger_parameters = block0_leadership.ledger_parameters().clone();

                self1
                    .create_and_store_reference(
                        block0_id,
                        block0_header,
                        Arc::new(block0_ledger),
                        Arc::new(time_frame),
                        Arc::new(block0_leadership),
                        Arc::new(ledger_parameters),
                        None,
                    )
                    .map_err(|_: Infallible| unreachable!())
            })
            .map(Branch::new)
            .and_then(move |branch| {
                branches
                    .add(branch.clone())
                    .map(|()| branch)
                    .map_err(|_: Infallible| unreachable!())
            })
    }

    /// function to do the initial application of the block0 in the `Blockchain` and its
    /// storage. We assume `Block0` is not already in the `NodeStorage`.
    ///
    /// This function returns the create block0 branch. Having it will
    /// avoid searching for it in the blockchain's `branches` and perform
    /// operations to update the branch as we move along already.
    ///
    /// # Errors
    ///
    /// The resulted future may fail if
    ///
    /// * the block0 already exists in the storage: `ErrorKind::Block0AlreadyInStorage`;
    /// * the block0 does build a valid `Ledger`: `ErrorKind::Block0InitialLedgerError`;
    /// * other errors while interacting with the storage (IO errors)
    ///
    pub async fn load_from_block0(&self, block0: Block) -> Result<Branch> {
        let block0_id = block0.header.hash();

        let existence = self
            .storage
            .block_exists(block0_id.clone())
            .await
            .map_err(|e| Error::with_chain(e, "Cannot check if block0 is in storage"))?;
        if existence {
            return Err(ErrorKind::Block0AlreadyInStorage.into());
        }

        let block0_branch = self.apply_block0(block0.clone()).compat().await?;

        self.storage
            .put_block(block0)
            .await
            .map_err(|e| Error::with_chain(e, "Cannot put block0 in storage"))?;

        self.storage
            .put_tag(MAIN_BRANCH_TAG.to_owned(), block0_id)
            .await
            .map_err(|e| Error::with_chain(e, "Cannot put block0's hash in the HEAD tag"))?;

        Ok(block0_branch)
    }

    /// returns a future that will propagate the initial states and leadership
    /// from the block0 to the `Head` of the storage (the last known block which
    /// made consensus).
    ///
    /// The Future will returns a branch pointing to the `Head`.
    ///
    /// # Errors
    ///
    /// The resulted future may fail if
    ///
    /// * the block0 is not already in the storage: `ErrorKind::Block0NotAlreadyInStorage`;
    /// * the block0 does build a valid `Ledger`: `ErrorKind::Block0InitialLedgerError`;
    /// * other errors while interacting with the storage (IO errors)
    ///
    pub async fn load_from_storage(&self, block0: Block) -> Result<Branch> {
        use futures_03::prelude::*;

        let block0_header = block0.header.clone();
        let block0_id = block0_header.hash();
        let block0_id_2 = block0_id.clone();

        let existence = self
            .storage
            .block_exists(block0_id.clone())
            .await
            .map_err(|e| Error::with_chain(e, "Cannot check if block0 is in storage"))?;
        if !existence {
            return Err(ErrorKind::Block0NotAlreadyInStorage.into());
        }

        let head_hash = self
            .storage
            .get_tag(MAIN_BRANCH_TAG.to_owned())
            .await
            .map_err(|e| Error::with_chain(e, "Cannot get hash of the HEAD tag"))?
            .ok_or::<Error>(ErrorKind::NoTag(MAIN_BRANCH_TAG.to_owned()).into())?;

        let branch = self.apply_block0(block0).compat().await?;

        let block_stream = self
            .storage
            .stream_from_to(block0_id_2, head_hash)
            .await
            .map_err(|e| Error::with_chain(e, "Cannot iterate blocks from block0 to HEAD"))?;
        
        // TODO process stream results

        Ok(branch)
    }

    pub fn get_checkpoints(
        &self,
        branch: Branch,
    ) -> impl Future<Item = Checkpoints, Error = Error> {
        branch.get_ref().map(Checkpoints::new_from)
    }
}

fn write_reward_info(
    epoch: Epoch,
    parent_hash: HeaderHash,
    rewards_info: EpochRewardsInfo,
) -> std::io::Result<()> {
    use std::{env::var, fs::rename, fs::File, io::BufWriter, path::PathBuf};

    if let Ok(directory) = var("JORMUNGANDR_REWARD_DUMP_DIRECTORY") {
        let directory = PathBuf::from(directory);

        std::fs::create_dir_all(&directory)?;

        let filepath = format!("reward-info-{}-{}", epoch, parent_hash);
        let filepath = directory.join(filepath);
        let filepath_tmp = format!("tmp.reward-info-{}-{}", epoch, parent_hash);
        let filepath_tmp = directory.join(filepath_tmp);

        {
            let file = File::create(&filepath_tmp)?;
            let mut buf = BufWriter::new(file);
            write!(&mut buf, "type,identifier,received,distributed\r\n")?;
            write!(&mut buf, "drawn,,,{}\r\n", rewards_info.drawn.0)?;
            write!(&mut buf, "fees,,,{}\r\n", rewards_info.fees.0)?;
            write!(&mut buf, "treasury,,{},\r\n", rewards_info.treasury.0)?;

            for (pool_id, (taxed, distr)) in rewards_info.stake_pools.iter() {
                write!(&mut buf, "pool,{},{},{}\r\n", pool_id, taxed.0, distr.0)?;
            }

            for (account_id, received) in rewards_info.accounts.iter() {
                write!(&mut buf, "account,{},{},\r\n", account_id, received.0)?;
            }

            buf.flush()?;
        }

        rename(filepath_tmp, filepath)?;
    }
    Ok(())
}

pub fn new_epoch_leadership_from(
    epoch: Epoch,
    parent: Arc<Ref>,
) -> (
    Arc<Ledger>,
    Arc<Leadership>,
    Arc<LedgerParameters>,
    Arc<TimeFrame>,
    Option<Arc<Ref>>,
) {
    let parent_ledger_state = parent.ledger().clone();
    let parent_epoch_leadership_schedule = parent.epoch_leadership_schedule().clone();
    let parent_epoch_ledger_parameters = parent.epoch_ledger_parameters().clone();
    let parent_time_frame = parent.time_frame().clone();

    let parent_date = parent.block_date();

    if parent_date.epoch < epoch {
        // TODO: the time frame may change in the future, we will need to handle this
        //       special case but it is not actually clear how to modify the time frame
        //       for the blockchain
        use chain_impl_mockchain::block::ConsensusVersion;

        // 1. distribute the rewards (if any) This will give us the transition state
        let transition_state =
            if let Some(distribution) = parent.epoch_leadership_schedule().stake_distribution() {
                let store_rewards = std::env::var("JORMUNGANDR_REWARD_DUMP_DIRECTORY").is_ok();
                let reward_info_dist = if store_rewards {
                    RewardsInfoParameters::report_all()
                } else {
                    RewardsInfoParameters::default()
                };

                let (ledger, rewards_info) = parent_ledger_state
                    .distribute_rewards(
                        distribution,
                        &parent.epoch_ledger_parameters(),
                        reward_info_dist,
                    )
                    .expect("Distribution of rewards will not overflow");
                if let Err(err) = write_reward_info(epoch, parent.hash(), rewards_info) {
                    panic!("Error while storing the reward dump, err {}", err)
                }
                Arc::new(ledger)
            } else {
                parent_ledger_state.clone()
            };

        // 2. now that the rewards have been distributed, prepare the schedule
        //    for the next leader
        let epoch_state = if transition_state.consensus_version() == ConsensusVersion::GenesisPraos
        {
            // if there is no parent state available this might be because it is not
            // available in memory or it is the epoch0 or epoch1
            parent
                .last_ref_previous_epoch()
                .map(|r| r.ledger().clone())
                .unwrap_or(parent_ledger_state.clone())
        } else {
            transition_state.clone()
        };

        let leadership = Arc::new(Leadership::new(epoch, &epoch_state));
        let ledger_parameters = Arc::new(leadership.ledger_parameters().clone());
        let previous_epoch_state = Some(parent);
        (
            transition_state,
            leadership,
            ledger_parameters,
            parent_time_frame,
            previous_epoch_state,
        )
    } else {
        (
            parent_ledger_state,
            parent_epoch_leadership_schedule,
            parent_epoch_ledger_parameters,
            parent_time_frame,
            parent.last_ref_previous_epoch().map(Arc::clone),
        )
    }
}
