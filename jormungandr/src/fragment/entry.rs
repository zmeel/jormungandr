use crate::{
    blockcfg::{Value, ValueError},
    fragment::{Fragment, FragmentId},
};
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum EntryError {
    #[error("Error computing the balance: {source}")]
    ErrorComputingBalance {
        #[source]
        #[from]
        source: ValueError,
    },

    #[error("Initial Fragments are not accepted outside the block0")]
    InitialRejected,

    #[error("Old UTxO Declaration Fragments are not accepted outside of the block0")]
    OldUTxORejected,

    #[error("Transaction has negative balance: {balance}")]
    NegativeBalance { balance: Value },
}

pub struct PoolEntry {
    // reference of the fragment stored in the pool
    fragment_ref: FragmentId,
    /// fee of the fragment, does not include the fee of
    /// descendants entries or ancestors
    fragment_fee: Value,
    /// size of the fragment in the memory pool
    fragment_size: usize,
    /// time when the entry was added to the pool
    received_at: SystemTime,
}

fn estimate_fee(fragment: &Fragment) -> Result<Value, EntryError> {
    use chain_impl_mockchain::transaction::Balance;
    let balance = match fragment {
        Fragment::Initial(_) => return Err(EntryError::InitialRejected),
        Fragment::OldUtxoDeclaration(_) => return Err(EntryError::OldUTxORejected),
        Fragment::UpdateProposal(_) => unimplemented!(),
        Fragment::UpdateVote(_) => unimplemented!(),

        Fragment::Transaction(tx) => tx.balance(Value::zero())?,
        Fragment::OwnerStakeDelegation(tx) => tx.balance(Value::zero())?,
        Fragment::StakeDelegation(tx) => tx.balance(Value::zero())?,
        Fragment::PoolRegistration(tx) => tx.balance(Value::zero())?,
        Fragment::PoolRetirement(tx) => tx.balance(Value::zero())?,
        Fragment::PoolUpdate(tx) => tx.balance(Value::zero())?,
    };

    match balance {
        Balance::Zero => Ok(Value::zero()),
        Balance::Positive(v) => Ok(v),
        Balance::Negative(v) => Err(EntryError::NegativeBalance { balance: v }),
    }
}

impl PoolEntry {
    pub fn new(fragment: &Fragment) -> Result<Self, EntryError> {
        let fragment_fee = estimate_fee(fragment)?;
        let raw = fragment.to_raw();
        let fragment_size = raw.size_bytes_plus_size();
        let fragment_ref = raw.id();

        Ok(PoolEntry {
            fragment_ref: fragment_ref,
            fragment_fee: fragment_fee,
            fragment_size: fragment_size,
            received_at: SystemTime::now(),
        })
    }

    #[inline]
    pub fn fragment_ref(&self) -> &FragmentId {
        &self.fragment_ref
    }
    #[inline]
    pub fn fragment_fee(&self) -> &Value {
        &self.fragment_fee
    }
    #[inline]
    pub fn fragment_size(&self) -> &usize {
        &self.fragment_size
    }
    #[inline]
    pub fn received_at(&self) -> &SystemTime {
        &self.received_at
    }
}
