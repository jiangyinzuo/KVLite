use crate::transactions::lock::RowLockType;
use crate::transactions::TransactionID;
use std::sync::{Condvar, Mutex};

#[derive(Default)]
pub struct RowLock {
    pub inner: Mutex<RowLockInner>,
}

#[derive(Default)]
pub struct RowLockInner {
    pub ty: RowLockType,
    pub owner: TransactionID,
}

impl RowLock {
    pub fn new(ty: RowLockType, txn_id: TransactionID) -> RowLock {
        RowLock {
            inner: Mutex::new(RowLockInner { ty, owner: txn_id }),
        }
    }
}
