pub(crate) mod point_lock_manager;
pub(crate) mod row_lock;

use super::transaction::Transaction;
use crate::db::key_types::RawUserKey;
use std::time::Duration;

pub enum RowLockType {
    Exclusive = 0,
    Shared = 1,
}

impl Default for RowLockType {
    fn default() -> Self {
        RowLockType::Exclusive
    }
}

pub(crate) const LOCK_TIMEOUT: Duration = Duration::from_millis(10);

pub trait LockManager {
    fn new() -> Self;
    fn try_lock(&self, txn: &impl Transaction, key: &RawUserKey, lock_type: RowLockType);
    fn unlock(&self, txn: &impl Transaction, key: &RawUserKey);
}
