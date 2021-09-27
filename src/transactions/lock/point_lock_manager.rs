use crate::db::key_types::RawUserKey;
use crate::transactions::lock::row_lock::RowLock;
use crate::transactions::lock::{LockManager, RowLockType};
use crate::transactions::transaction::Transaction;
use dashmap::DashMap;

pub struct PointLockManager {
    lock_map: DashMap<RawUserKey, RowLock>,
}

impl LockManager for PointLockManager {
    fn new() -> Self {
        PointLockManager {
            lock_map: DashMap::with_capacity(16),
        }
    }

    fn try_lock(&self, txn: &impl Transaction, key: &RawUserKey, lock_type: RowLockType) {
        match self.lock_map.get(key) {
            Some(entry) => {}
            None => {
                self.lock_map
                    .insert(key.clone(), RowLock::new(lock_type, txn.txn_id()));
            }
        }
    }

    fn unlock(&self, txn: &impl Transaction, key: &RawUserKey) {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
