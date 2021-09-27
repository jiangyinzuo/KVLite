use crate::db::key_types::RawUserKey;
use crate::transactions::lock::point_lock_manager::PointLockManager;
use crate::transactions::lock::RowLockType;
use crate::transactions::pessimistic_transaction_db::PessimisticTransactionDB;
use crate::transactions::TransactionID;
use std::sync::Arc;

pub trait Transaction {
    fn txn_id(&self) -> TransactionID;
    fn try_lock(&self, key: &RawUserKey, lock_type: RowLockType);
}

pub struct PessimisticTransaction {
    txn_id: TransactionID,
    txn_db: Arc<PessimisticTransactionDB<PointLockManager>>,
}

impl Transaction for PessimisticTransaction {
    #[inline]
    fn txn_id(&self) -> TransactionID {
        self.txn_id
    }

    fn try_lock(&self, key: &RawUserKey, lock_type: RowLockType) {
        self.txn_db.try_lock(self, key, lock_type);
        todo!()
    }
}
