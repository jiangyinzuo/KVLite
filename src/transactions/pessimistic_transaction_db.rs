use crate::collections::skip_list::skipmap::SrSwSkipMap;
use crate::db::key_types::{DBKey, RawUserKey};
use crate::db::options::WriteOptions;
use crate::db::{Value, DB};
use crate::memory::MemTable;
use crate::transactions::lock::{LockManager, RowLockType};
use crate::transactions::transaction::Transaction;
use crate::transactions::TransactionDB;
use std::path::Path;

pub struct PessimisticTransactionDB<LM: LockManager> {
    lock_manager: LM,
}

impl<SK: DBKey, UK: DBKey, M: MemTable<SK, UK>, LM: LockManager> DB<SK, UK, M>
    for PessimisticTransactionDB<LM>
{
    fn open(db_path: impl AsRef<Path>) -> crate::Result<Self> {
        let lock_manager = LM::new();
        let pessimistic_db = PessimisticTransactionDB { lock_manager };
        todo!()
    }

    fn get(&self, key: &SK) -> crate::Result<Option<Value>> {
        todo!()
    }

    fn set(&self, write_options: &WriteOptions, key: SK, value: Value) -> crate::Result<()> {
        todo!()
    }

    fn remove(&self, write_options: &WriteOptions, key: SK) -> crate::Result<()> {
        todo!()
    }

    fn range_get(&self, key_start: &SK, key_end: &SK) -> crate::Result<SrSwSkipMap<UK, Value>>
    where
        UK: From<SK>,
    {
        todo!()
    }

    fn db_path(&self) -> &String {
        todo!()
    }
}

impl<SK: DBKey, UK: DBKey, M: MemTable<SK, UK>, LM: LockManager> TransactionDB<SK, UK, M>
    for PessimisticTransactionDB<LM>
{
}

impl<LM: LockManager> PessimisticTransactionDB<LM> {
    pub fn try_lock(&self, txn: &impl Transaction, key: &RawUserKey, lock_type: RowLockType) {
        self.lock_manager.try_lock(txn, key, lock_type);
    }
}
