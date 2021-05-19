use std::path::Path;
use std::sync::Arc;

use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::UserKey;
use crate::db::no_transaction_db::NoTransactionDB;
use crate::db::{Value, DB};
use crate::memory::MemTable;
use crate::Result;

pub struct WriteBatch<M: MemTable<UserKey> + 'static> {
    db: Arc<WriteCommittedDB<M>>,
    table: SkipMap<UserKey, Value>,
}

impl<M: MemTable<UserKey> + 'static> WriteBatch<M> {
    pub fn range_get(&self, key_start: &UserKey, key_end: &UserKey) -> SkipMap<UserKey, Value> {
        let mut kvs = self.db.range_get(key_start, key_end).unwrap();
        self.table.range_get(key_start, key_end, &mut kvs);
        kvs
    }

    pub fn get(&self, key: &UserKey) -> Result<Option<Value>> {
        match self.table.get_clone(key) {
            Some(v) => Ok(Some(v)),
            None => self.db.get(key),
        }
    }

    pub fn set(&mut self, key: UserKey, value: Value) -> Result<()> {
        self.table.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: UserKey) -> Result<()> {
        self.table.insert(key, Value::default());
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        self.db.write_batch(self.table)
    }
}

/// Isolation level: Read committed
///
/// [See `https://github.com/facebook/rocksdb/wiki/WritePrepared-Transactions`]
/// With WriteCommitted write policy, the data is written to the memtable only after the transaction
/// commits. This greatly simplifies the read path as any data that is read by other transactions
/// can be assumed to be committed. This write policy, however, implies that the writes are buffered
/// in memory in the meanwhile. This makes memory a bottleneck for large transactions.
/// The delay of the commit phase in 2PC (two-phase commit) also becomes noticeable since most of
/// the work, i.e., writing to memtable, is done at the commit phase.
/// When the commit of multiple transactions are done in a serial fashion,
/// such as in 2PC implementation of MySQL, the lengthy commit latency
/// becomes a major contributor to lower throughput. Moreover this write policy
/// cannot provide weaker isolation levels, such as READ UNCOMMITTED, that could
/// potentially provide higher throughput for some applications.
pub struct WriteCommittedDB<M: MemTable<UserKey> + 'static> {
    inner: NoTransactionDB<M>,
}

impl<M: MemTable<UserKey> + 'static> DB<UserKey, M> for WriteCommittedDB<M> {
    fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let inner = NoTransactionDB::<M>::open(db_path)?;
        Ok(WriteCommittedDB { inner })
    }

    fn get(&self, key: &UserKey) -> Result<Option<Value>> {
        self.inner.get(key)
    }

    fn set(&self, key: UserKey, value: Value) -> Result<()> {
        self.inner.set(key, value)
    }

    fn remove(&self, key: UserKey) -> Result<()> {
        self.inner.remove(key)
    }

    fn range_get(&self, key_start: &UserKey, key_end: &UserKey) -> Result<SkipMap<UserKey, Value>> {
        self.inner.range_get(key_start, key_end)
    }
}

impl<M: MemTable<UserKey> + 'static> WriteCommittedDB<M> {
    pub fn start_transaction(db: &Arc<Self>) -> WriteBatch<M> {
        WriteBatch {
            db: db.clone(),
            table: SkipMap::default(),
        }
    }

    pub fn write_batch(&self, batch: SkipMap<UserKey, Value>) -> Result<()> {
        {
            let mut wal_guard = self.inner.wal.lock().unwrap();
            for (key, value) in batch.iter() {
                wal_guard.append(key, Some(value))?;
            }
        }

        let mut mem_table_guard = self.inner.mut_mem_table.write().unwrap();

        mem_table_guard.merge(batch);

        self.inner.may_freeze(mem_table_guard);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::db::transaction::write_committed::WriteCommittedDB;
    use crate::db::DB;
    use crate::memory::SkipMapMemTable;

    #[test]
    fn test_transaction() {
        let temp_dir = tempfile::Builder::new().prefix("txn").tempdir().unwrap();
        let path = temp_dir.path();

        let db = Arc::new(WriteCommittedDB::<SkipMapMemTable>::open(path).unwrap());
        let mut txn1 = WriteCommittedDB::start_transaction(&db);
        for i in 1..=10i32 {
            txn1.set(Vec::from(i.to_be_bytes()), Vec::from((i + 1).to_be_bytes()))
                .unwrap();
        }

        let key2 = Vec::from(2i32.to_be_bytes());
        let value2 = Vec::from(3i32.to_be_bytes());
        assert!(db.get(&key2).unwrap().is_none());
        txn1.commit().unwrap();
        assert_eq!(db.get(&key2).unwrap().unwrap(), value2);
    }
}
