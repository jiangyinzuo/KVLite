use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::no_transaction_db::NoTransactionDB;
use crate::db::{Key, Value, DB};
use crate::memory::MemTable;
use crate::Result;
use std::path::Path;
use std::sync::Arc;

pub struct WriteBatch<M: MemTable> {
    db: Arc<WriteCommittedDB<M>>,
    table: SkipMap<Key, Value>,
}

impl<M: MemTable + 'static> WriteBatch<M> {
    pub fn range_get(&self, key_start: &Key, key_end: &Key) -> SkipMap<Key, Value> {
        let mut kvs = self.db.range_get(key_start, key_end).unwrap();
        self.table.range_get(key_start, key_end, &mut kvs);
        kvs
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>> {
        match self.table.get_clone(key) {
            Some(v) => Ok(Some(v)),
            None => self.db.get(key),
        }
    }

    pub fn set(&mut self, key: Key, value: Value) -> Result<()> {
        self.table.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: Key) -> Result<()> {
        self.table.insert(key, Value::default());
        Ok(())
    }

    pub fn commit(self) -> Result<()> {
        self.db.write_batch(self.table)
    }
}

/// Isolation level: Read committed
pub struct WriteCommittedDB<M: MemTable> {
    inner: NoTransactionDB<M>,
}

impl<M: MemTable + 'static> DB<M> for WriteCommittedDB<M> {
    fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let inner = NoTransactionDB::<M>::open(db_path)?;
        Ok(WriteCommittedDB { inner })
    }

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        self.inner.get(key)
    }

    fn set(&self, key: Key, value: Value) -> Result<()> {
        self.inner.set(key, value)
    }

    fn remove(&self, key: Key) -> Result<()> {
        self.inner.remove(key)
    }

    fn range_get(&self, key_start: &Key, key_end: &Key) -> Result<SkipMap<Key, Value>> {
        self.inner.range_get(key_start, key_end)
    }
}

impl<M: MemTable + 'static> WriteCommittedDB<M> {
    pub fn start_transaction(db: &Arc<Self>) -> WriteBatch<M> {
        WriteBatch {
            db: db.clone(),
            table: SkipMap::default(),
        }
    }

    pub fn write_batch(&self, batch: SkipMap<Key, Value>) -> Result<()> {
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
    use crate::db::transaction::pessimistic::write_committed::WriteCommittedDB;
    use crate::db::DB;
    use crate::memory::SkipMapMemTable;
    use std::sync::Arc;

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
