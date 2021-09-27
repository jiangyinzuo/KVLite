use crate::collections::skip_list::skipmap::SrSwSkipMap;
use crate::db::dbimpl::DBImpl;
use crate::db::key_types::{DBKey, SeqNumKey, SequenceNumber};
use crate::db::options::WriteOptions;
use crate::db::{Value, DB};
use crate::memory::MemTable;
use crate::wal::TransactionWAL;
use crate::Result;
use std::path::Path;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

pub struct SnapShot<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK> + 'static,
{
    db: Arc<WriteBatchDB<UK, M, L>>,
    next_seq_num: SequenceNumber,
}

impl<UK, M, L> SnapShot<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK> + 'static,
{
    pub fn range_get(&self, key_start: UK, key_end: UK) -> SrSwSkipMap<UK, Value> {
        let key_start = SeqNumKey::new(key_start, self.next_seq_num - 1);
        let key_end = SeqNumKey::new(key_end, self.next_seq_num - 1);
        self.db.range_get(&key_start, &key_end).unwrap()
    }

    pub fn get(&self, key: UK) -> Result<Option<Value>> {
        let key = SeqNumKey::new(key, self.next_seq_num - 1);
        self.db.get(&key)
    }
}

impl<UK, M, L> Drop for SnapShot<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK> + 'static,
{
    fn drop(&mut self) {
        self.db.alive_seq_num_count.fetch_sub(1, Ordering::Release);
    }
}

pub struct WriteBatch<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK> + 'static,
{
    db: Arc<WriteBatchDB<UK, M, L>>,
    table: SrSwSkipMap<SeqNumKey<UK>, Value>,
    seq_num: SequenceNumber,
    write_options: WriteOptions,
    mem_usage: AtomicI64,
}

impl<UK, M, L> WriteBatch<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK>,
{
    pub fn range_get(&self, key_start: UK, key_end: UK) -> SrSwSkipMap<UK, Value> {
        let key_start_sn = SeqNumKey::new(key_start, self.seq_num);
        let key_end_sn = SeqNumKey::new(key_end, self.seq_num);
        let mut kvs: SrSwSkipMap<UK, Value> =
            self.db.range_get(&key_start_sn, &key_end_sn).unwrap();
        self.table
            .range_get::<UK>(&key_start_sn, &key_end_sn, &mut kvs);
        kvs
    }

    pub fn get(&self, key: UK) -> Result<Option<Value>> {
        let seq_num_key = SeqNumKey::new(key, self.seq_num);
        match self.table.get_clone(&seq_num_key) {
            Some(v) => Ok(Some(v)),
            None => self.db.get(&seq_num_key),
        }
    }

    pub fn set(&mut self, key: UK, value: Value) -> Result<()> {
        let key_len = key.mem_size() as i64;
        let value_len = value.len() as i64;
        let mem_add = match self.table.insert(SeqNumKey::new(key, self.seq_num), value) {
            Some(v) => value_len - (v.len() as i64),
            None => (key_len + value_len),
        } * std::mem::size_of::<u8>() as i64;
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    pub fn remove(&mut self, key: UK) -> Result<()> {
        let key_mem_size = key.mem_size();
        let mem_add = match self
            .table
            .insert(SeqNumKey::new(key, self.seq_num), Value::default())
        {
            Some(v) => -((v.len() * std::mem::size_of::<u8>()) as i64),
            None => key_mem_size as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    pub fn abort(&mut self) -> Result<()> {
        std::mem::take(&mut self.table);
        Ok(())
    }
}

impl<UK, M, L> Drop for WriteBatch<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK> + 'static,
{
    fn drop(&mut self) {
        if !self.table.is_empty() {
            let table = std::mem::take(&mut self.table);
            let mem_usage = self.mem_usage.load(Ordering::Acquire);
            debug_assert!(mem_usage >= 0);
            self.db
                .multi_write(&self.write_options, table, mem_usage as u64)
                .unwrap();
        }
        self.db.alive_seq_num_count.fetch_sub(1, Ordering::Release);
    }
}

pub struct WriteBatchDB<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK> + 'static,
{
    inner: DBImpl<SeqNumKey<UK>, UK, M, L>,
    next_seq_num: AtomicU64,
    alive_seq_num_count: AtomicU64,
}

impl<UK, M, L> DB<SeqNumKey<UK>, UK, M> for WriteBatchDB<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK>,
{
    fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let inner = DBImpl::<SeqNumKey<UK>, UK, M, L>::open(db_path)?;
        Ok(WriteBatchDB {
            inner,
            next_seq_num: AtomicU64::new(1),
            alive_seq_num_count: AtomicU64::new(0),
        })
    }

    #[inline]
    fn get(&self, key: &SeqNumKey<UK>) -> Result<Option<Value>> {
        self.inner.get(key)
    }

    #[inline]
    fn set(&self, write_options: &WriteOptions, key: SeqNumKey<UK>, value: Value) -> Result<()> {
        self.inner.set(write_options, key, value)
    }

    #[inline]
    fn remove(&self, write_options: &WriteOptions, key: SeqNumKey<UK>) -> Result<()> {
        self.inner.remove(write_options, key)
    }

    #[inline]
    fn range_get(
        &self,
        key_start: &SeqNumKey<UK>,
        key_end: &SeqNumKey<UK>,
    ) -> Result<SrSwSkipMap<UK, Value>> {
        self.inner.range_get(key_start, key_end)
    }

    fn db_path(&self) -> &String {
        self.inner.db_path()
    }
}

impl<UK, M, L> WriteBatchDB<UK, M, L>
where
    UK: DBKey + From<SeqNumKey<UK>> + 'static,
    M: MemTable<SeqNumKey<UK>, UK> + 'static,
    L: TransactionWAL<SeqNumKey<UK>, UK>,
{
    pub fn get_by_user_key(&self, key: UK) -> Result<Option<Value>> {
        let lsn_key = SeqNumKey::new(key, self.next_seq_num.fetch_add(1, Ordering::Release));
        self.get(&lsn_key)
    }

    pub fn set_by_user_key(
        &self,
        write_options: &WriteOptions,
        key: UK,
        value: Value,
    ) -> Result<()> {
        let lsn_key = SeqNumKey::new(key, self.next_seq_num.fetch_add(1, Ordering::Release));
        self.set(write_options, lsn_key, value)
    }

    pub fn remove_by_user_key(&self, write_options: &WriteOptions, key: UK) -> Result<()> {
        let lsn_key = SeqNumKey::new(key, self.next_seq_num.fetch_add(1, Ordering::Release));
        self.remove(write_options, lsn_key)
    }

    pub fn snapshot(db: &Arc<Self>) -> SnapShot<UK, M, L> {
        SnapShot {
            db: db.clone(),
            next_seq_num: db.next_seq_num.load(Ordering::Acquire),
        }
    }

    pub fn new_write_batch(db: &Arc<Self>, write_options: WriteOptions) -> WriteBatch<UK, M, L> {
        WriteBatch {
            db: db.clone(),
            table: SrSwSkipMap::default(),
            seq_num: db.next_seq_num.load(Ordering::Acquire),
            mem_usage: AtomicI64::default(),
            write_options,
        }
    }

    pub fn multi_write(
        &self,
        write_options: &WriteOptions,
        mut batch: SrSwSkipMap<SeqNumKey<UK>, Value>,
        mem_usage: u64,
    ) -> Result<()> {
        {
            let lsn = self.next_seq_num.fetch_add(1, Ordering::Release);
            let mut wal_guard = self.inner.wal.lock().unwrap();
            for (key, value) in batch.iter_mut() {
                key.set_seq_num(lsn);
                wal_guard.append(write_options, &key, Some(value))?;
            }
        }

        let mem_table = self.inner.get_mut_mem_table();
        mem_table.merge(batch, mem_usage);

        self.may_freeze();
        Ok(())
    }

    fn may_freeze(&self) {
        let mem_table = self.inner.get_mut_mem_table();
        if self.alive_seq_num_count.load(Ordering::Acquire) == 0
            && self
                .inner
                .should_freeze(mem_table.approximate_memory_usage())
        {
            self.inner.freeze();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::key_types::{I32UserKey, RawUserKey, SeqNumKey, SequenceNumber};
    use crate::db::options::WriteOptions;
    use crate::db::write_batch_db::WriteBatchDB;
    use crate::db::DB;
    use crate::memory::{MrSwSkipMapMemTable, MutexSkipMapMemTable};
    use crate::wal::lsn_wal::LSNWriteAheadLog;
    use std::sync::Arc;

    #[test]
    fn test_write_batch() {
        let temp_dir = tempfile::Builder::new().prefix("txn").tempdir().unwrap();
        let path = temp_dir.path();

        let db =
            Arc::new(
                WriteBatchDB::<
                    RawUserKey,
                    MutexSkipMapMemTable<SeqNumKey<RawUserKey>>,
                    LSNWriteAheadLog,
                >::open(path)
                .unwrap(),
            );

        let mut txn1 = WriteBatchDB::new_write_batch(&db, WriteOptions { sync: false });
        for i in 1..=10i32 {
            txn1.set(Vec::from(i.to_be_bytes()), Vec::from((i + 1).to_be_bytes()))
                .unwrap();
        }

        let key2 = SeqNumKey::new(Vec::from(2i32.to_be_bytes()), SequenceNumber::MAX);
        let value2 = Vec::from(3i32.to_be_bytes());
        assert!(db.get(&key2).unwrap().is_none());
        // commit write batch
        drop(txn1);
        assert_eq!(db.get(&key2).unwrap().unwrap(), value2);
        let key2 = SeqNumKey::new(Vec::from(2i32.to_be_bytes()), 0);
        assert!(db.get(&key2).unwrap().is_none());

        let snapshot = WriteBatchDB::snapshot(&db);
        {
            let mut txn2 = WriteBatchDB::new_write_batch(&db, WriteOptions { sync: true });
            txn2.set(
                Vec::from(10i32.to_be_bytes()),
                Vec::from(1000i32.to_be_bytes()),
            )
            .unwrap();
        }
        assert_eq!(
            snapshot.get(Vec::from(10i32.to_be_bytes())).unwrap(),
            Some(Vec::from(11i32.to_be_bytes()))
        );
    }

    #[test]
    fn test_i32key() {
        let temp_dir = tempfile::Builder::new().prefix("txn").tempdir().unwrap();
        let path = temp_dir.path();
        let db: WriteBatchDB<
            I32UserKey,
            MutexSkipMapMemTable<SeqNumKey<I32UserKey>>,
            LSNWriteAheadLog,
        > = WriteBatchDB::open(path).unwrap();
        let write_options = WriteOptions { sync: true };
        db.set_by_user_key(
            &write_options,
            I32UserKey::new(4),
            Vec::from(4i32.to_le_bytes()),
        )
        .unwrap();
        let value = db.get_by_user_key(I32UserKey::new(4)).unwrap();
        assert_eq!(value, Some(Vec::from(4i32.to_le_bytes())));
        assert!(db.get_by_user_key(I32UserKey::new(0)).unwrap().is_none());
        for _ in 0..4 {
            db.remove_by_user_key(&write_options, I32UserKey::new(4))
                .unwrap();
            assert!(db.get_by_user_key(I32UserKey::new(4)).unwrap().is_none());
        }
    }
}
