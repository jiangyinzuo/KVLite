use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{LSNKey, MemKey};
use crate::db::no_transaction_db::NoTransactionDB;
use crate::db::{Value, DB};
use crate::memory::MemTable;
use crate::Result;
use std::path::Path;
use std::sync::atomic::AtomicU64;

pub struct LSNDB<UK: MemKey + 'static, M: MemTable<LSNKey<UK>, UK> + 'static> {
    inner: NoTransactionDB<LSNKey<UK>, UK, M>,
    next_lsn: AtomicU64,
}

impl<K: MemKey, M: MemTable<LSNKey<K>, K> + 'static> DB<LSNKey<K>, K, M> for LSNDB<K, M>
where
    K: From<LSNKey<K>>,
{
    fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let inner = NoTransactionDB::open(db_path)?;
        Ok(LSNDB {
            inner,
            next_lsn: AtomicU64::new(1),
        })
    }

    fn get(&self, key: &LSNKey<K>) -> Result<Option<Value>> {
        todo!()
    }

    fn set(&self, key: LSNKey<K>, value: Value) -> Result<()> {
        todo!()
    }

    fn remove(&self, key: LSNKey<K>) -> Result<()> {
        todo!()
    }

    fn range_get(&self, key_start: &LSNKey<K>, key_end: &LSNKey<K>) -> Result<SkipMap<K, Value>> {
        todo!()
    }
}
