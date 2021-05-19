use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{LSNKey, UserKey};
use crate::db::no_transaction_db::NoTransactionDB;
use crate::db::{Value, DB};
use crate::memory::MemTable;
use crate::Result;
use std::path::Path;
use std::sync::atomic::AtomicU64;

pub struct LSNDB<M: MemTable<LSNKey> + 'static> {
    inner: NoTransactionDB<LSNKey, M>,

    next_lsn: AtomicU64,
}

impl<M: MemTable<LSNKey> + 'static> DB<LSNKey, M> for LSNDB<M> {
    fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let inner = NoTransactionDB::open(db_path)?;
        Ok(LSNDB {
            inner,
            next_lsn: AtomicU64::new(1),
        })
    }

    fn get(&self, key: &LSNKey) -> Result<Option<Value>> {
        todo!()
    }

    fn set(&self, key: LSNKey, value: Value) -> Result<()> {
        todo!()
    }

    fn remove(&self, key: LSNKey) -> Result<()> {
        todo!()
    }

    fn range_get(&self, key_start: &LSNKey, key_end: &LSNKey) -> Result<SkipMap<UserKey, Value>> {
        todo!()
    }
}
