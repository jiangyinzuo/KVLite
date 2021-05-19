use crate::cache::ShardLRUCache;
use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{LSNKey, UserKey};
use crate::db::no_transaction_db::NoTransactionDB;
use crate::db::{Value, DB};
use crate::memory::MemTable;
use crate::sstable::manager::level_0::Level0Manager;
use crate::sstable::manager::level_n::LevelNManager;
use crate::wal::simple_wal::SimpleWriteAheadLog;
use crate::Result;
use crossbeam_channel::Sender;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;

pub struct LSNDB<M: MemTable<LSNKey> + 'static> {
    inner: NoTransactionDB<LSNKey, M>,

    lsn: AtomicU64,
}

impl<M: MemTable<LSNKey> + 'static> DB<LSNKey, M> for LSNDB<M> {
    fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        todo!()
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
