use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::MemKey;
use crate::memory::MemTable;
use crate::Result;
use key_types::UserKey;
use std::path::Path;

pub mod key_types;
pub mod lsn_db;
pub mod no_transaction_db;
pub mod transaction;

#[cfg(debug_assertions)]
pub const ACTIVE_SIZE_THRESHOLD: usize = 300;
#[cfg(not(debug_assertions))]
pub const ACTIVE_SIZE_THRESHOLD: usize = 1000;

pub const MAX_LEVEL: usize = 7;

pub(crate) const fn max_level_shift() -> usize {
    let mut idx = 1;
    let mut value = 2;
    while value <= MAX_LEVEL {
        value *= 2;
        idx += 1;
    }
    idx
}

pub type Value = Vec<u8>;

pub trait DBCommand<K: MemKey> {
    fn range_get(&self, key_start: &K, key_end: &K, kvs: &mut SkipMap<UserKey, Value>);
    fn get(&self, key: &K) -> crate::Result<Option<Value>>;
    fn set(&mut self, key: K, value: Value) -> crate::Result<()>;
    fn remove(&mut self, key: K) -> crate::Result<()>;
}

pub trait DB<K: MemKey, M: MemTable<K>>: Sized {
    fn open(db_path: impl AsRef<Path>) -> Result<Self>;
    fn get(&self, key: &K) -> Result<Option<Value>>;
    fn set(&self, key: K, value: Value) -> Result<()>;
    fn remove(&self, key: K) -> Result<()>;
    fn range_get(&self, key_start: &K, key_end: &K) -> Result<SkipMap<UserKey, Value>>;
}
