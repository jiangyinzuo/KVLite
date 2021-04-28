use crate::collections::skip_list::skipmap::SkipMap;
use crate::memory::MemTable;
use crate::Result;
use std::path::Path;

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

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub trait DBCommand {
    fn range_get(&self, key_start: &Key, key_end: &Key, kvs: &mut SkipMap<Key, Value>);
    fn get(&self, key: &Key) -> crate::Result<Option<Value>>;
    fn set(&mut self, key: Key, value: Value) -> crate::Result<()>;
    fn remove(&mut self, key: Key) -> crate::Result<()>;
}

pub trait DB<M: MemTable>: Sized {
    fn open(db_path: impl AsRef<Path>) -> Result<Self>;
    fn get(&self, key: &Key) -> Result<Option<Value>>;
    fn set(&self, key: Key, value: Value) -> Result<()>;
    fn remove(&self, key: Key) -> Result<()>;
    fn range_get(&self, key_start: &Key, key_end: &Key) -> Result<SkipMap<Key, Value>>;
}
