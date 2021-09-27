use crate::collections::skip_list::skipmap::SrSwSkipMap;
use crate::db::key_types::DBKey;
use crate::db::options::WriteOptions;
use crate::memory::MemTable;
use crate::Result;
use std::path::Path;

pub mod db_iter;
pub mod dbimpl;
pub mod key_types;
pub mod options;
pub mod write_batch_db;

pub const WRITE_BUFFER_SIZE: u64 = 4 * 1024 * 1024;
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

pub trait DBCommand<SK: DBKey, UK: DBKey> {
    fn range_get(&self, key_start: &SK, key_end: &SK, kvs: &mut SrSwSkipMap<UK, Value>)
    where
        SK: Into<UK>,
        UK: From<SK>;
    fn get(&self, key: &SK) -> crate::Result<Option<Value>>;
    fn set(&self, key: SK, value: Value) -> crate::Result<()>;
    fn remove(&self, key: SK) -> crate::Result<()>;
}

pub trait DB<SK: DBKey, UK: DBKey, M: MemTable<SK, UK>>: Sized {
    fn open(db_path: impl AsRef<Path>) -> Result<Self>;
    fn get(&self, key: &SK) -> Result<Option<Value>>;
    fn set(&self, write_options: &WriteOptions, key: SK, value: Value) -> Result<()>;
    fn remove(&self, write_options: &WriteOptions, key: SK) -> Result<()>;
    fn range_get(&self, key_start: &SK, key_end: &SK) -> Result<SrSwSkipMap<UK, Value>>
    where
        UK: From<SK>;
    fn db_path(&self) -> &String;
}
