//! Memory table

pub use btree_mem_table::BTreeMemTable;
pub use mrmw_skip_map_mem_table::MrMwSkipMapMemTable;
pub use mrsw_skip_map_mem_table::MrSwSkipMapMemTable;
pub use skip_map_mem_table::SkipMapMemTable;

use crate::collections::skip_list::skipmap::SrSwSkipMap;
use crate::db::key_types::{InternalKey, MemKey};
use crate::db::{DBCommand, Value};

mod btree_mem_table;
mod mrmw_skip_map_mem_table;
mod mrsw_skip_map_mem_table;
mod skip_map_mem_table;

/// Table in Memory
pub trait MemTable<SK: MemKey, UK: MemKey>:
    DBCommand<SK, UK> + Default + InternalKeyValueIterator + Send + Sync
{
    fn merge(&self, kvs: SrSwSkipMap<SK, Value>, memory_size: u64);
    fn approximate_memory_usage(&self) -> u64;
}

pub trait InternalKeyValueIterator {
    fn len(&self) -> usize;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Note: InternalKey should not be duplicated.
    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&InternalKey, &Value)> + '_>;
}

impl InternalKeyValueIterator for SrSwSkipMap<InternalKey, Value> {
    fn len(&self) -> usize {
        self.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&InternalKey, &Value)>> {
        Box::new(
            self.iter_ptr()
                .map(|node| unsafe { (&(*node).entry.key, &(*node).entry.value) }),
        )
    }
}
