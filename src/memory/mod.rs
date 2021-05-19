//! Memory table

pub use btree_mem_table::BTreeMemTable;
pub use skip_map_mem_table::SkipMapMemTable;

use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{MemKey, UserKey};
use crate::db::{DBCommand, Value};

mod btree_mem_table;
mod skip_map_mem_table;

/// Table in Memory
pub trait MemTable<K: MemKey + Ord>:
    DBCommand<K> + Default + UserKeyValueIterator + Send + Sync
{
    fn merge(&mut self, kvs: SkipMap<K, Value>);
}

pub trait UserKeyValueIterator {
    fn len(&self) -> usize;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Note: UserKey should not be duplicated.
    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&UserKey, &Value)> + '_>;
}

impl UserKeyValueIterator for SkipMap<UserKey, Value> {
    fn len(&self) -> usize {
        self.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&UserKey, &Value)>> {
        Box::new(
            self.iter_ptr()
                .map(|node| unsafe { (&(*node).entry.key, &(*node).entry.value) }),
        )
    }
}
