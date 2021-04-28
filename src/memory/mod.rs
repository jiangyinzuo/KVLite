//! Memory table

mod btree_mem_table;
mod skip_map_mem_table;

use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::{DBCommand, Key, Value};
pub use btree_mem_table::BTreeMemTable;
pub use skip_map_mem_table::SkipMapMemTable;

/// Table in Memory
pub trait MemTable: DBCommand + KeyValue + Default + Send + Sync {}

pub trait KeyValue {
    fn len(&self) -> usize;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&Key, &Key)> + '_>;

    fn first_key(&self) -> Option<&Key>;

    fn last_key(&self) -> Option<&Key>;
}

impl KeyValue for SkipMap<Key, Value> {
    fn len(&self) -> usize {
        self.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&Key, &Value)>> {
        Box::new(
            self.iter()
                .map(|node| unsafe { (&(*node).entry.key, &(*node).entry.value) }),
        )
    }

    fn first_key(&self) -> Option<&Key> {
        self.first_key_value().map(|entry| &entry.key)
    }

    fn last_key(&self) -> Option<&Key> {
        self.last_key_value().map(|entry| &entry.key)
    }
}
