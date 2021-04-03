//! Memory table

mod btree_mem_table;
mod skip_map_mem_table;

use crate::db::DBCommandMut;

use crate::collections::skip_list::skipmap::{Node, SkipMap};
pub use btree_mem_table::BTreeMemTable;
pub use skip_map_mem_table::SkipMapMemTable;

/// Table in Memory
pub trait MemTable: DBCommandMut + KeyValue + Default + Send + Sync {}

pub trait KeyValue {
    fn len(&self) -> usize;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_>;

    fn first_key(&self) -> Option<&String>;

    fn last_key(&self) -> Option<&String>;
}

impl KeyValue for SkipMap<String, String> {
    fn len(&self) -> usize {
        self.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&String, &String)>> {
        Box::new(
            self.iter()
                .map(|node| unsafe { (&(*node).entry.key, &(*node).entry.value) }),
        )
    }

    fn first_key(&self) -> Option<&String> {
        self.first_key_value().map(|entry| &entry.key)
    }

    fn last_key(&self) -> Option<&String> {
        self.last_key_value().map(|entry| &entry.key)
    }
}
