//! Memory table

mod btree_mem_table;
mod skip_map_mem_table;

use crate::db::DBCommandMut;

pub use btree_mem_table::BTreeMemTable;
pub use skip_map_mem_table::SkipMapMemTable;

/// Table in Memory
pub trait MemTable: DBCommandMut + Default + Send + Sync {
    fn len(&self) -> usize;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_>;

    fn first_key(&self) -> Option<&String>;

    fn last_key(&self) -> Option<&String>;
}
