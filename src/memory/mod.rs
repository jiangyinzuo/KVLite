//! Memory table

pub use btree_mem_table::BTreeMemTable;
pub use mrmw_skip_map_mem_table::MrMwSkipMapMemTable;
pub use mrsw_skip_map_mem_table::MrSwSkipMapMemTable;
pub use skip_map_mem_table::MutexSkipMapMemTable;

use crate::collections::skip_list::skipmap::{Node, ReadWriteMode, SkipMap, SrSwSkipMap};
use crate::collections::skip_list::MemoryAllocator;
use crate::db::key_types::{InternalKey, MemKey};
use crate::db::{DBCommand, Value};
use std::marker::PhantomData;
use std::sync::Arc;

mod btree_mem_table;
mod mrmw_skip_map_mem_table;
mod mrsw_skip_map_mem_table;
mod skip_map_mem_table;

/// Table in Memory
pub trait MemTable<SK: MemKey, UK: MemKey>:
    DBCommand<SK, UK> + Default + InternalKeyValueIterator + Send + Sync + Sized
{
    fn merge(&self, kvs: SrSwSkipMap<SK, Value>, memory_size: u64);
    fn approximate_memory_usage(&self) -> u64;
}

pub trait SkipMapMemTable<SK: MemKey, UK: MemKey, const RW_MODE: ReadWriteMode>:
    MemTable<SK, UK> + 'static
{
    fn get_inner(&self) -> &SkipMap<SK, Value, RW_MODE>;
}

/// Used for iterate all the key-value pairs in database.
pub struct MemTableCloneIterator<
    SK: MemKey,
    UK: MemKey,
    const RW_MODE: ReadWriteMode,
    M: SkipMapMemTable<SK, UK, { RW_MODE }>,
> {
    _mem_table: Arc<M>,
    node: *mut Node<SK, Value, RW_MODE>,
    _marker: PhantomData<(UK, Value)>,
}

impl<
        SK: MemKey,
        UK: MemKey,
        const RW_MODE: ReadWriteMode,
        M: SkipMapMemTable<SK, UK, { RW_MODE }>,
    > MemTableCloneIterator<SK, UK, { RW_MODE }, M>
{
    pub fn new(mem_table: Arc<M>) -> Self {
        let node = mem_table.get_inner().first_node() as *mut _;
        MemTableCloneIterator {
            _mem_table: mem_table,
            node,
            _marker: PhantomData,
        }
    }
}

impl<
        SK: MemKey,
        UK: MemKey,
        const RW_MODE: ReadWriteMode,
        M: SkipMapMemTable<SK, UK, { RW_MODE }>,
    > Iterator for MemTableCloneIterator<SK, UK, { RW_MODE }, M>
{
    type Item = (SK, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.node.is_null() {
            None
        } else {
            let item = unsafe {
                let entry = &(*self.node).entry;
                self.node = (*self.node).get_next(0);
                entry.key_value_clone()
            };
            Some(item)
        }
    }
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
