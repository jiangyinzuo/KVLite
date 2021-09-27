use crate::collections::skip_list::skipmap::ReadWriteMode;
use crate::collections::skip_list::MemoryAllocator;
use crate::db::key_types::RawUserKey;
use crate::db::Value;
use crate::memory::{MemTableCloneIterator, SkipMapMemTable};
use crate::sstable::manager::level_iter::{Level0Iterator, MergingIterator};
use std::cmp::Ordering;

pub type InternalKeyValue = (RawUserKey, Value);

#[derive(PartialEq, Eq)]
pub(crate) struct KeyValueIterItem {
    pub(crate) key: RawUserKey,
    pub(crate) value: Value,
    pub(crate) iter_id: usize,
}

impl PartialOrd for KeyValueIterItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyValueIterItem {
    /// Larger iter idx is larger.
    /// Smaller key is larger.
    /// [std::collections::BinaryHeap] returns the greatest item
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then(self.iter_id.cmp(&other.iter_id))
    }
}

impl KeyValueIterItem {
    pub(crate) fn new(key: RawUserKey, value: Value, iter_id: usize) -> KeyValueIterItem {
        KeyValueIterItem {
            key,
            value,
            iter_id,
        }
    }
}
pub type DBIterator = MergingIterator<Box<dyn Iterator<Item = InternalKeyValue>>>;

impl DBIterator {
    pub(crate) fn new<
        M: SkipMapMemTable<RawUserKey, Value, { RW_MODE }> + 'static,
        const RW_MODE: ReadWriteMode,
    >(
        imm_mem_iterator: MemTableCloneIterator<RawUserKey, Value, M, { RW_MODE }>,
        mut_mem_iterator: MemTableCloneIterator<RawUserKey, Value, M, { RW_MODE }>,
        level0_iterator: Level0Iterator,
        mut leveln_iterators: Vec<Box<dyn Iterator<Item = InternalKeyValue>>>,
    ) -> DBIterator {
        leveln_iterators.reverse();
        leveln_iterators.reserve(3);
        leveln_iterators.push(Box::new(level0_iterator));
        leveln_iterators.push(Box::new(imm_mem_iterator));
        leveln_iterators.push(Box::new(mut_mem_iterator));
        Self::from_iterators(leveln_iterators)
    }
}
