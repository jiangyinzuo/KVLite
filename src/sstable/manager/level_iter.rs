use crate::db::db_iter::{InternalKeyValue, KeyValueIterItem};
use crate::db::key_types::InternalKey;
use crate::sstable::table_handle::{TableIterator, TableReadHandle};
use crate::sstable::TableID;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

pub type Level0Iterator = MergingIterator<TableIterator>;

impl Level0Iterator {
    pub(super) fn new(tables: &BTreeMap<TableID, Arc<TableReadHandle>>) -> Level0Iterator {
        let iterators: Vec<_> = tables
            .values()
            .map(|handle| TableIterator::new(handle.clone()))
            .collect();
        Self::from_iterators(iterators)
    }
}

pub struct MergingIterator<It: Iterator<Item = InternalKeyValue>> {
    pub(crate) iterators: Vec<It>,
    priority_queue: BinaryHeap<KeyValueIterItem>,
    #[cfg(debug_assertions)]
    prev_key: InternalKey,
}

impl<It: Iterator<Item = InternalKeyValue>> MergingIterator<It> {
    pub(crate) fn from_iterators(mut iterators: Vec<It>) -> MergingIterator<It> {
        let mut priority_queue = BinaryHeap::with_capacity(iterators.len());
        for (iter_id, iter) in iterators.iter_mut().enumerate() {
            if let Some((k, v)) = iter.next() {
                priority_queue.push(KeyValueIterItem::new(k, v, iter_id));
            }
        }
        MergingIterator {
            iterators,
            priority_queue,
            #[cfg(debug_assertions)]
            prev_key: InternalKey::default(),
        }
    }

    fn try_pop_ith_elem_to_queue(&mut self, iter_id: usize) {
        if let Some((k, v)) = self.iterators[iter_id].next() {
            self.priority_queue
                .push(KeyValueIterItem::new(k, v, iter_id));
        }
    }
}

impl<It: Iterator<Item = InternalKeyValue>> Iterator for MergingIterator<It> {
    type Item = InternalKeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        self.priority_queue.pop().map(|item| {
            self.try_pop_ith_elem_to_queue(item.iter_id);

            while let Some(next_item) = self.priority_queue.peek() {
                if next_item.key == item.key {
                    let next_item_iter_id = next_item.iter_id;
                    debug_assert!(item.iter_id > next_item_iter_id);
                    self.priority_queue.pop();
                    self.try_pop_ith_elem_to_queue(next_item_iter_id);
                } else {
                    break;
                }
            }
            #[cfg(debug_assertions)]
            {
                assert!(self.prev_key < item.key);
                self.prev_key = item.key.clone();
            }
            (item.key, item.value)
        })
    }
}

pub struct LevelNIterator {
    iterators: Vec<TableIterator>,
    idx: usize,
    #[cfg(debug_assertions)]
    prev_key: InternalKey,
    #[cfg(debug_assertions)]
    prev_idx: usize,
}

impl LevelNIterator {
    pub(super) fn new(
        table_handles: &BTreeMap<(InternalKey, TableID), Arc<TableReadHandle>>,
    ) -> LevelNIterator {
        #[cfg(debug_assertions)]
        {
            let mut last_max_key = InternalKey::default();
            for table in table_handles.values() {
                assert!(last_max_key.lt(table.min_key()));
                last_max_key = table.max_key().clone();
            }
        }

        let iterators: Vec<_> = table_handles
            .values()
            .map(|handle| TableIterator::new(handle.clone()))
            .collect();
        LevelNIterator {
            iterators,
            idx: 0,
            #[cfg(debug_assertions)]
            prev_key: InternalKey::default(),
            #[cfg(debug_assertions)]
            prev_idx: 0,
        }
    }
}

impl Iterator for LevelNIterator {
    type Item = InternalKeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.iterators.len() {
            return None;
        }
        #[cfg(debug_assertions)]
        let mut reenter = false;

        let item = self.iterators[self.idx].next().or_else(|| {
            #[cfg(debug_assertions)]
            {
                self.prev_idx = self.idx;
                reenter = true;
            }
            self.idx += 1;
            self.next()
        });
        #[cfg(debug_assertions)]
        if !reenter {
            if let Some((k, _v)) = &item {
                assert!(
                    self.prev_key.lt(k),
                    r#"prev_key: {:?} key: {:?}
                       prev_idx: {} idx: {}
                    "#,
                    self.prev_key,
                    k,
                    self.prev_idx,
                    self.idx
                );
                self.prev_key = k.clone();
            }
        }
        item
    }
}
