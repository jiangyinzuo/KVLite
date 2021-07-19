use crate::db::db_iter::InternalKeyValue;
use crate::sstable::table_handle::{TableIterator, TableReadHandle};
use crate::sstable::TableID;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct Level0Iterator {
    iterators: Vec<TableIterator>,
}

impl Level0Iterator {
    pub(super) fn new(tables: &BTreeMap<TableID, Arc<TableReadHandle>>) -> Level0Iterator {
        let iterators: Vec<_> = tables
            .values()
            .map(|handle| TableIterator::new(handle.clone()))
            .collect();

        Level0Iterator { iterators }
    }
}

impl Iterator for Level0Iterator {
    type Item = InternalKeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
