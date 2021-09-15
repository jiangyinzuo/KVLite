use crate::filter::DefaultBloomFilter;
use crate::sstable::data_block::DataBlock;
use crate::sstable::index_block::IndexBlock;
use std::collections::HashMap;

pub struct TableCache {
    pub filter: DefaultBloomFilter,
    pub index: IndexBlock,
    pub start_data_block_map: HashMap<u32, DataBlock>,
}

impl TableCache {
    pub fn new(filter: DefaultBloomFilter, index: IndexBlock) -> TableCache {
        TableCache {
            filter,
            index,
            start_data_block_map: HashMap::with_capacity(10),
        }
    }
}
