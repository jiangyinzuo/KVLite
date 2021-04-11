use crate::bloom::BloomFilter;
use crate::sstable::index_block::IndexBlock;

pub struct IndexCache {
    pub filter: BloomFilter,
    pub index: IndexBlock,
}
