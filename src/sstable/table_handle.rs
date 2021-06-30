use crate::bloom::BloomFilter;
use crate::cache::ShardLRUCache;
use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{InternalKey, MemKey};
use crate::db::{max_level_shift, Value, WRITE_BUFFER_SIZE};
use crate::env::file_system::{FileSystem, SequentialReadableFile};
use crate::hash::murmur_hash;
use crate::ioutils::{BufReaderWithPos, BufWriterWithPos};
use crate::memory::InternalKeyValueIterator;
use crate::sstable::data_block::{DataBlock, DataBlockIter};
use crate::sstable::filter_block::{load_filter_block, write_filter_block};
use crate::sstable::footer::{write_footer, Footer};
use crate::sstable::index_block::IndexBlock;
use crate::sstable::table_cache::TableCache;
use crate::sstable::DATA_BLOCK_SIZE;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::{Arc, RwLock};

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum TableStatus {
    /// Normally store in disk.
    Store,
    /// The sstable is merging to the next level.
    Compacting,
    /// Remove file when [TableHandle] is dropped.
    ToDelete,
}

/// Handle of new sstable for single-thread writing.
pub struct TableWriteHandle {
    pub(crate) file_path: String,
    level: usize,
    table_id: u64,
    pub(crate) writer: TableWriter,
}

impl TableWriteHandle {
    pub fn new(db_path: &str, level: usize, table_id: u64, kv_total: u32) -> TableWriteHandle {
        let file_path = format!("{}/{}/{}", db_path, level, table_id);
        let writer = {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .append(true)
                .open(temp_file_name(&file_path))
                .unwrap();
            debug_assert!(std::path::Path::new(&temp_file_name(&file_path)).exists());
            file.seek(SeekFrom::Start(0)).unwrap();
            let buf_writer = BufWriterWithPos::new(file).unwrap();
            TableWriter::new(buf_writer, kv_total)
        };

        TableWriteHandle {
            file_path,
            level,
            table_id,
            writer,
        }
    }

    pub fn write_sstable(&mut self, table: &impl InternalKeyValueIterator) -> crate::Result<()> {
        // write Data Blocks
        for (i, (k, v)) in table.kv_iter().enumerate() {
            self.writer.add_key_value(k.clone(), v.clone());
            if self.writer.data.len() >= DATA_BLOCK_SIZE || i == table.len() - 1 {
                self.writer.flush_data(k.clone());
            }
        }
        self.writer.write_index_filter_footer();
        Ok(())
    }

    pub fn write_sstable_from_vec(&mut self, kvs: Vec<(InternalKey, Value)>) -> crate::Result<()> {
        // write Data Blocks
        let length = kvs.len();
        for (i, (k, v)) in kvs.into_iter().enumerate() {
            self.writer.add_key_value(k.clone(), v);
            if self.writer.data.len() >= DATA_BLOCK_SIZE || i == length - 1 {
                self.writer.flush_data(k);
            }
        }
        self.writer.write_index_filter_footer();
        Ok(())
    }

    pub(crate) fn rename(&self) {
        debug_assert!(
            !std::path::Path::new(&self.file_path).exists(),
            "{}",
            self.file_path
        );
        debug_assert!(
            std::path::Path::new(&temp_file_name(&self.file_path)).exists(),
            "{}",
            temp_file_name(&self.file_path)
        );
        std::fs::rename(temp_file_name(&self.file_path), &self.file_path)
            .unwrap_or_else(|e| panic!("{:#?}, file_path: {}", e, &self.file_path));
    }

    #[inline]
    pub fn level(&self) -> usize {
        self.level
    }

    #[inline]
    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    #[inline]
    pub fn take_min_key(&mut self) -> InternalKey {
        debug_assert_ne!(self.writer.index_block.min_key.len(), 0);
        std::mem::take(&mut self.writer.index_block.min_key)
    }

    #[inline]
    pub fn max_key(&self) -> &InternalKey {
        self.writer.max_key()
    }
}

pub(crate) struct TableWriter {
    pub(crate) kv_total: u32,
    #[cfg(debug_assertions)]
    kv_count: u32,
    data: Vec<u8>,
    pub(crate) index_block: IndexBlock,
    pub(crate) writer: BufWriterWithPos<File>,
    record_offsets: Vec<u8>,
    filter: BloomFilter,
    #[cfg(feature = "snappy_compression")]
    snappy_encoder: snap::raw::Encoder,
}

impl TableWriter {
    fn new(writer: BufWriterWithPos<File>, kv_total: u32) -> TableWriter {
        TableWriter {
            kv_total,
            #[cfg(debug_assertions)]
            kv_count: 0,
            data: Vec::with_capacity(WRITE_BUFFER_SIZE as usize + 500),
            index_block: IndexBlock::default(),
            writer,
            record_offsets: Vec::with_capacity(kv_total as usize),
            filter: BloomFilter::create_filter(kv_total as usize),
            #[cfg(feature = "snappy_compression")]
            snappy_encoder: snap::raw::Encoder::new(),
        }
    }

    fn add_key_value(&mut self, mut k: InternalKey, mut v: Value) {
        debug_assert!(!k.is_empty(), "attempt to write empty key");
        self.filter.add(&k);
        debug_assert!(self.filter.may_contain(&k));

        #[cfg(debug_assertions)]
        let excepted_data_len = self.data.len() + 8 + k.len() + v.len();

        if unsafe { std::intrinsics::unlikely(self.index_block.min_key.is_empty()) } {
            self.index_block.min_key = k.clone();
        }

        let record_offset = (self.data.len() as u32).to_le_bytes();
        self.record_offsets.append(&mut Vec::from(record_offset));

        self.data
            .append(&mut Vec::from((k.len() as u32).to_le_bytes()));
        self.data
            .append(&mut Vec::from((v.len() as u32).to_le_bytes()));
        self.data.append(&mut k);
        self.data.append(&mut v);
        #[cfg(debug_assertions)]
        {
            self.kv_count += 1;
            assert_eq!(excepted_data_len, self.data.len());
        }
    }

    fn flush_data(&mut self, max_key: InternalKey) {
        let index_offset_uncompressed = self.writer.pos as u32 + self.data.len() as u32;
        self.data.append(&mut self.record_offsets);

        #[cfg(feature = "snappy_compression")]
        {
            #[cfg(debug_assertions)]
            let before_length = self.data.len();
            self.data = self.snappy_encoder.compress_vec(&self.data).unwrap();
            #[cfg(debug_assertions)]
            debug!(
                "snappy before: {}, after: {}",
                before_length,
                self.data.len()
            );
        }
        self.index_block.add_index(
            self.writer.pos as u32,
            self.data.len() as u32,
            index_offset_uncompressed,
            max_key,
        );
        self.writer.write_all(&self.data).unwrap();
        self.data.clear();
    }

    fn write_index_filter_footer(&mut self) {
        let index_block_offset = self.writer.pos as u32;
        self.index_block.write_to_file(&mut self.writer).unwrap();
        let index_block_length = self.writer.pos as u32 - index_block_offset;
        write_filter_block(&mut self.filter, &mut self.writer);
        write_footer(
            index_block_offset,
            index_block_length,
            &mut self.writer,
            self.filter.len(),
            self.kv_total,
        );
        #[cfg(debug_assertions)]
        debug_assert_eq!(self.kv_count, self.kv_total);

        self.writer.flush().unwrap();
        self.writer.sync_data().unwrap();
    }

    #[inline]
    pub(crate) fn max_key(&self) -> &InternalKey {
        self.index_block.max_key()
    }
}

pub struct TableReadHandle {
    file_path: String,
    level: usize,
    table_id: u64,
    table_key: u64,
    hash: u32,
    status: RwLock<TableStatus>,
    min_key: InternalKey,
    max_key: InternalKey,
    kv_total: u32,
    file_size: u64,
}

unsafe impl Send for TableReadHandle {}
unsafe impl Sync for TableReadHandle {}

impl TableReadHandle {
    /// Create a table handle for existing sstable.
    pub fn open_table(db_path: &str, level: usize, table_id: u64) -> TableReadHandle {
        let file_path = format!("{}/{}/{}", db_path, level, table_id);

        let file = File::open(&file_path).unwrap();
        let file_size = file.metadata().unwrap().len();

        let mut buf_reader = BufReaderWithPos::new(file).unwrap();

        let footer = Footer::load_footer(&mut buf_reader).unwrap();
        let mut index_block = IndexBlock::load_index(&mut buf_reader, &footer);

        let min_key = std::mem::take(&mut index_block.min_key);
        let max_key = index_block.max_key().clone();

        let table_key = Self::calc_table_key(table_id, level);
        TableReadHandle {
            file_path,
            level,
            table_id,
            table_key,
            hash: Self::calc_hash(table_key),
            status: RwLock::new(TableStatus::Store),
            min_key,
            max_key,
            kv_total: footer.kv_total,
            file_size,
        }
    }

    #[inline]
    fn calc_table_key(table_id: u64, level: usize) -> u64 {
        (table_id << max_level_shift()) + level as u64
    }

    fn calc_hash(key: u64) -> u32 {
        const SEED: u32 = 0x71f2e1a3;
        murmur_hash(&key.to_le_bytes(), SEED)
    }

    /// Create [TableReadHandle] from table write handle and rename the file.
    ///
    /// # Notice
    ///
    /// position of `table_write_handle` should be at the end.
    pub fn from_table_write_handle(mut table_write_handle: TableWriteHandle) -> Self {
        let file_size = table_write_handle.writer.writer.pos;
        debug_assert!(file_size > 0);

        #[cfg(debug_assertions)]
        if std::path::Path::new(&table_write_handle.file_path).exists() {
            error!("file `{}` already exists!!", table_write_handle.file_path);
        }

        table_write_handle.rename();

        let min_key = table_write_handle.take_min_key();
        let max_key: InternalKey = table_write_handle.max_key().clone();

        let table_id = table_write_handle.table_id;
        let level = table_write_handle.level;
        let table_key = Self::calc_table_key(table_id, level);
        TableReadHandle {
            file_path: table_write_handle.file_path,
            level,
            table_id,
            table_key,
            hash: Self::calc_hash(table_key),
            status: RwLock::new(TableStatus::Store),
            min_key,
            max_key,
            kv_total: table_write_handle.writer.kv_total,
            file_size,
        }
    }

    /// Used for read sstable
    pub fn create_buf_reader_with_pos(&self) -> impl SequentialReadableFile {
        FileSystem::create_seq_readable_file((&self.file_path).as_ref()).unwrap()
    }

    #[inline]
    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    #[inline]
    pub fn level(&self) -> usize {
        self.level
    }

    #[inline]
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    #[inline]
    pub fn kv_total(&self) -> u32 {
        self.kv_total
    }

    #[inline]
    pub fn table_key(&self) -> u64 {
        self.table_key
    }

    #[inline]
    pub fn hash(&self) -> u32 {
        self.hash
    }

    pub fn status(&self) -> TableStatus {
        let guard = self.status.read().unwrap();
        *guard.deref()
    }

    /// Query value by `key` with `cache`
    pub fn query_sstable_with_cache(
        &self,
        key: &InternalKey,
        cache: &mut TableCache,
    ) -> Option<Value> {
        if cache.filter.may_contain(key) {
            if let Some((offset, length, index_offset)) = cache.index.may_contain_key(key) {
                return match cache.start_data_block_map.get(&offset) {
                    Some(data_block) => data_block.get_value(key),
                    None => {
                        let mut buf_reader = self.create_buf_reader_with_pos();
                        let data_block =
                            DataBlock::from_reader(&mut buf_reader, offset, length, index_offset);
                        let option = data_block.get_value(key);
                        cache.start_data_block_map.insert(offset, data_block);
                        option
                    }
                };
            }
        }
        None
    }

    /// Query value by `key` and insert cache into `lru_cache`.
    pub fn query_sstable(
        &self,
        key: &InternalKey,
        lru_cache: &Arc<ShardLRUCache<u64, TableCache>>,
    ) -> Option<Value> {
        let mut buf_reader = self.create_buf_reader_with_pos();
        let footer = Footer::load_footer(&mut buf_reader).unwrap();
        let bloom_filter = load_filter_block(
            footer.index_block_offset as u64 + footer.index_block_length as u64,
            footer.filter_length as usize,
            &mut buf_reader,
        );

        if bloom_filter.may_contain(key) {
            let index_block = IndexBlock::load_index(&mut buf_reader, &footer);
            let may_contain_key = index_block.may_contain_key(key);
            let mut cache = TableCache::new(bloom_filter, index_block);

            let option = if let Some((offset, length, index_offset)) = may_contain_key {
                let mut data_block =
                    DataBlock::from_reader(&mut buf_reader, offset, length, index_offset);
                let option = data_block.get_value(key);
                cache.start_data_block_map.insert(offset, data_block);
                option
            } else {
                None
            };
            lru_cache.insert_no_exists(self.table_key, cache, self.hash);
            option
        } else {
            None
        }
    }

    /// Query all the key-value pairs in [`key_start`, `key_end`] and insert them into `kvs`
    /// Return whether table_read_handle is overlapping with [`key_start`, `key_end`]
    pub fn range_query<UK: MemKey>(
        &self,
        key_start: &InternalKey,
        key_end: &InternalKey,
        kvs: &mut SkipMap<UK, Value, false>,
    ) -> bool {
        if self.is_overlapping(key_start, key_end) {
            let mut buf_reader = self.create_buf_reader_with_pos();
            let footer = Footer::load_footer(&mut buf_reader).unwrap();
            let index_block = IndexBlock::load_index(&mut buf_reader, &footer);
            let data_blocks = index_block.find_all_ge(key_start);
            let mut remain = false;
            for (offset, length, index_offset, _key_length, max_key) in data_blocks {
                if max_key > key_end {
                    break;
                }
                let mut data_block =
                    DataBlock::from_reader(&mut buf_reader, *offset, *length, *index_offset);
                remain |= data_block.get_all_record_le(key_end, kvs);
            }
            return remain;
        }
        false
    }

    /// Check whether status of sstable is `Store`.
    /// If it is, change the status to `Compacting` and return true; or else return false.
    pub fn test_and_set_compacting(&self) -> bool {
        let mut guard = self.status.write().unwrap();
        if *guard.deref() == TableStatus::Store {
            *guard = TableStatus::Compacting;
            true
        } else {
            false
        }
    }

    pub(super) fn ready_to_delete(&self) {
        let mut guard = self.status.write().unwrap();
        debug_assert_eq!(*guard, TableStatus::Compacting, "invalid table status");
        *guard = TableStatus::ToDelete;
    }

    pub(crate) fn readable(&self) -> bool {
        let guard = self.status.read().unwrap();
        *guard != TableStatus::ToDelete
    }

    #[inline]
    pub fn min_max_key(&self) -> (&InternalKey, &InternalKey) {
        (&self.min_key, &self.max_key)
    }

    #[inline]
    pub fn min_key(&self) -> &InternalKey {
        &self.min_key
    }

    #[inline]
    pub fn max_key(&self) -> &InternalKey {
        &self.max_key
    }

    ///```text
    /// ----         ------      -----    ----
    ///   |---|       |--|     |---|    |------|
    ///```
    pub fn is_overlapping(&self, min_key: &InternalKey, max_key: &InternalKey) -> bool {
        self.min_key.le(min_key) && min_key.le(&self.max_key)
            || self.min_key.le(max_key) && max_key.le(&self.max_key)
            || min_key.le(&self.min_key) && self.max_key.le(max_key)
    }

    pub fn iter(&self) -> Iter {
        Iter::new(self)
    }
}

impl Drop for TableReadHandle {
    fn drop(&mut self) {
        if let TableStatus::ToDelete = self.status() {
            std::fs::remove_file(&self.file_path).unwrap();
        }
    }
}

pub(crate) fn temp_file_name(file_name: &str) -> String {
    format!("{}_write", file_name)
}

pub struct Iter<'table> {
    reader: Box<dyn SequentialReadableFile>,
    max_key: &'table InternalKey,
    index_block: IndexBlock,
    data_block: DataBlockIter,
    cur_data_block_idx: usize,
}

impl<'table> Iter<'table> {
    fn new(handle: &'table TableReadHandle) -> Iter<'table> {
        let mut reader = Box::new(handle.create_buf_reader_with_pos());
        let footer = Footer::load_footer(&mut reader).unwrap();
        let index_block = IndexBlock::load_index(&mut reader, &footer);

        let index = &index_block.indexes[0];
        let data_block = DataBlock::from_reader(&mut reader, index.0, index.1, index.2);

        Iter {
            reader,
            max_key: &handle.max_key,
            index_block,
            data_block: data_block.into_iter(),
            cur_data_block_idx: 0,
        }
    }

    #[inline]
    pub fn end(&self) -> bool {
        self.cur_data_block_idx == self.index_block.indexes.len()
    }
}

impl<'table> Iterator for Iter<'table> {
    type Item = (InternalKey, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.end() {
            None
        } else {
            match self.data_block.next() {
                Some(item) => Some(item),
                None => {
                    self.cur_data_block_idx += 1;
                    if self.end() {
                        None
                    } else {
                        let index = &self.index_block.indexes[self.cur_data_block_idx];
                        let data_block =
                            DataBlock::from_reader(&mut self.reader, index.0, index.1, index.2);
                        self.data_block = data_block.into_iter();
                        self.next()
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::Range;

    use crate::sstable::data_block::DataBlock;
    use crate::sstable::footer::Footer;
    use crate::sstable::index_block::IndexBlock;
    use crate::sstable::table_handle::{TableReadHandle, TableWriteHandle};

    pub(crate) fn create_write_handle(
        db_path: &str,
        level: usize,
        table_id: u64,
        range: Range<i32>,
    ) -> TableWriteHandle {
        let kv_total: u32 = (range.end - range.start) as u32;
        let mut write_handle = TableWriteHandle::new(db_path, level, table_id, kv_total);

        let mut kvs = vec![];
        for i in range {
            kvs.push((
                format!("key{:02}", i).into_bytes(),
                format!("value{:02}_{}", i, level).into_bytes(),
            ));
        }
        write_handle.write_sstable_from_vec(kvs).unwrap();
        write_handle
    }

    pub(crate) fn create_read_handle(
        db_path: &str,
        level: usize,
        table_id: u64,
        range: Range<i32>,
    ) -> TableReadHandle {
        let write_handle = create_write_handle(db_path, level, table_id, range);
        write_handle.rename();
        TableReadHandle::open_table(db_path, level, table_id)
    }

    #[test]
    fn test_handle() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("1")).unwrap();
        let path = temp_dir.path().to_str().unwrap().to_string();

        let read_handle = create_read_handle(&path, 1, 1, 0..100);
        assert_eq!(read_handle.table_key(), 9);
        assert_eq!(read_handle.min_key(), "key00".as_bytes());
        assert_eq!(read_handle.max_key(), "key99".as_bytes());
        for (i, kv) in read_handle.iter().enumerate() {
            assert_eq!(
                kv,
                (
                    format!("key{:02}", i).into_bytes(),
                    format!("value{:02}_1", i).into_bytes()
                )
            );
        }

        // test data_block
        let mut reader = read_handle.create_buf_reader_with_pos();
        let footer = Footer::load_footer(&mut reader).unwrap();
        let index_block = IndexBlock::load_index(&mut reader, &footer);
        assert_eq!(index_block.indexes.len(), 1);
        for index in index_block.indexes {
            let data_block = DataBlock::from_reader(&mut reader, index.0, index.1, index.2);
            for i in 0..100 {
                let res = data_block.get_value(&Vec::from(format!("key{:02}", i)));
                assert_eq!(
                    Some(Vec::from(format!("value{:02}_1", i))),
                    res,
                    "error: {}",
                    i
                );
            }
            for s in ["key1", "key", "key100", "key-1"] {
                let res = data_block.get_value(&Vec::from(s));
                assert!(res.is_none());
            }

            for (i, (k, v)) in data_block.into_iter().enumerate() {
                assert_eq!(format!("key{:02}", i), String::from_utf8(k).unwrap());
                assert_eq!(format!("value{:02}_1", i), String::from_utf8(v).unwrap());
            }
        }
    }
}
