use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use crate::bloom::BloomFilter;
use crate::cache::ShardLRUCache;
use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{InternalKey, MemKey};
use crate::db::{max_level_shift, Value};
use crate::hash::murmur_hash;
use crate::ioutils::{BufReaderWithPos, BufWriterWithPos};
use crate::memory::InternalKeyValueIterator;
use crate::sstable::data_block::{get_next_key_value, get_value_from_data_block};
use crate::sstable::filter_block::{load_filter_block, write_filter_block};
use crate::sstable::footer::{write_footer, Footer};
use crate::sstable::index_block::IndexBlock;
use crate::sstable::table_cache::IndexCache;
use crate::sstable::{get_min_key, MAX_BLOCK_KV_PAIRS};

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
    pub(crate) fn new(
        db_path: &str,
        level: usize,
        table_id: u64,
        kv_total: u32,
    ) -> TableWriteHandle {
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
            self.writer.write_key_value(k, v);
            if self.writer.count == MAX_BLOCK_KV_PAIRS || i == table.len() - 1 {
                self.writer.add_index(k.clone());
            }
        }
        self.writer.write_index_filter_footer();
        Ok(())
    }

    pub fn write_sstable_from_vec_ref(
        &mut self,
        kvs: Vec<(&InternalKey, &Value)>,
    ) -> crate::Result<()> {
        // write Data Blocks
        for (i, (k, v)) in kvs.iter().enumerate() {
            self.writer.write_key_value(*k, *v);
            if self.writer.count == MAX_BLOCK_KV_PAIRS || i == kvs.len() - 1 {
                self.writer.add_index((*k).clone());
            }
        }
        self.writer.write_index_filter_footer();
        Ok(())
    }

    pub fn write_sstable_from_vec(&mut self, kvs: Vec<(InternalKey, Value)>) -> crate::Result<()> {
        // write Data Blocks
        let length = kvs.len();
        for (i, (k, v)) in kvs.into_iter().enumerate() {
            self.writer.write_key_value(&k, &v);
            if self.writer.count == MAX_BLOCK_KV_PAIRS || i == length - 1 {
                self.writer.add_index(k);
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
    pub fn max_key(&self) -> &InternalKey {
        self.writer.max_key()
    }
}

pub(crate) struct TableWriter {
    pub(crate) kv_total: u32,
    #[cfg(debug_assertions)]
    kv_count: u32,

    pub(crate) count: u64,
    pub(crate) last_pos: u64,
    pub(crate) index_block: IndexBlock,
    pub(crate) writer: BufWriterWithPos<File>,
    filter: BloomFilter,
}

impl TableWriter {
    fn new(writer: BufWriterWithPos<File>, kv_total: u32) -> TableWriter {
        TableWriter {
            kv_total,
            #[cfg(debug_assertions)]
            kv_count: 0,
            count: 0,
            last_pos: 0,
            index_block: IndexBlock::default(),
            writer,
            filter: BloomFilter::create_filter(kv_total as usize),
        }
    }

    pub(crate) fn write_key_value(&mut self, k: &InternalKey, v: &InternalKey) {
        debug_assert!(!k.is_empty(), "attempt to write empty key");

        let (k_len, v_len) = (k.len() as u32, v.len() as u32);

        // length of key | length of value | key | value
        self.writer.write_all(&k_len.to_le_bytes()).unwrap();
        self.writer.write_all(&v_len.to_le_bytes()).unwrap();
        self.writer.write_all(k).unwrap();
        self.writer.write_all(v).unwrap();

        self.count += 1;

        #[cfg(debug_assertions)]
        {
            self.kv_count += 1;
        }

        self.filter.add(k);
        debug_assert!(self.filter.may_contain(k));
    }

    pub(crate) fn add_index(&mut self, max_key: InternalKey) {
        self.index_block.add_index(
            self.last_pos as u32,
            (self.writer.pos - self.last_pos) as u32,
            max_key,
        );
        self.last_pos = self.writer.pos;
        self.count = 0;
    }

    pub(crate) fn write_index_filter_footer(&mut self) {
        let index_block_offset = self.last_pos as u32;
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
        let index_block = IndexBlock::load_index(&mut buf_reader, &footer);

        let min_key = get_min_key(&mut buf_reader);
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
    pub fn from_table_write_handle(table_write_handle: TableWriteHandle) -> Self {
        let file_size = table_write_handle.writer.writer.pos;
        debug_assert!(file_size > 0);

        #[cfg(debug_assertions)]
        if std::path::Path::new(&table_write_handle.file_path).exists() {
            error!("file `{}` already exists!!", table_write_handle.file_path);
        }

        table_write_handle.rename();
        let file = File::open(&table_write_handle.file_path).unwrap();

        let mut buf_reader = BufReaderWithPos::new(file).unwrap();
        let min_key = get_min_key(&mut buf_reader);
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
    pub fn create_buf_reader_with_pos(&self) -> BufReaderWithPos<File> {
        let mut file = File::open(&self.file_path)
            .unwrap_or_else(|e| panic!("{:#?}\n\n file_path: {}", e, &self.file_path));
        file.seek(SeekFrom::Start(0)).unwrap();
        BufReaderWithPos::new(file).unwrap()
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
    pub fn query_sstable_with_cache(&self, key: &InternalKey, cache: &IndexCache) -> Option<Value> {
        if cache.filter.may_contain(key) {
            if let Some((offset, length)) = cache.index.may_contain_key(key) {
                let mut buf_reader = self.create_buf_reader_with_pos();
                let option = get_value_from_data_block(&mut buf_reader, key, offset, length);
                return option;
            }
        }
        None
    }

    /// Query value by `key` and insert cache into `lru_cache`.
    pub fn query_sstable(
        &self,
        key: &InternalKey,
        lru_cache: &Arc<ShardLRUCache<u64, IndexCache>>,
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
            let cache = IndexCache {
                filter: bloom_filter,
                index: index_block,
            };
            lru_cache.insert_no_exists(self.table_key, cache, self.hash);
            if let Some((offset, length)) = may_contain_key {
                let option = get_value_from_data_block(&mut buf_reader, key, offset, length);
                return option;
            }
        }
        None
    }

    /// Query all the key-value pairs in [`key_start`, `key_end`] and insert them into `kvs`
    /// Return whether table_read_handle is overlapping with [`key_start`, `key_end`]
    pub fn range_query<UK: MemKey>(
        &self,
        key_start: &InternalKey,
        key_end: &InternalKey,
        kvs: &mut SkipMap<UK, Value>,
    ) -> bool {
        if self.is_overlapping(key_start, key_end) {
            let mut buf_reader = self.create_buf_reader_with_pos();
            let footer = Footer::load_footer(&mut buf_reader).unwrap();
            let index_block = IndexBlock::load_index(&mut buf_reader, &footer);
            if let Some(offset) = index_block.find_first_ge(key_start) {
                buf_reader.seek(SeekFrom::Start(offset as u64)).unwrap();
                while buf_reader.position() < footer.index_block_offset as u64 {
                    let (k, v) = get_next_key_value(&mut buf_reader);
                    if k.le(key_end) {
                        kvs.insert(k.into(), v);
                    } else {
                        return true;
                    }
                }
            }
            return true;
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
    reader: BufReaderWithPos<File>,
    max_key: &'table InternalKey,
    end: bool,
}

impl<'table> Iter<'table> {
    fn new(handle: &'table TableReadHandle) -> Iter<'table> {
        let reader = handle.create_buf_reader_with_pos();
        Iter {
            reader,
            max_key: &handle.max_key,
            end: false,
        }
    }

    #[inline]
    pub fn end(&self) -> bool {
        self.end
    }
}

impl<'table> Iterator for Iter<'table> {
    type Item = (InternalKey, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.end {
            None
        } else {
            let (k, v) = get_next_key_value(&mut self.reader);
            if k.eq(self.max_key) {
                self.end = true;
            }
            Some((k, v))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::Range;

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
                format!("key{}", i).into_bytes(),
                format!("value{}_{}", i, level).into_bytes(),
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
        let path = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(path.path().join("1")).unwrap();
        let path = path.path().to_str().unwrap().to_string();

        let read_handle = create_read_handle(&path, 1, 1, 0..100);
        assert_eq!(read_handle.table_key(), 9);
        assert_eq!(read_handle.min_key(), "key0".as_bytes());
        assert_eq!(read_handle.max_key(), "key99".as_bytes());
        for (i, kv) in read_handle.iter().enumerate() {
            assert_eq!(
                kv,
                (
                    format!("key{}", i).into_bytes(),
                    format!("value{}_1", i).into_bytes()
                )
            );
        }
    }
}
