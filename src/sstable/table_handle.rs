use crate::ioutils::{BufReaderWithPos, BufWriterWithPos};
use crate::memory::KeyValue;
use crate::sstable::data_block::{get_next_key_value, get_value_from_data_block};
use crate::sstable::index_block::SSTableIndex;
use crate::sstable::{get_min_key, TableWriter, MAX_BLOCK_KV_PAIRS};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::ops::Deref;
use std::sync::RwLock;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum TableStatus {
    /// Normally store in disk.
    Store,
    /// The sstable is merging to the next level.
    Compacting,
    /// Remove file when [TableHandle] is dropped.
    ToDelete,
}

/// Handle of new sstable for writing.
pub struct TableWriteHandle {
    file_path: String,
    level: usize,
    table_id: u128,
}

impl TableWriteHandle {
    pub(super) fn new(db_path: &str, level: usize, table_id: u128) -> TableWriteHandle {
        TableWriteHandle {
            file_path: format!("{}/{}/{}", db_path, level, table_id),
            level,
            table_id,
        }
    }

    /// Used for write sstable
    pub fn create_buf_writer_with_pos(&self) -> BufWriterWithPos<File> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(temp_file_name(&self.file_path))
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        BufWriterWithPos::new(file).unwrap()
    }

    pub fn write_sstable(&self, table: &impl KeyValue) -> crate::Result<()> {
        let writer = self.create_buf_writer_with_pos();
        let mut table_writer = TableWriter::new(writer);

        // write Data Blocks
        for (i, (k, v)) in table.kv_iter().enumerate() {
            table_writer.write_key_value(k, v);
            if table_writer.count == MAX_BLOCK_KV_PAIRS || i == table.len() - 1 {
                table_writer.add_index(k.clone());
            }
        }
        table_writer.write_index_and_footer();
        Ok(())
    }

    pub fn write_sstable_from_vec(&self, kvs: Vec<(&String, &String)>) -> crate::Result<()> {
        let writer = self.create_buf_writer_with_pos();
        let mut table_writer = TableWriter::new(writer);

        // write Data Blocks
        for (i, (k, v)) in kvs.iter().enumerate() {
            table_writer.write_key_value(k, v);
            if table_writer.count == MAX_BLOCK_KV_PAIRS || i == kvs.len() - 1 {
                table_writer.add_index(k.to_string());
            }
        }
        table_writer.write_index_and_footer();
        Ok(())
    }

    pub fn write_sstable_from_iter(
        &self,
        kv_iter: crate::collections::skip_list::skipmap::Iter<String, String>,
    ) -> crate::Result<()> {
        let writer = self.create_buf_writer_with_pos();
        let mut table_writer = TableWriter::new(writer);

        // write Data Blocks
        for (i, node) in kv_iter.enumerate() {
            let k = unsafe { &(*node).entry.key };
            let v = unsafe { &(*node).entry.value };
            table_writer.write_key_value(k, v);
            unsafe {
                if table_writer.count == MAX_BLOCK_KV_PAIRS || (*node).get_next(0).is_null() {
                    table_writer.add_index(k.to_string());
                }
            }
        }
        table_writer.write_index_and_footer();
        Ok(())
    }

    pub(crate) fn rename(&self) {
        std::fs::rename(temp_file_name(&self.file_path), &self.file_path)
            .unwrap_or_else(|e| panic!("{:#?}, file_path: {}", e, &self.file_path));
    }

    #[inline]
    pub fn level(&self) -> usize {
        self.level
    }
}

pub struct TableReadHandle {
    file_path: String,
    level: usize,
    table_id: u128,
    status: RwLock<TableStatus>,
    min_key: String,
    max_key: String,
}

unsafe impl Send for TableReadHandle {}
unsafe impl Sync for TableReadHandle {}

impl TableReadHandle {
    /// Create a table handle for existing sstable.
    pub fn open_table(db_path: &str, level: usize, table_id: u128) -> TableReadHandle {
        let file_path = format!("{}/{}/{}", db_path, level, table_id);

        let file = File::open(&file_path).unwrap();
        let mut buf_reader = BufReaderWithPos::new(file).unwrap();

        let sstable_index = SSTableIndex::load_index(&mut buf_reader);

        let min_key = get_min_key(&mut buf_reader);
        let max_key = sstable_index.max_key().to_string();

        TableReadHandle {
            file_path,
            level,
            table_id,
            status: RwLock::new(TableStatus::Store),
            min_key,
            max_key,
        }
    }

    pub fn from_table_write_handle(
        table_write_handle: TableWriteHandle,
        min_key: String,
        max_key: String,
    ) -> Self {
        TableReadHandle {
            file_path: table_write_handle.file_path,
            level: table_write_handle.level,
            table_id: table_write_handle.table_id,
            status: RwLock::new(TableStatus::Store),
            min_key,
            max_key,
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
    pub fn table_id(&self) -> u128 {
        self.table_id
    }

    pub fn status(&self) -> TableStatus {
        let guard = self.status.read().unwrap();
        *guard.deref()
    }

    /// Query value by `key`
    pub fn query_sstable(&self, key: &String) -> Option<String> {
        let mut buf_reader = self.create_buf_reader_with_pos();
        let sstable_index = SSTableIndex::load_index(&mut buf_reader);
        if let Some((offset, length)) = sstable_index.may_contain_key(key) {
            let option = get_value_from_data_block(&mut buf_reader, key, offset, length);
            return option;
        }
        None
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
        debug_assert_eq!(*guard, TableStatus::Compacting);
        *guard = TableStatus::ToDelete;
    }

    #[inline]
    pub fn min_max_key(&self) -> (&String, &String) {
        (&self.min_key, &self.max_key)
    }

    pub fn max_key(&self) -> &String {
        &self.max_key
    }

    pub fn is_overlapping(&self, min_key: &String, max_key: &String) -> bool {
        self.min_key.le(min_key) && min_key.le(&self.max_key)
            || self.min_key.le(max_key) && max_key.le(&self.max_key)
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

fn temp_file_name(file_name: &str) -> String {
    format!("{}_temp", file_name)
}

pub struct Iter<'table> {
    reader: BufReaderWithPos<File>,
    max_key: &'table str,
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
}

impl<'table> Iterator for Iter<'table> {
    /// key, value
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        if self.end {
            None
        } else {
            let (k, v) = get_next_key_value(&mut self.reader);
            if k == self.max_key {
                self.end = true;
            }
            Some((k, v))
        }
    }
}
