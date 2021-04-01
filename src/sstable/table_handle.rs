use crate::ioutils::{BufReaderWithPos, BufWriterWithPos};
use crate::sstable::index_block::SSTableIndex;
use crate::sstable::{get_min_key, get_value_from_data_block};
use std::fs::{File, OpenOptions};
use std::ops::Deref;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Copy, Clone, PartialEq)]
pub enum TableStatus {
    /// Normally store in disk.
    Store,
    /// The sstable is merging to the next level.
    Compacting,
    /// Remove file when [TableHandle] is dropped.
    ToDelete,
}

pub struct TableHandle {
    file_path: String,
    level: u8,
    table_id: u128,
    status: RwLock<TableStatus>,
    min_key: String,
    max_key: String,
    /// Ensure `file` has one writer or multiple readers.
    rw_lock: RwLock<()>,
}

unsafe impl Send for TableHandle {}
unsafe impl Sync for TableHandle {}

impl TableHandle {
    /// Create a table handle for existing sstable.
    pub fn open_table(db_path: &str, level: u8, table_id: u128) -> TableHandle {
        let file_path = format!("{}/{}/{}", db_path, level, table_id);

        let file = File::open(&file_path).unwrap();
        let mut buf_reader = BufReaderWithPos::new(file).unwrap();

        let sstable_index = SSTableIndex::load_index(&mut buf_reader);

        let min_key = get_min_key(&mut buf_reader);
        let max_key = sstable_index.max_key().to_string();

        let handle = TableHandle {
            file_path,
            level,
            table_id,
            status: RwLock::new(TableStatus::Store),
            min_key,
            max_key,
            rw_lock: RwLock::default(),
        };
        handle
    }

    /// Create a table handle for new sstable.
    pub fn new(
        db_path: &str,
        level: u8,
        table_id: u128,
        min_key: &str,
        max_key: &str,
    ) -> TableHandle {
        let file_path = format!("{}/{}/{}", db_path, level, table_id);
        TableHandle {
            file_path,
            level,
            table_id,
            status: RwLock::new(TableStatus::Store),
            min_key: min_key.to_string(),
            max_key: max_key.to_string(),
            rw_lock: RwLock::default(),
        }
    }

    /// Used for read sstable
    pub fn create_buf_reader_with_pos(&self) -> (RwLockReadGuard<()>, BufReaderWithPos<File>) {
        let lock = self.rw_lock.read().unwrap();
        let file = File::open(&self.file_path).unwrap();
        (lock, BufReaderWithPos::new(file).unwrap())
    }

    /// Used for write sstable
    pub fn create_buf_writer_with_pos(&self) -> (RwLockWriteGuard<()>, BufWriterWithPos<File>) {
        let lock = self.rw_lock.write().unwrap();
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .append(true)
            .open(&self.file_path)
            .unwrap();
        (lock, BufWriterWithPos::new(file).unwrap())
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
        let (_sstable_guard, mut buf_reader) = self.create_buf_reader_with_pos();
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

    #[inline]
    pub fn min_max_key(&self) -> (&String, &String) {
        (&self.min_key, &self.max_key)
    }

    pub fn is_overlapping(&self, min_key: &String, max_key: &String) -> bool {
        self.min_key.le(min_key) && min_key.le(&self.max_key)
            || self.min_key.le(max_key) && max_key.le(&self.max_key)
    }
}

impl Drop for TableHandle {
    fn drop(&mut self) {
        if let TableStatus::ToDelete = self.status() {
            std::fs::remove_file(&self.file_path).unwrap();
        }
    }
}
