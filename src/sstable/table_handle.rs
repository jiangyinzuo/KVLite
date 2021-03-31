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
    file: File,
    /// Ensure `file` has one writer or multiple readers.
    rw_lock: RwLock<()>,
}

unsafe impl Send for TableHandle {}
unsafe impl Sync for TableHandle {}

impl TableHandle {
    pub fn new(db_path: &str, level: u8, table_id: u128) -> TableHandle {
        let file_path = format!("{}/{}/{}", db_path, level, table_id);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(&file_path)
            .unwrap();
        TableHandle {
            file_path,
            level,
            table_id,
            status: RwLock::new(TableStatus::Store),
            file,
            rw_lock: RwLock::default(),
        }
    }

    /// Used for read sstable
    pub fn create_buf_reader_with_pos(&self) -> (RwLockReadGuard<()>, BufReaderWithPos<File>) {
        (
            self.rw_lock.read().unwrap(),
            BufReaderWithPos::new(self.file.try_clone().unwrap()).unwrap(),
        )
    }

    /// Used for write sstable
    pub fn create_buf_writer_with_pos(&self) -> (RwLockWriteGuard<()>, BufWriterWithPos<File>) {
        (
            self.rw_lock.write().unwrap(),
            BufWriterWithPos::new(self.file.try_clone().unwrap()).unwrap(),
        )
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
    pub async fn test_and_set_compacting(&self) -> bool {
        let mut guard = self.status.write().unwrap();
        if *guard.deref() == TableStatus::Store {
            *guard = TableStatus::Compacting;
            true
        } else {
            false
        }
    }

    pub async fn get_min_max_key(&self) -> (String, String) {
        let (_read_guard, mut buf_reader) = self.create_buf_reader_with_pos();
        let sstable_index = SSTableIndex::load_index(&mut buf_reader);
        (
            get_min_key(&mut buf_reader),
            sstable_index.max_key().to_string(),
        )
    }
}

impl Drop for TableHandle {
    fn drop(&mut self) {
        if let TableStatus::ToDelete = self.status() {
            std::fs::remove_file(&self.file_path).unwrap();
        }
    }
}
