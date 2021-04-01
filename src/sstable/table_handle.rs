use crate::ioutils::{BufReaderWithPos, BufWriterWithPos};
use crate::memory::{KeyValue, MemTable};
use crate::sstable::data_block::{get_next_key_value, get_value_from_data_block};
use crate::sstable::footer::Footer;
use crate::sstable::index_block::{IndexBlock, SSTableIndex};
use crate::sstable::{get_min_key, MAX_BLOCK_KV_PAIRS};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::Deref;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Copy, Clone, PartialEq, Debug)]
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

    pub fn write_sstable(&self, table: &impl KeyValue) -> crate::Result<()> {
        let mut count = 0;
        let mut last_pos = 0;
        let mut index_block = IndexBlock::default();

        let (_write_guard, mut writer) = self.create_buf_writer_with_pos();

        // write Data Blocks
        for (i, (k, v)) in table.iter().enumerate() {
            let (k, v) = (k.as_bytes(), v.as_bytes());
            let (k_len, v_len) = (k.len() as u32, v.len() as u32);

            // length of key | length of value | key | value
            writer.write_all(&k_len.to_le_bytes())?;
            writer.write_all(&v_len.to_le_bytes())?;
            writer.write_all(k)?;
            writer.write_all(v)?;
            if count == MAX_BLOCK_KV_PAIRS || i == table.len() - 1 {
                index_block.add_index(last_pos as u32, (writer.pos - last_pos) as u32, k);
                last_pos = writer.pos;
                count = 0;
            } else {
                count += 1;
            }
        }

        let index_block_offset = last_pos as u32;

        index_block.write_to_file(&mut writer)?;

        // write footer
        let footer = Footer {
            index_block_offset,
            index_block_length: writer.pos as u32 - index_block_offset,
        };
        footer.write_to_file(&mut writer)?;
        writer.flush()?;
        Ok(())
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

    #[inline]
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

impl Drop for TableHandle {
    fn drop(&mut self) {
        if let TableStatus::ToDelete = self.status() {
            std::fs::remove_file(&self.file_path).unwrap();
        }
    }
}

pub struct Iter<'table> {
    read_guard: RwLockReadGuard<'table, ()>,
    reader: BufReaderWithPos<File>,
    max_key: &'table str,
    end: bool,
}

impl<'table> Iter<'table> {
    fn new(handle: &'table TableHandle) -> Iter<'table> {
        let (guard, reader) = handle.create_buf_reader_with_pos();
        Iter {
            read_guard: guard,
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
