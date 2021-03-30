use crate::ioutils::{BufReaderWithPos, BufWriterWithPos};
use std::fs::{File, OpenOptions};
use std::ops::{Deref, DerefMut};

/// Remove file when `TableHandle` is dropped.
pub struct TableHandle {
    file_path: String,
    level: u8,
    table_id: u128,
    file: File,
}

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
            file,
        }
    }

    /// Used for read sstable
    pub fn create_buf_reader_with_pos(&self) -> BufReaderWithPos<File> {
        BufReaderWithPos::new(self.file.try_clone().unwrap()).unwrap()
    }

    /// Used for write sstable
    pub fn create_buf_writer_with_pos(&self) -> BufWriterWithPos<File> {
        BufWriterWithPos::new(self.file.try_clone().unwrap()).unwrap()
    }

    #[inline]
    pub fn table_id(&self) -> u128 {
        self.table_id
    }
}
