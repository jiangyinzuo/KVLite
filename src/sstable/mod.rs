//! Sorted String Table, which is stored in disk.
//!
//! # SSTable
//!
//!
//! ```text
//! +-------------------------+ (offset 0)
//! | Data Block 1            |<-+
//! +-------------------------+  |
//! | Data Block 2            |<-+
//! +-------------------------+  |
//! | ...                     |  |
//! +-------------------------+  |
//! | Data Block n            |<-+
//! +-------------------------+  |
//! | Index Block             |--+
//! +-------------------------+
//! | Footer                  |
//! +-------------------------+
//! ```
//!
//! ## Data Block
//!
//! ```text
//! +-----------------------------------------------------------------+
//! | Key/Value Entry 1 | Key/Value Entry 2 | ... | Key/Value Entry n |
//! +-----------------------------------------------------------------+
//! ```
//!
//! ### Key/Value Entry
//!
//! ```text
//! +-----------------------------------------+
//! | key length | value length | key | value |
//! +-----------------------------------------+
//! \-----------/\-------------/\-----/\------/
//!      u32           u32      var-len var-len
//! ```
//!
//! ## Index Block
//!
//! ```text
//! +------------------------------------------+
//! | offset | length | key1 length | max key1 | -> Data Block1
//! +------------------------------------------+
//! | offset | length | key2 length | max key2 | -> Data BLock2
//! +------------------------------------------+
//! |                   ...                    |
//! +------------------------------------------+
//! \-------/\-------/\------------/\----------/
//!    u32      u32         u32       var-len
//! ```
//!
//! ## Footer
//!
//! Length of Footer is fixed (64bit).
//!
//! ```text
//! +-----------------------------------------------------------------+
//! | IndexBlock offset | IndexBlock length | Magic Number 0xdb991122 |
//! +-----------------------------------------------------------------+
//! \------------------/\-------------------/\------------------------/
//!         u32                  u32                    u32
//! ```
//!
//! NOTE: All fixed-length integer are little-endian.

use crate::ioutils::{read_string_exact, read_u32, BufReaderWithPos, BufWriterWithPos};
use crate::sstable::footer::write_footer;
use crate::sstable::index_block::IndexBlock;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

mod compact;
pub(super) mod data_block;
pub(crate) mod footer;
pub(crate) mod index_block;
pub mod level0_table;
pub(crate) mod manager;
pub(crate) mod table_handle;

pub const MAX_BLOCK_KV_PAIRS: u64 = 5;
pub const NUM_LEVEL0_TABLE_TO_COMPACT: usize = 2;

fn get_min_key(reader: &mut BufReaderWithPos<File>) -> String {
    reader.seek(SeekFrom::Start(0)).unwrap();
    let key_length = read_u32(reader);
    // value_length
    reader.seek(SeekFrom::Current(4)).unwrap();
    read_string_exact(reader, key_length)
}

pub fn sstable_file(db_path: &String, level: u32, table_id: u128) -> String {
    format!("{}/{}/{}", db_path, level, table_id)
}

pub(crate) struct TableWriter<'a> {
    pub count: u64,
    pub last_pos: u64,
    pub index_block: IndexBlock<'a>,
    pub writer: BufWriterWithPos<File>,
}

impl<'a> TableWriter<'a> {
    pub(crate) fn new(writer: BufWriterWithPos<File>) -> TableWriter<'a> {
        TableWriter {
            count: 0,
            last_pos: 0,
            index_block: IndexBlock::default(),
            writer,
        }
    }

    pub(crate) fn write_key_value(&mut self, k: &String, v: &String) {
        let (k, v) = (k.as_bytes(), v.as_bytes());
        let (k_len, v_len) = (k.len() as u32, v.len() as u32);

        // length of key | length of value | key | value
        self.writer.write_all(&k_len.to_le_bytes()).unwrap();
        self.writer.write_all(&v_len.to_le_bytes()).unwrap();
        self.writer.write_all(k).unwrap();
        self.writer.write_all(v).unwrap();
        self.count += 1;
    }

    pub(crate) fn add_index(&mut self, max_key: &'a String) {
        self.index_block.add_index(
            self.last_pos as u32,
            (self.writer.pos - self.last_pos) as u32,
            max_key,
        );
        self.last_pos = self.writer.pos;
        self.count = 0;
    }

    pub(crate) fn write_key_value_and_try_add_index(&mut self, k: &'a String, v: &String) {
        self.write_key_value(k, v);
        if self.count == MAX_BLOCK_KV_PAIRS {
            self.add_index(k);
        }
    }

    pub(crate) fn write_index_and_footer(&mut self) {
        let index_block_offset = self.last_pos as u32;
        self.index_block.write_to_file(&mut self.writer).unwrap();
        write_footer(index_block_offset, &mut self.writer);
        self.writer.flush().unwrap();
    }
}
