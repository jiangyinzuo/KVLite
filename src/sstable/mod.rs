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

use crate::ioutils::{read_string_exact, read_u32, BufReaderWithPos};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Seek, SeekFrom};

mod compact;
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

fn get_value_from_data_block(
    reader: &mut BufReaderWithPos<File>,
    key: &str,
    start: u32,
    length: u32,
) -> Option<String> {
    reader.seek(SeekFrom::Start(start as u64)).unwrap();
    let mut offset = 0u32;
    while offset < length {
        let key_length = read_u32(reader);
        let value_length = read_u32(reader);
        let key_read = read_string_exact(reader, key_length);
        match key.cmp(&key_read) {
            Ordering::Less => return None,
            Ordering::Equal => return Some(read_string_exact(reader, value_length)),
            Ordering::Greater => {
                reader.seek(SeekFrom::Current(value_length as i64)).unwrap();
            }
        }
        offset += 8 + key_length + value_length;
    }
    None
}

pub fn sstable_file(db_path: &String, level: u32, table_id: u128) -> String {
    format!("{}/{}/{}", db_path, level, table_id)
}
