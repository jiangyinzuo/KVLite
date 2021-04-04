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

pub(super) mod data_block;
pub(crate) mod footer;
pub(crate) mod index_block;
mod level0_compact;
pub mod level0_table;
mod level_compact;
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
