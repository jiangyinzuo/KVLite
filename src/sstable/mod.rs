//! Sorted String Table, which is stored in disk.
//!
//! # SSTable
//!
//!
//! ```text
//! +-------------------------+ (offset 0)
//! | Data Block 1            |<-+
//! +-------------------------+  |
//! | Data Block 2            |<-+-+
//! +-------------------------+  | |
//! | ...                     |  | |
//! +-------------------------+  | |
//! | Data Block n            |<-+ |
//! +-------------------------+    |
//! | Index Block             |----+<-+
//! +-------------------------+       |
//! | Filter Block            |<-+    |
//! +-------------------------+  |    |
//! | Footer                  |--+----+
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
//! +-------------------------------+
//! | min_key length(u32) | min_key |
//! +---------------------------------------------------------+
//! | offset | length | index_offset | key1 length | max key1 | -> Data Block1
//! +---------------------------------------------------------+
//! | offset | length | index_offset | key2 length | max key2 | -> Data BLock2
//! +---------------------------------------------------------+
//! |                            ...                          |
//! +---------------------------------------------------------+
//! \-------/\-------/\------------/\-------------/\----------/
//!    u32      u32         u32            u32       var-len
//! ```
//!
//! ## Filter Block
//!
//! ```text
//! +---------------------------------+
//! | FilterBlock length | bit vector |
//! +---------------------------------+
//! ```
//!
//! ## Footer
//!
//! Length of Footer is fixed (64bit).
//!
//! ```text
//! +--------------------------------------------------------------------------------------------+
//! | IndexBlock offset | IndexBlock length | filter length | kv_total | Magic Number 0xdb991122 |
//! +--------------------------------------------------------------------------------------------+
//! \------------------/\-------------------/\-------------/\----------/\------------------------/
//!         u32                  u32             u32            u32               u32
//! ```
//!
//! NOTE: All fixed-length integer are little-endian.

pub(super) mod data_block;
pub(super) mod filter_block;
pub(crate) mod footer;
pub(crate) mod index_block;
pub mod manager;
mod table_cache;
pub mod table_handle;

pub type TableID = u64;

pub const DATA_BLOCK_SIZE: usize = 4096;
pub const NUM_LEVEL0_TABLE_TO_COMPACT: usize = 4;

pub fn sstable_file(db_path: &str, level: u32, table_id: u128) -> String {
    format!("{}/{}/{}", db_path, level, table_id)
}
