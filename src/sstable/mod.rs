//! Sorted String Table, which is stored in disk.
//!
//! # SSTable
//!
//! A SSTable is stored in a file named "<version>.sst", where <version> is a Base 62 number(A-Z a-z 0-9).
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
//! ## Key/Value Entry
//!
//! ```text
//! +-----------------------------------------+
//! | key length | value length | key | value |
//! +-----------------------------------------+
//! \-----------/\-------------/\-----/\------/
//!      u32           u32      var-len var-len
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
//! ## Index Block
//!
//! ```text
//! +----------------------------+
//! | offset | length | max key1 | -> Data Block1
//! +----------------------------+
//! | offset | length | max key2 | -> Data BLock2
//! +----------------------------+
//! |           ...              |
//! +----------------------------+
//! \-------/\-------/\----------/
//!    u32      u32     var-len
//! ```
//!
//! ## Footer
//!
//! Length of Footer is fixed.
//!
//! ```text
//! +----------------------------------------------------+
//! | Index Block Start Offset | Magic Number 0xdb991122 |
//! +----------------------------------------------------+
//! \-------------------------/
//!            u32
//! ```
//!
//! NOTE: All fixed-length integer are little-endian.

pub(crate) mod footer;
pub(crate) mod index_block;
pub mod table;

pub struct SSTableWriter {}

impl Default for SSTableWriter {
    fn default() -> Self {
        SSTableWriter {}
    }
}
