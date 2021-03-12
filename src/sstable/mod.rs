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
//! +----------------------------------------------------+
//! | Index Block Start Offset | Magic Number 0xdb991122 |
//! +----------------------------------------------------+
//! \-------------------------/\-------------------------/
//!            u32                         u32
//! ```
//!
//! NOTE: All fixed-length integer are little-endian.

use crate::ioutils::{read_string_exact, read_u32, BufWriterWithPos};
use crate::sstable::footer::Footer;
use crate::sstable::index_block::{IndexBlock, SSTableIndex};
use crate::Result;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::RwLock;

pub(crate) mod footer;
pub(crate) mod index_block;
pub mod table;

pub const MIN_BLOCK_SIZE: u64 = 2 << 12;

/// The collection of all the Versions produced
pub struct SSTableManager {
    db_path: String,
    next_sstable_id: RwLock<u64>,
    level0_sstables: BTreeSet<OsString>,
}

impl SSTableManager {
    pub fn new(db_path: String) -> SSTableManager {
        let level0_path = PathBuf::from(format!("{}/0", db_path));
        let dir = std::fs::read_dir(level0_path).unwrap();
        let level0_sstables: BTreeSet<OsString> = dir.map(|d| d.unwrap().file_name()).collect();

        SSTableManager {
            db_path,
            next_sstable_id: RwLock::default(),
            level0_sstables,
        }
    }

    /// Persistently write the immutable memory table to level0 sstable.
    pub fn write_level0_sstable(
        &self,
        mem_table_iter: &mut dyn Iterator<Item = (&String, &String)>,
    ) -> crate::Result<()> {
        let next_sstable_id = self.get_next_sstable_id();
        let sstable_path = sstable_path(&self.db_path, 0, next_sstable_id);
        let mut writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&sstable_path)?,
        )?;
        writer.seek(SeekFrom::Start(0))?;

        let mut last_pos = 0;
        let mut index_block = IndexBlock::default();

        // write Data Blocks
        for (k, v) in mem_table_iter {
            let (k, v) = (k.as_bytes(), v.as_bytes());
            let (k_len, v_len) = (k.len() as u32, v.len() as u32);
            writer.write_all(&k_len.to_be_bytes())?;
            writer.write_all(&v_len.to_be_bytes())?;
            writer.write_all(k)?;
            writer.write_all(v)?;
            if writer.pos - last_pos >= MIN_BLOCK_SIZE {
                index_block.add_index(writer.pos as u32, (writer.pos - last_pos) as u32, k);
                last_pos = writer.pos;
            }
        }

        let index_block_offset = last_pos as u32;

        index_block.write_to_file(&mut writer)?;

        let footer = Footer {
            index_block_offset,
            index_block_length: writer.pos as u32 - index_block_offset,
        };

        // write footer
        footer.write_to_file(&mut writer)?;
        Ok(())
    }

    fn get_next_sstable_id(&self) -> u64 {
        let mut lock_guard = self.next_sstable_id.write().unwrap();
        let id = *lock_guard;
        *lock_guard += 1;
        id
    }

    /// Query sstable
    pub fn get(&self, key: &String) -> Result<Option<String>> {
        self.query_level0_sstable(key)?;
        Ok(None)
    }

    fn query_level0_sstable(&self, key: &String) -> Result<Option<String>> {
        // traverse all the level0 sstables
        for filename in &self.level0_sstables {
            let mut file = std::fs::File::open(filename)?;
            let sstable_index = SSTableIndex::load_index(&mut file)?;

            if let Some((offset, length)) = sstable_index.may_contain_key(key) {
                let option = data_block_get_value(&mut file, key, offset, length)?;
                if option.is_some() {
                    return Ok(option);
                }
            }
        }
        Ok(None)
    }
}

fn sstable_path(db_path: &str, level: i32, sstable_id: u64) -> String {
    format!("{}/{}/{}.sst", db_path, level, sstable_id)
}

fn data_block_get_value(
    reader: &mut (impl Read + Seek),
    key: &str,
    start: u32,
    length: u32,
) -> Result<Option<String>> {
    reader.seek(SeekFrom::Start(start as u64))?;
    let mut offset = 0u32;
    while offset < length {
        let key_length = read_u32(reader)?;
        let value_length = read_u32(reader)?;
        let key_read = read_string_exact(reader, key_length)?;
        match key.cmp(&key_read) {
            Ordering::Less => unreachable!(),
            Ordering::Equal => return Ok(Some(read_string_exact(reader, value_length)?)),
            Ordering::Greater => {
                reader.seek(SeekFrom::Current(value_length as i64))?;
            }
        }
        offset += 8 + key_length + value_length;
    }
    Ok(None)
}
