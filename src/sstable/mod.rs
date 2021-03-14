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

use crate::db::MAX_LEVEL;
use crate::ioutils::{read_string_exact, read_u32, BufWriterWithPos};
use crate::sstable::footer::Footer;
use crate::sstable::index_block::{IndexBlock, SSTableIndex};
use crate::wal::WalWriter;
use crate::Result;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

pub(crate) mod footer;
pub(crate) mod index_block;

pub const MAX_BLOCK_KV_PAIRS: u64 = 5;
pub const LEVEL0_FILES_THRESHOLD: usize = 4;

/// The collection of all the Versions produced
pub struct SSTableManager {
    db_path: String,
    wal_writer: Arc<Mutex<WalWriter>>,
    next_sstable_id: [RwLock<u64>; MAX_LEVEL + 1],
    level0_sstables: Arc<RwLock<BTreeSet<PathBuf>>>,
    level0_to_compact: Mutex<Vec<u64>>,
    level_manifests: [Arc<RwLock<BTreeSet<Manifest>>>; MAX_LEVEL],
    handle: tokio::runtime::Handle,
}

impl SSTableManager {
    pub fn new(db_path: String, wal_writer: Arc<Mutex<WalWriter>>) -> Result<SSTableManager> {
        let level0_path = PathBuf::from(format!("{}/0", db_path));
        let dir = std::fs::read_dir(level0_path).unwrap();
        let level0_sstables: BTreeSet<PathBuf> = dir.map(|d| d.unwrap().path()).collect();
        let handle = tokio::runtime::Handle::current();
        Ok(SSTableManager {
            db_path,
            wal_writer,
            next_sstable_id: [
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
            ],
            level0_sstables: Arc::new(RwLock::new(level0_sstables)),
            level0_to_compact: Mutex::default(),
            level_manifests: [
                Arc::default(),
                Arc::default(),
                Arc::default(),
                Arc::default(),
                Arc::default(),
                Arc::default(),
                Arc::default(),
            ],
            handle,
        })
    }

    /// Persistently write the immutable memory table to level0 sstable.
    pub fn write_level0_sstable(
        &self,
        mem_table_iter: &mut dyn Iterator<Item = (&String, &String)>,
        length: usize,
    ) -> crate::Result<()> {
        let next_sstable_id = self.get_next_sstable_id(0);
        let sstable_path = sstable_path(&self.db_path, 0, next_sstable_id);
        let mut writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&sstable_path)?,
        )?;
        writer.seek(SeekFrom::Start(0))?;

        let mut count = 0;
        let mut last_pos = 0;
        let mut index_block = IndexBlock::default();

        // write Data Blocks
        for (i, (k, v)) in mem_table_iter.enumerate() {
            let (k, v) = (k.as_bytes(), v.as_bytes());
            let (k_len, v_len) = (k.len() as u32, v.len() as u32);
            writer.write_all(&k_len.to_le_bytes())?;
            writer.write_all(&v_len.to_le_bytes())?;
            writer.write_all(k)?;
            writer.write_all(v)?;
            if count == MAX_BLOCK_KV_PAIRS || i == length - 1 {
                index_block.add_index(last_pos as u32, (writer.pos - last_pos) as u32, k);
                last_pos = writer.pos;
                count = 0;
            } else {
                count += 1;
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

        {
            let mut level0_guard = self.level0_sstables.write().unwrap();
            level0_guard.insert(PathBuf::from(sstable_path));
        }
        {
            let mut guard = self.level0_to_compact.lock().unwrap();
            guard.push(next_sstable_id);
        }
        writer.flush()?;

        {
            // delete log after writing to level0 sstable
            let mut wal_guard = self.wal_writer.lock().unwrap();
            wal_guard.remove_log()?;
        }

        self.may_compact(0);
        Ok(())
    }

    fn get_next_sstable_id(&self, level: usize) -> u64 {
        debug_assert!(level <= MAX_LEVEL);
        let mut lock_guard = self.next_sstable_id[level].write().unwrap();
        let id = *lock_guard;
        *lock_guard += 1;
        id
    }

    /// Query sstable
    pub fn get(&self, key: &String) -> Result<Option<String>> {
        let option = self.query_level0_sstable(key)?;
        if option.is_some() {
            return Ok(option);
        }
        Ok(None)
    }

    fn query_level0_sstable(&self, key: &String) -> Result<Option<String>> {
        // traverse all the level0 sstables
        let level0_guard = self.level0_sstables.read().unwrap();
        for file_path in level0_guard.iter() {
            let mut file = std::fs::File::open(file_path)?;
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

    /// Check if the level need compacting and run a thread if needed.
    fn may_compact(&self, level: usize) {
        let mut to_compact_guard = self.level0_to_compact.lock().unwrap();
        if level == 0 && to_compact_guard.len() > LEVEL0_FILES_THRESHOLD {
            let level0_sstables = self.level0_sstables.clone();
            let to_compact: Vec<u64> = std::mem::take(to_compact_guard.as_mut());
            let manifest = self.level_manifests.get(level).unwrap().clone();
            self.handle.spawn(async move {
                // TODO
                debug!("compact level {}: {:?}", level, to_compact);
                Self::compact_level0(manifest, to_compact);
            });
        }
    }

    fn compact_level0(manifest: Arc<RwLock<BTreeSet<Manifest>>>, to_compact: Vec<u64>) {
        let read_guard = manifest.read().unwrap();
        if read_guard.is_empty() {}
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
            Ordering::Less => return Ok(None),
            Ordering::Equal => return Ok(Some(read_string_exact(reader, value_length)?)),
            Ordering::Greater => {
                reader.seek(SeekFrom::Current(value_length as i64))?;
            }
        }
        offset += 8 + key_length + value_length;
    }
    Ok(None)
}

#[derive(Ord, PartialOrd, PartialEq, Eq)]
struct Manifest {
    pub sstable_id: u32,
    pub min_key: String,
}
