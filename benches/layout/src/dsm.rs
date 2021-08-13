use crate::setup;
use memmap::{Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::{TrustedRandomAccess, TrustedRandomAccessNoCoerce};
use std::ops::Range;
use tempfile::TempDir;

pub fn setup_column_major() -> (TempDir, Vec<DSMHandle>) {
    let (_temp_dir, db_path) = setup();
    let mut dsms = vec![];
    for i in 1..=4 {
        let dsm = DSMHandle::new(&db_path, 1, i, 0..40000);
        dsms.push(dsm);
    }
    (_temp_dir, dsms)
}

pub struct DSMHandle {
    level: usize,
    table_id: u64,
    length: usize,
    file: File,
    mmap: Mmap,
}

impl DSMHandle {
    pub fn new(db_path: &str, level: usize, table_id: u64, range: Range<u64>) -> DSMHandle {
        let file_path = format!("{}/{}/{}", db_path, level, table_id);
        let length = range.size();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        for pk in range {
            file.write_all(&pk.to_le_bytes()).unwrap();
        }
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };

        DSMHandle {
            level,
            table_id,
            length,
            file,
            mmap,
        }
    }

    pub fn mmap_single_query(&mut self, pk: u64) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.clone_from_slice(&self.mmap[pk as usize * 8..(pk as usize + 1) * 8]);
        u64::from_le_bytes(bytes)
    }

    pub fn single_query(&mut self, pk: u64) -> u64 {
        let mut buffer = [0u8; 8];
        self.file.seek(SeekFrom::Start(pk * 8)).unwrap();
        self.file.read_exact(&mut buffer).unwrap();
        u64::from_le_bytes(buffer)
    }

    pub fn mmap_read_sum(&mut self) -> u64 {
        let mut start = 0;
        let mut result = 0;
        for _ in 0..self.length {
            let mut buffer = [0u8; 8];
            buffer.clone_from_slice(&self.mmap[start..start + 8]);
            start += 8;
            result += u64::from_le_bytes(buffer);
        }
        result
    }

    pub fn read_sum(&mut self) -> u64 {
        let mut buffer = [0u8; 8];
        let mut result = 0u64;
        self.file.seek(SeekFrom::Start(0)).unwrap();
        for _ in 0..self.length {
            self.file.read_exact(&mut buffer).unwrap();
            result += u64::from_le_bytes(buffer);
        }
        result
    }
}
