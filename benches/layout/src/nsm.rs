use crate::setup;
use memmap::{Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter::TrustedRandomAccess;
use std::ops::Range;
use tempfile::TempDir;

pub fn setup_row_major<const N: usize>() -> (TempDir, Vec<NSMHandle<N>>) {
    let (temp_dir, db_path) = setup();
    let mut nsms = vec![];
    for i in 1..=4 {
        let nsm = NSMHandle::<N>::new(&db_path, 1, i, (i - 1) * 10000..i * 10000);
        nsms.push(nsm);
    }
    (temp_dir, nsms)
}

pub fn setup_column_group() -> (TempDir, Vec<NSMHandle<2>>) {
    let (temp_dir, db_path) = setup();
    let mut cgs = vec![];

    for i in 1..=2 {
        let cg = NSMHandle::<2>::new(&db_path, 1, 2 * i - 1, (i - 1) * 20000..i * 20000);
        cgs.push(cg);
    }
    for i in 1..=2 {
        let cg = NSMHandle::<2>::new(&db_path, 1, 2 * i, (i - 1) * 20000..i * 20000);
        cgs.push(cg);
    }
    (temp_dir, cgs)
}

pub struct NSMHandle<const N: usize> {
    level: usize,
    table_id: u64,
    length: usize,
    file: File,
    mmap: Mmap,
}

impl<const N: usize> NSMHandle<N> {
    pub fn new(db_path: &str, level: usize, table_id: u64, range: Range<u64>) -> NSMHandle<N> {
        let file_path = format!("{}/{}/{}", db_path, level, table_id);
        let length = range.size();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();
        for pk in range {
            for _ in 0..N {
                file.write_all(&pk.to_le_bytes()).unwrap();
            }
        }
        let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
        NSMHandle {
            level,
            table_id,
            length,
            file,
            mmap,
        }
    }

    pub fn single_query(&mut self, pk: u64) -> [u64; N]
    where
        [(); N * 8]: ,
    {
        let mut buffer = [0u8; N * 8];
        let mut result = [0u64; N];
        self.file.seek(SeekFrom::Start(pk * 8 * N as u64));
        self.file.read_exact(&mut buffer);
        for i in 0..N {
            let mut bytes = [0u8; 8];
            bytes.clone_from_slice(&buffer[8 * i..8 * (i + 1)]);
            result[i] = u64::from_le_bytes(bytes);
        }
        result
    }

    pub fn mmap_single_query(&mut self, pk: u64) -> [u64; N] {
        let mut results = [0u64; N];
        for i in 0..N {
            let mut bytes = [0u8; 8];
            bytes.clone_from_slice(
                &self.mmap[pk as usize * 8 * N + 8 * i..pk as usize * 8 * N + 8 * (i + 1)],
            );
            results[i] = u64::from_le_bytes(bytes);
        }
        results
    }

    pub fn mmap_read_sum(&mut self) -> u64 {
        let mut result = 0u64;
        let mut start = 0;
        for _ in 0..self.length {
            let mut buffer = [0u8; 8];
            buffer.clone_from_slice(&self.mmap[start..start + 8]);
            result += u64::from_le_bytes(buffer);
            start += 8 * N;
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
            self.file.seek(SeekFrom::Current(8 * (N as i64 - 1)));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::nsm::NSMHandle;
    const MAX_LEVEL: i32 = 4;
    #[test]
    fn read_test() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap().to_string();
        for i in 1..=MAX_LEVEL {
            std::fs::create_dir_all(format!("{}/{}", db_path, i)).unwrap();
        }
        let mut handle1 = NSMHandle::<4>::new(&db_path, 1, 1, 1..10);
        let mut handle2 = NSMHandle::<4>::new(&db_path, 1, 1, 1..10);
        let res = handle1.read_sum() + handle2.read_sum();
        assert_eq!(res, 90);
        let res = handle2.single_query(8);
        println!("{:?}", res);
    }
}
