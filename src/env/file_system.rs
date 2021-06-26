use crate::ioutils::BufReaderWithPos;
use crate::Result;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::Path;

pub struct FileSystem {}

impl FileSystem {
    pub fn create_seq_readable_file(path: &Path) -> Result<impl SequentialReadableFile> {
        #[cfg(feature = "mmap")]
        {
            use crate::env::file_system::mmap::MmapFile;
            MmapFile::open(path)
        }
        #[cfg(not(feature = "mmap"))]
        {
            BufReaderWithPos::new(File::open(path)?)
        }
    }
}

pub trait SequentialReadableFile: Read + Seek {
    fn position(&self) -> usize;
}

#[cfg(feature = "mmap")]
pub mod mmap {
    use crate::env::file_system::SequentialReadableFile;
    use crate::Result;
    use memmap::{Mmap, MmapOptions};
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;

    pub struct MmapFile {
        file: File,
        mmap: Mmap,
        pos: usize,
    }

    impl MmapFile {
        pub fn open(path: &Path) -> Result<MmapFile> {
            let file = File::open(path)?;
            let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
            Ok(MmapFile { file, mmap, pos: 0 })
        }
    }

    impl Read for MmapFile {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let new_pos = (self.pos + buf.len()).min(self.mmap.len());
            buf.clone_from_slice(&self.mmap[self.pos..new_pos]);
            let nbytes = new_pos - self.pos;
            self.pos = new_pos;
            Ok(nbytes)
        }
    }

    impl Seek for MmapFile {
        fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
            match pos {
                SeekFrom::Start(p) => self.pos = p as usize,
                SeekFrom::Current(p) => self.pos = (self.pos as i64 + p) as usize,
                SeekFrom::End(p) => self.pos = (self.mmap.len() as i64 + p) as usize,
            }
            Ok(self.pos as u64)
        }
    }

    impl SequentialReadableFile for MmapFile {
        fn position(&self) -> usize {
            self.pos
        }
    }
}
