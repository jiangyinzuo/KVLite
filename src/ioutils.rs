use crate::Result;
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

pub struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
    #[cfg(debug_assertions)]
    end: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    pub(crate) fn new(mut inner: R) -> Result<Self> {
        #[cfg(debug_assertions)]
        let end = inner.seek(SeekFrom::End(0))?;
        let pos = inner.seek(SeekFrom::Current(0))?;

        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
            #[cfg(debug_assertions)]
            end,
        })
    }

    pub fn into_inner(self) -> R {
        self.reader.into_inner()
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        debug_assert!(self.pos <= self.end, "{}, {}", self.pos, self.end);
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        debug_assert!(self.pos <= self.end, "{}, {}", self.pos, self.end);
        Ok(self.pos)
    }
}

pub struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pub pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    pub fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

#[inline]
pub fn read_u32(reader: &mut BufReaderWithPos<File>) -> u32 {
    let mut nums = [0u8; 4];
    debug_assert!(reader.pos + 4 <= reader.end);
    reader
        .read_exact(&mut nums)
        .unwrap_or_else(|e| panic!("{:#?}\n pos: {}, end: {}", e, reader.pos, reader.end));
    u32::from_le_bytes(nums)
}

pub fn read_string_exact(reader: &mut (impl Read + Seek), length: u32) -> String {
    let mut max_key = String::new();
    let mut handle = reader.take(length as u64);
    handle.read_to_string(&mut max_key).unwrap();
    max_key
}
