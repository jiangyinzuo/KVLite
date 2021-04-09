use crate::error::KVLiteError;
use crate::ioutils::{read_u32, BufReaderWithPos, BufWriterWithPos};
use crate::Result;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

pub const FOOTER_MAGIC_NUMBER: u32 = 0xdb991122;
pub const FOOTER_BYTE_SIZE: i64 = 20;

pub(crate) struct Footer {
    pub index_block_offset: u32,
    pub index_block_length: u32,
    pub filter_length: u32,
    pub kv_total: u32,
}

impl Footer {
    pub(crate) fn new(
        index_block_offset: u32,
        index_block_length: u32,
        filter_length: u32,
        kv_total: u32,
    ) -> Footer {
        Footer {
            index_block_offset,
            index_block_length,
            filter_length,
            kv_total,
        }
    }

    pub(crate) fn write_to_file(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.write_all(&self.index_block_offset.to_le_bytes())?;
        writer.write_all(&self.index_block_length.to_le_bytes())?;
        writer.write_all(&self.filter_length.to_le_bytes())?;
        writer.write_all(&self.kv_total.to_le_bytes())?;
        writer.write_all(&FOOTER_MAGIC_NUMBER.to_le_bytes())?;
        Ok(())
    }

    pub(crate) fn load_footer(reader: &mut BufReaderWithPos<File>) -> Result<Footer> {
        reader.seek(SeekFrom::End(-FOOTER_BYTE_SIZE))?;

        let index_block_offset = read_u32(reader)?;
        let index_block_length = read_u32(reader)?;
        let filter_length = read_u32(reader)?;
        let kv_total = read_u32(reader)?;

        let footer = Footer {
            index_block_offset,
            index_block_length,
            filter_length,
            kv_total,
        };

        // validate magic number
        let magic_number = read_u32(reader)?;
        if magic_number != FOOTER_MAGIC_NUMBER {
            return Err(KVLiteError::Custom("invalid footer magic number".into()));
        }

        Ok(footer)
    }
}

#[inline]
pub(super) fn write_footer(
    index_block_offset: u32,
    index_block_length: u32,
    writer: &mut BufWriterWithPos<File>,
    filter_length: u32,
    kv_total: u32,
) {
    let footer = Footer {
        index_block_offset,
        index_block_length,
        filter_length,
        kv_total,
    };
    footer.write_to_file(writer).unwrap();
}
