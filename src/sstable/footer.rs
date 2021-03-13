use crate::error::KVLiteError;
use crate::ioutils::read_u32;
use crate::Result;
use std::io::{Read, Seek, SeekFrom, Write};

pub const FOOTER_MAGIC_NUMBER: u32 = 0xdb991122;
pub const FOOTER_BYTE_SIZE: i64 = 12;

pub(crate) struct Footer {
    pub index_block_offset: u32,
    pub index_block_length: u32,
}

impl Footer {
    pub(crate) fn new(index_block_offset: u32, index_block_length: u32) -> Footer {
        Footer {
            index_block_offset,
            index_block_length,
        }
    }

    pub(crate) fn write_to_file(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.write_all(&self.index_block_offset.to_le_bytes())?;
        writer.write_all(&self.index_block_length.to_le_bytes())?;
        writer.write_all(&FOOTER_MAGIC_NUMBER.to_le_bytes())?;
        Ok(())
    }

    pub(crate) fn load_footer(reader: &mut (impl Read + Seek)) -> Result<Footer> {
        reader.seek(SeekFrom::End(-FOOTER_BYTE_SIZE))?;

        let index_block_offset = read_u32(reader)?;
        let index_block_length = read_u32(reader)?;

        let footer = Footer {
            index_block_offset,
            index_block_length,
        };

        // validate magic number
        let magic_number = read_u32(reader)?;
        if magic_number != FOOTER_MAGIC_NUMBER {
            return Err(KVLiteError::Custom("invalid footer magic number".into()));
        }

        Ok(footer)
    }
}
