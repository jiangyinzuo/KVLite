use crate::error::KVLiteError;
use crate::Result;
use std::io::{Read, Seek, SeekFrom, Write};

pub const FOOTER_MAGIC_NUMBER: u32 = 0xdb991122;
pub const FOOTER_BYTE_SIZE: i64 = 8;

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
        writer.write_all(&self.index_block_offset.to_be_bytes())?;
        writer.write_all(&self.index_block_length.to_be_bytes())?;
        writer.write_all(&FOOTER_MAGIC_NUMBER.to_be_bytes())?;
        Ok(())
    }

    pub(crate) fn load_footer(reader: &mut (impl Read + Seek)) -> Result<Footer> {
        reader.seek(SeekFrom::End(-FOOTER_BYTE_SIZE))?;

        let mut index_block_offset = [0u8; 4];
        reader.read_exact(&mut index_block_offset)?;
        let index_block_offset = u32::from_le_bytes(index_block_offset);

        let mut index_block_length = [0u8; 4];
        reader.read_exact(&mut index_block_length);
        let index_block_length = u32::from_le_bytes(index_block_length);

        let footer = Footer {
            index_block_offset,
            index_block_length,
        };

        // validate magic number
        let mut magic_number = [0u8; 4];
        reader.read_exact(&mut magic_number)?;

        let magic_number = u32::from_le_bytes(magic_number);
        if magic_number != FOOTER_MAGIC_NUMBER {
            return Err(KVLiteError::Custom("invalid footer magic number".into()));
        }

        Ok(footer)
    }
}
