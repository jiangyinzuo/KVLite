use crate::error::KVLiteError;
use crate::ioutils::BufWriterWithPos;
use crate::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

pub const FOOTER_MAGIC_NUMBER: u32 = 0xdb991122;
pub const FOOTER_BYTE_SIZE: i64 = 20;

pub(crate) struct Footer {
    pub index_block_offset: u32,
    pub index_block_length: u32,
    pub filter_length: u32,
    pub kv_total: u32,
}

impl Footer {
    pub(crate) fn write_to_file(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.write_all(&self.index_block_offset.to_le_bytes())?;
        writer.write_all(&self.index_block_length.to_le_bytes())?;
        writer.write_all(&self.filter_length.to_le_bytes())?;
        writer.write_all(&self.kv_total.to_le_bytes())?;
        writer.write_all(&FOOTER_MAGIC_NUMBER.to_le_bytes())?;
        Ok(())
    }

    pub(crate) fn load_footer(reader: &mut (impl Read + Seek)) -> Result<Footer> {
        reader.seek(SeekFrom::End(-FOOTER_BYTE_SIZE))?;

        let mut buffer = [0u8; 20];
        reader.read_exact(&mut buffer).unwrap();

        let mut index_block_offset = [0u8; 4];
        index_block_offset.clone_from_slice(&buffer[0..4]);

        let mut index_block_length = [0u8; 4];
        index_block_length.clone_from_slice(&buffer[4..8]);

        let mut filter_length = [0u8; 4];
        filter_length.clone_from_slice(&buffer[8..12]);

        let mut kv_total = [0u8; 4];
        kv_total.clone_from_slice(&buffer[12..16]);

        let footer = Footer {
            index_block_offset: u32::from_le_bytes(index_block_offset),
            index_block_length: u32::from_le_bytes(index_block_length),
            filter_length: u32::from_le_bytes(filter_length),
            kv_total: u32::from_le_bytes(kv_total),
        };

        // validate magic number
        if buffer[16..20] != FOOTER_MAGIC_NUMBER.to_le_bytes() {
            return Err(KVLiteError::Custom("invalid footer magic number".into()));
        }

        Ok(footer)
    }
}

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
