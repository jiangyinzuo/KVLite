use crate::Result;
use std::io::{Seek, Write};

pub(crate) struct Footer {
    index_block_start_offset: u32,
}

impl Footer {
    pub(crate) fn new(index_block_start_offset: u32) -> Footer {
        Footer {
            index_block_start_offset,
        }
    }
    pub(crate) fn write_to_file(&self, writer: &mut (impl Write + Seek)) -> Result<()> {
        writer.write_all(&self.index_block_start_offset.to_be_bytes())?;
        let buf = [0xdb, 0x99, 0x11, 0x22];
        writer.write_all(&buf)?;
        Ok(())
    }
}
