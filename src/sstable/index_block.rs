use std::io::{Write, Seek};
use crate::Result;

#[derive(Default)]
pub(crate) struct IndexBlock<'a> {
    /// offset, length, max key
    indexs: Vec<(u32, u32, &'a [u8])>,
}

impl<'a> IndexBlock<'a> {
    pub(crate) fn add_index(&mut self, offset: u32, length: u32, max_key: &'a [u8]) {
        self.indexs.push((offset, length, max_key));
    }
    
    pub(crate) fn write_to_file(&mut self, writer: &mut (impl Write + Seek)) -> Result<()> {
        for index in &self.indexs {
            writer.write_all(&index.0.to_be_bytes())?;
            writer.write_all(&index.1.to_be_bytes())?;
            writer.write_all(index.2)?;
        }
        Ok(())
    }
}
