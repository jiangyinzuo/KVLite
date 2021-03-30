use crate::ioutils::{read_string_exact, read_u32};
use crate::sstable::footer::Footer;
use crate::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Default)]
pub(crate) struct IndexBlock<'a> {
    /// offset, length, max key length, max key
    indexes: Vec<(u32, u32, u32, &'a [u8])>,
}

impl<'a> IndexBlock<'a> {
    pub(crate) fn add_index(&mut self, offset: u32, length: u32, max_key: &'a [u8]) {
        self.indexes
            .push((offset, length, max_key.len() as u32, max_key));
    }

    pub(crate) fn write_to_file(&mut self, writer: &mut (impl Write + Seek)) -> Result<()> {
        for index in &self.indexes {
            writer.write_all(&index.0.to_le_bytes())?;
            writer.write_all(&index.1.to_le_bytes())?;
            writer.write_all(&index.2.to_le_bytes())?;
            writer.write_all(index.3)?;
        }
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct SSTableIndex {
    /// offset, length, max key length, max key
    indexes: Vec<(u32, u32, u32, String)>,
}

impl SSTableIndex {
    pub(crate) fn load_index(reader: &mut (impl Read + Seek)) -> SSTableIndex {
        let footer = Footer::load_footer(reader).unwrap();
        reader
            .seek(SeekFrom::Start(footer.index_block_offset as u64))
            .unwrap();

        let mut sstable_index = SSTableIndex::default();
        let mut index_offset = 0;
        while index_offset < footer.index_block_length {
            let block_offset = read_u32(reader).unwrap();
            let block_length = read_u32(reader).unwrap();
            let max_key_length = read_u32(reader).unwrap();

            let max_key = read_string_exact(reader, max_key_length);
            sstable_index
                .indexes
                .push((block_offset, block_length, max_key_length, max_key));

            index_offset += 12 + max_key_length;
        }
        sstable_index
    }

    /// Returns (offset, length)
    pub(crate) fn may_contain_key(&self, key: &String) -> Option<(u32, u32)> {
        self.binary_search(key)
    }

    fn binary_search(&self, key: &String) -> Option<(u32, u32)> {
        match self.indexes.binary_search_by(|probe| probe.3.cmp(key)) {
            Ok(i) | Err(i) => match self.indexes.get(i) {
                Some(e) => Some((e.0, e.1)),
                _ => None,
            },
        }
    }
}

#[test]
fn test_may_contain_key() {
    let mut index = SSTableIndex::default();
    index.indexes.push((1, 1, 1, "key298".into()));
    let option = index.may_contain_key(&"key299".to_string());
    assert!(option.is_none());
}
