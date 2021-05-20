use crate::db::key_types::InternalKey;
use crate::ioutils::{read_bytes_exact, read_u32, BufReaderWithPos};
use crate::sstable::footer::Footer;
use crate::Result;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

#[derive(Default)]
pub struct IndexBlock {
    /// offset, length, max key length, max key
    indexes: Vec<(u32, u32, u32, InternalKey)>,
}

impl IndexBlock {
    pub(crate) fn add_index(&mut self, offset: u32, length: u32, max_key: InternalKey) {
        self.indexes
            .push((offset, length, max_key.len() as u32, max_key));
    }

    pub(crate) fn write_to_file(&mut self, writer: &mut (impl Write + Seek)) -> Result<()> {
        for index in &self.indexes {
            writer.write_all(&index.0.to_le_bytes())?;
            writer.write_all(&index.1.to_le_bytes())?;
            writer.write_all(&index.2.to_le_bytes())?;
            writer.write_all(&index.3)?;
        }
        Ok(())
    }

    pub(crate) fn load_index(reader: &mut BufReaderWithPos<File>, footer: &Footer) -> IndexBlock {
        reader
            .seek(SeekFrom::Start(footer.index_block_offset as u64))
            .unwrap();

        let mut index_block = IndexBlock::default();
        let mut index_offset = 0;

        debug_assert!(index_offset < footer.index_block_length);
        while index_offset < footer.index_block_length {
            let block_offset = read_u32(reader).unwrap();
            let block_length = read_u32(reader).unwrap();
            let max_key_length = read_u32(reader).unwrap();

            let max_key = read_bytes_exact(reader, max_key_length).unwrap();
            index_block
                .indexes
                .push((block_offset, block_length, max_key_length, max_key));

            index_offset += 12 + max_key_length;
        }
        index_block
    }

    /// Returns (offset, length)
    pub(crate) fn may_contain_key(&self, key: &InternalKey) -> Option<(u32, u32)> {
        self.binary_search(key)
    }

    /// Returns first Data Block's start offset whose max key is greater or equal to `key`
    pub fn find_first_ge(&self, key: &InternalKey) -> Option<u32> {
        match self.indexes.binary_search_by(|probe| probe.3.cmp(key)) {
            Ok(i) | Err(i) => self.indexes.get(i).map(|e| e.0),
        }
    }

    /// Get maximum key from [SSTableIndex]
    pub(crate) fn max_key(&self) -> &InternalKey {
        let last = self.indexes.last().unwrap_or_else(|| unsafe {
            std::hint::unreachable_unchecked();
        });
        &last.3
    }

    /// Returns (offset, length)
    fn binary_search(&self, key: &InternalKey) -> Option<(u32, u32)> {
        match self.indexes.binary_search_by(|probe| probe.3.cmp(key)) {
            Ok(i) | Err(i) => self.indexes.get(i).map(|e| (e.0, e.1)),
        }
    }
}

#[test]
fn test_may_contain_key() {
    let mut index = IndexBlock::default();
    index.indexes.push((1, 1, 1, "key298".into()));
    let option = index.may_contain_key(&Vec::from("key299"));
    assert!(option.is_none());
}
