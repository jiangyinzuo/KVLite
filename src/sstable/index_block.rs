use crate::db::key_types::InternalKey;
use crate::ioutils::{read_bytes_exact, read_u32};
use crate::sstable::footer::Footer;
use crate::Result;
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Default)]
pub struct IndexBlock {
    pub(crate) min_key: InternalKey,
    /// offset, length, index_offset_uncompressed, max key length, max key
    pub(crate) indexes: Vec<(u32, u32, u32, u32, InternalKey)>,
}

impl IndexBlock {
    pub(crate) fn add_index(
        &mut self,
        offset: u32,
        length: u32,
        index_offset_uncompressed: u32,
        max_key: InternalKey,
    ) {
        debug_assert!(offset < index_offset_uncompressed);
        self.indexes.push((
            offset,
            length,
            index_offset_uncompressed,
            max_key.len() as u32,
            max_key,
        ));
    }

    pub(crate) fn write_to_file(&mut self, writer: &mut (impl Write + Seek)) -> Result<()> {
        let min_key_len = self.min_key.len() as u32;
        debug_assert_ne!(min_key_len, 0);
        writer.write_all(&min_key_len.to_le_bytes()).unwrap();
        writer.write_all(&self.min_key).unwrap();
        for index in &self.indexes {
            writer.write_all(&index.0.to_le_bytes())?;
            writer.write_all(&index.1.to_le_bytes())?;
            writer.write_all(&index.2.to_le_bytes())?;
            writer.write_all(&index.3.to_le_bytes())?;
            writer.write_all(&index.4)?;
        }
        Ok(())
    }

    pub(crate) fn load_index<R: Read + Seek>(reader: &mut R, footer: &Footer) -> IndexBlock {
        reader
            .seek(SeekFrom::Start(footer.index_block_offset as u64))
            .unwrap();

        let mut index_block = IndexBlock::default();

        let min_key_length = read_u32(reader).unwrap();
        let min_key = read_bytes_exact(reader, min_key_length as u64).unwrap();
        let mut offset: u32 = (std::mem::size_of::<u32>() + min_key.len()) as u32;
        index_block.min_key = min_key;
        debug_assert!(offset < footer.index_block_length);
        while offset < footer.index_block_length {
            let block_offset = read_u32(reader).unwrap();
            let block_length = read_u32(reader).unwrap();
            let index_offset_uncompressed = read_u32(reader).unwrap();
            debug_assert!(block_offset < index_offset_uncompressed);
            let max_key_length = read_u32(reader).unwrap();

            let max_key = read_bytes_exact(reader, max_key_length as u64).unwrap();
            index_block.indexes.push((
                block_offset,
                block_length,
                index_offset_uncompressed,
                max_key_length,
                max_key,
            ));

            offset += 16 + max_key_length;
        }
        index_block
    }

    /// Returns (offset, length)
    pub(crate) fn may_contain_key(&self, key: &InternalKey) -> Option<(u32, u32, u32)> {
        self.binary_search(key)
    }

    /// Get maximum key from [SSTableIndex]
    pub(crate) fn max_key(&self) -> &InternalKey {
        let last = self.indexes.last().unwrap_or_else(|| unsafe {
            std::hint::unreachable_unchecked();
        });
        &last.4
    }

    /// Find the first data block whose max key is greater or equal to `key`
    /// Returns (offset, length, index_offset)
    pub(crate) fn binary_search(&self, key: &InternalKey) -> Option<(u32, u32, u32)> {
        match self.indexes.binary_search_by(|probe| probe.4.cmp(key)) {
            Ok(i) | Err(i) => self.indexes.get(i).map(|e| (e.0, e.1, e.2)),
        }
    }

    /// Find all the first data block whose max key is greater or equal to `key`
    pub(crate) fn find_all_ge(&self, key: &InternalKey) -> &[(u32, u32, u32, u32, InternalKey)] {
        match self.indexes.binary_search_by(|probe| probe.4.cmp(key)) {
            Ok(i) | Err(i) => &self.indexes[i..],
        }
    }
}

#[test]
fn test_may_contain_key() {
    let mut index = IndexBlock::default();
    index.indexes.push((1, 1, 1, 1, "key298".into()));
    let option = index.may_contain_key(&Vec::from("key299"));
    assert!(option.is_none());
    let option = index.may_contain_key(&Vec::from("key298"));
    assert!(option.is_some());
}
