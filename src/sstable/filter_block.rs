use crate::bloom::BloomFilter;
use std::io::{Read, Seek, SeekFrom, Write};

pub(super) fn write_filter_block(filter: &mut BloomFilter, writer: &mut (impl Write + Seek)) {
    debug_assert!(filter.len() >= 8);
    writer.write_all(&filter.0).unwrap();
    writer.flush().unwrap();
}

pub(super) fn load_filter_block(
    offset: u64,
    length: usize,
    reader: &mut (impl Read + Seek),
) -> BloomFilter {
    debug_assert!(length >= 8);
    reader.seek(SeekFrom::Start(offset)).unwrap();
    let mut arr: Vec<u8> = vec![0; length];
    reader.read_exact(&mut arr).unwrap();
    BloomFilter(arr)
}

#[cfg(test)]
mod tests {
    use crate::bloom::BloomFilter;
    use crate::ioutils::{BufReaderWithPos, BufWriterWithPos};
    use crate::sstable::filter_block::{load_filter_block, write_filter_block};
    use std::io::{Seek, SeekFrom, Write};

    #[test]
    fn test_load_filter_block() {
        let mut filter = BloomFilter::create_filter(300);
        for i in 300..600 {
            filter.add(format!("key{}", i).as_bytes());
        }
        for i in 300..600 {
            assert!(filter.may_contain(format!("key{}", i).as_bytes()));
        }

        let temp_file = tempfile::tempfile().unwrap();
        let mut temp_file2 = temp_file.try_clone().unwrap();
        let mut writer = BufWriterWithPos::new(temp_file).unwrap();
        write_filter_block(&mut filter, &mut writer);
        writer.flush().unwrap();
        temp_file2.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = BufReaderWithPos::new(temp_file2).unwrap();
        let filter2 = load_filter_block(0, filter.len() as usize, &mut reader);
        assert_eq!(filter.0, filter2.0);
        for i in 300..600 {
            assert!(filter2.may_contain(format!("key{}", i).as_bytes()));
        }
    }
}
