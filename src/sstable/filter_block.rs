use crate::filter::DefaultBloomFilter;
use std::alloc::Layout;
use std::io::{Read, Seek, SeekFrom, Write};

pub(super) fn write_filter_block(
    filter: &mut DefaultBloomFilter,
    writer: &mut (impl Write + Seek),
) {
    debug_assert!(filter.len() >= 8);

    writer
        .write_all(unsafe { std::slice::from_raw_parts(filter.get_raw_part(), filter.len()) })
        .unwrap();
}

pub(super) fn load_filter_block(
    offset: u64,
    length: usize,
    reader: &mut (impl Read + Seek),
) -> DefaultBloomFilter {
    debug_assert!(length >= 8);
    reader.seek(SeekFrom::Start(offset)).unwrap();
    let buf = unsafe { std::alloc::alloc(Layout::from_size_align(length, 64).unwrap()) };

    reader
        .read_exact(unsafe { std::slice::from_raw_parts_mut(buf, length) })
        .unwrap();
    DefaultBloomFilter::from_raw_part(buf, length)
}

#[cfg(test)]
mod tests {
    use crate::filter::{DefaultBloomFilter, SEED};
    use crate::hash::murmur_hash;
    use crate::ioutils::{BufReaderWithPos, BufWriterWithPos};
    use crate::sstable::filter_block::{load_filter_block, write_filter_block};
    use std::io::{Seek, SeekFrom, Write};

    #[test]
    fn test_load_filter_block() {
        let mut filter = DefaultBloomFilter::create_filter(300);
        for i in 300..600 {
            let h = murmur_hash(format!("key{}", i).as_bytes(), SEED);
            filter.add(h);
        }
        for i in 300..600 {
            let h = murmur_hash(format!("key{}", i).as_bytes(), SEED);
            assert!(filter.may_contain(h));
        }

        let temp_file = tempfile::tempfile().unwrap();
        let mut temp_file2 = temp_file.try_clone().unwrap();
        let mut writer = BufWriterWithPos::new(temp_file).unwrap();
        write_filter_block(&mut filter, &mut writer);
        writer.flush().unwrap();
        temp_file2.seek(SeekFrom::Start(0)).unwrap();
        let mut reader = BufReaderWithPos::new(temp_file2).unwrap();
        let filter2 = load_filter_block(0, filter.len() as usize, &mut reader);

        for i in 300..600 {
            let h = murmur_hash(format!("key{}", i).as_bytes(), SEED);
            assert!(filter2.may_contain(h));
        }
    }
}
