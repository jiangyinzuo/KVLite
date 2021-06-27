use crate::byteutils::u32_from_le_bytes;
use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{InternalKey, MemKey};
use crate::db::Value;
use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom};

pub struct DataBlock {
    data: Vec<u8>,
    num_records: i64,
    data_idx_offset: usize,
}

impl DataBlock {
    pub(super) fn from_reader(
        reader: &mut (impl Read + Seek),
        start: u32,
        length: u32,
        index_offset: u32,
    ) -> DataBlock {
        debug_assert!(index_offset < start + length);
        reader.seek(SeekFrom::Start(start as u64)).unwrap();
        let mut data_block = vec![0u8; length as usize];
        reader.read_exact(data_block.as_mut_slice()).unwrap();
        debug_assert_eq!(
            (start + length - index_offset) as usize % std::mem::size_of::<u32>(),
            0
        );
        DataBlock {
            data: data_block,
            num_records: (start + length - index_offset) as i64 / std::mem::size_of::<u32>() as i64,
            data_idx_offset: (index_offset - start) as usize,
        }
    }

    pub(super) fn get_value(&self, key: &InternalKey) -> Option<Value> {
        let mut left = 0;
        let mut right = self.num_records;
        while left <= right {
            let mid = (left + right) / 2;
            let record_start_offset = self.data_idx_offset + mid as usize * 4;

            debug_assert!(
                record_start_offset < self.data.len(),
                "{}, {}",
                record_start_offset,
                self.data.len()
            );
            let record_start =
                u32_from_le_bytes(&self.data[record_start_offset..record_start_offset + 4])
                    as usize;
            let key_length = u32_from_le_bytes(&self.data[record_start..record_start + 4]) as usize;
            let key_start = record_start + 8;
            let value_length = u32_from_le_bytes(&self.data[record_start + 4..key_start]) as usize;
            let value_start = key_start + key_length;
            let key_read = &self.data[key_start..value_start];
            match key_read.cmp(&key) {
                Ordering::Less => left = mid + 1,
                Ordering::Equal => {
                    return Some(Value::from(
                        &self.data[value_start..value_start + value_length],
                    ))
                }
                Ordering::Greater => right = mid - 1,
            }
        }
        None
    }

    /// Return whether the data block remains keys.
    pub(super) fn get_all_record_le<UK: MemKey>(
        &self,
        key: &InternalKey,
        kvs: &mut SkipMap<UK, Value>,
    ) -> bool {
        let mut left = 0;
        let mut right = self.num_records;
        while left <= right {
            let mid = (left + right + 1) / 2;
            let record_start_offset = self.data_idx_offset + mid as usize * 4;

            debug_assert!(
                record_start_offset < self.data.len(),
                "{}, {}",
                record_start_offset,
                self.data.len()
            );
            let record_start =
                u32_from_le_bytes(&self.data[record_start_offset..record_start_offset + 4])
                    as usize;
            let key_length = u32_from_le_bytes(&self.data[record_start..record_start + 4]) as usize;
            let key_start = record_start + 8;
            let value_start = key_start + key_length;
            let key_read = &self.data[key_start..value_start];

            match key_read.cmp(&key) {
                Ordering::Less => left = mid,
                Ordering::Equal => {
                    left = mid;
                    break;
                }
                Ordering::Greater => right = mid - 1,
            }
        }
        for i in 0..=(left as usize) {
            let (key_read, value_read) = self.key_value_at(i);
            kvs.insert(key_read.into(), value_read);
        }
        right < self.num_records
    }

    fn key_value_at(&self, idx: usize) -> (InternalKey, Value) {
        let record_start_offset = self.data_idx_offset + idx as usize * 4;

        debug_assert!(
            record_start_offset < self.data.len(),
            "{}, {}",
            record_start_offset,
            self.data.len()
        );
        let record_start =
            u32_from_le_bytes(&self.data[record_start_offset..record_start_offset + 4]) as usize;
        let key_length = u32_from_le_bytes(&self.data[record_start..record_start + 4]) as usize;
        let key_start = record_start + 8;
        let value_length = u32_from_le_bytes(&self.data[record_start + 4..key_start]) as usize;
        let value_start = key_start + key_length;
        let key_read = InternalKey::from(&self.data[key_start..value_start]);
        let value_read = Value::from(&self.data[value_start..value_start + value_length]);
        (key_read, value_read)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.num_records as usize
    }
}

impl IntoIterator for DataBlock {
    type Item = (InternalKey, Value);
    type IntoIter = DataBlockIter;

    fn into_iter(self) -> Self::IntoIter {
        DataBlockIter {
            data_block: self,
            idx: 0,
        }
    }
}

pub struct DataBlockIter {
    data_block: DataBlock,
    idx: usize,
}

impl Iterator for DataBlockIter {
    type Item = (InternalKey, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.data_block.len() {
            let record = self.data_block.key_value_at(self.idx);
            self.idx += 1;
            Some(record)
        } else {
            None
        }
    }
}
