use crate::db::key_types::InternalKey;
use crate::db::Value;
use crate::ioutils::{read_bytes_exact, read_u32};
use std::cmp::Ordering;
use std::io::{Read, Seek, SeekFrom};

/// TODO: modify the format of datablock, change this function
pub(super) fn get_value_from_data_block(
    reader: &mut (impl Read + Seek),
    key: &InternalKey,
    start: u32,
    length: u32,
) -> Option<Value> {
    reader.seek(SeekFrom::Start(start as u64)).unwrap();
    let mut offset = 0u32;
    while offset < length {
        let key_length = read_u32(reader).unwrap();
        let value_length = read_u32(reader).unwrap();
        let key_read = read_bytes_exact(reader, key_length as u64).unwrap();
        match key.cmp(&key_read) {
            Ordering::Less => return None,
            Ordering::Equal => return Some(read_bytes_exact(reader, value_length as u64).unwrap()),
            Ordering::Greater => {
                reader.seek(SeekFrom::Current(value_length as i64)).unwrap();
            }
        }
        offset += 8 + key_length + value_length;
    }
    None
}

pub(super) fn get_next_key_value(reader: &mut (impl Read + Seek)) -> (InternalKey, Value) {
    let key_length = read_u32(reader).unwrap();
    let value_length = read_u32(reader).unwrap();
    let key_read = read_bytes_exact(reader, key_length as u64).unwrap();
    let value_read = read_bytes_exact(reader, value_length as u64).unwrap_or_else(|e| {
        panic!(
            "{:#?}, key_length: {}, value_length: {}",
            e, key_length, value_length
        );
    });
    (key_read, value_read)
}
