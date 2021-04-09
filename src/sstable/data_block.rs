use crate::ioutils::{read_string_exact, read_u32, BufReaderWithPos};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{Seek, SeekFrom};

pub(super) fn get_value_from_data_block(
    reader: &mut BufReaderWithPos<File>,
    key: &str,
    start: u32,
    length: u32,
) -> Option<String> {
    reader.seek(SeekFrom::Start(start as u64)).unwrap();
    let mut offset = 0u32;
    while offset < length {
        let key_length = read_u32(reader).unwrap();
        let value_length = read_u32(reader).unwrap();
        let key_read = read_string_exact(reader, key_length).unwrap();
        match key.cmp(&key_read) {
            Ordering::Less => return None,
            Ordering::Equal => return Some(read_string_exact(reader, value_length).unwrap()),
            Ordering::Greater => {
                reader.seek(SeekFrom::Current(value_length as i64)).unwrap();
            }
        }
        offset += 8 + key_length + value_length;
    }
    None
}

pub(super) fn get_next_key_value(reader: &mut BufReaderWithPos<File>) -> (String, String) {
    let key_length = read_u32(reader).unwrap();
    let value_length = read_u32(reader).unwrap();
    let key_read = read_string_exact(reader, key_length).unwrap();
    let value_read = read_string_exact(reader, value_length).unwrap_or_else(|e| {
        panic!(
            "{:#?}, key_length: {}, value_length: {}",
            e, key_length, value_length
        );
    });
    (key_read, value_read)
}
