#![feature(test)]
extern crate kvlite;
extern crate test;

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use test::Bencher;

const RIGHT_VALUE: i32 = 1024;

fn write_file(filename: &str) {
    let mut file = File::create(filename).unwrap();
    for i in 0..=RIGHT_VALUE {
        file.write_all(&i.to_le_bytes()).unwrap();
    }
}

#[bench]
fn read_file(b: &mut Bencher) {
    write_file("read_file.txt");
    b.iter(|| {
        let mut reader = BufReader::new(File::open("read_file.txt").unwrap());
        let mut left = 0usize;
        let mut right = RIGHT_VALUE as usize;
        let mut buf = [0u8; 4];
        while left < right {
            let mid = (left + right) / 2;
            reader.seek(SeekFrom::Start(mid as u64)).unwrap();
            reader.read_exact(&mut buf).unwrap();
            let value = i32::from_le_bytes(buf);
            match value.cmp(&RIGHT_VALUE) {
                Ordering::Equal => break,
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid - 1,
            }
        }
    });
    std::fs::remove_file("read_file.txt").unwrap();
}

#[bench]
fn read_file_pre_read_all(b: &mut Bencher) {
    write_file("read_file_pre_read_all.txt");
    b.iter(|| {
        let mut reader = BufReader::new(File::open("read_file_pre_read_all.txt").unwrap());
        let mut left = 0usize;
        let mut right = RIGHT_VALUE as usize;
        let mut buf = [0u8; 4];
        let mut buffer = Vec::with_capacity((RIGHT_VALUE * 4) as usize);
        reader.read_to_end(&mut buffer).unwrap();
        while left < right {
            let mid = ((left + right) / 2) as usize;
            buf.clone_from_slice(&buffer[mid * 4..(mid + 1) * 4]);

            let value = i32::from_le_bytes(buf);
            match value.cmp(&RIGHT_VALUE) {
                Ordering::Equal => break,
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid - 1,
            }
        }
    });
    std::fs::remove_file("read_file_pre_read_all.txt").unwrap();
}
