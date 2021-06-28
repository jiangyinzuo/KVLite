#![feature(test)]
extern crate kvlite;
extern crate test;

use rand::RngCore;
use std::fs::File;
use std::io::{BufWriter, Write};
use test::Bencher;

fn write_file(compress: bool) -> BufWriter<File> {
    let mut random = rand::thread_rng();
    let file = tempfile::tempfile().unwrap();
    let mut encoder = snap::raw::Encoder::new();
    let mut buf_writer = BufWriter::new(file);
    let mut buffer = vec![0u8; 4096];

    random.fill_bytes(&mut buffer);
    for _ in 0..10 {
        if compress {
            let compressed_buffer = encoder.compress_vec(&buffer).unwrap();
            buf_writer.write_all(compressed_buffer.as_slice()).unwrap();
        } else {
            buf_writer.write_all(buffer.as_slice()).unwrap();
        }
    }
    buf_writer
}

#[bench]
fn write_file_one_time(b: &mut Bencher) {
    b.iter(|| {
        write_file(false);
    })
}

#[bench]
fn write_file_one_time_with_snappy(b: &mut Bencher) {
    b.iter(|| {
        write_file(true);
    })
}

#[bench]
fn write_file_multiple_times(b: &mut Bencher) {
    let mut random = rand::thread_rng();
    let mut buffer = vec![123u8; 128];
    random.fill_bytes(buffer.as_mut_slice());
    b.iter(|| {
        let file = tempfile::tempfile().unwrap();
        let mut buf_writer = BufWriter::new(file);
        for _ in 0..320 {
            buf_writer.write_all(buffer.as_slice()).unwrap();
        }
    })
}
