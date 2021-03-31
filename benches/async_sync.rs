#![feature(test)]
extern crate kvlite;
extern crate test;

use std::io::{Seek, SeekFrom};
use tempfile::TempDir;
use test::Bencher;
use tokio::io::AsyncSeekExt;

fn write_file_sync(dir: &TempDir, file_id: u32) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(dir.path().join(format!("{}.txt", file_id)))
        .unwrap();
    let buf = [12; 1600];
    file.write_all(&buf).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
}

async fn write_file(dir: &TempDir, file_id: u32) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(dir.path().join(format!("{}.txt", file_id)))
        .unwrap();

    let buf = [12; 1600];
    file.write_all(&buf).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
}

async fn write_file2(dir: &TempDir, file_id: u32) {
    use tokio::io::AsyncWriteExt;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(dir.path().join(format!("{}.txt", file_id)))
        .await
        .unwrap();

    let buf = [12; 1600];
    file.write_all(&buf).await.unwrap();
    file.seek(SeekFrom::Start(0)).await.unwrap();
}

#[bench]
fn bench_sync_write(b: &mut Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    b.iter(|| {
        rt.block_on(async {
            write_file_sync(&dir, 1);
            write_file_sync(&dir, 2);
            write_file_sync(&dir, 3);
        });
    })
}

#[bench]
fn bench_async_write1(b: &mut Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    b.iter(|| {
        rt.block_on(async {
            tokio::join!(
                write_file(&dir, 1),
                write_file(&dir, 2),
                write_file(&dir, 3)
            );
        });
    })
}

#[bench]
fn bench_async_write2(b: &mut Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    b.iter(|| {
        rt.block_on(async {
            tokio::join!(
                write_file2(&dir, 1),
                write_file2(&dir, 2),
                write_file2(&dir, 3)
            );
        });
    })
}
