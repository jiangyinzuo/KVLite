#![feature(backtrace)]
#![feature(map_first_last)]
#![feature(maybe_uninit_ref)]

#[macro_use]
extern crate log;

mod bloom;
pub mod cache;
pub mod collections;
mod compact;
pub mod db;
pub mod error;
mod hash;
pub mod ioutils;
pub mod memory;
pub mod sstable;
pub mod wal;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;
