#![feature(backtrace)]
#![feature(map_first_last)]
#![feature(maybe_uninit_ref)]
#![feature(num_as_ne_bytes)]

#[macro_use]
extern crate log;

mod bloom;
pub mod cache;
pub mod collections;
mod compact;
pub mod db;
pub mod error;
mod hash;
mod ioutils;
pub mod memory;
pub mod sstable;
mod wal;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;
