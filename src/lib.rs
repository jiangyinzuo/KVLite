#![feature(backtrace)]
#![feature(map_first_last)]
#![feature(maybe_uninit_ref)]

#[macro_use]
extern crate log;

pub use db::KVLite;

mod bloom;
mod cache;
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
