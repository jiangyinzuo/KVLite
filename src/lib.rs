#![feature(backtrace)]
#![feature(map_first_last)]

#[macro_use]
extern crate log;

pub use db::KVLite;

pub mod collections;
pub mod command;
pub mod db;
pub mod error;
mod ioutils;
pub mod memory;
pub mod sstable;
mod version;
mod wal;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;
