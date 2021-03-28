#![feature(backtrace)]

#[macro_use]
extern crate log;

pub use db::KVLite;

pub mod collections;
pub mod command;
pub mod db;
pub mod error;
mod ioutils;
mod level0_table;
pub mod memory;
pub mod sstable;
mod version;
mod wal_writer;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;
