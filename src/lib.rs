#![feature(backtrace)]
#![feature(map_first_last)]
#![feature(core_intrinsics)]
#![feature(const_generics)]

#[macro_use]
extern crate log;

use std::ptr::NonNull;

mod bloom;
pub mod byteutils;
pub mod cache;
pub mod collections;
mod compaction;
pub mod db;
mod env;
pub mod error;
mod hash;
pub mod ioutils;
pub mod memory;
pub mod sstable;
pub mod wal;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;

#[cfg(feature = "use_jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
