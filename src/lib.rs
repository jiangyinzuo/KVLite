mod buffer;
mod command;
mod error;
pub mod kv;
mod memory;
#[cfg(test)]
mod tests;
mod wal;
mod config;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;

pub use kv::KvStore;
