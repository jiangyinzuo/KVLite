mod buffer;
pub mod command;
mod config;
pub mod db;
mod error;
pub mod memory;
mod wal;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;

pub use db::KVLite;
