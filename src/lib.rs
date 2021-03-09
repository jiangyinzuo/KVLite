mod buffer;
pub mod command;
pub mod config;
pub mod db;
pub mod error;
pub mod memory;
mod wal;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;

pub use db::KVLite;
