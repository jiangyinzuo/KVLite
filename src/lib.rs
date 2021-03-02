mod buffer;
mod command;
mod error;
pub mod kv;
#[cfg(test)]
mod tests;

pub type Result<T> = std::result::Result<T, error::KVLiteError>;

pub use kv::KvStore;
