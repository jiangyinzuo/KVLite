use std::io;

#[derive(thiserror::Error, Debug)]
pub enum KVLiteError {
    #[error("{0}")]
    Disconnect(#[from] io::Error),
    #[error("{0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("key not found")]
    KeyNotFound,

    #[error("invalid command")]
    InvalidCommand,
}
