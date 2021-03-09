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

impl PartialEq for KVLiteError {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Disconnect(_), Self::Disconnect(_))
                | (Self::SerdeError(_), Self::SerdeError(_))
                | (Self::KeyNotFound, Self::KeyNotFound)
                | (Self::InvalidCommand, Self::InvalidCommand)
        )
    }
}
