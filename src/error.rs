use rayon::ThreadPoolBuildError;
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum KVLiteError {
    #[error("{0}")]
    IOError(#[from] io::Error),

    #[error("{0}")]
    SerdeError(#[from] serde_json::Error),

    #[error("{0}")]
    SendError(#[from] crossbeam_channel::SendError<()>),

    #[error("{0}")]
    ThreadPoolBuildError(#[from] ThreadPoolBuildError),

    #[error("key not found")]
    KeyNotFound,

    #[error("invalid command")]
    InvalidCommand,

    #[error("{0}")]
    Custom(String),
}

impl PartialEq for KVLiteError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::IOError(_), Self::IOError(_))
            | (Self::SerdeError(_), Self::SerdeError(_))
            | (Self::KeyNotFound, Self::KeyNotFound)
            | (Self::InvalidCommand, Self::InvalidCommand) => true,
            (Self::Custom(s1), Self::Custom(s2)) => s1.eq(s2),
            _ => false,
        }
    }
}
