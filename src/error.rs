use rayon::ThreadPoolBuildError;
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum KVLiteError {
    #[error("{0}")]
    IOError(#[from] io::Error),

    #[error("{0}")]
    SendError(#[from] crossbeam_channel::SendError<()>),

    #[error("{0}")]
    ThreadPoolBuildError(#[from] ThreadPoolBuildError),

    #[error("invalid command")]
    InvalidCommand,

    #[error("{0}")]
    Custom(String),
}

impl PartialEq for KVLiteError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::IOError(_), Self::IOError(_)) | (Self::InvalidCommand, Self::InvalidCommand) => {
                true
            }
            (Self::Custom(s1), Self::Custom(s2)) => s1.eq(s2),
            _ => false,
        }
    }
}
