#[derive(Debug, thiserror::Error)]
pub enum Error {
    /*
    #[error("data store disconnected")]
    Disconnect(#[from] io::Error),
    #[error("the data for key `{0}` is not available")]
    Redaction(String),
    #[error("invalid header (expected {expected:?}, found {found:?})")]
    InvalidHeader { expected: String, found: String },
    #[error("unknown data store error")]
    Unknown,
    */
    #[error(transparent)]
    Io(#[from] tokio::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
}

pub type Result<T, Err = Error> = core::result::Result<T, Err>;
