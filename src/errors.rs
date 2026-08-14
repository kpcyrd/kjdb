#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] tokio::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),
    #[error("attempted to access closed pool handle")]
    ClosedPoolHandle,
}

pub type Result<T, Err = Error> = core::result::Result<T, Err>;
