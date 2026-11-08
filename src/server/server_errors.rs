use thiserror::Error;

#[derive(Debug, Error)]
pub enum IrohAcceptClientError {
    #[error("Server could not accept on iroh: {0}")]
    NetworkError(#[from] std::io::Error),
    #[error("Wrong state")]
    StateError(),
    #[error("Empty accept handle")]
    EmptyAcceptHandle,
    #[error("Accept handle join failed")]
    AcceptHandleJoinFailed,
}
