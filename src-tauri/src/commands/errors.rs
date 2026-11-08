use crate::state::app_state::{ApplicationStateError, SessionError, StopWatchingError};
use crate::state::streamer_state::StreamingStateError;
use serde::{Serialize, Serializer};
use streaming_server::client::client_session::ClientError;
use streaming_server::error::IrohStateError;
use streaming_server::error::{IrohProtocolError, PipelineError};
use streaming_server::network::transport::ServerTransportError;
use streaming_server::network::ConnectionBuildError;
use streaming_server::server::server_errors::IrohAcceptClientError;
use thiserror::Error;
use tracing::info;

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Server tranpsort error: {0}")]
    ServerTransport(#[from] ServerTransportError),
    #[error("Pipeline error: {0}")]
    PipelineError(#[from] PipelineError),
    #[error("Application state error: {0}")]
    ApplicationStateError(#[from] ApplicationStateError),
    #[error("Streaming state error: {0}")]
    StreamingStateError(#[from] StreamingStateError),
    #[error("Iroh accept client error: {0}")]
    IrohAcceptClientError(#[from] IrohAcceptClientError),
    #[error("Iroh protocol error: {0}")]
    IrohProtocolError(#[from] IrohProtocolError),
    #[error("Iroh state error: {0}")]
    IrohStateError(#[from] IrohStateError),
    #[error("Connection build error: {0}")]
    ConnectionBuildError(#[from] ConnectionBuildError),
    #[error("Client error: {0}")]
    ClientError(#[from] ClientError),
    #[error("Session error: {0}")]
    Session(#[from] SessionError),
    #[error("Stop watching error: {0}")]
    StopWatchingError(#[from] StopWatchingError),
}

impl Serialize for CommandError {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        info!("Command error: {}", self);
        serializer.serialize_str(&self.to_string())
    }
}
