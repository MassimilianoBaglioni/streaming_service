use crate::client::network::iroh::iroh_transport::FrameReceiveError;
use crate::network::transport::TransmissionError;
use gstreamer::StateChangeError;
use iroh::endpoint::{BindError, ConnectError, ConnectionError, WriteError};
use thiserror::Error;

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("Pipeline launch Failed: {0}")]
    FailedLaunch(#[from] gstreamer::glib::Error),
    #[error("Pipeline downcasting failed")]
    FailedDowncast,
    #[error("Pipeline bus fetching failed")]
    BusFetchFail,
    #[error("Pipeline state change failed: {0}")]
    ChangeStateError(#[from] StateChangeError),
}

#[derive(Debug, Error)]
pub enum IrohProtocolError {
    #[error(transparent)]
    FrameReceive(#[from] FrameReceiveError),
    #[error(transparent)]
    TransmissionError(#[from] TransmissionError),
    #[error(transparent)]
    IrohStateError(#[from] IrohStateError),
    #[error("Could not create pipeline: {0}")]
    PipelineError(#[from] PipelineError),
    #[error("Error accepting the client")]
    ServerAcceptError,
    #[error("Iroh builder empty")]
    EmptyIrohNetworkInfo,
    #[error("Connect failed: {0}")]
    ConnectError(#[from] ConnectError),
    #[error("Open bi failed: {0}")]
    OpenBiFailed(#[from] ConnectionError),
    #[error("Stream accept error: {0}")]
    StreamAcceptError(String),
    #[error("Write failed: {0}")]
    WriteAllFailed(#[from] WriteError),
    #[error("Bind error: {0}")]
    BindError(#[from] BindError),
}

#[derive(Debug, Error)]
pub enum IrohStateError {
    #[error("Already receiving")]
    AlreadyReceiving,
    #[error("No endpoint up")]
    NoEndpointFound,
}
