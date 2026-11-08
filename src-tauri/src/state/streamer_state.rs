use streaming_server::error::IrohProtocolError;
use streaming_server::network::iroh::IrohNetworkInfo;
use streaming_server::server::server_session::ServerSession;
use thiserror::Error;
use tokio::task::JoinHandle;

#[derive(Debug, Error)]
pub enum StreamingStateError {
    #[error("Stream already in ready state")]
    AlreadyReady,
    #[error("Wrong protocol error")]
    WrongProtocol,
    #[error("Stream state is not in waiting")]
    NotWaiting,
    #[error("No accept handle set")]
    NoAcceptHandle,
    #[error("Stream state is not in ready state")]
    NotReady,
    #[error("Wrong streamer state")]
    WrongStreamerState,
}

#[derive(Default)]
pub struct IrohBuilder {
    pub iroh_network_info: Option<IrohNetworkInfo>,
    pub accept_client_handle:
        Option<JoinHandle<anyhow::Result<IrohNetworkInfo, IrohProtocolError>>>,
}

impl IrohBuilder {
    pub fn set_network_info(&mut self, iroh_network_info: IrohNetworkInfo) {
        self.iroh_network_info = Some(iroh_network_info);
    }

    pub fn set_accept_client_handle(
        &mut self,
        accept_handle: JoinHandle<anyhow::Result<IrohNetworkInfo, IrohProtocolError>>,
    ) {
        self.accept_client_handle = Some(accept_handle);
    }
}

pub enum ProtocolBuilder {
    Direct,
    Iroh(IrohBuilder),
}
impl ProtocolBuilder {
    pub fn set_iroh_accept_handle(
        &mut self,
        handle: JoinHandle<Result<IrohNetworkInfo, IrohProtocolError>>,
    ) -> Result<(), StreamingStateError> {
        match self {
            ProtocolBuilder::Iroh(builder) => Ok(builder.set_accept_client_handle(handle)),
            _ => Err(StreamingStateError::WrongProtocol),
        }
    }
}

// TODO consider generics for the builders
pub enum StreamerState {
    Empty,
    Waiting(ProtocolBuilder),
    Ready(ServerSession),
}

impl Default for StreamerState {
    fn default() -> Self {
        StreamerState::Empty
    }
}

impl StreamerState {
    pub fn update_iroh_network(
        &mut self,
        new_network_info: IrohNetworkInfo,
    ) -> Result<(), StreamingStateError> {
        match self {
            StreamerState::Empty => {
                let mut iroh_builder = IrohBuilder::default();
                iroh_builder.set_network_info(new_network_info);
                *self = StreamerState::Waiting(ProtocolBuilder::Iroh(iroh_builder));

                Ok(())
            }
            StreamerState::Waiting(ProtocolBuilder::Iroh(builder)) => {
                builder.set_network_info(new_network_info);

                Ok(())
            }
            StreamerState::Waiting(_) => Err(StreamingStateError::WrongProtocol),
            StreamerState::Ready(_) => Err(StreamingStateError::AlreadyReady),
        }
    }

    pub fn set_iroh_accept_handle(
        &mut self,
        handle: JoinHandle<Result<IrohNetworkInfo, IrohProtocolError>>,
    ) -> Result<(), StreamingStateError> {
        match self {
            StreamerState::Waiting(builder) => builder.set_iroh_accept_handle(handle),
            StreamerState::Empty => {
                let mut iroh_builder = IrohBuilder::default();
                iroh_builder.set_accept_client_handle(handle);
                *self = StreamerState::Waiting(ProtocolBuilder::Iroh(iroh_builder));
                Ok(())
            }
            _ => Err(StreamingStateError::WrongStreamerState),
        }
    }

    // Takes ownership. It is meant to build the stream state once
    pub fn take_iroh_builder(self) -> Result<IrohBuilder, StreamingStateError> {
        match self {
            StreamerState::Waiting(ProtocolBuilder::Iroh(builder)) => Ok(builder),
            _ => Err(StreamingStateError::NotWaiting),
        }
    }

    pub async fn stop_streaming(&mut self) -> Result<(), StreamingStateError> {
        match std::mem::replace(self, StreamerState::Empty) {
            StreamerState::Ready(server_session) => {
                server_session.stop_streaming().await;
                Ok(())
            }
            other => {
                *self = other;
                Err(StreamingStateError::NotReady)
            }
        }
    }
}
