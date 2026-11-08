use crate::state::client_state::ClientState;
use crate::state::streamer_state::{
    IrohBuilder, ProtocolBuilder, StreamerState, StreamingStateError,
};
use iroh::Endpoint;
use std::sync::Arc;
use streaming_server::client::client_session::ClientError;
use streaming_server::error::IrohProtocolError;
use streaming_server::network::iroh::{build_endpoint, IrohNetworkInfo};
use streaming_server::platform::capture_backend::CaptureBackend;
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tracing::info;

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("Wrong session type")]
    WrongSessionType,
    #[error("Session set to None")]
    NoSession,
}

#[derive(Debug, Error)]
pub enum ApplicationStateError {
    #[error("Session Error: {0}")]
    SessionError(#[from] SessionError),
    #[error("Streaming state Error: {0}")]
    StreamingStateError(#[from] StreamingStateError),
}

#[derive(Debug, Error)]
pub enum StopWatchingError {
    #[error("Watcher wrong state: {0}")]
    StateError(#[from] SessionError),
    #[error("Watcher network failed to send stop watching event: {0}")]
    TransmissionError(#[from] ClientError),
}
pub enum Protocol {
    Iroh,
    Direct,
}
pub enum Session {
    Streamer(StreamerState),
    Client(ClientState),
}

impl Session {
    pub fn new_streamer() -> Self {
        Session::Streamer(StreamerState::Empty)
    }
    pub fn set_iroh_accept_handle(
        &mut self,
        handle: JoinHandle<Result<IrohNetworkInfo, IrohProtocolError>>,
    ) -> Result<(), ApplicationStateError> {
        match self {
            Session::Streamer(s) => Ok(s.set_iroh_accept_handle(handle)?),
            _ => Err(SessionError::WrongSessionType)?,
        }
    }

    pub fn set_streamer_state(
        &mut self,
        streamer_state: StreamerState,
    ) -> Result<(), ApplicationStateError> {
        match self {
            Session::Streamer(s) => {
                *s = streamer_state;
                Ok(())
            }
            _ => Err(SessionError::WrongSessionType)?,
        }
    }

    pub fn set_client_state(
        &mut self,
        client_state: ClientState,
    ) -> Result<(), ApplicationStateError> {
        match self {
            Session::Client(s) => {
                *s = client_state;
                Ok(())
            }
            _ => Err(SessionError::WrongSessionType)?,
        }
    }

    pub fn take_streamer_iroh_builder(&mut self) -> Result<IrohBuilder, StreamingStateError> {
        let Session::Streamer(streamer_state) = self else {
            return Err(StreamingStateError::NotWaiting);
        };

        let StreamerState::Waiting(ProtocolBuilder::Iroh(_)) = streamer_state else {
            return Err(StreamingStateError::NotWaiting);
        };

        let StreamerState::Waiting(ProtocolBuilder::Iroh(builder)) =
            std::mem::replace(streamer_state, StreamerState::Empty)
        else {
            unreachable!("just matched this shape above")
        };

        Ok(builder)
    }

    pub async fn stop_streaming(&mut self) -> Result<(), ApplicationStateError> {
        match self {
            Session::Streamer(s) => Ok(s.stop_streaming().await?),
            _ => Err(SessionError::WrongSessionType)?,
        }
    }

    pub async fn stop_watching(&mut self) -> Result<(), StopWatchingError> {
        match self {
            Session::Client(ClientState::Watching(s)) => Ok(s.stop_watching().await?),
            _ => Err(SessionError::WrongSessionType)?,
        }
    }

    pub fn update_iroh_network_info(
        &mut self,
        iroh_network_info: IrohNetworkInfo,
    ) -> Result<(), ApplicationStateError> {
        match self {
            Session::Streamer(s) => Ok(s.update_iroh_network(iroh_network_info)?),
            _ => Err(SessionError::WrongSessionType)?,
        }
    }
}

pub struct AppState {
    pub streaming_session: Arc<Mutex<Option<Session>>>,
    // not a smell, this stays here and not in the connection because the endpoint creation is expensive and is once per process, not per streaming session
    pub iroh_endpoint: Arc<Mutex<Option<Endpoint>>>,
    pub capture_backend: CaptureBackend,
}

impl AppState {
    pub async fn lock_session(
        &self,
    ) -> Result<MutexGuard<'_, Option<Session>>, ApplicationStateError> {
        Ok(self.streaming_session.lock().await)
    }

    pub async fn set_session(&self, session: Session) {
        self.streaming_session.lock().await.replace(session);
    }
    pub async fn get_or_create_iroh_endpoint(
        &self,
    ) -> Result<Arc<Mutex<Option<Endpoint>>>, IrohProtocolError> {
        {
            let mut guard = self.iroh_endpoint.lock().await;
            if guard.is_none() {
                *guard = Some(build_endpoint().await?);
                info!("Endpoint set, it was None");
            }
        }

        Ok(self.iroh_endpoint.clone())
    }

    pub fn new(capture_backend: CaptureBackend) -> Self {
        Self {
            streaming_session: Arc::new(Mutex::new(None)),
            iroh_endpoint: Arc::new(Mutex::new(None)),
            capture_backend,
        }
    }
}
