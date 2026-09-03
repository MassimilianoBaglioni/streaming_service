use iroh::Endpoint;
use std::sync::Arc;
use streaming_server::network::server_connection::ServerConnection;
use streaming_server::network::streaming_event::StreamingEvent;
use streaming_server::network::ConnectionMode;
use streaming_server::video::video_source::VideoSourceKind;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct StreamingSession {
    pub video_source: Option<VideoSourceKind>,
    pub stop_watching_sender: Option<Sender<StreamingEvent>>,
    pub connection_mode: Option<ConnectionMode>,
    pub tokio_handler: Option<JoinHandle<anyhow::Result<()>>>,
    pub server_connection: Option<ServerConnection>,
    pub cancel_token: Option<CancellationToken>,
}

impl Default for StreamingSession {
    fn default() -> Self {
        Self {
            video_source: None,
            stop_watching_sender: None,
            connection_mode: None,
            tokio_handler: None,
            server_connection: None,
            cancel_token: None,
        }
    }
}

#[derive(Default)]
pub struct AppState {
    pub streaming_session: Arc<Mutex<StreamingSession>>,
    // not a smell, this stays here and not in the connection because the endpoint creation is expensive and is once per process, not per streaming session
    pub iroh_endpoint: Arc<Mutex<Option<Endpoint>>>,
}

impl AppState {
    pub async fn get_or_create_iroh_endpoint(&self) -> anyhow::Result<Endpoint> {
        let mut guard = self.iroh_endpoint.lock().await;
        if guard.is_none() {
            *guard = Some(streaming_server::network::iroh::build_endpoint().await?);
        }
        Ok(guard.as_ref().unwrap().clone())
    }
}
