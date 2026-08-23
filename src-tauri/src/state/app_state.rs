use std::sync::Arc;
use streaming_server::network::server_connection::ServerConnection;
use streaming_server::network::streaming_event::StreamingEvent;
use streaming_server::network::ConnectionMode;
use streaming_server::video::video_source::VideoSourceKind;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

pub struct StreamingSession {
    pub video_source: Option<VideoSourceKind>,
    pub stop_watching_sender: Option<Sender<StreamingEvent>>,
    pub connection_mode: Option<ConnectionMode>,
    pub tokio_handler: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    pub server_connection: Option<ServerConnection>,
}

impl Default for StreamingSession {
    fn default() -> Self {
        Self {
            video_source: None,
            stop_watching_sender: None,
            connection_mode: None,
            tokio_handler: None,
            server_connection: None,
        }
    }
}

#[derive(Default)]
pub struct AppState {
    pub streaming_session: Arc<Mutex<StreamingSession>>,
}
