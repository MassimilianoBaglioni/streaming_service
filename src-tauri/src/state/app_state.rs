use std::sync::Arc;
use streaming_server::network::server_connection::ServerConnection;
use streaming_server::network::streaming_event::StreamingEvent;
use streaming_server::network::{ConnectionBuildInfo, ConnectionMode};
use streaming_server::video::video_source::VideoSourceKind;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

pub struct AppState {
    pub video_source: Arc<Mutex<Option<VideoSourceKind>>>, // TODO, simplify the state as much as possible. I fear that a lot of mutexes are not needed ans some fields are not as well
    pub stop_watching_sender: Mutex<Option<Sender<StreamingEvent>>>,
    pub connection_mode: Mutex<Option<ConnectionMode>>,
    pub connection_build_info: Arc<Mutex<Option<ConnectionBuildInfo>>>,
    pub tokio_handler: Arc<Mutex<Option<tokio::task::JoinHandle<(anyhow::Result<()>)>>>>,
    pub server_connection: Arc<Mutex<Option<ServerConnection>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            video_source: Arc::new(Mutex::new(None)),
            stop_watching_sender: Mutex::new(None),
            connection_mode: Mutex::new(None),
            connection_build_info: Arc::new(Mutex::new(None)),
            tokio_handler: Arc::new(Mutex::new(None)),
            server_connection: Arc::new(Mutex::new(None)),
        }
    }
}
