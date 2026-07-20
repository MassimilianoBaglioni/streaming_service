use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

use streaming_server::network::iroh::IrohInfo;
use streaming_server::network::streaming_event::StreamingEvent;
use streaming_server::video::video_source::VideoSourceKind;

pub struct AppState {
    pub client_receiving_handler: Mutex<Option<JoinHandle<()>>>,
    pub video_source: Mutex<Option<Arc<Mutex<VideoSourceKind>>>>,
    pub stop_watching_sender: Mutex<Option<Sender<StreamingEvent>>>,
    pub iroh_info: Mutex<Option<IrohInfo>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            client_receiving_handler: Mutex::new(None),
            video_source: Mutex::new(None),
            stop_watching_sender: Mutex::new(None),
            iroh_info: Mutex::new(Some(IrohInfo::default())),
        }
    }
}
