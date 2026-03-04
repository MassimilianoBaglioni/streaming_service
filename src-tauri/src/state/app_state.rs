use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::thread::JoinHandle;
use streaming_server::video::pipewire_source::StreamSession;
use streaming_server::video::video_source::VideoSource;

pub struct AppState {
    pub server_streaming_handler: Mutex<Option<JoinHandle<StreamSession>>>,
    pub client_receiving_handler: Mutex<Option<JoinHandle<()>>>,
    pub stop_watching_flag: Arc<AtomicBool>,
    pub video_source: Mutex<Option<Arc<Mutex<dyn VideoSource + Send + Sync>>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            server_streaming_handler: Mutex::new(None),
            client_receiving_handler: Mutex::new(None),
            stop_watching_flag: Arc::new(AtomicBool::new(false)),
            video_source: Mutex::new(None),
        }
    }
}
