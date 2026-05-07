use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use streaming_server::video::video_source::VideoSourceKind;

pub struct AppState {
    pub client_receiving_handler: Mutex<Option<JoinHandle<()>>>,
    pub stop_watching_flag: Arc<AtomicBool>,
    pub video_source: Mutex<Option<Arc<Mutex<VideoSourceKind>>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            client_receiving_handler: Mutex::new(None),
            stop_watching_flag: Arc::new(AtomicBool::new(false)),
            video_source: Mutex::new(None),
        }
    }
}
