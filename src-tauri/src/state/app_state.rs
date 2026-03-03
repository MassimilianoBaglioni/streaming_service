use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::thread::JoinHandle;
use streaming_server::video::pipewire_source::StreamSession;

pub struct AppState {
    pub server_streaming_handler: Mutex<Option<JoinHandle<StreamSession>>>,
    pub stop_streaming_flag: Arc<AtomicBool>,
    pub client_receiving_handler: Mutex<Option<JoinHandle<()>>>,
    pub stop_watching_flag: Arc<AtomicBool>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            server_streaming_handler: Mutex::new(None),
            stop_streaming_flag: Arc::new(AtomicBool::new(false)),
            client_receiving_handler: Mutex::new(None),
            stop_watching_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}
