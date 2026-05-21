use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use streaming_server::video::video_source::VideoSourceKind;
use streaming_server::video::windows_impl::client::windows_client::StopWatchingEvent;

pub struct AppState {
    pub client_receiving_handler: Mutex<Option<JoinHandle<()>>>,
    pub video_source: Mutex<Option<Arc<Mutex<VideoSourceKind>>>>,
    pub stop_watching_sender: Mutex<Option<Sender<StopWatchingEvent>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            client_receiving_handler: Mutex::new(None),
            video_source: Mutex::new(None),
            stop_watching_sender: Mutex::new(None),
        }
    }
}
