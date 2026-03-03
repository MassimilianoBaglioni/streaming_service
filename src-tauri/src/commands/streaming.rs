use crate::state::app_state::AppState;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use streaming_server::video::client::receive;
use streaming_server::video::pipewire_source::PipewireSource;
use tracing::{error, info};

#[tauri::command]
pub fn start_streaming(state: tauri::State<AppState>) {
    info!("Starting streaming");
    state.stop_streaming_flag.store(false, Relaxed);
    let pipewire = PipewireSource::new();
    let streaming_thread_handle = pipewire.entry_point_gstreamer(state.stop_streaming_flag.clone());
}

#[tauri::command]
pub fn stop_streaming(state: tauri::State<AppState>) {
    state.stop_streaming_flag.store(true, Relaxed);
    info!("Stop streaming flag set to false");
}

#[tauri::command]
pub fn start_watching() {
    info!("Starting to listen");
    std::thread::spawn(|| {
        match receive() {
            Ok(()) => {}
            Err(e) => error!("Client error: {:?}", e),
        };
    });
}
