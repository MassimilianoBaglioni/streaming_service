use streaming_server::video::client::receive;
use streaming_server::video::pipewire_source::PipewireSource;
use tracing::{error, info};

#[tauri::command]
pub fn start_streaming() {
    info!("Starting streaming");
    let pipewire = PipewireSource::new();
    pipewire.entry_point_gstreamer();
}

#[tauri::command]
pub fn start_watching() {
    info!("Starting to listen");
    match receive() {
        Ok(()) => {}
        Err(e) => error!("Client error: {:?}", e),
    };
}
