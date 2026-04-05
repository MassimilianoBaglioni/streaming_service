use std::net::IpAddr;

use crate::get_wayland_handles;
use crate::state::app_state::AppState;
use streaming_server::video::client::receive;
use streaming_server::video::create_video_source;
use tauri::{AppHandle, Emitter};
use tracing::{error, info, warn};

#[tauri::command]
pub fn start_streaming(
    state: tauri::State<AppState>,
    app: AppHandle,
    stream_port: String,
    tcp_port: String,
) {
    info!(
        "Starting streaming, with stream_port: {}, tcp_port: {}",
        stream_port, tcp_port
    );

    let mut video_source_lock = state.video_source.lock().unwrap();

    if video_source_lock.is_none() {
        info!("Video source not initialized, initializing inside start_streaming");
        let handles = get_wayland_handles(&app).expect("Cannot find wayland handles");
        let new_source = create_video_source(handles);
        *video_source_lock = Some(new_source);
    }

    if let Some(video_source) = video_source_lock.as_ref() {
        info!("Video source initialized already starting stream");
        video_source.lock().unwrap().start_streaming();
    }
}

#[tauri::command]
pub fn stop_streaming(state: tauri::State<AppState>) {
    // TODO check that we close the tcp sockets when stop streaming or stop watching are called
    if let Some(video_source) = state.video_source.lock().unwrap().as_ref() {
        video_source.lock().unwrap().stop_streaming();
    } else {
        warn!("No video source obj to stop the stream");
    }
    info!("Stop streaming flag set to true");
}

#[tauri::command]
pub fn start_watching(app: AppHandle, host_ip: String, stream_port: String, tcp_port: String) {
    info!(
        "Starting to listen with stream_port: {}, tcp_port: {}, host_ip: {}",
        stream_port, tcp_port, host_ip
    );
    std::thread::spawn(move || {
        // <-- move here
        match receive() {
            Ok(()) => {}
            Err(e) => error!("Client error: {:?}", e),
        };
        app.emit("streaming-stopped", ()).unwrap();
    });
}
