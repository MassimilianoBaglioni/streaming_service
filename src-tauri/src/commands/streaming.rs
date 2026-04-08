use std::net::Ipv4Addr;

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
    watcher_address: String,
) {
    // TODO add a toast for the frontend in order to notify the user that the passed value is not correct.
    let stream_port: u16 = match stream_port.parse() {
        Ok(value) => value,
        Err(e) => {
            warn!("Invalid stream port {}: {}", stream_port, e);
            return;
        }
    };

    let tcp_port: u16 = match tcp_port.parse() {
        Ok(value) => value,
        Err(e) => {
            warn!("Invalid tcp port {}: {}", stream_port, e);
            return;
        }
    };

    let watcher_address: Ipv4Addr = match watcher_address.parse() {
        Ok(value) => value,
        Err(e) => {
            warn!("Invalid address {}: {}", watcher_address, e);
            return;
        }
    };

    info!(
        "Starting streaming, with stream_port: {}, tcp_port: {}, watcher_address: {}",
        stream_port, tcp_port, watcher_address
    );

    let mut video_source_lock = state.video_source.lock().unwrap();

    if video_source_lock.is_none() {
        info!("Video source not initialized, initializing inside start_streaming");
        let handles = get_wayland_handles(&app).expect("Cannot find wayland handles");
        let new_source = create_video_source(handles, tcp_port, stream_port, watcher_address);
        *video_source_lock = Some(new_source);
    }

    if let Some(video_source) = video_source_lock.as_ref() {
        info!("Video source initialized already starting stream");

        let mut vs = video_source.lock().unwrap();
        vs.update_network_info(watcher_address, stream_port, tcp_port);
        vs.start_streaming();
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
pub fn start_watching(app: AppHandle, stream_port: String, tcp_port: String) {
    let stream_port: u16 = match stream_port.parse() {
        Ok(value) => value,
        Err(e) => {
            warn!("Invalid stream port {}: {}", stream_port, e);
            // TODO add a toast for the frontend in order to notify the user that the passed value is not correct.
            return;
        }
    };

    let tcp_port: u16 = match tcp_port.parse() {
        Ok(value) => value,
        Err(e) => {
            warn!("Invalid tcp port {}: {}", stream_port, e);
            // TODO add a toast for the frontend in order to notify the user that the passed value is not correct.
            return;
        }
    };

    info!(
        "Starting to listen with stream_port: {}, tcp_port: {}",
        stream_port, tcp_port
    );
    std::thread::spawn(move || {
        match receive(stream_port, tcp_port) {
            Ok(()) => {}
            Err(e) => error!("Client error: {:?}", e),
        };
        app.emit("streaming-stopped", ()).unwrap();
    });
}
