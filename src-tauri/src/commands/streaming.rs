use crate::state::app_state::AppState;
#[cfg(target_os = "windows")]
use crate::windows_impl::show_picker;
use streaming_server::video::client::receive;
use streaming_server::{network::NetInfo, video::video_source::VideoSourceKind};
#[cfg(target_os = "windows")]
use tauri::{AppHandle, Emitter, Manager};
use tracing::{error, info, warn};

#[tauri::command]
pub async fn start_streaming(
    state: tauri::State<'_, AppState>,
    app: AppHandle,
    stream_port: String,
    tcp_port: String,
    watcher_address: String,
) -> Result<(), String> {
    let net_info = NetInfo::parse_info(stream_port, tcp_port, watcher_address)
        .expect("Parsing net info from fontend error");

    #[cfg(target_os = "windows")]
    let capture_item = show_picker(app.clone()).await;

    // This must stay AFTER async calls, can't lock with async functions
    let mut lock = state.video_source.lock().unwrap();

    if lock.is_none() {
        info!("Video source not initialized, initializing inside start_streaming");
        #[cfg(target_os = "linux")]
        {
            let new_source = create_pipewire_video_source(handles, &net_info);
            *lock = Some(new_source);
        }
        #[cfg(target_os = "windows")]
        {
            use streaming_server::video::windows_impl::windows_source::create_windows_video_source;
            let hwnd_raw = app.get_webview_window("main").unwrap().hwnd().unwrap().0 as isize;
            let new_source = create_windows_video_source(hwnd_raw, &net_info, capture_item.clone());
            *lock = Some(new_source);
        }
    }

    if let Some(video_source) = lock.as_ref() {
        info!("Video source already initialized, starting stream");
        let mut vs = video_source.lock().unwrap();
        match &mut *vs {
            #[cfg(target_os = "windows")]
            VideoSourceKind::Windows(windows_source) => {
                windows_source.set_graphics_capture_item(capture_item.clone());
                windows_source.update_network_info(&net_info);
                windows_source.start_streaming();
            }
            #[cfg(target_os = "linux")]
            VideoSourceKind::Pipewire(pipewire_source) => {}
        }
    }

    Ok(())
}

#[tauri::command]
pub fn stop_streaming(state: tauri::State<AppState>) {
    // TODO check that we close the tcp sockets when stop streaming or stop watching are called
    if let Some(video_source) = state.video_source.lock().unwrap().as_ref() {
        let mut vs = video_source.lock().unwrap();
        match &mut *vs {
            #[cfg(target_os = "windows")]
            VideoSourceKind::Windows(windows_source) => {
                windows_source.stop_streaming();
            }
            #[cfg(target_os = "linux")]
            VideoSourceKind::Pipewire(pipewire_source) => {
                pipewire_source.stop_streaming();
            }
        }
    } else {
        warn!("No video source obj to stop the stream");
    }
    info!("Stop streaming flag set to true");
}

#[tauri::command]
pub fn start_watching(app: AppHandle, stream_port: String, tcp_port: String, streamer_ip: String) {
    let net_info = NetInfo::parse_info(stream_port, tcp_port, streamer_ip)
        .expect("Parsing net info from fontend error");

    info!(
        "Starting to listen with stream_port: {}, tcp_port: {}",
        net_info.stream_port, net_info.tcp_port
    );
    std::thread::spawn(move || {
        match receive(&net_info) {
            Ok(()) => {}
            Err(e) => error!("Client error: {:?}", e),
        };
        app.emit("streaming-stopped", ()).unwrap();
    });
}
