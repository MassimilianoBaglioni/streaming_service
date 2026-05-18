use crate::state::app_state::AppState;
use streaming_server::{network::NetInfo, video::video_source::VideoSourceKind};
use tauri::{AppHandle, Emitter};
use tracing::{error, info, warn};
#[cfg(target_os = "windows")]
use {
    crate::windows_impl::show_picker,
    streaming_server::video::windows_impl::{
        windows_client::WindowsClient, windows_source::create_windows_video_source,
    },
};

#[cfg(target_os = "linux")]
use streaming_server::video::linux_impl::pipewire_client::PipewireClient;

#[tauri::command]
pub fn start_streaming(
    state: tauri::State<'_, AppState>,
    app: AppHandle,
    stream_port: String,
    tcp_port: String,
    watcher_address: String,
) -> Result<(), String> {
    let net_info = NetInfo::parse_info(stream_port, tcp_port, watcher_address)
        .expect("Parsing net info from fontend error");

    #[cfg(target_os = "windows")]
    let capture_item = tokio::runtime::Handle::current().block_on(show_picker(app.clone()));

    // This must stay AFTER async calls, can't lock with async functions
    let mut lock = state.video_source.lock().unwrap();

    if lock.is_none() {
        info!("Video source not initialized, initializing inside start_streaming");
        #[cfg(target_os = "linux")]
        {
            use crate::linux_impl::get_wayland_handles;
            use streaming_server::video::linux_impl::pipewire_source::create_pipewire_video_source;

            let handles = get_wayland_handles(&app).expect("Failed to retrieve wayland handles.");

            let new_source = create_pipewire_video_source(handles, &net_info);
            *lock = Some(new_source);
        }
        #[cfg(target_os = "windows")]
        {
            let new_source = create_windows_video_source(&net_info, capture_item.clone());
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
            VideoSourceKind::Pipewire(pipewire_source) => {
                pipewire_source.update_network_info(&net_info);
                pipewire_source.start_streaming();
            }
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

    #[cfg(target_os = "windows")]
    let client = WindowsClient::new(net_info);

    #[cfg(target_os = "linux")]
    let client = PipewireClient::new(net_info);

    std::thread::spawn(move || {
        match client.receive() {
            Ok(()) => {}
            Err(e) => error!("Client error: {:?}", e),
        };
        app.emit("streaming-stopped", ()).unwrap();
    });
}
