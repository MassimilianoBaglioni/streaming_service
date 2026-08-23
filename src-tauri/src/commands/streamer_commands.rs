use crate::state::app_state::AppState;
#[cfg(target_os = "windows")]
use crate::windows_impl::show_picker;
use iroh_tickets::endpoint::EndpointTicket;
use serde::Deserialize;
use streaming_server::video::video_source::VideoSourceKind;
use tauri::{AppHandle, State};
use tracing::{info, warn};
use windows::Graphics::Capture::GraphicsCaptureItem;

use streaming_server::network::ConnectionBuildInfo;
use streaming_server::video::windows_impl::windows_streaming_settings::WindowsStreamingSettings;

use streaming_server::network::iroh::{build_ticket, establish_iroh_server_connection};
use streaming_server::network::server_connection::ServerConnection;
use streaming_server::video::commons::scaling_method::ScalingMethod;
use streaming_server::video::windows_impl::windows_source::WindowsSource;
#[cfg(target_os = "linux")]
use {
    crate::linux_impl::get_wayland_handles,
    streaming_server::video::linux_impl::pipewire_client::PipewireClient,
};

#[cfg(target_os = "linux")]
#[tauri::command]
pub fn start_streaming(
    state: tauri::State<AppState>,
    app: AppHandle,
    stream_port: String,
    tcp_port: String,
    watcher_address: String,
) {
    let net_info = NetInfo::parse_info(stream_port, tcp_port, watcher_address)
        .expect("Parsing net info from fontend error");

    // This must stay AFTER async calls, can't lock with async functions
    let mut lock = state.video_source.lock().unwrap();

    if lock.is_none() {
        info!("Video source not initialized, initializing inside start_streaming");
        use streaming_server::video::linux_impl::pipewire_source::create_pipewire_video_source;

        let handles = get_wayland_handles(&app).expect("Failed to retrieve wayland handles.");

        let new_source = create_pipewire_video_source(handles, &net_info);
        *lock = Some(new_source);
    }

    if let Some(video_source) = lock.as_ref() {
        info!("Video source already initialized, starting stream");
        let mut vs = video_source.lock().unwrap();
        match &mut *vs {
            VideoSourceKind::Pipewire(pipewire_source) => {
                pipewire_source.update_network_info(&net_info);
                pipewire_source.start_streaming();
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoSettings {
    pub fps: u16,
    pub bitrate: u32,
    pub resolution: u16,
    pub scaling_method: ScalingMethod,
}

#[cfg(target_os = "windows")]
#[tauri::command]
pub async fn start_streaming_direct(
    state: State<'_, AppState>,
    app: AppHandle,
    watcher_stream_port: String,
    watcher_address: String,
    video_settings: VideoSettings,
) -> Result<(), String> {
    // Take the capture item asap, Windows requires the window to be in foreground to show the picker, otherwise this fails.
    let graphics_capture_item = show_picker(app.clone()).await;

    let connection_build_info =
        ConnectionBuildInfo::from_direct_info(watcher_stream_port, watcher_address)
            .expect("Failed to build connection info");

    let server_connection = ServerConnection::from(connection_build_info);

    let result = start_streaming(
        state,
        &video_settings,
        server_connection,
        graphics_capture_item.expect("Failed to get graphics capture item from picker"),
    )
    .await;

    result
}

#[cfg(target_os = "windows")]
#[tauri::command]
pub async fn start_streaming_iroh(
    state: State<'_, AppState>,
    app: AppHandle,
    video_settings: VideoSettings,
) -> Result<(), String> {
    // Take the capture item asap, Windows requires the window to be in foreground to show the picker, otherwise this fails.
    let graphics_capture_item = show_picker(app.clone()).await;

    let handle = {
        let mut streaming_session = state.streaming_session.lock().await;
        streaming_session
            .tokio_handler
            .take()
            .expect("Tokio connection task was not started")
        // guard dropped here, at end of scope
    };

    handle
        .await
        .expect("Tokio connection start failed")
        .map_err(|e| e.to_string())?;

    let mut streaming_session = state.streaming_session.lock().await;
    let server_connection = streaming_session
        .server_connection
        .take()
        .expect("No server connection found for iroh.");
    drop(streaming_session);

    start_streaming(
        state,
        &video_settings,
        server_connection,
        graphics_capture_item.expect("Failed to get graphics capture item from picker"),
    )
    .await
}

async fn start_streaming(
    state: State<'_, AppState>,
    video_settings: &VideoSettings,
    server_connection: ServerConnection,
    graphics_capture_item: GraphicsCaptureItem,
) -> Result<(), String> {
    // This must stay AFTER async calls, can't lock with async functions
    let mut streaming_session = state.streaming_session.lock().await;

    let windows_streaming_settings = map_windows_settings(&video_settings);

    let new_source = VideoSourceKind::Windows(WindowsSource::new(
        server_connection,
        Some(graphics_capture_item),
        windows_streaming_settings,
    ));

    streaming_session.video_source = Some(new_source);

    match streaming_session.video_source.as_mut().unwrap() {
        VideoSourceKind::Windows(ref mut windows_source) => {
            windows_source.start_streaming().await;
        }
    }

    Ok(())
}

#[tauri::command]
pub async fn stop_streaming(state: State<'_, AppState>) -> Result<(), String> {
    // TODO check that we close the tcp sockets when stop streaming or stop watching are called
    if let Some(video_source) = state.streaming_session.lock().await.video_source.as_mut() {
        match &mut *video_source {
            #[cfg(target_os = "windows")]
            VideoSourceKind::Windows(windows_source) => {
                windows_source.stop_streaming().await;
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
    Ok(())
}
#[tauri::command]
pub async fn generate_ticket(state: State<'_, AppState>) -> Result<EndpointTicket, String> {
    let (ticket, endpoint) = build_ticket().await.expect("Failed to generate ticket");

    let streaming_session = state.streaming_session.clone();
    let stream_session_clone = streaming_session.clone();

    let iroh_connection_task_handler = tokio::spawn(async move {
        info!("Generate ticket routine started");
        let server_connection = establish_iroh_server_connection(endpoint).await?;

        stream_session_clone.lock().await.server_connection = Some(server_connection);
        Ok(())
    });

    streaming_session.lock().await.tokio_handler = Some(iroh_connection_task_handler);

    Ok(ticket)
}

fn map_windows_settings(frontend_settings: &VideoSettings) -> WindowsStreamingSettings {
    let mut windows_settings = WindowsStreamingSettings::default();

    windows_settings.fps = frontend_settings.fps as u32;
    windows_settings.bitrate = frontend_settings.bitrate;
    windows_settings.resolution = frontend_settings.resolution;
    windows_settings.scaling_method = frontend_settings.scaling_method;

    windows_settings
}
