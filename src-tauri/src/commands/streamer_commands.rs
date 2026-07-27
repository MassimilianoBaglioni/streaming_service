use crate::state::app_state::AppState;
use iroh_tickets::endpoint::EndpointTicket;
use serde::Deserialize;
use streaming_server::{network::ConnectionMode, video::video_source::VideoSourceKind};
use tauri::{AppHandle, State};
use tracing::{info, warn};
#[cfg(target_os = "windows")]
use {
    crate::windows_impl::show_picker,
    streaming_server::video::windows_impl::windows_source::create_windows_video_source,
};

use streaming_server::network::ConnectionBuildInfo;
use streaming_server::video::windows_impl::windows_streaming_settings::WindowsStreamingSettings;

use streaming_server::video::commons::scaling_method::ScalingMethod;
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
    let connection_build_info =
        ConnectionBuildInfo::from_direct_info(watcher_stream_port, watcher_address)
            .expect("Failed to build connection info");

    let result = start_streaming(state, app, &video_settings, connection_build_info).await;

    result
}

#[cfg(target_os = "windows")]
#[tauri::command]
pub async fn start_streaming_iroh(
    state: State<'_, AppState>,
    app: AppHandle,
    video_settings: VideoSettings,
) -> Result<(), String> {
    let (ticket, endpoint) = match state.connection_mode.lock().await.as_mut().unwrap() {
        ConnectionMode::Iroh {
            ticket, endpoint, ..
        } => (ticket.clone(), endpoint.clone()),
        ConnectionMode::Direct { .. } => {
            return Err("Invalid connection mode for iroh streaming".to_string());
        }
    };

    let connection_build_info = ConnectionBuildInfo::from_endpoint_and_ticket(endpoint, ticket)
        .await
        .expect("Failed to build connection info from ticket");

    start_streaming(state, app, &video_settings, connection_build_info).await
}

async fn start_streaming(
    state: State<'_, AppState>,
    app: AppHandle,
    video_settings: &VideoSettings,
    connection_build_info: ConnectionBuildInfo,
) -> Result<(), String> {
    let capture_item = show_picker(app.clone()).await;

    // This must stay AFTER async calls, can't lock with async functions
    let mut lock = state.video_source.lock().await;

    let windows_streaming_settings = map_windows_settings(&video_settings);

    if lock.is_none() {
        info!("Video source not initialized, initializing inside start_streaming");

        let new_source = create_windows_video_source(
            connection_build_info.clone(),
            capture_item.clone(),
            windows_streaming_settings.clone(),
        );
        *lock = Some(new_source);
    }

    if let Some(video_source) = lock.as_ref() {
        info!("Video source already initialized, starting stream");
        let mut vs = video_source.lock().await;
        match &mut *vs {
            VideoSourceKind::Windows(windows_source) => {
                windows_source.set_graphics_capture_item(capture_item.clone());
                info!("ConnectionBuildInfo value: {:?}", connection_build_info);

                windows_source
                    .update_network_info(connection_build_info)
                    .await;
                windows_source.windows_settings = windows_streaming_settings;

                windows_source.start_streaming().await;
            }
        }
    }

    Ok(())
}

#[tauri::command]
pub async fn stop_streaming(state: State<'_, AppState>) -> Result<(), String> {
    // TODO check that we close the tcp sockets when stop streaming or stop watching are called
    if let Some(video_source) = state.video_source.lock().await.as_ref() {
        let mut vs = video_source.lock().await;
        match &mut *vs {
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
    let (ticket, endpoint) = streaming_server::network::iroh::generate_ticket()
        .await
        .expect("Failed to generate ticket/endpoint");

    *state.connection_mode.lock().await = Some(ConnectionMode::Iroh {
        connection: None,
        endpoint,
        ticket: ticket.clone(),
    });

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
