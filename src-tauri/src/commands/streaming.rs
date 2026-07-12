use std::{str::FromStr, sync::mpsc::channel};

use crate::state::app_state::AppState;
use ::iroh::{endpoint::presets, Endpoint};
use iroh_tickets::endpoint::EndpointTicket;
use serde::Deserialize;
use streaming_server::network::iroh;
#[cfg(target_os = "windows")]
use streaming_server::{
    network::{iroh::IrohInfo, ConnectionMode, NetInfo},
    video::{
        commons::scaling_method::ScalingMethod,
        video_source::VideoSourceKind,
        windows_impl::{
            client::windows_client::StopWatchingEvent,
            windows_streaming_settings::WindowsStreamingSettings,
        },
    },
};
use tauri::{AppHandle, Emitter};
use tracing::{error, info, warn};
#[cfg(target_os = "windows")]
use {
    crate::windows_impl::show_picker,
    streaming_server::video::windows_impl::{
        client::windows_client::WindowsClient, windows_source::create_windows_video_source,
    },
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoSettings {
    pub fps: u16,
    pub bitrate: u32,
    pub resolution: u16,
    pub scaling_method: ScalingMethod,
}

#[derive(Deserialize)]
pub enum ConnectionModeFrontend {
    Direct,
    Iroh,
}

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

#[cfg(target_os = "windows")]
#[tauri::command]
pub async fn start_streaming(
    state: tauri::State<'_, AppState>,
    app: AppHandle,
    stream_port: String,
    tcp_port: String,
    watcher_address: String,
    video_settings: VideoSettings,
    connection_mode: ConnectionModeFrontend,
) -> Result<(), String> {
    let connection_mode = get_connection_mode(connection_mode);

    let is_direct = match connection_mode {
        ConnectionMode::Direct => true,
        ConnectionMode::Iroh { .. } => false,
    };

    let mut net_info = NetInfo::parse_info(stream_port, tcp_port, watcher_address, connection_mode)
        .expect("Parsing net info from frontend error");

    /*
     *   TODO this is trash actually. We only set the values inside the state when we press the generate ticket button from the ui instead of passing it.
     */

    if is_direct {
    } else {
        let iroh_info = state.iroh_info.lock().await.clone().unwrap();
        net_info.connection_mode = ConnectionMode::Iroh { info: iroh_info };
    }

    let capture_item = show_picker(app.clone()).await;

    // This must stay AFTER async calls, can't lock with async functions
    let mut lock = state.video_source.lock().await;

    let windows_streaming_settings = map_windows_settings(&video_settings);

    if lock.is_none() {
        info!("Video source not initialized, initializing inside start_streaming");

        let new_source = create_windows_video_source(
            &net_info,
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
                info!("NETINFO value: {:?}", net_info);
                windows_source.update_network_info(&net_info);
                windows_source.windows_settings = windows_streaming_settings;
                windows_source.start_streaming().await;
            }
        }
    }

    Ok(())
}

#[tauri::command]
pub async fn stop_streaming(state: tauri::State<'_, AppState>) -> Result<(), String> {
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
pub async fn start_watching(
    app: AppHandle,
    state: tauri::State<'_, AppState>,
    stream_port: String,
    tcp_port: String,
    streamer_ip: String,
    ticket: Option<String>,
) -> Result<(), String> {
    let connection_mode = if let Some(ticket) = ticket.filter(|value| !value.trim().is_empty()) {
        let endpoint = Endpoint::bind(presets::N0)
            .await
            .expect("Failed to create endpoint");
        let parsed_ticket = EndpointTicket::from_str(ticket.trim())
            .map_err(|error| format!("Invalid invite link: {error}"))?;
        ConnectionMode::Iroh {
            info: IrohInfo::new(parsed_ticket, endpoint),
        }
    } else {
        ConnectionMode::Direct
    };

    let target_ip = match connection_mode {
        ConnectionMode::Direct => streamer_ip,
        ConnectionMode::Iroh { .. } => "127.0.0.1".to_string(),
    };

    let net_info = NetInfo::parse_info(stream_port, tcp_port, target_ip, connection_mode)
        .map_err(|error| format!("Parsing net info from frontend error: {error:?}"))?;

    let (sender, receiver) = channel::<StopWatchingEvent>();

    *state.stop_watching_sender.lock().await = Some(sender.clone());

    #[cfg(target_os = "windows")]
    let mut client = WindowsClient::new(net_info, sender, receiver);

    #[cfg(target_os = "linux")]
    let client = PipewireClient::new(net_info);

    tokio::spawn(async move {
        {
            match client.receive().await {
                Ok(()) => {}
                Err(e) => {
                    if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                        if io_err.kind() == std::io::ErrorKind::ConnectionRefused {
                            app.emit("server-not-streaming", ()).unwrap();
                        } else {
                            error!("Client error: {:?}", e);
                            app.emit("streaming-stopped", ()).unwrap();
                        }
                    } else {
                        error!("Client error: {:?}", e);
                        app.emit("streaming-stopped", ()).unwrap();
                    }
                }
            };
            app.emit("streaming-stopped", ()).unwrap();
        }
    });

    Ok(())
}

#[tauri::command]
pub async fn stop_watching(state: tauri::State<'_, AppState>) -> Result<(), String> {
    if let Some(sender) = state.stop_watching_sender.lock().await.as_ref() {
        sender
            .send(StopWatchingEvent::ClientStop)
            .expect("Failed to send client stop event");
    }
    info!("Called stop watching");
    Ok(())
}

#[tauri::command]
pub async fn generate_ticket(state: tauri::State<'_, AppState>) -> Result<EndpointTicket, String> {
    let (ticket, endpoint) = streaming_server::network::iroh::generate_ticket()
        .await
        .expect("Failed to generate ticket/endpoint");

    *state.iroh_info.lock().await = Some(IrohInfo::new(ticket.clone(), endpoint.clone()));

    return Ok(ticket);
}

fn map_windows_settings(frontend_settings: &VideoSettings) -> WindowsStreamingSettings {
    let mut windows_settings = WindowsStreamingSettings::default();

    windows_settings.fps = frontend_settings.fps as u32;
    windows_settings.bitrate = frontend_settings.bitrate;
    windows_settings.resolution = frontend_settings.resolution;
    windows_settings.scaling_method = frontend_settings.scaling_method;

    windows_settings
}

fn get_connection_mode(connection_mode: ConnectionModeFrontend) -> ConnectionMode {
    match connection_mode {
        ConnectionModeFrontend::Direct => ConnectionMode::Direct,
        ConnectionModeFrontend::Iroh => ConnectionMode::Iroh {
            info: IrohInfo::default(),
        },
    }
}
