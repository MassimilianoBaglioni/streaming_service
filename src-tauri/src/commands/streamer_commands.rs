#[cfg(target_os = "linux")]
use crate::screen::linux::screen_picker::get_wayland_handles;
use crate::state::app_state::{AppState, ApplicationStateError, Session, SessionError};
#[cfg(target_os = "windows")]
use iroh_tickets::endpoint::EndpointTicket;
use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use streaming_server::media::element_lookup::get_app_sink;
use streaming_server::network::iroh::establish_iroh_server_connection;
use streaming_server::network::ConnectionBuildError;
use streaming_server::server::capture::capture_source::CaptureSource;
#[cfg(target_os = "linux")]
use streaming_server::server::capture::linux::pipewire_client::PipewireClient;
use streaming_server::server::capture::scaling_method::ScalingMethod;
use streaming_server::server::capture::windows::windows_source::WindowsCaptureSource;
use streaming_server::server::capture::windows::windows_streaming_settings::VideoStreamingSettings;
use streaming_server::server::network::connection_config::ConnectionConfig;
use streaming_server::server::network::iroh::iroh_transport::IrohProtocol;
use streaming_server::server::network::iroh::request::IrohConnectionRequest;
use streaming_server::server::network::server_protocol::ServerProtocol;
use streaming_server::server::network::server_protocol::ServerProtocol::Direct;

use crate::commands::errors::CommandError;
use crate::screen::windows::screen_picker::show_picker;
use crate::state::streamer_state::{StreamerState, StreamingStateError};
use streaming_server::error::IrohStateError;
use streaming_server::server::network::direct::direct_transport::DirectProtocol;
use streaming_server::server::network::direct::request::DirectConnectionRequest;
use streaming_server::server::pipeline::builder::PipelineBuilder;
use streaming_server::server::server_errors::IrohAcceptClientError;
use streaming_server::server::server_session::ServerSession;
use tauri::{AppHandle, State};
use tracing::{info, warn};
use windows::Graphics::Capture::GraphicsCaptureItem;

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
        use streaming_server::server::capture::linux::pipewire_source::create_pipewire_video_source;

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

impl Into<VideoStreamingSettings> for VideoSettings {
    fn into(self) -> VideoStreamingSettings {
        let mut default_windows_stream_settings = VideoStreamingSettings::default();
        default_windows_stream_settings.fps = self.fps as u32;
        default_windows_stream_settings.bitrate = self.bitrate;
        default_windows_stream_settings.resolution = self.resolution;
        default_windows_stream_settings.scaling_method = self.scaling_method;

        default_windows_stream_settings
    }
}

#[cfg(target_os = "windows")]
#[tauri::command]
pub async fn start_streaming_direct(
    state: State<'_, AppState>,
    app: AppHandle,
    watcher_stream_port: String,
    watcher_address: String,
    video_settings: VideoSettings,
) -> Result<(), CommandError> {
    // Take the capture item asap, Windows requires the window to be in foreground to show the picker, otherwise this fails.
    let graphics_capture_item = show_picker(app.clone()).await;

    let connection_config = create_direct_connection_config(
        &video_settings,
        graphics_capture_item.as_ref().unwrap(),
        watcher_stream_port,
        watcher_address,
    )?;

    let mut server_protocol = Direct(DirectProtocol::from(&connection_config));

    let evets_communication_channel = server_protocol.accept().await?;

    let pipeline = PipelineBuilder::new(state.capture_backend).build_pipeline(connection_config)?;

    let capture_source = CaptureSource::Windows(WindowsCaptureSource::new(
        graphics_capture_item.unwrap(),
        video_settings.into(),
        pipeline,
    ));

    let server_session = ServerSession::start_streaming(
        evets_communication_channel,
        server_protocol,
        capture_source,
    )
    .await;

    state
        .set_session(Session::Streamer(StreamerState::Ready(server_session)))
        .await;

    Ok(())
}

#[cfg(target_os = "windows")]
#[tauri::command]
pub async fn start_streaming_iroh(
    state: State<'_, AppState>,
    app: AppHandle,
    video_settings: VideoSettings,
) -> Result<(), CommandError> {
    // Take the capture item asap, Windows requires the window to be in foreground to show the picker, otherwise this fails.
    let graphics_capture_item = show_picker(app.clone()).await.unwrap();

    let connection_config = create_iroh_connection_config(&video_settings, &graphics_capture_item);

    let pipeline = PipelineBuilder::new(state.capture_backend).build_pipeline(connection_config)?;

    let builder = state
        .lock_session()
        .await?
        .as_mut()
        .ok_or(ApplicationStateError::from(SessionError::NoSession))?
        .take_streamer_iroh_builder()?;

    // Await the task accepting clients
    let network_info = builder
        .accept_client_handle
        .ok_or(IrohAcceptClientError::EmptyAcceptHandle)?
        .await
        .map_err(|_| IrohAcceptClientError::AcceptHandleJoinFailed)??;

    let server_protocol = ServerProtocol::Iroh(IrohProtocol::new(
        get_app_sink(&pipeline, "rtp_sink"),
        network_info.frames_channel,
        network_info.iroh_connection,
    ));

    // TODO this assumes windows, should be based on os instead
    let capture_source = CaptureSource::Windows(WindowsCaptureSource::new(
        graphics_capture_item,
        video_settings.into(),
        pipeline,
    ));

    let server_session = ServerSession::start_streaming(
        network_info.client_channel,
        server_protocol,
        capture_source,
    )
    .await;

    state
        .lock_session()
        .await?
        .as_mut()
        .ok_or(StreamingStateError::NotWaiting)?
        .set_streamer_state(StreamerState::Ready(server_session))?;

    Ok(())
}

#[tauri::command]
pub async fn stop_streaming(state: State<'_, AppState>) -> Result<(), CommandError> {
    info!("Stop streaming command called");
    state
        .lock_session()
        .await?
        .as_mut()
        .ok_or(StreamingStateError::NotWaiting)?
        .stop_streaming()
        .await?;

    Ok(())
}

#[tauri::command]
pub async fn generate_ticket(state: State<'_, AppState>) -> Result<EndpointTicket, CommandError> {
    let endpoint = state.get_or_create_iroh_endpoint().await?;

    let ticket = EndpointTicket::new(
        endpoint
            .lock()
            .await
            .as_ref()
            .ok_or(IrohStateError::NoEndpointFound)?
            .addr(),
    );

    state.set_session(Session::new_streamer()).await;

    let streaming_session = state.streaming_session.clone();

    let accept_client_handler = tokio::spawn({
        let endpoint = endpoint
            .lock()
            .await
            .clone()
            .ok_or(IrohStateError::NoEndpointFound)?;

        async move {
            info!("Generate ticket routine started");
            establish_iroh_server_connection(endpoint).await
        }
    });

    state
        .lock_session()
        .await?
        .as_mut()
        .ok_or(IrohStateError::NoEndpointFound)?
        .set_iroh_accept_handle(accept_client_handler)?;

    Ok(ticket)
}

fn create_iroh_connection_config(
    video_settings: &VideoSettings,
    graphics_capture_item: &GraphicsCaptureItem,
) -> ConnectionConfig {
    let size = graphics_capture_item
        .Size()
        .expect("Failed to get capture item");
    let width = size.Width as u32;
    let height = size.Height as u32;

    ConnectionConfig::Iroh(IrohConnectionRequest {
        scaling_method: video_settings.scaling_method,
        resolution: video_settings.resolution,
        bitrate: video_settings.bitrate,
        height,
        width,
    })
}

fn create_direct_connection_config(
    video_settings: &VideoSettings,
    graphics_capture_item: &GraphicsCaptureItem,
    watcher_stream_port: String,
    watcher_address: String,
) -> Result<ConnectionConfig, ConnectionBuildError> {
    let watcher_stream_port: u16 = watcher_stream_port.parse().map_err(|e| {
        warn!("Invalid stream port {}: {}", watcher_stream_port, e);
        ConnectionBuildError::InvalidPort(e)
    })?;

    let watcher_address: SocketAddr = format!("{}:{}", watcher_address, watcher_stream_port)
        .parse()
        .map_err(|e| ConnectionBuildError::InvalidIp(e))?;

    let size = graphics_capture_item
        .Size()
        .expect("Failed to get capture item");
    let width = size.Width as u32;
    let height = size.Height as u32;

    let client_ip = match watcher_address.ip() {
        IpAddr::V4(ipv4) => ipv4,
        IpAddr::V6(_) => return Err(ConnectionBuildError::NotIpv4),
    };

    Ok(ConnectionConfig::DirectConnection(
        DirectConnectionRequest {
            scaling_method: video_settings.scaling_method,
            resolution: video_settings.resolution,
            bitrate: video_settings.bitrate,
            height,
            width,
            client_ip,
            client_port: watcher_address.port(),
            socket_addr: watcher_address,
        },
    ))
}
