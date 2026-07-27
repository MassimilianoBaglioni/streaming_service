use crate::state::app_state::AppState;
#[cfg(target_os = "windows")]
use streaming_server::video::windows_impl::client::windows_client::WindowsClient;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::mpsc;
use tracing::{error, info};

use streaming_server::network::streaming_event::StreamingEvent;
use streaming_server::network::ConnectionBuildInfo;

#[tauri::command]
pub async fn start_watching_direct(
    app: AppHandle,
    state: State<'_, AppState>,
    stream_port: String,
    tcp_port: String,
    streamer_ip: String,
) -> Result<(), String> {
    info!(
        "Starting watch direct with stream_port: {}, tcp_port: {}, streamer_ip: {}",
        stream_port, tcp_port, streamer_ip
    );
    let connection_build_info = ConnectionBuildInfo::from_direct_info(stream_port, streamer_ip)
        .expect("Failed to build connection info");

    start_watching(app, state, connection_build_info).await
}
#[tauri::command]
pub async fn start_watching_iroh(
    app: AppHandle,
    state: State<'_, AppState>,
    ticket: Option<String>,
) -> Result<(), String> {
    let connection_build_info = ConnectionBuildInfo::from_ticket(ticket.unwrap().parse().unwrap())
        .await
        .expect("Failed to build connection info");

    start_watching(app, state, connection_build_info).await
}

async fn start_watching(
    app: AppHandle,
    state: State<'_, AppState>,
    connection_build_info: ConnectionBuildInfo,
) -> Result<(), String> {
    let (sender, receiver) = mpsc::channel::<StreamingEvent>(16);

    *state.stop_watching_sender.lock().await = Some(sender.clone());

    #[cfg(target_os = "windows")]
    let mut client = WindowsClient::new(connection_build_info, receiver);

    #[cfg(target_os = "linux")]
    let client = PipewireClient::new(net_info);

    tokio::spawn(async move {
        {
            match client.receive().await {
                Ok(_) => {}
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
pub async fn stop_watching(state: State<'_, AppState>) -> Result<(), String> {
    if let Some(sender) = state.stop_watching_sender.lock().await.as_ref() {
        sender
            .send(StreamingEvent::ClientQuit)
            .await
            .expect("Failed to send client stop event");
    }
    info!("Called stop watching");
    Ok(())
}
