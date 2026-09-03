use crate::state::app_state::AppState;
use iroh_tickets::endpoint::EndpointTicket;
use streaming_server::network::client_connection::ClientConnection;
use streaming_server::network::iroh::build_endpoint;
use streaming_server::network::streaming_event::StreamingEvent;
use streaming_server::network::ConnectionBuildInfo;
#[cfg(target_os = "windows")]
use streaming_server::video::windows_impl::client::windows_client::WindowsClient;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::mpsc;
use tracing::{error, info};

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

    let (sender, receiver) = mpsc::channel::<StreamingEvent>(16);

    let mut streaming_session = state.streaming_session.lock().await;
    streaming_session.stop_watching_sender = Some(sender.clone());

    let client_connection = ClientConnection::new(connection_build_info, Some(receiver));

    start_watching(app, client_connection).await
}
#[tauri::command]
pub async fn start_watching_iroh(
    app: AppHandle,
    state: State<'_, AppState>,
    ticket: Option<String>,
) -> Result<(), String> {
    let ticket: EndpointTicket = ticket
        .expect("No ticket received from frontend")
        .parse()
        .expect("Failed to parse ticket");

    let (sender, receiver) = mpsc::channel::<StreamingEvent>(16);

    let mut streaming_session = state.streaming_session.lock().await;
    streaming_session.stop_watching_sender = Some(sender.clone());

    let endpoint = state.get_or_create_iroh_endpoint().await.map_err(|e| e.to_string())?;

    let client_connection =
        ClientConnection::new_from_ticket_and_recv(ticket, receiver, &endpoint).await;

    start_watching(app, client_connection).await
}

async fn start_watching(app: AppHandle, client_connection: ClientConnection) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    let mut client = WindowsClient::from(client_connection);

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
    if let Some(sender) = state
        .streaming_session
        .lock()
        .await
        .stop_watching_sender
        .as_ref()
    {
        sender
            .send(StreamingEvent::ClientQuit)
            .await
            .expect("Failed to send client stop event");
    }
    info!("Called stop watching");
    Ok(())
}
