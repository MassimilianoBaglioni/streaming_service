use crate::commands::errors::CommandError;
use crate::state::app_state::{AppState, Session, SessionError};
use crate::state::client_state::ClientState;
use iroh_tickets::endpoint::EndpointTicket;
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use streaming_server::client::client_session::ClientSession;
use streaming_server::client::network::direct::request::DirectNetworkRequest;
use streaming_server::network::transport::StreamingEvent;
use streaming_server::network::ConnectionBuildError;
use streaming_server::platform::decode_backend::DecodeBackend;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info};

#[tauri::command]
pub async fn start_watching_direct(
    app: AppHandle,
    state: State<'_, AppState>,
    stream_port: String,
    tcp_port: String,
    streamer_ip: String,
) -> Result<(), CommandError> {
    info!(
        "Starting watch direct with stream_port: {}, tcp_port: {}, streamer_ip: {}",
        stream_port, tcp_port, streamer_ip
    );

    let port: u16 = stream_port
        .parse()
        .map_err(|e| ConnectionBuildError::InvalidPort(e))?;
    let ip: Ipv4Addr = streamer_ip
        .parse()
        .map_err(|e| ConnectionBuildError::InvalidIp(e))?;
    let address: SocketAddr = format!("{ip}:{port}")
        .parse()
        .map_err(|e| ConnectionBuildError::InvalidIp(e))?;

    let direct_request = DirectNetworkRequest { address };

    let (frontend_sender, frontend_receiver) = mpsc::channel::<StreamingEvent>(16);

    let session = match ClientSession::accept_direct(
        DecodeBackend::WindowsD3D11,
        direct_request,
        frontend_receiver,
    )
    .await
    {
        Ok(session) => session,
        Err(e) => {
            info!("Direct connection refused {:?}", e);
            let _ = app.emit("server-not-streaming", ());
            return Err(CommandError::from(e));
        }
    };

    {
        state
            .set_session(Session::Client(ClientState::Watching(session)))
            .await;
    }
    start_watching(
        app,
        state.streaming_session.clone(),
        frontend_sender.clone(),
    )
    .await?;
    Ok(())
}

#[tauri::command]
pub async fn start_watching_iroh(
    app: AppHandle,
    state: State<'_, AppState>,
    ticket: String,
) -> Result<(), CommandError> {
    let ticket = EndpointTicket::from_str(&ticket).map_err(|e| {
        error!("Failed to parse ticket: {e}");
        CommandError::ConnectionBuildError(ConnectionBuildError::InvalidTicket(e.to_string()))
    })?;

    let (frontend_sender, frontend_receiver) = mpsc::channel::<StreamingEvent>(16);

    let endpoint = state.get_or_create_iroh_endpoint().await?;

    // TODO this assumes windows, should be created based on the info given at compile time
    let session = match ClientSession::accept_iroh(
        endpoint,
        ticket,
        DecodeBackend::WindowsD3D11,
        frontend_receiver,
    )
    .await
    {
        Ok(session) => session,
        Err(e) => {
            info!("Iroh connection refused {:?}", e);
            if let Some(old_endpoint) = state.iroh_endpoint.lock().await.take() {
                old_endpoint.close().await;
            }
            *state.streaming_session.lock().await = None;
            let _ = app.emit("server-not-streaming", ());
            return Err(CommandError::from(e));
        }
    };

    *state.streaming_session.lock().await = Some(Session::Client(ClientState::Watching(session)));
    start_watching(
        app,
        state.streaming_session.clone(),
        frontend_sender.clone(),
    )
    .await?;
    Ok(())
}

async fn start_watching(
    app: AppHandle,
    streaming_session: Arc<Mutex<Option<Session>>>,
    frontend_sender: mpsc::Sender<StreamingEvent>,
) -> Result<(), CommandError> {
    let mut guard = streaming_session.lock().await;

    match guard.as_mut().ok_or(SessionError::NoSession)? {
        Session::Client(ClientState::Watching(session)) => {
            let session_clone = streaming_session.clone();
            session
                .start_watching(move |result| {
                    let _frontend_sender = frontend_sender;
                    if let Err(e) = result {
                        error!("Client error: {:?}", e);
                    }
                    tokio::spawn(async move {
                        let _ = session_clone.lock().await.take();
                    });
                    let _ = app.emit("streaming-stopped", ());
                })
                .await?;
            Ok(())
        }
        _ => Err(SessionError::NoSession.into()),
    }
}

#[tauri::command]
pub async fn stop_watching(state: State<'_, AppState>) -> Result<(), CommandError> {
    if let Some(mut session) = state.streaming_session.lock().await.take() {
        session.stop_watching().await?;
    }
    info!("Called stop watching");
    Ok(())
}
