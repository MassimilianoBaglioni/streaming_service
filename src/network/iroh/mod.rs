use crate::network::server_connection::{ServerConnection, ServerConnectionMode};
use anyhow::{anyhow, Result};
use iroh::endpoint::{RecvStream, SendStream};
use iroh::{endpoint::presets, Endpoint};
use iroh_tickets::endpoint::EndpointTicket;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub mod connection;

pub(crate) const ALPN: &[u8] = b"myapp/test/1";

pub async fn build_ticket() -> Result<(EndpointTicket, Endpoint)> {
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    endpoint.online().await;

    Ok((EndpointTicket::new(endpoint.addr()), endpoint))
}

pub async fn establish_iroh_server_connection(endpoint: Endpoint) -> Result<ServerConnection> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| anyhow!("Endpoint closed while waiting for incoming connection"))?;

    info!("Accepted iroh client");

    let iroh_connection = incoming.await?;
    let (send_stream, recv_stream) = iroh_connection.open_bi().await?;

    info!("Opened bi connection on the server");

    Ok(ServerConnection {
        connection_mode: ServerConnectionMode::Iroh {
            frames_stream: FramesStreaming::new(send_stream, recv_stream),
            endpoint,
            iroh_connection,
        },
    })
}

pub struct FramesStreaming {
    send_stream: Arc<Mutex<SendStream>>,
    recv_stream: Arc<Mutex<RecvStream>>,
}

impl FramesStreaming {
    pub fn new(send_stream: SendStream, recv_stream: RecvStream) -> Self {
        Self {
            send_stream: Arc::new(Mutex::new(send_stream)),
            recv_stream: Arc::new(Mutex::new(recv_stream)),
        }
    }

    pub async fn get_send_lock(&self) -> tokio::sync::MutexGuard<'_, SendStream> {
        self.send_stream.lock().await
    }

    pub async fn get_recv_lock(&self) -> tokio::sync::MutexGuard<'_, RecvStream> {
        self.recv_stream.lock().await
    }
}
