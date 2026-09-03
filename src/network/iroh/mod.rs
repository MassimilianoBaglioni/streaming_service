use crate::network::server_connection::{ServerConnection, ServerConnectionMode};
use crate::network::streaming_events_server::IrohEventsStream;
use anyhow::{anyhow, Result};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{endpoint::presets, Endpoint};
use iroh_tickets::endpoint::EndpointTicket;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

const FRAMES_TAG: u8 = 0;
const EVENTS_TAG: u8 = 1;

pub mod connection;

pub(crate) const ALPN: &[u8] = b"myapp/test/1";

pub async fn build_endpoint() -> Result<Endpoint> {
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    endpoint.online().await;

    Ok(endpoint)
}

pub async fn establish_iroh_server_connection(endpoint: Endpoint) -> Result<ServerConnection> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| anyhow!("Endpoint closed while waiting for incoming connection"))?;

    info!("Accepted iroh client");

    let iroh_connection = incoming.await?;
    let (send_frames_stream, recv_frames_stream) =
        open_tagged_bi(&iroh_connection, FRAMES_TAG).await?;
    info!("Opened bi connection on the server for frames");
    let (send_events_stream, recv_events_stream) =
        open_tagged_bi(&iroh_connection, EVENTS_TAG).await?;
    info!("Opened bi connection on the server for events");

    info!("Opened bi connection on the server");

    Ok(ServerConnection {
        connection_mode: ServerConnectionMode::Iroh {
            frames_stream: IrohStream::new(send_frames_stream, recv_frames_stream),
            events_stream: IrohEventsStream::new(send_events_stream, recv_events_stream),
            iroh_connection,
        },
    })
}

// The server opens two connections, that the client will accept. The server writes a tag to distinguish them on the client side.
// This is needed because nothing guarantees that these arrive in the same order we write them in the code i.e. packet loss and similar.
async fn open_tagged_bi(conn: &Connection, tag: u8) -> Result<(SendStream, RecvStream)> {
    let (mut send, recv) = conn.open_bi().await?;
    send.write_all(&[tag]).await?;
    Ok((send, recv))
}

#[derive(Debug)]
pub struct IrohStream {
    pub send_stream: Arc<Mutex<SendStream>>,
    pub recv_stream: Arc<Mutex<RecvStream>>,
}

impl IrohStream {
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
