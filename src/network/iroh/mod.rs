use crate::network::server_connection::{ServerConnection, ServerConnectionMode};
use crate::network::transport::{TaskTransport};
use anyhow::{anyhow, Result};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{endpoint::presets, Endpoint};
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

    let events_transport = Some(TaskTransport::new(
        send_events_stream,
        recv_events_stream,
        32,
        1024,
    ));

    let events_sender = Some(events_transport.as_ref().unwrap().serialized_sender());

    Ok(ServerConnection {
        connection_mode: ServerConnectionMode::Iroh {
            frames_stream: Some(TaskTransport::new(
                send_frames_stream,
                recv_frames_stream,
                8,
                65536,
            )),
            iroh_connection,
        },
        events_transport,
        events_sender,
    })
}

// The server opens two connections, that the client will accept. The server writes a tag to distinguish them on the client side.
// This is needed because nothing guarantees that these arrive in the same order we write them in the code i.e. packet loss and similar.
async fn open_tagged_bi(conn: &Connection, tag: u8) -> Result<(SendStream, RecvStream)> {
    let (mut send, recv) = conn.open_bi().await?;
    send.write_all(&[tag]).await?;
    Ok((send, recv))
}
