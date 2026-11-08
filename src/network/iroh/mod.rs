use crate::error::IrohProtocolError;
use crate::network::transport::{PeerCommunicationChannel, SerializedSender, StreamingEvent};
use iroh::endpoint::{BindError, Connection, RecvStream, SendStream};
use iroh::{endpoint::presets, Endpoint};
use tracing::info;
use crate::network::{EVENTS_TAG, FRAMES_TAG};

pub struct IrohNetworkInfo {
    pub iroh_connection: Connection,
    pub client_channel: PeerCommunicationChannel,
    pub events_sender: SerializedSender<StreamingEvent>,
    pub frames_channel: PeerCommunicationChannel,
}

pub(crate) const ALPN: &[u8] = b"myapp/test/1";

pub async fn build_endpoint() -> Result<Endpoint, BindError> {
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    endpoint.online().await;

    Ok(endpoint)
}

pub async fn establish_iroh_server_connection(
    endpoint: Endpoint,
) -> Result<IrohNetworkInfo, IrohProtocolError> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| IrohProtocolError::ServerAcceptError)?;
    info!("Accepted iroh client");

    let iroh_connection = incoming
        .await
        .map_err(|_| IrohProtocolError::ServerAcceptError)?;

    let (send_frames, recv_frames) = open_tagged_bi(&iroh_connection, FRAMES_TAG)
        .await
        .map_err(|_| IrohProtocolError::ServerAcceptError)?;
    let frames_channel = PeerCommunicationChannel::new(send_frames, recv_frames, 8, 65536);
    info!("Opened bi connection on the server for frames");

    let (send_events_stream, recv_events_stream) = open_tagged_bi(&iroh_connection, EVENTS_TAG)
        .await
        .map_err(|_| IrohProtocolError::ServerAcceptError)?;
    info!("Opened bi connection on the server for events");

    info!("Opened bi connection on the server");

    let client_channel =
        PeerCommunicationChannel::new(send_events_stream, recv_events_stream, 32, 1024);

    let events_sender = client_channel.serialized_sender();

    Ok(IrohNetworkInfo {
        iroh_connection,
        client_channel,
        events_sender,
        frames_channel,
    })
}

// The server opens two connections, that the client will accept. The server writes a tag to distinguish them on the client side.
// This is needed because nothing guarantees that these arrive in the same order we write them in the code i.e. packet loss and similar.
async fn open_tagged_bi(
    conn: &Connection,
    tag: u8,
) -> Result<(SendStream, RecvStream), IrohProtocolError> {
    let (mut send, recv) = conn.open_bi().await?;
    send.write_all(&[tag]).await?;
    Ok((send, recv))
}
