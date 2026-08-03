use crate::network::server_connection::{ServerConnection, ServerConnectionMode};
use anyhow::{anyhow, Result};
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

pub async fn establish_iroh_server_connection(
    ticket: EndpointTicket,
    endpoint: Endpoint,
) -> Result<ServerConnection> {
    let incoming = endpoint
        .accept()
        .await
        .ok_or_else(|| anyhow!("Endpoint closed while waiting for incoming connection"))?;

    info!("Accepted iroh client");

    let iroh_connection = incoming.await?;
    let (send_stream, recv_stream) = iroh_connection.open_bi().await?;

    info!("Opened bi connection on the server");

    return Ok(ServerConnection {
        connection_mode: ServerConnectionMode::Iroh {
            send_stream: Arc::new(Mutex::new(send_stream)),
            recv_stream: Arc::new(Mutex::new(recv_stream)),
            endpoint,
            iroh_connection,
        },
    });
}
