use crate::network::streaming_event::Transport;
use std::net::SocketAddr;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tracing::info;

pub struct StreamingEventsSocketClient;

impl StreamingEventsSocketClient {
    pub async fn connect(
        address: SocketAddr,
    ) -> std::io::Result<Transport<OwnedReadHalf, OwnedWriteHalf>> {
        let stream = TcpStream::connect(address).await?;

        info!("Connected to {:?}", address);

        let (recv, send) = stream.into_split();

        Ok(Transport::new(recv, send))
    }
}
