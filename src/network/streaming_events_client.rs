use crate::network::streaming_event::EventsTransport;
use std::net::SocketAddr;

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tracing::info;

pub struct StreamingEventsSocketClient;

impl StreamingEventsSocketClient {
    pub async fn connect(
        address: SocketAddr,
    ) -> std::io::Result<EventsTransport<OwnedReadHalf, OwnedWriteHalf>> {
        let stream = TcpStream::connect(address).await?;

        info!("Connected to {:?}", address);

        let (recv, send) = stream.into_split();

        Ok(EventsTransport::new(recv, send))
    }
}
