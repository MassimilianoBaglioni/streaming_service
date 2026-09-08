use crate::network::streaming_event::Transport;
use std::net::SocketAddr;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpListener;
use tracing::info;

pub struct StreamingEventsSocketServer {
    listener: TcpListener,
}

impl StreamingEventsSocketServer {
    pub async fn bind(address: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(address).await?;

        Ok(Self { listener })
    }

    pub async fn accept(self) -> std::io::Result<Transport<OwnedReadHalf, OwnedWriteHalf>> {
        let (stream, addr) = self.listener.accept().await?;

        info!("Accepted connection from {:?}", addr);

        let (recv, send) = stream.into_split();

        Ok(Transport::new(recv, send))
    }
}
