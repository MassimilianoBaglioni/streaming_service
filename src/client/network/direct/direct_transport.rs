use crate::network::transport::PeerCommunicationChannel;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::TcpStream;
use tracing::info;

#[derive(Debug, Error)]
pub enum DirectProtocolError {
    #[error("Could not connect to peer: {0}")]
    ConnectionError(#[from] std::io::Error),
}
pub struct DirectProtocol {}

impl DirectProtocol {
    pub async fn accept(
        address: SocketAddr,
    ) -> Result<(Self, PeerCommunicationChannel), DirectProtocolError> {
        let stream = TcpStream::connect(address).await?;

        info!("Connected to {:?}", address);

        let (recv, send) = stream.into_split();

        Ok((
            Self {},
            PeerCommunicationChannel::new(send, recv, 32, 1024),
        ))
    }

    pub async fn stop(&mut self) {}
}
