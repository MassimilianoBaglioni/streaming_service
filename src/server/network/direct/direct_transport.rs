use crate::network::transport::{PeerCommunicationChannel, ServerTransportError};
use crate::server::network::connection_config::ConnectionConfig;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::net::TcpListener;
use tracing::info;

pub struct DirectProtocol {
    client_address: SocketAddr,
}

impl From<&ConnectionConfig> for DirectProtocol {
    fn from(connection_config: &ConnectionConfig) -> Self {
        if let ConnectionConfig::DirectConnection(direct_config) = connection_config {
            return DirectProtocol {
                client_address: direct_config.socket_addr,
            };
        };

        panic!("Wrong connection configuration");
    }
}

impl DirectProtocol {
    pub fn get_client_address(&self) -> Ipv4Addr {
        match self.client_address.ip() {
            IpAddr::V4(addr) => addr,
            IpAddr::V6(_) => panic!("Expected an IPv4 address"),
        }
    }
    pub fn get_client_port(&self) -> u16 {
        self.client_address.port()
    }
    pub async fn open_client_connection(
        &mut self,
    ) -> Result<PeerCommunicationChannel, ServerTransportError> {
        let streaming_socket = TaskTransferSocketBuilder::bind(self.client_address)
            .await
            .map_err(ServerTransportError::SocketBind)?;

        let transport = streaming_socket
            .accept()
            .await
            .map_err(ServerTransportError::SocketAccept)?;

        info!("Accepted client");

        Ok(transport)
    }

    pub fn start_streaming(&self) {
        info!("Start streaming on direct mode")
    }

    pub fn stop_streaming(&self) {
        info!("Stop streaming on direct mode")
    }
}

pub struct TaskTransferSocketBuilder {
    listener: TcpListener,
}

impl TaskTransferSocketBuilder {
    pub async fn bind(address: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(address).await?;

        Ok(Self { listener })
    }

    pub async fn accept(self) -> std::io::Result<PeerCommunicationChannel> {
        let (stream, addr) = self.listener.accept().await?;

        info!("Accepted connection from {:?}", addr);

        let (recv, send) = stream.into_split();

        Ok(PeerCommunicationChannel::new(send, recv, 32, 1024))
    }
}
