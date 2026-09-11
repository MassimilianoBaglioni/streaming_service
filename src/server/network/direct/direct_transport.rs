use crate::network::server_connection::TaskTransferSocketBuilder;
use crate::network::transport::{ClientCommunicationChannel, ServerTransportError};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tracing::info;

pub struct DirectProtocol {
    client_address: SocketAddr,
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
    ) -> Result<ClientCommunicationChannel, ServerTransportError> {
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
}
