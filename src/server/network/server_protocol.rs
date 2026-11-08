use crate::network::transport::{PeerCommunicationChannel, ServerTransportError};
use crate::server::network::direct::direct_transport::DirectProtocol;
use crate::server::network::iroh::iroh_transport::IrohProtocol;

pub enum ServerProtocol {
    Direct(DirectProtocol),
    Iroh(IrohProtocol),
}

impl ServerProtocol {
    pub fn start_streaming(&self) {
        match self {
            ServerProtocol::Direct(direct) => {
                direct.start_streaming();
            }
            ServerProtocol::Iroh(iroh) => {
                iroh.start_streaming();
            }
        }
    }

    pub async fn stop_streaming(self) {
        match self {
            ServerProtocol::Direct(direct) => direct.stop_streaming(),
            ServerProtocol::Iroh(iroh) => {
                iroh.stop_streaming().await;
            }
        }
    }

    pub async fn accept(&mut self) -> Result<PeerCommunicationChannel, ServerTransportError> {
        match self {
            ServerProtocol::Direct(direct) => direct.open_client_connection().await,
            ServerProtocol::Iroh(_) => Err(ServerTransportError::WrongProtocol),
        }
    }
}
