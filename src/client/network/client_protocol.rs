use crate::client::network::direct::direct_transport::{DirectProtocol, DirectProtocolError};
use crate::client::network::iroh::iroh_transport::IrohProtocol;
use crate::error::IrohProtocolError;
use crate::network::transport::PeerCommunicationChannel;
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use std::net::SocketAddr;

pub enum ClientProtocol {
    Direct(DirectProtocol),
    Iroh(IrohProtocol),
}

impl ClientProtocol {
    pub async fn accept_iroh(
        endpoint: &Endpoint,
        ticket: EndpointTicket,
    ) -> Result<(Self, PeerCommunicationChannel), IrohProtocolError> {
        let (iroh_protocol, events_stream) = IrohProtocol::accept(&endpoint, ticket).await?;
        Ok((Self::Iroh(iroh_protocol), events_stream))
    }

    pub async fn accept_direct(
        address: SocketAddr,
    ) -> Result<(Self, PeerCommunicationChannel), DirectProtocolError> {
        let (direct_protocol, events_stream) = DirectProtocol::accept(address).await?;
        Ok((Self::Direct(direct_protocol), events_stream))
    }

    pub async fn stop(&mut self) {
        match self {
            ClientProtocol::Iroh(iroh) => iroh.stop().await,
            ClientProtocol::Direct(direct) => direct.stop().await,
        }
    }
}
