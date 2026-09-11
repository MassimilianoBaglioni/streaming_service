use crate::new_client::network::direct::direct_transport::DirectProtocol;
use crate::new_client::network::iroh::iroh_transport::{IrohProtocol, IrohProtocolError};
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use crate::network::transport::PeerCommunicationChannel;

pub enum ClientProtocol {
    Direct(DirectProtocol),
    Iroh(IrohProtocol),
}

impl ClientProtocol {
    pub async fn accept_iroh(endpoint: &Endpoint, ticket: EndpointTicket) -> Result<(Self, PeerCommunicationChannel), IrohProtocolError> {
        let (iroh_protocol, events_stream) = IrohProtocol::accept(&endpoint, ticket).await?;
        Ok((Self::Iroh(iroh_protocol), events_stream))
    }
}
