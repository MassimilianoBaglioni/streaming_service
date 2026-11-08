use crate::server::network::direct::request::DirectConnectionRequest;
use crate::server::network::iroh::request::IrohConnectionRequest;

pub enum ConnectionConfig {
    Iroh(IrohConnectionRequest),
    DirectConnection(DirectConnectionRequest),
}