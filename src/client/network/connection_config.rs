use crate::new_client::network::direct::request::DirectConnectionRequest;

pub enum ConnectionConfig {
    Iroh,
    Direct(DirectConnectionRequest),
}
