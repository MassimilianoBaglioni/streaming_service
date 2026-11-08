use crate::client::network::direct::request::DirectNetworkRequest;

pub enum ConnectionConfig {
    Iroh,
    Direct(DirectNetworkRequest),
}
