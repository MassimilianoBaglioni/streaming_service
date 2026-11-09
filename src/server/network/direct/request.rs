use crate::server::capture::scaling_method::ScalingMethod;
use std::net::{Ipv4Addr, SocketAddr};

pub struct DirectConnectionRequest {
    pub scaling_method: ScalingMethod,
    pub resolution: u16,
    pub height: u32,
    pub width: u32,
    pub bitrate: u32,
    pub client_ip: Ipv4Addr,
    pub client_port: u16,
    pub socket_addr: SocketAddr,
}
