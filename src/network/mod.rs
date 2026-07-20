pub(crate) mod client_connection;
pub mod iroh;
pub(crate) mod server_connection;
pub mod streaming_event;
pub mod streaming_events_client;
pub mod streaming_events_server;

use crate::network::iroh::IrohInfo;
use crate::video::gs::{build_client_iroh_pipeline, build_client_udp_pipeline};
use gstreamer::Pipeline;
use std::{
    net::{AddrParseError, Ipv4Addr},
    num::ParseIntError,
};
use tracing::warn;

#[derive(Debug)]
pub enum ParseError {
    InvalidIp(AddrParseError),
    InvalidPort(ParseIntError),
}

#[derive(Clone, Debug)]
pub struct NetInfo {
    pub stream_port: u16,
    pub tcp_port: u16,
    pub target_ip: Ipv4Addr,
    pub connection_mode: ConnectionMode,
}

impl NetInfo {
    pub fn parse_info(
        stream_port: String,
        tcp_port: String,
        target_ip: String,
        connection_mode: ConnectionMode,
    ) -> Result<Self, ParseError> {
        let stream_port: u16 = match stream_port.parse() {
            Ok(value) => value,
            Err(e) => {
                warn!("Invalid stream port {}: {}", stream_port, e);
                return Err(ParseError::InvalidPort(e));
            }
        };
        let tcp_port: u16 = match tcp_port.parse() {
            Ok(value) => value,
            Err(e) => {
                warn!("Invalid tcp port {}: {}", tcp_port, e);
                return Err(ParseError::InvalidPort(e));
            }
        };
        let target_ip: Ipv4Addr = match target_ip.parse() {
            Ok(value) => value,
            Err(e) => {
                warn!("Invalid watcher address {}: {}", target_ip, e);
                return Err(ParseError::InvalidIp(e));
            }
        };

        Ok(Self {
            stream_port,
            tcp_port,
            target_ip,
            connection_mode,
        })
    }

    pub fn build_pipeline(&mut self) -> Pipeline {
        match &self.connection_mode {
            crate::network::ConnectionMode::Direct => build_client_udp_pipeline(self.stream_port),
            crate::network::ConnectionMode::Iroh { info } => build_client_iroh_pipeline(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ConnectionMode {
    Direct,
    Iroh { info: IrohInfo },
}
