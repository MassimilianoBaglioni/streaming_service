pub(crate) mod client_connection;
pub mod iroh;
pub(crate) mod server_connection;
pub mod streaming_event;
pub mod streaming_events_client;
pub mod streaming_events_server;

use crate::video::gs::{build_client_iroh_pipeline, build_client_udp_pipeline};
use gstreamer::Pipeline;
use ::iroh::endpoint::{presets, Connection};
use ::iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use std::net::SocketAddr;
use std::{net::AddrParseError, num::ParseIntError};
use tracing::warn;

#[derive(Debug)]
pub enum ParseError {
    InvalidIp(AddrParseError),
    InvalidPort(ParseIntError),
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionBuildError {
    #[error("invalid port: {0}")]
    InvalidPort(#[from] ParseIntError),

    #[error("invalid ip: {0}")]
    InvalidIp(#[from] AddrParseError),

    #[error("invalid invite link: {0}")]
    InvalidTicket(String),
}

#[derive(Debug, Clone)]
pub enum ConnectionBuildInfo {
    Direct {
        watcher_stream_port: u16,
        tcp_socket_address: SocketAddr,
    },
    Iroh {
        endpoint: Endpoint,
        ticket: EndpointTicket,
    },
}

impl ConnectionBuildInfo {
    pub fn from_direct_info(
        watcher_stream_port: String,
        watcher_address: String,
    ) -> Result<ConnectionBuildInfo, ConnectionBuildError> {
        let watcher_stream_port: u16 = watcher_stream_port.parse().map_err(|e| {
            warn!("Invalid stream port {}: {}", watcher_stream_port, e);
            ConnectionBuildError::InvalidPort(e)
        })?;

        let watcher_address: SocketAddr = format!("{}:{}", watcher_address, watcher_stream_port)
            .parse()
            .map_err(|e| {
                warn!("Invalid watcher address {}: {}", watcher_address, e);
                ConnectionBuildError::InvalidIp(e)
            })?;

        let tcp_socket_address = SocketAddr::new(watcher_address.ip(), watcher_stream_port);

        Ok(ConnectionBuildInfo::Direct {
            watcher_stream_port,
            tcp_socket_address,
        })
    }
    pub async fn from_ticket(
        ticket: EndpointTicket,
    ) -> Result<ConnectionBuildInfo, ConnectionBuildError> {
        let endpoint = Endpoint::bind(presets::N0)
            .await
            .expect("Failed to create endpoint");

        Ok(ConnectionBuildInfo::Iroh { endpoint, ticket })
    }

    pub async fn from_endpoint_and_ticket(
        endpoint: Endpoint,
        ticket: EndpointTicket,
    ) -> Result<ConnectionBuildInfo, ConnectionBuildError> {
        Ok(ConnectionBuildInfo::Iroh { endpoint, ticket })
    }
}

#[derive(Clone, Debug)]
pub enum ConnectionMode {
    Direct {
        socket_addr: SocketAddr,
        watcher_stream_port: u16,
    },
    Iroh {
        connection: Option<Connection>,
        endpoint: Endpoint,
        ticket: EndpointTicket,
    },
}

impl ConnectionMode {
    pub fn build_pipeline(&mut self) -> Pipeline {
        match self {
            ConnectionMode::Direct {
                watcher_stream_port: stream_port,
                ..
            } => build_client_udp_pipeline(*stream_port),
            ConnectionMode::Iroh { .. } => build_client_iroh_pipeline(),
        }
    }
}
