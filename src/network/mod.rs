pub mod client_connection;
pub mod iroh;
pub mod server_connection;
pub mod streaming_event;
pub mod streaming_events_client;
pub mod streaming_events_server;

use crate::video::gs::{build_client_iroh_pipeline, build_client_udp_pipeline};
use gstreamer::Pipeline;
use ::iroh::endpoint::{Connection, RecvStream, SendStream};
use ::iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{net::AddrParseError, num::ParseIntError};
use tokio::sync::Mutex;
use tracing::warn;
use crate::network::streaming_events_client::StreamingEventSocketClient;

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
        connection: Option<Connection>,
        send: Option<Arc<Mutex<SendStream>>>,
        recv: Option<Arc<Mutex<RecvStream>>>,
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

    pub fn from_endpoint_and_ticket(
        endpoint: Endpoint,
        ticket: EndpointTicket,
    ) -> ConnectionBuildInfo {
        ConnectionBuildInfo::Iroh {
            endpoint,
            ticket,
            connection: None,
            send: None,
            recv: None,
        }
    }
}

#[derive(Debug)]
pub enum ConnectionMode {
    Direct {
        socket_addr: SocketAddr,
        watcher_stream_port: u16,
        streaming_events_socket_client: Option<Arc<Mutex<StreamingEventSocketClient>>>,
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
