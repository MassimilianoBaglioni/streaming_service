pub mod iroh;
pub mod protocol;
pub mod transport;

use std::{net::AddrParseError, num::ParseIntError};

pub const FRAMES_TAG: u8 = 0;
pub const EVENTS_TAG: u8 = 1;

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

    #[error("Not ipv4 client")]
    NotIpv4,
}
