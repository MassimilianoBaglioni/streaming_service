use std::{
    io::Read,
    net::{Shutdown, TcpStream},
};

use crate::network::streaming_event::StreamingEvent;

#[derive(Debug)]
pub struct StreamingEventSocketClient {
    stream: TcpStream,
}

impl StreamingEventSocketClient {
    pub fn connect(address: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(address)?;
        Ok(Self { stream })
    }

    pub fn read_event(&mut self) -> std::io::Result<StreamingEvent> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf)?;
        Ok(serde_json::from_slice(&buf).expect("deserialise"))
    }

    pub fn disconnect(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}
