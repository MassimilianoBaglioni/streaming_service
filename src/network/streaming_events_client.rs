use crate::network::streaming_event::StreamingEvent;
use iroh::endpoint::RecvStream;
use std::sync::Arc;
use std::{
    io::Read,
    net::{Shutdown, TcpStream},
};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct StreamingEventSocketClient {
    stream: TcpStream,
}

impl StreamingEventSocketClient {
    pub fn connect(address: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(address)?;
        Ok(Self { stream })
    }
    pub fn disconnect(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}

impl EventsReceiver for StreamingEventSocketClient {
    async fn read_event(&mut self) -> anyhow::Result<StreamingEvent> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.stream.read_exact(&mut buf)?;
        Ok(serde_json::from_slice(&buf).expect("deserialise"))
    }
}

pub trait EventsReceiver {
    fn read_event(
        &mut self,
    ) -> impl std::future::Future<Output = anyhow::Result<StreamingEvent>> + Send;
}

pub struct StreamingEventIrohClient {
    pub(crate) recv: Arc<Mutex<RecvStream>>,
}

impl From<RecvStream> for StreamingEventIrohClient {
    fn from(recv: RecvStream) -> Self {
        Self {
            recv: Arc::new(Mutex::new(recv)),
        }
    }
}

impl EventsReceiver for StreamingEventIrohClient {
    async fn read_event(&mut self) -> anyhow::Result<StreamingEvent> {
        let mut len_buf = [0u8; 4];
        let mut recv = self.recv.lock().await;

        recv.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        recv.read_exact(&mut buf).await?;

        let event = serde_json::from_slice(&buf)?;
        Ok(event)
    }
}
