use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Serialize, Deserialize, Debug)]
pub enum StreamingEvent {
    ClientQuit,
    ServerEndsStream,
    GenericError,
}

#[derive(Debug, Error)]
pub enum EventTransmissionError {
    #[error("Streaming event transmission network error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Streaming event serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

#[derive(Debug)]
pub struct EventsTransport<R, W> {
    pub recv: R,
    pub send: W,
}

impl<R, W> EventsTransport<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    pub fn new(recv: R, send: W) -> Self {
        Self { recv, send }
    }
    pub async fn send_event(
        &mut self,
        event: &StreamingEvent,
    ) -> Result<(), EventTransmissionError> {
        let payload = serde_json::to_vec(event)?;
        let len = (payload.len() as u32).to_be_bytes();
        self.send.write_all(&len).await?;
        self.send.write_all(&payload).await?;
        Ok(())
    }

    pub async fn read_event(&mut self) -> Result<StreamingEvent, EventTransmissionError> {
        let mut len_buf = [0u8; 4];
        self.recv.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.recv.read_exact(&mut buf).await?;
        Ok(serde_json::from_slice::<StreamingEvent>(&buf)?)
    }

    pub async fn close(&mut self) -> Result<(), EventTransmissionError> {
        self.send.shutdown().await?;
        Ok(())
    }
}
