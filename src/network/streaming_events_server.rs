use tracing::info;

use crate::network::streaming_event::StreamingEvent;
use iroh::endpoint::{RecvStream, SendStream};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct StreamingEventSocketServer {
    // Listens for incoming connections
    listener: TcpListener,
    // Actual data incoming from connected clients
    stream: Option<TcpStream>,
}

impl StreamingEventSocketServer {
    pub fn bind(address: SocketAddr) -> std::io::Result<Self> {
        info!("Calling bind on tcp socket with address {:?}", address);
        let listener = TcpListener::bind(address)?;
        Ok(Self {
            listener,
            stream: None,
        })
    }

    pub fn accept(&mut self) -> std::io::Result<()> {
        if let Some(old) = self.stream.take() {
            let _ = old.shutdown(Shutdown::Both);
        };

        let (stream, addr) = self.listener.accept()?;

        info!("Accepted connection from addr: {:?}", addr);
        self.stream = Some(stream);
        Ok(())
    }

    pub fn send_event(&mut self, event: &StreamingEvent) -> std::io::Result<()> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "no active connection")
        })?;
        let payload = serde_json::to_vec(event).expect("serialise");
        let len = payload.len() as u32;
        stream.write_all(&len.to_be_bytes())?;
        stream.write_all(&payload)?;
        Ok(())
    }

    pub fn read_event(&mut self) -> std::io::Result<StreamingEvent> {
        let stream = self.stream.as_mut().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotConnected, "no active connection")
        })?;
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf)?;
        Ok(serde_json::from_slice(&buf).expect("deserialise"))
    }

    pub fn disconnect(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

pub struct IrohEventsStream {
    send_stream: Arc<Mutex<SendStream>>,
    recv_stream: Arc<Mutex<RecvStream>>,
}

impl IrohEventsStream {
    pub(crate) fn new(send_stream: SendStream, recv_stream: RecvStream) -> Self {
        Self {
            send_stream: Arc::new(Mutex::new(send_stream)),
            recv_stream: Arc::new(Mutex::new(recv_stream)),
        }
    }

    pub async fn get_send_lock(&self) -> tokio::sync::MutexGuard<'_, SendStream> {
        self.send_stream.lock().await
    }

    pub async fn send_event(&mut self, event: &StreamingEvent) -> std::io::Result<()> {
        let payload = serde_json::to_vec(event).expect("serialise");
        let len = payload.len() as u32;
        let mut send_stream = self.send_stream.lock().await;
        send_stream.write_all(&len.to_be_bytes()).await?;
        send_stream.write_all(&payload).await?;
        Ok(())
    }
}
