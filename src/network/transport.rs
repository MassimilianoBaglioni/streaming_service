use bytes::{Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc::{unbounded_channel, Receiver, Sender, UnboundedSender};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize, Debug)]
pub enum StreamingEvent {
    ClientQuit,
    ServerEndsStream,
    GenericError,
}

#[derive(Debug, Error)]
pub enum TransmissionError {
    #[error("Transmission network error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Recv error: {0}")]
    Recv(#[from] RecvError),

    #[error("Connection closed")]
    Closed,
}

#[derive(Debug)]
pub struct TaskTransport {
    send_handle: JoinHandle<()>,
    recv_handle: JoinHandle<()>,
    listen_channel: broadcast::Sender<Bytes>,
    feed_channel: UnboundedSender<Bytes>,
    shutdown: CancellationToken,
}

impl TaskTransport {
    pub fn new<R, W>(send: W, recv: R, channel_capacity: usize, recv_buffer_capacity: usize) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let shutdown = CancellationToken::new();
        // No need to keep the receiver, we can just call .subscribe on the sender to create a new receiver
        let (listen_channel, _) = broadcast::channel::<Bytes>(channel_capacity);
        let communicate_received_channel = listen_channel.clone();

        let recv_cancellation = shutdown.clone();
        let recv_handle = tokio::spawn(async move {
            let mut recv = recv;
            let mut buf = BytesMut::with_capacity(recv_buffer_capacity);

            loop {
                let mut len_buf = [0u8; 4];
                if recv.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let len = u32::from_be_bytes(len_buf) as usize;

                if buf.capacity() < len {
                    buf.reserve(len - buf.capacity());
                }
                buf.resize(len, 0);

                tokio::select! {
                    _ = recv_cancellation.cancelled() => {break},
                    res = recv.read_exact(&mut buf[..len]) => {
                        if res.is_err() { break; }
                    }
                }

                let frame = buf.split_to(len).freeze();

                let _ = communicate_received_channel.send(frame);
            }
        });

        let (feed_channel, mut get_data_channel) = unbounded_channel::<Bytes>();

        let send_cancellation = shutdown.clone();
        let send_handle = tokio::spawn(async move {
            let mut send = send;
            loop {
                tokio::select! {
                    _ = send_cancellation.cancelled() => break,
                    maybe_payload = get_data_channel.recv() => {
                        match maybe_payload {
                            Some(payload) => {
                                let len = (payload.len() as u32).to_be_bytes();
                                if send.write_all(&len).await.is_err() { break; }
                                if send.write_all(&payload).await.is_err() { break; }
                            }
                            None => break,
                        }
                    }
                }
            }
            let _ = send.shutdown().await;
        });

        Self {
            send_handle,
            recv_handle,
            feed_channel,
            listen_channel,
            shutdown,
        }
    }

    pub async fn close(self) {
        self.shutdown.cancel();
        let _ = self.send_handle.await;
        let _ = self.recv_handle.await;
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Bytes> {
        self.listen_channel.subscribe()
    }

    pub fn sender(&self) -> UnboundedSender<Bytes> {
        self.feed_channel.clone()
    }

    pub fn raw_sender(&self) -> RawSender {
        RawSender {
            tx: self.feed_channel.clone(),
        }
    }

    pub fn raw_receiver(&self) -> RawReceiver {
        RawReceiver {
            rx: self.listen_channel.subscribe(),
        }
    }

    pub fn serialized_sender<T>(&self) -> SerializedSender<T> {
        SerializedSender {
            tx: self.feed_channel.clone(),
            _marker: PhantomData,
        }
    }

    pub fn serialized_receiver<T>(&self) -> SerializedReceiver<T> {
        SerializedReceiver {
            rx: self.listen_channel.subscribe(),
            _marker: PhantomData,
        }
    }
}

pub struct RawSender {
    tx: UnboundedSender<Bytes>,
}

impl RawSender {
    pub fn send(&self, payload: Bytes) -> Result<(), TransmissionError> {
        self.tx.send(payload).map_err(|_| TransmissionError::Closed)
    }
}

pub struct RawReceiver {
    rx: broadcast::Receiver<Bytes>,
}

impl RawReceiver {
    pub async fn recv(&mut self) -> Result<Bytes, TransmissionError> {
        self.rx.recv().await.map_err(Into::into) // maps Closed / Lagged(n)
    }
}

pub struct SerializedSender<T> {
    tx: UnboundedSender<Bytes>,
    _marker: PhantomData<T>,
}

impl<T: Serialize> SerializedSender<T> {
    pub fn send(&self, value: &T) -> Result<(), TransmissionError> {
        let bytes = Bytes::from(serde_json::to_vec(value)?);
        self.tx.send(bytes).map_err(|_| TransmissionError::Closed)
    }
}

impl<T> Clone for SerializedSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _marker: PhantomData,
        }
    }
}

pub struct SerializedReceiver<T> {
    rx: broadcast::Receiver<Bytes>,
    _marker: PhantomData<T>,
}

impl<T: DeserializeOwned> SerializedReceiver<T> {
    pub async fn recv(&mut self) -> Result<T, TransmissionError> {
        let bytes = self.rx.recv().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

/// Initially TaskTransport was designed to handle frames as well. But using broadcast, which is a requirement for events, made it impossible.
/// Frames arrive too fast if there's no backpressure handling client's receiver is not fast enough, which makes broadcast drop frames.
/// So we need separate transport for frames, which uses mpsc instead of broadcast.
#[derive(Debug)]
pub struct FrameTransport {
    send_handle: Option<JoinHandle<()>>,
    recv_handle: Option<JoinHandle<()>>,
    feed_channel: Sender<Bytes>,
    listen_channel: Option<Receiver<Bytes>>,
    shutdown: CancellationToken,
}

impl FrameTransport {
    pub fn new<R, W>(send: W, recv: R, channel_capacity: usize, recv_buffer_capacity: usize) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let shutdown = CancellationToken::new();

        let (listen_tx, listen_rx) = mpsc::channel::<Bytes>(channel_capacity);

        let recv_shutdown = shutdown.clone();
        let recv_handle = Some(tokio::spawn(async move {
            let mut recv = recv;
            let mut buf = BytesMut::with_capacity(recv_buffer_capacity);

            loop {
                let mut len_buf = [0u8; 4];
                tokio::select! {
                    _ = recv_shutdown.cancelled() => break,
                    res = recv.read_exact(&mut len_buf) => {
                        if res.is_err() { break; }
                    }
                }
                let len = u32::from_be_bytes(len_buf) as usize;

                // TODO: bounds-check `len` against a MAX_FRAME_SIZE before reserving —
                // same gap flagged for TaskTransport, applies here too.
                if buf.capacity() < len {
                    buf.reserve(len - buf.capacity());
                }
                buf.resize(len, 0);

                tokio::select! {
                    _ = recv_shutdown.cancelled() => break,
                    res = recv.read_exact(&mut buf[..len]) => {
                        if res.is_err() { break; }
                    }
                }

                let frame = buf.split_to(len).freeze();

                // .send().await, not a fire-and-forget try_send: this is what
                // makes a full channel stall recv_handle instead of dropping
                // frames, which is the whole point of using mpsc here.
                tokio::select! {
                    _ = recv_shutdown.cancelled() => break,
                    res = listen_tx.send(frame) => {
                        if res.is_err() { break; } // consumer dropped its receiver
                    }
                }
            }
        }));

        let (feed_channel, mut get_data_channel) = mpsc::channel::<Bytes>(channel_capacity);
        let send_shutdown = shutdown.clone();
        let send_handle = Some(tokio::spawn(async move {
            let mut send = send;
            loop {
                tokio::select! {
                    _ = send_shutdown.cancelled() => break,
                    maybe_payload = get_data_channel.recv() => {
                        match maybe_payload {
                            Some(payload) => {
                                let len = (payload.len() as u32).to_be_bytes();
                                if send.write_all(&len).await.is_err() { break; }
                                if send.write_all(&payload).await.is_err() { break; }
                            }
                            None => break,
                        }
                    }
                }
            }
            let _ = send.shutdown().await;
        }));

        Self {
            send_handle,
            recv_handle,
            feed_channel,
            listen_channel: Some(listen_rx),
            shutdown,
        }
    }

    pub fn take_receiver(&mut self) -> Option<Receiver<Bytes>> {
        self.listen_channel.take()
    }

    pub fn sender(&self) -> Sender<Bytes> {
        self.feed_channel.clone()
    }

    pub async fn close(&mut self) {
        self.shutdown.cancel();
        if let Some(h) = self.recv_handle.take() {
            let _ = h.await;
        }
        if let Some(h) = self.send_handle.take() {
            let _ = h.await;
        }
    }
}

impl Drop for FrameTransport {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
