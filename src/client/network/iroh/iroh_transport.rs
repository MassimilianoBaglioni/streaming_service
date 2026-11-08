use crate::error::{IrohProtocolError, IrohStateError};
use crate::network::iroh::ALPN;
use crate::network::transport::{FrameTransport, PeerCommunicationChannel, TransmissionError};
use crate::network::{EVENTS_TAG, FRAMES_TAG};
use bytes::Bytes;
use gstreamer_app::AppSrc;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum FrameReceiveError {
    #[error("iroh read error: {0}")]
    Read(#[from] iroh::endpoint::ReadError),
    #[error("frame length {0} exceeds max allowed size {MAX_FRAME_SIZE}")]
    FrameTooLarge(usize),
    #[error("gstreamer buffer error: {0}")]
    Buffer(String),
    #[error("gstreamer push_buffer failed: {0}")]
    Push(#[from] gstreamer::FlowError),
    #[error("transmission failed: {0}")]
    Transmission(#[from] TransmissionError),
    #[error("No frames connection found")]
    NoConnection,
}

pub enum IrohProtocol {
    Connected(ConnectedState),
    Receiving(ReceivingState),
}

impl IrohProtocol {
    pub async fn accept(
        endpoint: &Endpoint,
        ticket: EndpointTicket,
    ) -> Result<(Self, PeerCommunicationChannel), IrohProtocolError> {
        endpoint.online().await;
        info!("Client connecting to Node ID: {:?}", ticket.endpoint_addr());
        let connection = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            endpoint.connect(ticket.endpoint_addr().clone(), ALPN),
        )
        .await
        .map_err(|_| IrohProtocolError::StreamAcceptError("Connection timed out".into()))??;
        info!("Iroh connected");

        let mut frames_stream = None;
        let mut events_stream = None;

        // Accepts TWO connections, one for the frames and the other for streaming events
        for _ in 0..2 {
            let (tag, send, recv) = IrohProtocol::accept_tagged_bi(&connection).await?;
            match tag {
                FRAMES_TAG => {
                    frames_stream = Some(FrameTransport::new(send, recv, 32, 65536));
                    info!("Accepted frames stream");
                }
                EVENTS_TAG => {
                    events_stream = Some(PeerCommunicationChannel::new(send, recv, 32, 1024));
                    info!("Accepted events stream");
                }
                other => warn!("Unknown stream tag: {other}"),
            }
        }

        let frames_stream = frames_stream.ok_or(TransmissionError::NoFramesConnection)?;
        let events_stream = events_stream.ok_or(TransmissionError::NoEventsConnection)?;

        Ok((
            IrohProtocol::Connected(ConnectedState {
                frames_stream,
                connection,
            }),
            events_stream,
        ))
    }

    async fn accept_tagged_bi(
        conn: &Connection,
    ) -> Result<(u8, SendStream, RecvStream), IrohProtocolError> {
        let (send, mut recv) =
            tokio::time::timeout(std::time::Duration::from_secs(10), conn.accept_bi())
                .await
                .map_err(|_| {
                    IrohProtocolError::StreamAcceptError(
                        "Timed out waiting for server to open streams".into(),
                    )
                })?
                .map_err(IrohProtocolError::OpenBiFailed)?;
        let mut tag = [0u8; 1];
        recv.read_exact(&mut tag)
            .await
            .map_err(|e| IrohProtocolError::StreamAcceptError(e.to_string()))?;
        Ok((tag[0], send, recv))
    }

    pub async fn start_watching(
        &mut self,
        app_src: AppSrc,
        cancellation_token: CancellationToken,
    ) -> Result<(), IrohProtocolError> {
        // Borrow only to take the receiver; the borrow ends before the replace below.
        let IrohProtocol::Connected(state) = self else {
            return Err(IrohStateError::AlreadyReceiving.into());
        };

        let mut frames_receiver = state
            .frames_stream
            .take_receiver()
            .ok_or(TransmissionError::NoFramesConnection)?;

        let receive_frames_handle = tokio::spawn(async move {
            tokio::select! {
                result = IrohProtocol::receive_frames(&mut frames_receiver, app_src) => {
                    match result {
                        Ok(()) => info!("Finished receiving frames"),
                        Err(e) => error!("Error receiving frames: {:?}", e),
                    }
                }
                _ = cancellation_token.cancelled() => {
                    info!("Receive frames cancelled");
                }
            }
        });

        // Now take ownership of the old state (a by-value match, so fields are owned).
        let old = std::mem::replace(
            self,
            IrohProtocol::Receiving(ReceivingState {
                frames_stream: None,
                connection: None,
                receive_frames_handle: None,
            }),
        );

        let IrohProtocol::Connected(ConnectedState {
            frames_stream,
            connection,
        }) = old
        else {
            unreachable!("state was Connected above");
        };

        *self = IrohProtocol::Receiving(ReceivingState {
            frames_stream: Some(frames_stream),
            connection: Some(connection),
            receive_frames_handle: Some(receive_frames_handle),
        });

        Ok(())
    }

    pub async fn stop(&mut self) {
        match self {
            IrohProtocol::Connected(ConnectedState {
                frames_stream,
                connection,
            }) => {
                frames_stream.close().await;
                connection.close(0u8.into(), b"client quit");
            }
            IrohProtocol::Receiving(ReceivingState {
                frames_stream,
                connection,
                receive_frames_handle,
            }) => {
                if let Some(h) = receive_frames_handle.take() {
                    h.abort();
                    let _ = h.await;
                }
                if let Some(mut fs) = frames_stream.take() {
                    fs.close().await;
                }
                if let Some(conn) = connection.take() {
                    conn.close(0u8.into(), b"client quit");
                }
            }
        }
        info!("IrohProtocol on client stopped cleanly");
    }

    async fn receive_frames(
        recv: &mut Receiver<Bytes>,
        appsrc: AppSrc,
    ) -> Result<(), FrameReceiveError> {
        info!("Starting to receive frames from iroh connection");
        loop {
            let payload = match recv.recv().await {
                Some(bytes) => bytes,
                None => {
                    info!("Frames channel closed, stopping frame receiver");
                    return Ok(());
                }
            };

            let mut gst_buffer = gstreamer::Buffer::with_size(payload.len())
                .map_err(|_| FrameReceiveError::Buffer("allocation failed".into()))?;
            {
                let buffer_ref = gst_buffer
                    .get_mut()
                    .expect("uniquely owned, just allocated");
                let mut map = buffer_ref
                    .map_writable()
                    .map_err(|_| FrameReceiveError::Buffer("map_writable failed".into()))?;
                map.as_mut_slice().copy_from_slice(&payload);
            }
            if let Err(e) = appsrc.push_buffer(gst_buffer) {
                info!("appsrc push_buffer stopped ({e})");
                return Ok(());
            }
        }
    }
}

pub struct ConnectedState {
    frames_stream: FrameTransport,
    connection: Connection,
}

pub struct ReceivingState {
    frames_stream: Option<FrameTransport>,
    connection: Option<Connection>,
    receive_frames_handle: Option<JoinHandle<()>>,
}
