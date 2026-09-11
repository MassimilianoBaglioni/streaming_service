use crate::error::PipelineError;
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
pub enum IrohProtocolError {
    #[error(transparent)]
    FrameReceive(#[from] FrameReceiveError),
    #[error(transparent)]
    TransmissionError(#[from] TransmissionError),
    #[error(transparent)]
    IrohStateError(#[from] IrohStateError),
    #[error("Could not create pipeline: {0}")]
    PipelineError(#[from] PipelineError),
}
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

#[derive(Debug, Error)]
pub enum IrohStateError {
    #[error("Already receiving")]
    AlreadyReceiving,
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
        let connection = endpoint
            .connect(ticket.endpoint_addr().clone(), ALPN)
            .await
            .expect("Failed to connect to the endpoint");
        info!("Iroh connected");

        let mut frames_stream = None;
        let mut events_stream = None;

        // Accepts TWO connections, one for the frames and the other for streaming events
        for _ in 0..2 {
            let (tag, send, recv) = IrohProtocol::accept_tagged_bi(&connection)
                .await
                .expect("Failed to accept connection");
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

    async fn accept_tagged_bi(conn: &Connection) -> anyhow::Result<(u8, SendStream, RecvStream)> {
        let (send, mut recv) = conn.accept_bi().await?;
        let mut tag = [0u8; 1];
        recv.read_exact(&mut tag).await?;
        Ok((tag[0], send, recv))
    }

    pub async fn start_watching(
        &mut self,
        app_src: AppSrc,
        cancellation_token: CancellationToken,
    ) -> Result<Self, IrohProtocolError> {
        let state = match self {
            IrohProtocol::Connected(connected_state) => connected_state,
            IrohProtocol::Receiving(_) => Err(IrohStateError::AlreadyReceiving)?,
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

        Ok(Self::Receiving(ReceivingState {
            receive_frames_handle,
        }))
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
                    error!("Receiver channel closed unexpectedly");
                    return Err(FrameReceiveError::Buffer(
                        "Frames channel returned None".into(),
                    ));
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
            appsrc.push_buffer(gst_buffer)?;
        }
    }
}
pub struct ConnectedState {
    frames_stream: FrameTransport,
    connection: Connection,
}

pub struct ReceivingState {
    receive_frames_handle: JoinHandle<()>,
}
