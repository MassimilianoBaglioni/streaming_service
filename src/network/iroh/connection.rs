use crate::network::transport::TransmissionError;
use bytes::Bytes;
use gstreamer_app::AppSrc;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info};

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
}

pub async fn receive_frames_iroh(
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
