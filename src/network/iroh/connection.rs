use crate::network::streaming_event::{TransmissionError, Transport};
use bytes::{Buf, BytesMut};
use gstreamer_app::AppSrc;
use iroh::endpoint::{RecvStream, SendStream};
use thiserror::Error;
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
    recv: &mut Transport<RecvStream, SendStream>,
    appsrc: AppSrc,
) -> Result<(), FrameReceiveError> {
    let mut read_buf = [0u8; 64 * 1024];
    let mut pending = BytesMut::new();

    info!("Starting to receive frames from iroh connection");

    loop {
        match recv.read(&mut read_buf).await {
            Ok(n) => {
                pending.extend_from_slice(&read_buf[..n]);

                while pending.len() >= 4 {
                    let frame_len = u32::from_be_bytes(pending[0..4].try_into().unwrap()) as usize;

                    if frame_len > MAX_FRAME_SIZE {
                        return Err(FrameReceiveError::FrameTooLarge(frame_len));
                    }
                    if pending.len() < 4 + frame_len {
                        break; // wait for more data
                    }

                    pending.advance(4);
                    let payload = pending.split_to(frame_len);

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
            Err(e) => {
                error!("Error reading from iroh connection: {}", e);
                return Err(FrameReceiveError::Transmission(e));
            }
        }
    }
}
