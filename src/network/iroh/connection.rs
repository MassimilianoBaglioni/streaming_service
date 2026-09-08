use bytes::{Buf, BytesMut};
use gstreamer_app::AppSrc;
use iroh::endpoint::RecvStream;
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
}

pub async fn receive_frames_iroh(
    recv: &mut RecvStream,
    appsrc: AppSrc,
) -> Result<(), FrameReceiveError> {
    let mut read_buf = [0u8; 64 * 1024];
    let mut pending = BytesMut::new();

    info!("Starting to receive frames from iroh connection");

    loop {
        match recv.read(&mut read_buf).await? {
            Some(n) => {
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
            None => {
                info!("Stream finished");
                return Ok(());
            }
        }
    }
}
