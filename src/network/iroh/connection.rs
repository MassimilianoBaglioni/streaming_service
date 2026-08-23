use gstreamer_app::AppSrc;
use iroh::endpoint::RecvStream;
use tracing::{error, info};
pub async fn receive_frames_iroh(mut recv: RecvStream, appsrc: AppSrc) {
    let mut buf = vec![0u8; 64 * 1024]; // scratch read buffer
    let mut pending = Vec::new(); // accumulator for partial data

    info!("Starting to receive frames from iroh connection");

    loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => {
                pending.extend_from_slice(&buf[..n]);

                loop {
                    if pending.len() < 4 {
                        break;
                    }

                    let frame_len = u32::from_be_bytes(pending[0..4].try_into().unwrap()) as usize;

                    if pending.len() < 4 + frame_len {
                        break;
                    }

                    let payload = pending[4..4 + frame_len].to_vec();

                    let mut gst_buffer = gstreamer::Buffer::with_size(payload.len())
                        .expect("Failed to allocate gstreamer buffer");
                    {
                        let buffer_ref = gst_buffer.get_mut().unwrap();
                        let mut map = buffer_ref.map_writable().expect("map failed");
                        map.as_mut_slice().copy_from_slice(&payload);
                    }
                    appsrc.push_buffer(gst_buffer).expect("push_buffer failed");

                    pending.drain(0..4 + frame_len);
                }
            }
            Ok(None) => {
                info!("Stream finished");
                break;
            }
            Err(e) => {
                error!("Read error: {:?}", e);
                break;
            }
        }
    }
}
