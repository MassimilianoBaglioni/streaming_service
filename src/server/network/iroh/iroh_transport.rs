use crate::network::transport::{PeerCommunicationChannel, RawSender};
use anyhow::Context;
use bytes::BytesMut;
use gstreamer::Sample;
use gstreamer_app::AppSink;
use iroh::endpoint::Connection;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct IrohProtocol {
    app_sink: AppSink,
    cancellation_token: CancellationToken,
    frames_stream: PeerCommunicationChannel,
    iroh_connection: Connection,
}

impl IrohProtocol {
    pub fn new(
        app_sink: AppSink,
        frames_stream: PeerCommunicationChannel,
        iroh_connection: Connection,
    ) -> Self {
        Self {
            app_sink,
            cancellation_token: CancellationToken::new(),
            frames_stream,
            iroh_connection,
        }
    }
    pub fn start_streaming(&self) {
        let (sender, receiver) = channel::<Sample>(32);

        let cancel_for_pull = self.cancellation_token.clone();
        let app_sink = self.app_sink.clone();

        let _frame_sender_thread_handle = tokio::task::spawn_blocking(move || {
            loop {
                if cancel_for_pull.is_cancelled() {
                    info!("frame pull thread stopping (cancelled)");
                    break;
                }
                match app_sink.pull_sample() {
                    Ok(sample) => {
                        if sender.blocking_send(sample).is_err() {
                            break; // receiver dropped, nothing to do
                        }
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        warn!("Error on receiving the sample, before sending");
                        break; // EOS or sink shut down — no point looping forever
                    }
                }
            }
        });

        let cancel_for_send = self.cancellation_token.clone();
        let frames_sender = self.frames_stream.raw_sender();
        let _iroh_sender_task_handle = tokio::task::spawn(async move {
            tokio::select! {
                _ = cancel_for_send.cancelled() => {
                    info!("iroh sender task cancelled");
                }
                _ = async {
                    send_frames_iroh(frames_sender, receiver).await;
                } => {}
            }
        });

        let _ = self.cancellation_token.clone();
    }

    pub async fn stop_streaming(mut self) {
        info!("Iroh protocol stopped streaming called");
        self.cancellation_token.cancel();
        self.frames_stream.close().await;
        self.iroh_connection.close(0u8.into(), b"session closed");
    }
}
async fn send_frame(send: &RawSender, frame: &Sample) -> anyhow::Result<()> {
    let buffer = frame.buffer().context("Sample has no buffer")?;
    let map = buffer
        .map_readable()
        .context("Failed to map buffer readable")?;
    let payload = map.as_slice();

    let mut framed = BytesMut::with_capacity(4 + payload.len());
    // framed.put_u32(payload.len() as u32);
    framed.extend_from_slice(payload);

    send.send(framed.freeze())?;

    Ok(())
}

async fn send_frames_iroh(frames_sender: RawSender, mut recv: Receiver<Sample>) {
    loop {
        let Some(frame) = recv.recv().await else {
            warn!("Sample channel closed, stopping send loop");
            break;
        };

        if let Err(e) = send_frame(&frames_sender, &frame).await {
            error!("Failed to send frame: {e:#}");
            break;
        }
    }
}
