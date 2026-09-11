use tokio_util::sync::CancellationToken;
use crate::network::transport::ClientCommunicationChannel;
use crate::server::server_protocol::ServerProtocol;
use crate::server::video::CaptureSource;
use tracing::info;

pub struct ServerSession {
    events_sender: ClientCommunicationChannel,
    protocol: ServerProtocol,
    capture_source: CaptureSource,
    cancellation_token: CancellationToken,
}

impl ServerSession {
    pub fn new(events_sender: ClientCommunicationChannel, protocol: ServerProtocol, capture_source: CaptureSource, cancellation_token: CancellationToken) -> Self {
        Self {
            events_sender,
            protocol,
            capture_source,
            cancellation_token,
        }
    }
    pub async fn start_streaming(&mut self) {
        info!("Start streaming video source called");
        let (width, height) = self.capture_source.get_capture_item_size();
        let video_streaming_settings = self.capture_source.get_video_streaming_settings();
        self.capture_source.start_streaming(); 
        
        self.protocol.start_streaming();
    }
}
