use crate::network::transport::{PeerCommunicationChannel, SerializedReceiver, StreamingEvent};
use crate::server::capture::capture_source::CaptureSource;
use crate::server::network::server_protocol::ServerProtocol;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{info, warn};

enum StopStreamingReason {
    ClientQuit,
    ServerFrontendQuit,
}

struct ServerState {
    events_sender: PeerCommunicationChannel,
    protocol: ServerProtocol,
    capture_source: CaptureSource,
}

impl ServerState {
    pub fn new(
        events_sender: PeerCommunicationChannel,
        protocol: ServerProtocol,
        capture_source: CaptureSource,
    ) -> Self {
        Self {
            events_sender,
            protocol,
            capture_source,
        }
    }

    pub async fn shutdown(mut self, stop_reason: StopStreamingReason) {
        match stop_reason {
            StopStreamingReason::ServerFrontendQuit => {
                let _ = self
                    .events_sender
                    .serialized_sender()
                    .send(&StreamingEvent::ServerEndsStream)
                    .inspect_err(|e| warn!("Failed to send stop event from server: {e}"));
            }
            _ => {}
        }

        // Do not close the event transport before the stop notification is sent.
        // The queued ServerEndsStream event must be delivered to the client.
        self.events_sender.close().await;
        info!("Events sender closed");

        self.protocol.stop_streaming().await;
        info!("Protocol stopped streaming");

        self.capture_source.stop();
        info!("Capture source stopped");
    }
}

pub struct ServerSession {
    stop_streaming_sender: Sender<StopStreamingReason>,
    shutdown_handle: JoinHandle<()>,
    client_event_handle: JoinHandle<()>,
}

impl ServerSession {
    pub async fn start_streaming(
        events_sender: PeerCommunicationChannel,
        protocol: ServerProtocol,
        mut capture_source: CaptureSource,
    ) -> Self {
        info!("Start streaming capture source called");

        capture_source.start_streaming();
        protocol.start_streaming();

        let state = ServerState::new(events_sender, protocol, capture_source);
        let events_receiver: SerializedReceiver<StreamingEvent> =
            state.events_sender.serialized_receiver();
        let (stop_streaming_sender, stop_streaming_receiver) =
            tokio::sync::mpsc::channel::<StopStreamingReason>(1);

        let shutdown_handle =
            tokio::spawn(async move { Self::shutdown_task(stop_streaming_receiver, state).await });
        let client_event_handle = tokio::spawn({
            let sender = stop_streaming_sender.clone();
            async move { Self::receive_client_events(events_receiver, sender).await }
        });

        Self {
            stop_streaming_sender,
            shutdown_handle,
            client_event_handle,
        }
    }

    async fn shutdown_task(
        mut receiver: Receiver<StopStreamingReason>,
        state_to_shutdown: ServerState,
    ) {
        match receiver.recv().await {
            Some(stop_reason) => state_to_shutdown.shutdown(stop_reason).await,
            None => warn!("Shutdown channel closed unexpectedly"),
        }
    }

    async fn receive_client_events(
        mut client_events_receiver: SerializedReceiver<StreamingEvent>,
        send_cancellation_channel: Sender<StopStreamingReason>,
    ) {
        loop {
            match client_events_receiver.recv().await {
                Ok(StreamingEvent::ClientQuit) => {
                    send_cancellation_channel
                        .send(StopStreamingReason::ClientQuit)
                        .await
                        .unwrap_or_else(|e| {
                            warn!("Failed to send cancellation reason: {e}");
                        });
                    return;
                }
                Ok(_event) => {
                    warn!("Handle received client events here, currently not implemented")
                }
                Err(_) => {
                    warn!("Failed to receive client event, stopping client event task");
                    return;
                }
            }
        }
    }

    pub async fn stop_streaming(self) {
        // If the client already quit, the shutdown task has finished and dropped
        // its receiver, so this send fails and can be ignored.
        let _ = self
            .stop_streaming_sender
            .send(StopStreamingReason::ServerFrontendQuit)
            .await;

        // Wait for the shutdown to complete (events sender, protocol, capture source).
        if let Err(e) = self.shutdown_handle.await {
            warn!("Shutdown task failed: {e}");
        }

        // The client event task may still be blocked on recv, so abort it.
        self.client_event_handle.abort();
        let _ = self.client_event_handle.await;
        info!("Server session stopped");
    }
}
