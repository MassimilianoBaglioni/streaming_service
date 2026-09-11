use crate::error::PipelineError;
use crate::media::element_lookup::get_app_src;
use crate::network::transport::{PeerCommunicationChannel, StreamingEvent};
use crate::new_client::network::client_protocol::ClientProtocol;
use crate::new_client::network::connection_config::ConnectionConfig;
use crate::new_client::network::iroh::iroh_transport::IrohProtocolError;
use crate::new_client::pipeline::builder::Builder;
use crate::platform::decode_backend::DecodeBackend;
use gstreamer::prelude::ElementExt;
use gstreamer::Pipeline;
use gstreamer_app::gst;
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("pipeline error: {0}")]
    Pipeline(#[from] PipelineError),
    #[error("no frontend communication channel available")]
    NoFrontendChannel,
    #[error("Iroh Protocol error: {0}")]
    IrohError(#[from] IrohProtocolError),
}

pub struct ClientSession {
    protocol: ClientProtocol,
    pipeline: Pipeline,
    events_channel: PeerCommunicationChannel,
    cancellation_token: CancellationToken,
    frontend_events_receiver: Option<Receiver<StreamingEvent>>,
}

impl ClientSession {
    pub async fn accept_iroh(
        endpoint: &Endpoint,
        ticket: EndpointTicket,
        decoder: DecodeBackend,
        frontend_events_receiver: Receiver<StreamingEvent>,
    ) -> Result<Self, ClientError> {
        let pipeline = Builder::new(decoder).build_pipeline(ConnectionConfig::Iroh)?;

        let (protocol, events_channel) = ClientProtocol::accept_iroh(endpoint, ticket).await?;

        let cancellation_token = CancellationToken::new();

        Ok(ClientSession {
            protocol,
            pipeline,
            events_channel,
            cancellation_token,
            frontend_events_receiver: Some(frontend_events_receiver),
        })
    }

    pub async fn start_watching(&mut self) -> Result<(), ClientError> {
        match &mut self.protocol {
            ClientProtocol::Iroh(protocol) => {
                self.protocol = ClientProtocol::Iroh(
                    protocol
                        .start_watching(
                            get_app_src(&self.pipeline),
                            self.cancellation_token.clone(),
                        )
                        .await?,
                );

                Ok(())
            }
            ClientProtocol::Direct(_) => {
                todo!();
            }
        }
    }

    pub async fn handle_events(&mut self) -> Result<(), ClientError> {
        let bus = self.pipeline.bus().ok_or(PipelineError::BusFetchFail)?;
        // With "take()" we are moving the ownership away from the struct, since it is inside self, and we need it in the task.
        // Only the receiver needs this because it is single consumer, we could use tokio::sync::broadcast that allows cloning, but I don't like it now.

        let mut events_channel = self.events_channel.serialized_receiver();

        // Thread checking for streaming events that can stop the stream
        let mut streaming_events_handler = tokio::spawn(async move {
            info!("Starting the streaming events handler task");

            loop {
                let event = events_channel.recv().await.expect("Failed to read event");

                match event {
                    StreamingEvent::ServerEndsStream => {
                        info!("Received End event.");
                        return;
                    }
                    other => {
                        warn!("Received unexpected event: {:?}", other);
                    }
                }
            }
        });

        let mut frontend_events_receiver = self
            .frontend_events_receiver
            .take()
            .ok_or(ClientError::NoFrontendChannel)?;
        let mut frontend_events_handle = tokio::spawn(async move {
            info!("Starting the frontend events handler task");
            while let Some(event) = frontend_events_receiver.recv().await {
                match event {
                    StreamingEvent::ClientQuit => {
                        info!("Received Quit event, from frontend, stopping");
                        // TODO send stop watching event to the server now
                        return;
                    }
                    other => {
                        warn!("Received unexpected event: {:?}, from tcp socket", other);
                    }
                }
            }
        });

        let mut gst_listener_task_handler = tokio::task::spawn_blocking(move || {
            info!("Starting the gst listener task");
            loop {
                if let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(100)) {
                    match msg.view() {
                        gst::MessageView::Eos(e) => {
                            info!("Eos received, stopping the stream! {:?}", e);
                            break;
                        }
                        gst::MessageView::Error(_err) => {
                            error!("Error case");
                            break;
                        }
                        gst::MessageView::Warning(w) => {
                            warn!("{:?}", w);
                        }
                        _ => {}
                    }
                }
            }
        });

        tokio::select! {
            _result = &mut streaming_events_handler => {
                info!("Socket events handler stopped the client.");
                gst_listener_task_handler.abort();
                frontend_events_handle.abort();
                Ok(())
            }
            _result = &mut gst_listener_task_handler => {
                info!("Gst listener task handler stopped the client.");
                streaming_events_handler.abort();
                frontend_events_handle.abort();
                Ok(())
            }
            _result = &mut frontend_events_handle => {
                info!("Frontend events handler stopped the client.");
                streaming_events_handler.abort();
                gst_listener_task_handler.abort();
                Ok(())
            }
            _ = self.cancellation_token.cancelled() => {
                info!("Cancellation token triggered, stopping all client tasks.");
                streaming_events_handler.abort();
                gst_listener_task_handler.abort();
                frontend_events_handle.abort();
                Ok(())
            }
        }
    }
}
