use crate::client::network::client_protocol::ClientProtocol;
use crate::client::network::connection_config::ConnectionConfig;
use crate::client::network::direct::direct_transport::DirectProtocolError;
use crate::client::network::direct::request::DirectNetworkRequest;
use crate::client::pipeline::builder::Builder;
use crate::error::PipelineError;
use crate::error::{IrohProtocolError, IrohStateError};
use crate::media::element_lookup::get_app_src;
use crate::network::transport::{
    PeerCommunicationChannel, SerializedReceiver, StreamingEvent, TransmissionError,
};
use crate::platform::decode_backend::DecodeBackend;
use gstreamer::prelude::{ElementExt, ElementExtManual};
use gstreamer::Pipeline;
use gstreamer_app::gst;
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("pipeline error: {0}")]
    Pipeline(#[from] PipelineError),
    #[error("no frontend communication channel available")]
    NoFrontendChannel,
    #[error("Iroh protocol error: {0}")]
    IrohError(#[from] IrohProtocolError),
    #[error("Direct protocol error: {0}")]
    DirectError(#[from] DirectProtocolError),
    #[error("Transmission error: {0}")]
    TransmissionError(#[from] TransmissionError),
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
        endpoint: Arc<Mutex<Option<Endpoint>>>,
        ticket: EndpointTicket,
        decoder: DecodeBackend,
        frontend_events_receiver: Receiver<StreamingEvent>,
    ) -> Result<Self, ClientError> {
        let pipeline = Builder::new(decoder).build_pipeline(ConnectionConfig::Iroh)?;

        let endpoint = {
            let guard = endpoint.lock().await;
            guard
                .clone()
                .ok_or_else(|| IrohProtocolError::from(IrohStateError::NoEndpointFound))?
        };
        let (protocol, events_channel) = ClientProtocol::accept_iroh(&endpoint, ticket).await?;

        let cancellation_token = CancellationToken::new();

        Ok(ClientSession {
            protocol,
            pipeline,
            events_channel,
            cancellation_token,
            frontend_events_receiver: Some(frontend_events_receiver),
        })
    }

    pub async fn accept_direct(
        decoder: DecodeBackend,
        connection_config_direct: DirectNetworkRequest,
        frontend_events_receiver: Receiver<StreamingEvent>,
    ) -> Result<Self, ClientError> {
        let address = connection_config_direct.address.clone();

        let pipeline = Builder::new(decoder)
            .build_pipeline(ConnectionConfig::Direct(connection_config_direct))?;

        let (protocol, events_channel) = ClientProtocol::accept_direct(address).await?;

        Ok(ClientSession {
            protocol,
            pipeline,
            events_channel,
            cancellation_token: CancellationToken::new(),
            frontend_events_receiver: Some(frontend_events_receiver),
        })
    }
    async fn start_watching_task(&mut self) -> Result<(), ClientError> {
        self.pipeline
            .set_state(gst::State::Playing)
            .map_err(|e| PipelineError::ChangeStateError(e))?;

        match &mut self.protocol {
            ClientProtocol::Iroh(protocol) => {
                protocol
                    .start_watching(get_app_src(&self.pipeline), self.cancellation_token.clone())
                    .await?;

                Ok(())
            }
            ClientProtocol::Direct(_) => {
                info!("Direct implementation no extra steps after starting the pipeline");
                Ok(())
            }
        }
    }

    pub async fn start_watching(
        &mut self,
        on_finish: impl FnOnce(Result<(), ClientError>) + Send + 'static,
    ) -> Result<(), ClientError> {
        info!("Starting watch task");
        self.start_watching_task().await?;

        let bus = self.pipeline.bus().ok_or(PipelineError::BusFetchFail)?;
        let events = self.events_channel.serialized_receiver();
        let frontend = self
            .frontend_events_receiver
            .take()
            .ok_or(ClientError::NoFrontendChannel)?;
        let pipeline = self.pipeline.clone();
        let token = self.cancellation_token.clone();

        tokio::spawn(async move {
            let result = Self::run(pipeline, bus, events, frontend, token).await;
            on_finish(result);
        });

        Ok(())
    }

    async fn run(
        pipeline: Pipeline,
        bus: gst::Bus,
        mut events: SerializedReceiver<StreamingEvent>,
        mut frontend: Receiver<StreamingEvent>,
        token: CancellationToken,
    ) -> Result<(), ClientError> {
        let gst_token = token.child_token();
        let mut gst_task = Self::spawn_gst_listener(bus, gst_token.clone());

        let stopped_by =
            Self::handle_events(&mut events, &mut frontend, &mut gst_task, &token).await;
        info!("{stopped_by} stopped the client.");

        Self::cleanup(&pipeline, gst_token, gst_task).await
    }

    async fn handle_events(
        events: &mut SerializedReceiver<StreamingEvent>,
        frontend: &mut Receiver<StreamingEvent>,
        gst_task: &mut JoinHandle<()>,
        token: &CancellationToken,
    ) -> &'static str {
        tokio::select! {
            _ = Self::wait_for_server_end(events) => "server events",
            _ = Self::wait_for_frontend_quit(frontend) => "frontend events",
            _ = gst_task => "gst listener",
            _ = token.cancelled() => "cancellation token",
        }
    }

    async fn wait_for_server_end(events: &mut SerializedReceiver<StreamingEvent>) {
        info!("Starting the streaming events handler");
        while let Ok(event) = events.recv().await {
            match event {
                StreamingEvent::ServerEndsStream => {
                    info!("Received End event.");
                    return;
                }
                other => warn!("Received unexpected event: {:?}", other),
            }
        }
        warn!("Streaming events channel closed");
    }

    async fn wait_for_frontend_quit(frontend: &mut Receiver<StreamingEvent>) {
        info!("Starting the frontend events handler");
        while let Some(event) = frontend.recv().await {
            match event {
                StreamingEvent::ClientQuit => {
                    info!("Received Quit event from frontend, stopping");
                    // TODO: notify the server here
                    return;
                }
                other => warn!("Received unexpected event from frontend: {:?}", other),
            }
        }
    }

    fn spawn_gst_listener(bus: gst::Bus, token: CancellationToken) -> JoinHandle<()> {
        tokio::task::spawn_blocking(move || {
            info!("Starting the gst listener task");
            while !token.is_cancelled() {
                if let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(100)) {
                    match msg.view() {
                        gst::MessageView::Eos(_) => break,
                        gst::MessageView::Error(err) => {
                            error!("Pipeline error: {:?}", err);
                            break;
                        }
                        gst::MessageView::Warning(w) => warn!("{:?}", w),
                        _ => {}
                    }
                }
            }
        })
    }

    async fn cleanup(
        pipeline: &Pipeline,
        gst_token: CancellationToken,
        gst_task: JoinHandle<()>,
    ) -> Result<(), ClientError> {
        gst_token.cancel();

        pipeline.send_event(gst::event::Eos::new());
        let result = pipeline
            .set_state(gst::State::Null)
            .map(|_| ())
            .map_err(|e| ClientError::from(PipelineError::ChangeStateError(e)));
        let _ = pipeline.state(gst::ClockTime::from_seconds(5));

        let _ = gst_task.await;

        result
    }
    pub async fn stop_watching(&self) -> Result<(), ClientError> {
        self.cancellation_token.cancel();
        info!("Token cancelled");
        self.events_channel
            .serialized_sender()
            .send(&StreamingEvent::ClientQuit)?;
        info!("Sent ClientQuit event to server");
        Ok(())
    }
}
