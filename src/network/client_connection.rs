use crate::network::iroh::connection::receive_frames_iroh;
use crate::network::iroh::ALPN;
use crate::network::transport::{FrameTransport, StreamingEvent, TaskTransport};
use crate::network::{ConnectionBuildInfo, ConnectionMode};
use crate::video::gs;
use gstreamer::prelude::ElementExt;
use gstreamer::{Bus, Pipeline};
use gstreamer_app::gst;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

const FRAMES_TAG: u8 = 0;
const EVENTS_TAG: u8 = 1;

pub struct ClientConnection {
    pub connection_mode: ConnectionMode,
    events_transport: Option<TaskTransport>,
    frontend_events_receiver: Option<Receiver<StreamingEvent>>,
    pub pipeline: Option<Arc<Pipeline>>, // TODO, I think that the pipeline does not belong here. This should just be network related. Counter argument -> direct pipeline handles udp
}
impl ClientConnection {
    pub async fn new_from_ticket_and_recv(
        ticket: EndpointTicket,
        frontend_events_receiver: Receiver<StreamingEvent>,
        endpoint: &Endpoint,
    ) -> Self {
        let connection = ClientConnection::iroh_connect(&ticket, endpoint).await;

        ClientConnection {
            connection_mode: ConnectionMode::Iroh {
                connection: Some(connection),
                frames_stream: None,
                ticket,
            },
            events_transport: None,
            frontend_events_receiver: Some(frontend_events_receiver),
            pipeline: None,
        }
    }
    pub async fn new(
        connection_build_info: ConnectionBuildInfo,
        frontend_events_receiver: Option<Receiver<StreamingEvent>>,
    ) -> Self {
        match connection_build_info {
            ConnectionBuildInfo::Direct {
                watcher_stream_port,
                tcp_socket_address,
                ..
            } => {
                let connection_mode = ConnectionMode::Direct {
                    socket_addr: tcp_socket_address,
                    watcher_stream_port,
                };
                let events_transport = Some(
                    StreamingEventsSocketClient::connect(tcp_socket_address)
                        .await
                        .expect("Failed to connect to the streaming events socket"),
                );

                Self {
                    connection_mode,
                    frontend_events_receiver,
                    pipeline: None,
                    events_transport,
                }
            }
            ConnectionBuildInfo::Iroh { .. } => {
                todo!();
            }
        }
    }
    pub async fn connect(&mut self) -> Result<(), std::io::Error> {
        match &mut self.connection_mode {
            ConnectionMode::Direct { .. } => Ok(()),
            ConnectionMode::Iroh { .. } => {
                info!("Iroh connection initiated");
                Ok(())
            }
        }
    }

    async fn iroh_connect(ticket: &EndpointTicket, endpoint: &Endpoint) -> Connection {
        endpoint.online().await;
        info!("Client connecting to Node ID: {:?}", ticket.endpoint_addr());
        let conn = endpoint
            .connect(ticket.endpoint_addr().clone(), ALPN)
            .await
            .expect("Failed to connect to the endpoint");
        info!("Iroh connected");

        conn
    }

    async fn accept_tagged_bi(conn: &Connection) -> anyhow::Result<(u8, SendStream, RecvStream)> {
        let (send, mut recv) = conn.accept_bi().await?;
        let mut tag = [0u8; 1];
        recv.read_exact(&mut tag).await?;
        Ok((tag[0], send, recv))
    }

    pub async fn receive(&mut self) {
        let bus_clone = self
            .pipeline
            .as_ref()
            .expect("No bus found")
            .bus()
            .clone()
            .unwrap();

        match &mut self.connection_mode {
            ConnectionMode::Direct { .. } => {
                self.handle_events(bus_clone).await;
            }
            ConnectionMode::Iroh {
                connection,
                frames_stream,
                ..
            } => {
                Self::accept_connections(connection, frames_stream, &mut self.events_transport)
                    .await;

                let app_src = gs::get_app_src(self.pipeline.as_ref().unwrap()).clone();

                // Take ownership, the Client connection won't need it anymore, just hand it to the task
                let mut frames_stream = frames_stream.as_mut().unwrap().take_receiver().unwrap();

                let receive_frames_handler = tokio::spawn(async move {
                    match receive_frames_iroh(&mut frames_stream, app_src).await {
                        Ok(()) => {
                            info!("Finished receiving frames");
                        }
                        Err(e) => {
                            error!("Error receiving frames: {:?}", e);
                        }
                    }
                });

                self.handle_events(bus_clone).await;

                receive_frames_handler.abort();
            }
        }
    }

    async fn handle_events(&mut self, bus: Bus) {
        // With "take()" we are moving the ownership away from the struct, since it is inside self, and we need it in the task.
        // Only the receiver needs this because it is single consumer, we could use tokio::sync::broadcast that allows cloning, but I don't like it now.
        let mut frontend_receiver_clone = self
            .frontend_events_receiver
            .take()
            .expect("No receiver found");

        let mut events_channel = self
            .events_transport
            .as_ref()
            .unwrap()
            .serialized_receiver();

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

        let mut frontend_events_handler = tokio::spawn(async move {
            info!("Starting the frontend events handler task");
            while let Some(event) = frontend_receiver_clone.recv().await {
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
                frontend_events_handler.abort();
            }
            _result = &mut gst_listener_task_handler => {
                info!("Gst listener task handler stopped the client.");
                streaming_events_handler.abort();
                frontend_events_handler.abort();
            }
            _result = &mut frontend_events_handler => {
                info!("Frontend events handler stopped the client.");
                streaming_events_handler.abort();
                gst_listener_task_handler.abort();
            }
        }
    }
    async fn accept_connections(
        connection: &Option<Connection>,
        frames_stream: &mut Option<FrameTransport>,
        streaming_events_stream: &mut Option<TaskTransport>,
    ) {
        info!("Iroh accepting connection bi on client");

        let connection = connection.as_ref().unwrap();
        // Accepts TWO connections, one for the frames and the other for streaming events
        for _ in 0..2 {
            let (tag, send, recv) = ClientConnection::accept_tagged_bi(connection)
                .await
                .expect("Failed to accept connection");
            match tag {
                FRAMES_TAG => {
                    *frames_stream = Some(FrameTransport::new(send, recv, 32, 65536));
                    info!("Accepted frames stream");
                }
                EVENTS_TAG => {
                    *streaming_events_stream = Some(TaskTransport::new(send, recv, 32, 1024));
                    info!("Accepted events stream");
                }
                other => warn!("Unknown stream tag: {other}"),
            }
        }
    }
}

pub struct StreamingEventsSocketClient;

impl StreamingEventsSocketClient {
    pub async fn connect(address: SocketAddr) -> std::io::Result<TaskTransport> {
        let stream = TcpStream::connect(address).await?;

        info!("Connected to {:?}", address);

        let (recv, send) = stream.into_split();

        Ok(TaskTransport::new(send, recv, 32, 1024))
    }
}
