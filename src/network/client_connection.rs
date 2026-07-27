use crate::network::iroh::connection::receive_frames_iroh;
use crate::network::iroh::ALPN;
use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_client::StreamingEventSocketClient;
use crate::network::{ConnectionBuildInfo, ConnectionMode};
use crate::video::gs;
use gstreamer::prelude::ElementExt;
use gstreamer::{Bus, Pipeline};
use gstreamer_app::gst;
use iroh::endpoint::Connection;
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

pub struct ClientConnection {
    streaming_events_socket_client: Option<Arc<Mutex<StreamingEventSocketClient>>>,
    pub connection_mode: ConnectionMode,
    frontend_events_receiver: Option<Receiver<StreamingEvent>>,
    pub pipeline: Option<Arc<Pipeline>>,
}
impl ClientConnection {
    pub fn new(
        connection_build_info: ConnectionBuildInfo,
        events_receiver: Option<Receiver<StreamingEvent>>,
    ) -> Self {
        match connection_build_info {
            ConnectionBuildInfo::Direct {
                watcher_stream_port, // TODO I renamed to watcher stream port, but this struct is used for the client as well. In this case we are passing the streamer IP to connect to the tcp IP socket. Rename it better
                tcp_socket_address,
                ..
            } => {
                let connection_mode = ConnectionMode::Direct {
                    socket_addr: tcp_socket_address,
                    watcher_stream_port,
                };

                Self {
                    streaming_events_socket_client: None,
                    connection_mode,
                    frontend_events_receiver: events_receiver,
                    pipeline: None,
                }
            }
            ConnectionBuildInfo::Iroh { endpoint, ticket } => {
                let connection_mode = ConnectionMode::Iroh {
                    connection: None,
                    endpoint,
                    ticket,
                };
                Self {
                    streaming_events_socket_client: None,
                    connection_mode,
                    frontend_events_receiver: events_receiver,
                    pipeline: None,
                }
            }
        }
    }
    pub async fn connect(&mut self) -> Result<(), std::io::Error> {
        match &mut self.connection_mode {
            ConnectionMode::Direct { socket_addr, .. } => {
                self.streaming_events_socket_client = Some(Arc::new(Mutex::new(
                    ClientConnection::direct_connect(*socket_addr)?,
                )));

                Ok(())
            }
            ConnectionMode::Iroh {
                endpoint,
                ticket,
                connection,
            } => {
                *connection = Some(ClientConnection::iroh_connect(ticket, endpoint).await);
                Ok(())
            }
        }
    }

    fn direct_connect(socket_addr: SocketAddr) -> std::io::Result<StreamingEventSocketClient> {
        tokio::task::block_in_place(|| {
            StreamingEventSocketClient::connect(&socket_addr.to_string())
        })
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

    pub async fn receive(&mut self) {
        match &self.connection_mode {
            ConnectionMode::Direct { .. } => {
                let bus_clone = self
                    .pipeline
                    .as_ref()
                    .expect("No bus found")
                    .bus()
                    .clone()
                    .unwrap();

                self.handle_events_direct(bus_clone).await;
            }
            ConnectionMode::Iroh { connection, .. } => {
                let (_send, recv) = connection
                    .as_ref()
                    .unwrap()
                    .accept_bi()
                    .await
                    .expect("Failed to open connection from the client");

                receive_frames_iroh(recv, gs::get_app_src(self.pipeline.as_ref().unwrap())).await;
            }
        }
    }
    async fn handle_events_direct(&mut self, bus: Bus) {
        // With "take()" we are moving the ownership away from the struct, since it is inside self, and we need it in the task.
        // Only the receiver needs this because it is single consumer, we could use tokio::sync::broadcast that allows cloning, but I don't like it now.
        let mut frontend_receiver_clone = self
            .frontend_events_receiver
            .take()
            .expect("No receiver found");

        let socket_receiver_clone = self
            .streaming_events_socket_client
            .as_mut()
            .unwrap()
            .clone();

        // Thread checking for socket events that can stop the stream
        // Spawn it before the loop to avoid ownership issues
        let mut socket_events_handler = tokio::spawn(async move {
            info!("Starting the socket events handler task");
            let mut socket = socket_receiver_clone.lock().await;
            while let Ok(event) = socket.read_event() {
                match event {
                    StreamingEvent::ServerEndsStream => {
                        info!("Received End event, from tcp socket");
                        return;
                    }
                    other => {
                        warn!("Received unexpected event: {:?}, from tcp socket", other);
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
            _result = &mut socket_events_handler => {
                info!("Socket events handler stopped the client.");
                gst_listener_task_handler.abort();
                frontend_events_handler.abort();
            }
            _result = &mut gst_listener_task_handler => {
                info!("Gst listener task handler stopped the client.");
                socket_events_handler.abort();
                frontend_events_handler.abort();
            }
            _result = &mut frontend_events_handler => {
                info!("Frontend events handler stopped the client.");
                socket_events_handler.abort();
                gst_listener_task_handler.abort();
            }
        }
    }
}
