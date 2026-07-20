use crate::network::iroh::connection::receive_frames_iroh;
use crate::network::iroh::ALPN;
use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_client::StreamingEventSocketClient;
use crate::network::{ConnectionMode, NetInfo};
use crate::video::gs;
use crate::video::gs::{build_client_iroh_pipeline, build_client_udp_pipeline};
use gstreamer::prelude::ElementExt;
use gstreamer::{Bus, Pipeline};
use gstreamer_app::gst;
use iroh::endpoint::{presets, Connection};
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, warn};

pub struct ClientConnection {
    socket_addr: Option<SocketAddr>,
    streaming_port: Option<u16>,
    streaming_events_client: Option<StreamingEventSocketClient>,
    iroh_connection: Option<Connection>,
    iroh_endpoint: Option<Endpoint>,
    connection_mode: Option<ConnectionMode>,
    events_sender: Option<Sender<StreamingEvent>>,
    events_receiver: Option<Receiver<StreamingEvent>>,
    pub pipeline: Option<Arc<Pipeline>>,
}
impl ClientConnection {
    pub fn new(
        net_info: NetInfo,
        events_sender: Option<Sender<StreamingEvent>>,
        events_receiver: Option<Receiver<StreamingEvent>>,
    ) -> Self {
        match net_info.connection_mode {
            ConnectionMode::Direct => {
                let streaming_port = net_info.stream_port;
                let streamer_ip = net_info.target_ip;
                let tcp_port = net_info.tcp_port;

                let tcp_address = format!("{}:{}", streamer_ip, tcp_port);
                let socket_addr =
                    Some(tcp_address.parse().expect("Failed to parse the ip address"));

                Self {
                    streaming_port: Some(streaming_port),
                    socket_addr,
                    streaming_events_client: None,
                    iroh_connection: None,
                    iroh_endpoint: None,
                    connection_mode: Some(net_info.connection_mode),
                    events_sender,
                    events_receiver,
                    pipeline: None,
                }
            }
            ConnectionMode::Iroh { info } => Self {
                streaming_port: None,
                socket_addr: None,
                streaming_events_client: None,
                iroh_connection: None,
                iroh_endpoint: None,
                connection_mode: Some(ConnectionMode::Iroh { info }),
                events_sender: None,
                events_receiver: None,
                pipeline: None,
            },
        }
    }
    pub async fn connect(&mut self) {
        match &self.connection_mode {
            Some(ConnectionMode::Direct) => {
                self.streaming_events_client = Some(
                    tokio::task::block_in_place(|| {
                        StreamingEventSocketClient::connect(&self.socket_addr.unwrap().to_string())
                    })
                    .expect("Failed to connect to the socket client side"),
                );
            }
            Some(ConnectionMode::Iroh { info }) => {
                let endpoint = Endpoint::bind(presets::N0)
                    .await
                    .expect("Failed to bind endpoint");
                endpoint.online().await;

                let ticket = EndpointTicket::from_str(
                    &info.ticket.as_ref().expect("No ticket set").to_string(),
                )
                .expect("Failed to parse ticket");
                info!("Iroh Connecting");

                let conn = endpoint
                    .connect(ticket.endpoint_addr().clone(), ALPN)
                    .await
                    .expect("Failed to connect to the endpoint");

                self.iroh_connection = Some(conn);
                self.iroh_endpoint = Some(endpoint);
                info!("Iroh connected");
            }
            None => {
                warn!("Client connection is None");
            }
        }
    }

    pub async fn receive(&mut self) {
        match &self.connection_mode {
            Some(ConnectionMode::Direct) => {
                let bus_clone = self
                    .pipeline
                    .as_ref()
                    .expect("No bus found")
                    .bus()
                    .clone()
                    .unwrap();

                self.handle_events_direct(bus_clone).await;
            }
            Some(ConnectionMode::Iroh { info }) => {
                let (_send, recv) = self
                    .iroh_connection
                    .as_ref()
                    .unwrap()
                    .accept_bi()
                    .await
                    .expect("Failed to open connection from the client");

                receive_frames_iroh(recv, gs::get_app_src(self.pipeline.as_ref().unwrap())).await;
            }
            None => {
                warn!("Client connection is None");
            }
        }
    }
    pub fn build_pipeline(&mut self) {
        self.pipeline = match self
            .connection_mode
            .as_ref()
            .expect("No connection mode set")
        {
            ConnectionMode::Direct => Some(Arc::new(build_client_udp_pipeline(
                self.streaming_port.unwrap(),
            ))),
            ConnectionMode::Iroh { info } => Some(Arc::new(build_client_iroh_pipeline())),
        };
    }

    async fn handle_events_direct(&mut self, bus: Bus) {
        let socket_sender_clone = self.events_sender.clone().expect("Cannot unwrap sender");

        // With "take()" we are moving the ownership away from the struct, since it is inside self, and we need it in the task.
        // Only the receiver needs this because it is single consumer, we could use tokio::sync::broadcast that allows cloning, but I don't like it now.
        let mut socket_receiver = self.events_receiver.take().expect("No receiver found");

        // Thread checking for socket events that can stop the stream
        // Spawn it before the loop to avoid ownership issues
        let mut socket_events_handler = tokio::spawn(async move {
            while let Some(event) = socket_receiver.recv().await {
                match event {
                    StreamingEvent::ServerEndsStream => {
                        info!("Received End event, from tcp socket");
                        return;
                    }
                    StreamingEvent::ClientQuit => {
                        info!("Received quit from frontend, proceeding to quit");
                        socket_sender_clone
                            .send(StreamingEvent::ClientQuit)
                            .await
                            .expect("Error on sending the event from the event socket");
                        return;
                    }
                    other => {
                        warn!("Received unexpected event: {:?}, from tcp socket", other);
                    }
                }
            }
        });

        let mut gst_listener_task_handler = tokio::task::spawn_blocking(move || {
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
            }
            _result = &mut gst_listener_task_handler => {
                info!("Gst listener task handler stopped the client.");
                socket_events_handler.abort();
            }
        }
    }
}
