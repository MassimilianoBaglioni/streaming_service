use crate::network::iroh::connection::receive_frames_iroh;
use crate::network::iroh::{IrohStream, ALPN};
use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_client::{
    EventsReceiver, StreamingEventIrohClient, StreamingEventSocketClient,
};
use crate::network::{ConnectionBuildInfo, ConnectionMode};
use crate::video::gs;
use gstreamer::prelude::ElementExt;
use gstreamer::{Bus, Pipeline};
use gstreamer_app::gst;
use iroh::endpoint::{presets, Connection, RecvStream, SendStream};
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

const FRAMES_TAG: u8 = 0;
const EVENTS_TAG: u8 = 1;

pub struct ClientConnection {
    pub connection_mode: ConnectionMode,
    frontend_events_receiver: Option<Receiver<StreamingEvent>>,
    pub pipeline: Option<Arc<Pipeline>>, // TODO, I think that the pipeline does not belong here. This should just be network related. Counter argument -> direct pipeline handles udp
}
impl ClientConnection {
    pub async fn new_from_ticket_and_recv(
        ticket: EndpointTicket,
        frontend_events_receiver: Receiver<StreamingEvent>,
        endpoint: &Endpoint,
    ) -> Self {
        let connection = ClientConnection::iroh_connect(&ticket, &endpoint).await;

        ClientConnection {
            connection_mode: ConnectionMode::Iroh {
                connection: Some(connection),
                streaming_events_stream: None,
                frames_stream: None,
                ticket,
            },
            frontend_events_receiver: Some(frontend_events_receiver),
            pipeline: None,
        }
    }
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
                    streaming_events_socket_client: None,
                    watcher_stream_port,
                };

                Self {
                    connection_mode,
                    frontend_events_receiver: events_receiver,
                    pipeline: None,
                }
            }
            ConnectionBuildInfo::Iroh {
                endpoint, ticket, ..
            } => {
                let connection_mode = ConnectionMode::Iroh {
                    connection: None,
                    streaming_events_stream: None,
                    frames_stream: None,
                    ticket,
                };
                Self {
                    connection_mode,
                    frontend_events_receiver: events_receiver,
                    pipeline: None,
                }
            }
        }
    }
    pub async fn connect(&mut self) -> Result<(), std::io::Error> {
        match &mut self.connection_mode {
            ConnectionMode::Direct {
                socket_addr,
                streaming_events_socket_client,
                ..
            } => {
                *streaming_events_socket_client = Some(Arc::new(Mutex::new(
                    ClientConnection::direct_connect(*socket_addr)?,
                )));

                Ok(())
            }
            ConnectionMode::Iroh { .. } => {
                info!("Iroh connection initiated");
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
            ConnectionMode::Direct {
                streaming_events_socket_client,
                ..
            } => {
                let streaming_events_stream = streaming_events_socket_client
                    .as_mut()
                    .expect("No streaming events socket client found")
                    .clone();

                self.handle_events(bus_clone, streaming_events_stream).await;
            }
            ConnectionMode::Iroh {
                connection,
                frames_stream,
                streaming_events_stream,
                ..
            } => {
                Self::accept_connections(connection, frames_stream, streaming_events_stream).await;

                let app_src = gs::get_app_src(self.pipeline.as_ref().unwrap()).clone();

                // Take ownership, the Client connection won't need it anymore, just hand it to the task
                let mut frames_stream = frames_stream.take();
                let receive_frames_handler = tokio::spawn(async move {
                    receive_frames_iroh(
                        &mut *frames_stream.as_mut().unwrap().get_recv_lock().await,
                        app_src,
                    )
                    .await;
                });

                let streaming_events_stream = streaming_events_stream.take();
                let iroh_events_stream = StreamingEventIrohClient {
                    recv: streaming_events_stream
                        .expect("No streaming events stream found")
                        .recv_stream,
                };

                self.handle_events(bus_clone, Arc::new(Mutex::new(iroh_events_stream)))
                    .await;

                // TODO, check if this causes problems.
                receive_frames_handler.abort();
            }
        }
    }

    async fn handle_events<T: EventsReceiver + Send + 'static>(
        &mut self,
        bus: Bus,
        receiver: Arc<Mutex<T>>,
    ) {
        // With "take()" we are moving the ownership away from the struct, since it is inside self, and we need it in the task.
        // Only the receiver needs this because it is single consumer, we could use tokio::sync::broadcast that allows cloning, but I don't like it now.
        let mut frontend_receiver_clone = self
            .frontend_events_receiver
            .take()
            .expect("No receiver found");

        // Thread checking for streaming events that can stop the stream
        let mut streaming_events_handler = tokio::spawn(async move {
            info!("Starting the streaming events handler task");
            let mut events_receiver = receiver.lock().await;

            loop {
                let event = events_receiver
                    .read_event()
                    .await
                    .expect("Failed to read event");

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
        frames_stream: &mut Option<IrohStream>,
        streaming_events_stream: &mut Option<IrohStream>,
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
                    *frames_stream = IrohStream::new(send, recv).into();
                    info!("Accepted frames stream");
                }
                EVENTS_TAG => {
                    *streaming_events_stream = IrohStream::new(send, recv).into();
                    info!("Accepted events stream");
                }
                other => warn!("Unknown stream tag: {other}"),
            }
        }
    }

    // async fn handle_events_direct(&mut self, bus: Bus) {
    //     // With "take()" we are moving the ownership away from the struct, since it is inside self, and we need it in the task.
    //     // Only the receiver needs this because it is single consumer, we could use tokio::sync::broadcast that allows cloning, but I don't like it now.
    //     let mut frontend_receiver_clone = self
    //         .frontend_events_receiver
    //         .take()
    //         .expect("No receiver found");
    //
    //     let socket_receiver_clone = match &self.connection_mode {
    //         ConnectionMode::Direct {
    //             streaming_events_socket_client,
    //             ..
    //         } => streaming_events_socket_client
    //             .as_ref()
    //             .expect("No socket for event handling")
    //             .clone(),
    //         ConnectionMode::Iroh { .. } => {
    //             warn!("Iroh connection mode is not supported for direct event handling");
    //             return;
    //         }
    //     };
    //
    //     // Thread checking for socket events that can stop the stream
    //     // Spawn it before the loop to avoid ownership issues
    //     let mut socket_events_handler = tokio::spawn(async move {
    //         info!("Starting the socket events handler task");
    //         let mut socket = socket_receiver_clone.lock().await;
    //         while let Ok(event) = socket.read_event() {
    //             match event {
    //                 StreamingEvent::ServerEndsStream => {
    //                     info!("Received End event, from tcp socket");
    //                     return;
    //                 }
    //                 other => {
    //                     warn!("Received unexpected event: {:?}, from tcp socket", other);
    //                 }
    //             }
    //         }
    //     });
    //
    //     let mut frontend_events_handler = tokio::spawn(async move {
    //         info!("Starting the frontend events handler task");
    //         while let Some(event) = frontend_receiver_clone.recv().await {
    //             match event {
    //                 StreamingEvent::ClientQuit => {
    //                     info!("Received Quit event, from frontend, stopping");
    //                     return;
    //                 }
    //                 other => {
    //                     warn!("Received unexpected event: {:?}, from tcp socket", other);
    //                 }
    //             }
    //         }
    //     });
    //
    //     let mut gst_listener_task_handler = tokio::task::spawn_blocking(move || {
    //         info!("Starting the gst listener task");
    //         loop {
    //             if let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(100)) {
    //                 match msg.view() {
    //                     gst::MessageView::Eos(e) => {
    //                         info!("Eos received, stopping the stream! {:?}", e);
    //                         break;
    //                     }
    //                     gst::MessageView::Error(_err) => {
    //                         error!("Error case");
    //                         break;
    //                     }
    //                     gst::MessageView::Warning(w) => {
    //                         warn!("{:?}", w);
    //                     }
    //                     _ => {}
    //                 }
    //             }
    //         }
    //     });
    //
    //     tokio::select! {
    //         _result = &mut socket_events_handler => {
    //             info!("Socket events handler stopped the client.");
    //             gst_listener_task_handler.abort();
    //             frontend_events_handler.abort();
    //         }
    //         _result = &mut gst_listener_task_handler => {
    //             info!("Gst listener task handler stopped the client.");
    //             socket_events_handler.abort();
    //             frontend_events_handler.abort();
    //         }
    //         _result = &mut frontend_events_handler => {
    //             info!("Frontend events handler stopped the client.");
    //             socket_events_handler.abort();
    //             gst_listener_task_handler.abort();
    //         }
    //     }
    // }
}
