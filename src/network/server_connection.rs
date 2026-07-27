use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_server::StreamingEventSocketServer;
use crate::network::ConnectionBuildInfo;
use anyhow::Context;
use gstreamer::Sample;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::Endpoint;
use std::net::SocketAddr;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

pub enum ServerConnectionMode {
    Direct {
        server_socket: Option<StreamingEventSocketServer>,
        client_address: SocketAddr,
        client_streaming_port: u16,
    },
    Iroh {
        send_stream: Option<SendStream>,
        recv_stream: Option<RecvStream>,
        endpoint: Endpoint,
        iroh_connection: Option<Connection>,
    },
}

impl From<ConnectionBuildInfo> for ServerConnectionMode {
    fn from(mode: ConnectionBuildInfo) -> Self {
        match mode {
            ConnectionBuildInfo::Direct {
                watcher_stream_port,
                tcp_socket_address: tcp_address,
            } => ServerConnectionMode::Direct {
                server_socket: None,
                client_address: tcp_address,
                client_streaming_port: watcher_stream_port,
            },
            ConnectionBuildInfo::Iroh {
                endpoint,
                ticket: _,
            } => ServerConnectionMode::Iroh {
                send_stream: None,
                recv_stream: None,
                endpoint,
                iroh_connection: None,
            },
        }
    }
}

// We are keeping this struct even if it's only wrapping the other, because we will
// eventually have shared fields between the two connections and here is where those fields
// will be placed
pub struct ServerConnection {
    pub connection_mode: ServerConnectionMode,
}

impl From<ConnectionBuildInfo> for ServerConnection {
    fn from(connection_build_info: ConnectionBuildInfo) -> Self {
        Self {
            connection_mode: ServerConnectionMode::from(connection_build_info),
        }
    }
}

impl ServerConnection {
    pub async fn accept(&mut self) {
        match &mut self.connection_mode {
            ServerConnectionMode::Direct {
                server_socket,
                client_address,
                ..
            } => {
                if server_socket.is_none() {
                    *server_socket = Some(
                        StreamingEventSocketServer::bind(*client_address)
                            .expect("Could not bind the socket"),
                    );
                }
                tokio::task::block_in_place(|| server_socket.as_mut().unwrap().accept())
                    .expect("Failed to accept client");

                // Accept a client (closes previous connection if any and waits for a new one)
                info!("Accepted client");
            }
            ServerConnectionMode::Iroh {
                endpoint,
                iroh_connection,
                send_stream,
                recv_stream,
            } => {
                info!("Ready to accept incoming iroh connections");
                info!("Server generated Node ID: {:?}", endpoint.addr());
                let incoming = endpoint
                    .accept()
                    .await
                    .expect("Error connecting to the endpoint");

                info!("Accepted iroh client");

                let conn = incoming.await.expect("Error accepting connection");
                let (send, recv) = conn.open_bi().await.expect("Failed to open connection");

                info!("Opened bi connection on the server");

                *send_stream = Some(send);
                *recv_stream = Some(recv);
                *iroh_connection = Some(conn);

                info!("Successfully opened connection");
            }
        }
    }

    pub fn send_event(&mut self, streaming_event: StreamingEvent) {
        match &mut self.connection_mode {
            ServerConnectionMode::Direct { server_socket, .. } => {
                match server_socket.as_mut().unwrap().send_event(&streaming_event) {
                    Ok(_) => info!("Sent End event"),
                    Err(e) => warn!("Failed to send End event: {:?}", e),
                }

                info!("Streaming stopped");
            }
            ServerConnectionMode::Iroh { .. } => todo!(),
        }
    }

    pub fn close_socket(&mut self) {
        match &mut self.connection_mode {
            ServerConnectionMode::Direct { server_socket, .. } => {
                // Take leaves None after the block is executed
                if let Some(mut socket) = server_socket.take() {
                    socket.disconnect();
                }
            }
            _ => {
                error!("Calling close socket on non direct type of connection");
            }
        }
    }

    pub fn send_end_event_and_close_conn(&mut self) {
        self.send_event(StreamingEvent::ServerEndsStream);
        self.close_socket();
    }

    pub async fn send_frames_iroh(&mut self, mut recv: Receiver<Sample>) {
        let ServerConnectionMode::Iroh { send_stream, .. } = &mut self.connection_mode else {
            error!("No iroh connection mode");
            return;
        };
        let send = send_stream.as_mut().expect("No iroh send stream");

        loop {
            let Some(frame) = recv.recv().await else {
                warn!("Sample channel closed, stopping send loop");
                break;
            };

            if let Err(e) = ServerConnection::send_frame(send, &frame).await {
                error!("Failed to send frame: {e}");
                break;
            }
        }
    }

    async fn send_frame(send: &mut SendStream, frame: &Sample) -> anyhow::Result<()> {
        let buffer = frame.buffer().context("Sample has no buffer")?;
        let map = buffer
            .map_readable()
            .context("Failed to map buffer readable")?;
        let payload = map.as_slice();

        let len = payload.len() as u32;
        send.write_all(&len.to_be_bytes()).await?;
        send.write_all(payload).await?;

        Ok(())
    }
}
