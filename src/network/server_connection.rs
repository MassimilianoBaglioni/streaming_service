use crate::network::iroh::IrohStream;
use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_server::{IrohEventsStream, StreamingEventSocketServer};
use crate::network::ConnectionBuildInfo;
use anyhow::Context;
use gstreamer::Sample;
use iroh::endpoint::{Connection, SendStream};
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
        frames_stream: IrohStream,
        events_stream: IrohEventsStream,
        iroh_connection: Connection,
    },
}

impl ServerConnectionMode {
    pub async fn close_connection(&mut self) {
        match self {
            ServerConnectionMode::Direct { server_socket, .. } => {
                if let Some(mut socket) = server_socket.take() {
                    socket.disconnect();
                }
            }
            ServerConnectionMode::Iroh {
                frames_stream,
                events_stream,
                iroh_connection,
                ..
            } => {
                {
                    let mut send = events_stream.get_send_lock().await;
                    if let Err(e) = send.finish() {
                        warn!("Failed to finish events stream: {:?}", e);
                    }
                    if let Err(e) = send.stopped().await {
                        warn!("events stream not confirmed received: {:?}", e);
                    }
                }

                {
                    let mut send = frames_stream.get_send_lock().await;
                    if let Err(e) = send.finish() {
                        warn!("Failed to finish frames stream: {:?}", e);
                    }
                    if let Err(e) = send.stopped().await {
                        warn!("frames stream not confirmed received: {:?}", e);
                    }
                }

                iroh_connection.close(0u32.into(), b"session ended");
                iroh_connection.closed().await;
            }
        }
    }

    pub fn accept(&mut self) {
        match self {
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
            ServerConnectionMode::Iroh { .. } => {
                info!("Iroh server connection is already established, no need to accept");
            }
        }
    }

    pub async fn send_event(&mut self, streaming_event: StreamingEvent) {
        match self {
            ServerConnectionMode::Direct { server_socket, .. } => {
                match server_socket.as_mut().unwrap().send_event(&streaming_event) {
                    Ok(_) => info!("Sent End event"),
                    Err(e) => warn!("Failed to send End event: {:?}", e),
                }

                info!("Streaming stopped");
            }
            ServerConnectionMode::Iroh { events_stream, .. } => {
                info!("Sending event via Iroh connection");
                events_stream
                    .send_event(&streaming_event)
                    .await
                    .expect("Failed to send event");

                info!("Streaming stopped");
            }
        }
    }
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
            ConnectionBuildInfo::Iroh { .. } => todo!(),
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
        self.connection_mode.accept();
    }

    pub async fn send_event(&mut self, streaming_event: StreamingEvent) {
        self.connection_mode.send_event(streaming_event).await;
    }

    async fn close_conn(&mut self) {
        self.connection_mode.close_connection().await;
    }

    pub async fn send_end_event_and_close_conn(&mut self) {
        self.send_event(StreamingEvent::ServerEndsStream).await;
        self.close_conn().await;
    }

    pub async fn send_frames_iroh(&mut self, mut recv: Receiver<Sample>) {
        let ServerConnectionMode::Iroh { frames_stream, .. } = &mut self.connection_mode else {
            error!("No iroh connection mode");
            return;
        };

        loop {
            let Some(frame) = recv.recv().await else {
                warn!("Sample channel closed, stopping send loop");
                break;
            };

            let mut send_ref = frames_stream.get_send_lock().await;
            if let Err(e) = ServerConnection::send_frame(&mut send_ref, &frame).await {
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
