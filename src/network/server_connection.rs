use crate::network::streaming_event::{StreamingEvent, Transport};
use crate::network::streaming_events_server::StreamingEventsSocketServer;
use crate::network::ConnectionBuildInfo;
use anyhow::Context;
use gstreamer::Sample;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use std::net::SocketAddr;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

pub enum ServerConnectionMode {
    Direct {
        client_address: SocketAddr,
        client_streaming_port: u16,
        events_connection: Option<Transport<OwnedReadHalf, OwnedWriteHalf>>,
    },
    Iroh {
        frames_stream: Transport<RecvStream, SendStream>,
        iroh_connection: Connection,
        events_connection: Option<Transport<RecvStream, SendStream>>,
    },
}

impl From<ConnectionBuildInfo> for ServerConnectionMode {
    fn from(mode: ConnectionBuildInfo) -> Self {
        match mode {
            ConnectionBuildInfo::Direct {
                watcher_stream_port,
                tcp_socket_address: tcp_address,
            } => ServerConnectionMode::Direct {
                client_address: tcp_address,
                client_streaming_port: watcher_stream_port,
                events_connection: None,
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
        match &mut self.connection_mode {
            ServerConnectionMode::Direct {
                client_address,
                events_connection,
                ..
            } => {
                let streaming_socket = StreamingEventsSocketServer::bind(*client_address)
                    .await
                    .expect("Could not bind the socket");

                *events_connection = Some(
                    streaming_socket
                        .accept()
                        .await
                        .expect("Could not accept the connection"),
                );

                // Accept a client (closes previous connection if any and waits for a new one)
                info!("Accepted client");
            }
            ServerConnectionMode::Iroh { .. } => {
                info!("Iroh server connection is already established, no need to accept");
            }
        }
    }

    pub async fn send_event(&mut self, streaming_event: StreamingEvent) {
        match &mut self.connection_mode {
            ServerConnectionMode::Direct {
                events_connection, ..
            } => {
                events_connection
                    .as_mut()
                    .unwrap()
                    .send_serializable(&streaming_event)
                    .await
                    .expect("Failed to send event");
            }
            ServerConnectionMode::Iroh {
                events_connection, ..
            } => {
                events_connection
                    .as_mut()
                    .unwrap()
                    .send_serializable(&streaming_event)
                    .await
                    .expect("Failed to send event");
            }
        }
    }

    pub async fn close_conn(&mut self) {
        match &mut self.connection_mode {
            ServerConnectionMode::Direct {
                events_connection, ..
            } => {
                events_connection
                    .as_mut()
                    .unwrap()
                    .close()
                    .await
                    .expect("Failed to close connection");

                *events_connection = None;
            }
            ServerConnectionMode::Iroh {
                events_connection, ..
            } => {
                events_connection
                    .as_mut()
                    .unwrap()
                    .close()
                    .await
                    .expect("Failed to close connection");

                *events_connection = None;
            }
        }
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

            if let Err(e) = ServerConnection::send_frame(frames_stream, &frame).await {
                error!("Failed to send frame: {e}");
                break;
            }
        }
    }

    async fn send_frame(
        send: &mut Transport<RecvStream, SendStream>,
        frame: &Sample,
    ) -> anyhow::Result<()> {
        let buffer = frame.buffer().context("Sample has no buffer")?;
        let map = buffer
            .map_readable()
            .context("Failed to map buffer readable")?;
        let payload = map.as_slice();

        let len = payload.len() as u32;
        send.send(&len.to_be_bytes()).await?;
        send.send(payload).await?;

        Ok(())
    }
}
