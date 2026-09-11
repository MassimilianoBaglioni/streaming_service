use crate::network::transport::{RawSender, SerializedSender, StreamingEvent, TaskTransport};
use crate::network::ConnectionBuildInfo;
use anyhow::Context;
use bytes::BytesMut;
use gstreamer::Sample;
use iroh::endpoint::Connection;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::mpsc::Receiver;
use tracing::{error, info, warn};

pub enum ServerConnectionMode {
    Direct {
        client_address: SocketAddr,
        client_streaming_port: u16,
    },
    Iroh {
        frames_stream: Option<TaskTransport>,
        iroh_connection: Connection,
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
    pub events_transport: Option<TaskTransport>,
    pub events_sender: Option<SerializedSender<StreamingEvent>>,
}

impl From<ConnectionBuildInfo> for ServerConnection {
    fn from(connection_build_info: ConnectionBuildInfo) -> Self {
        Self {
            connection_mode: ServerConnectionMode::from(connection_build_info),
            events_transport: None,
            events_sender: None,
        }
    }
}

impl ServerConnection {
    pub async fn accept(&mut self) {
        match &mut self.connection_mode {
            ServerConnectionMode::Direct { client_address, .. } => {
                let streaming_socket = StreamingEventsSocketServer::bind(*client_address)
                    .await
                    .expect("Could not bind the socket");

                self.events_transport = Some(
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
        match self.events_sender.as_mut().unwrap().send(&streaming_event) {
            Err(e) => {
                error!("Failed to send event: {e}");
            }
            Ok(streaming_event) => {
                info!("Sent event: {:?}", streaming_event);
            }
        }
    }

    pub async fn close_conn(&mut self) {
        match &mut self.connection_mode {
            ServerConnectionMode::Direct { .. } => {
                self.events_transport.take().unwrap().close().await;
            }
            ServerConnectionMode::Iroh {
                iroh_connection,
                frames_stream,
                ..
            } => {
                self.events_transport.take().unwrap().close().await;
                frames_stream.take().unwrap().close().await;
                iroh_connection.close(0u8.into(), b"session closed");
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

        let frames_sender = frames_stream.as_mut().unwrap().raw_sender();

        loop {
            let Some(frame) = recv.recv().await else {
                warn!("Sample channel closed, stopping send loop");
                break;
            };

            if let Err(e) = ServerConnection::send_frame(&frames_sender, &frame).await {
                error!("Failed to send frame: {e}");
                break;
            }
        }
    }

    async fn send_frame(send: &RawSender, frame: &Sample) -> anyhow::Result<()> {
        let buffer = frame.buffer().context("Sample has no buffer")?;
        let map = buffer
            .map_readable()
            .context("Failed to map buffer readable")?;
        let payload = map.as_slice();

        let mut framed = BytesMut::with_capacity(4 + payload.len());
        // framed.put_u32(payload.len() as u32);
        framed.extend_from_slice(payload);

        send.send(framed.freeze())?;

        Ok(())
    }
}

pub struct StreamingEventsSocketServer {
    listener: TcpListener,
}

impl StreamingEventsSocketServer {
    pub async fn bind(address: SocketAddr) -> std::io::Result<Self> {
        let listener = TcpListener::bind(address).await?;

        Ok(Self { listener })
    }

    pub async fn accept(self) -> std::io::Result<TaskTransport> {
        let (stream, addr) = self.listener.accept().await?;

        info!("Accepted connection from {:?}", addr);

        let (recv, send) = stream.into_split();

        Ok(TaskTransport::new(send, recv, 32, 1024))
    }
}
