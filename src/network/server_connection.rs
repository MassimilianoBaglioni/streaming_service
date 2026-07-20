use crate::network::iroh::IrohInfo;
use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_server::StreamingEventSocketServer;
use crate::network::ConnectionMode;
use gstreamer::Sample;
use iroh::endpoint::{RecvStream, SendStream};
use tokio::sync::mpsc::Receiver;
use tracing::{info, warn};

pub struct ServerConnection {
    server_socket: Option<StreamingEventSocketServer>,
    iroh_connection: Option<(SendStream, RecvStream)>,
    pub connection_mode: Option<ConnectionMode>,
}

impl Default for ServerConnection {
    fn default() -> Self {
        ServerConnection {
            server_socket: None,
            iroh_connection: None,
            connection_mode: None,
        }
    }
}

impl From<u16> for ServerConnection {
    fn from(port: u16) -> Self {
        let server_socket = Some(
            StreamingEventSocketServer::bind(&format!("0.0.0.0:{}", port))
                .expect("Failed to bind tcp socket."),
        );

        ServerConnection {
            server_socket,
            iroh_connection: None,
            connection_mode: Some(ConnectionMode::Direct),
        }
    }
}

impl From<IrohInfo> for ServerConnection {
    fn from(iroh_info: IrohInfo) -> Self {
        ServerConnection {
            server_socket: None,
            iroh_connection: None,
            connection_mode: Some(ConnectionMode::Iroh {
                // TODO double check that this clone does not create any problem, because we are also storing the tickets in the appstate idk if that can create issues
                info: iroh_info,
            }),
        }
    }
}

impl ServerConnection {
    pub async fn accept(&mut self) {
        match &self.connection_mode {
            Some(ConnectionMode::Direct) => {
                if let Some(server_socket) = self.server_socket.as_mut() {
                    tokio::task::block_in_place(|| server_socket.accept())
                        .expect("Failed to accept client");

                    // Accept a client (closes previous connection if any and waits for a new one)
                    info!("Accepted client");
                } else {
                    panic!(
                        "Server socket not init, probably did not call from u8 before calling accept or it was disconnected before"
                    );
                }
            }
            Some(ConnectionMode::Iroh { info }) => {
                let incoming = info
                    .endpoint
                    .as_ref()
                    .expect("No endpoint set")
                    .accept()
                    .await
                    .expect("Error connecting to the endpoint");

                let conn = incoming.await.expect("Error accepting connection");

                // TODO is this open_bi or accept_bi????
                let (send, recv) = conn.open_bi().await.expect("Failed to open connection");
                info!("Successfully opened connection");

                self.iroh_connection = Some((send, recv));
            }
            None => warn!("Connection mode set to None"),
        }
    }

    pub fn send_event(&mut self, streaming_event: StreamingEvent) {
        let socket = self.server_socket.as_mut().unwrap();

        match socket.send_event(&streaming_event) {
            Ok(_) => info!("Sent End event"),
            Err(e) => warn!("Failed to send End event: {:?}", e),
        }

        info!("Streaming stopped");
    }

    pub fn close_socket(&mut self) {
        if let Some(socket) = self.server_socket.as_mut() {
            socket.disconnect();
            self.server_socket = None;
        }
    }

    pub fn send_end_event_and_close_conn(&mut self) {
        self.send_event(StreamingEvent::ServerEndsStream);
        self.close_socket();
    }

    pub async fn send_frames_iroh(&mut self, mut recv: Receiver<Sample>) {
        let (iroh_send_conn, _) = self.iroh_connection.as_mut().expect("No iroh connection");

        loop {
            let frame = recv.recv().await.expect("Failed to receive sample from gs");

            let bytes = frame.buffer().unwrap();
            let map = bytes.map_readable().unwrap();
            let rtp_bytes = map.as_slice();
            let len = rtp_bytes.len() as u32;

            iroh_send_conn
                .write_all(&len.to_be_bytes())
                .await
                .expect("Failed to send len");

            iroh_send_conn
                .write_all(rtp_bytes)
                .await
                .expect("Failed to send payload");

            // TODO add a method to quit this loop when needed. Can an async task be stopped jsut with the handle?
        }
    }
}
