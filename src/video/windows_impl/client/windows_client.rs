use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, MutexGuard};

use std::thread;
use tokio::sync::Mutex;

use crate::network::iroh::connection::ClientConnection;
use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_client::StreamingEventSocketClient;
use crate::network::NetInfo;
use crate::video::gs::{build_client_iroh_pipeline, build_client_udp_pipeline};
use gstreamer as gst;
use gstreamer::{prelude::*, Pipeline};
use gstreamer_app::AppSrc;
use iroh::endpoint::RecvStream;
use iroh::endpoint::Side::Client;
use iroh::Endpoint;
use iroh_tickets::endpoint::EndpointTicket;
use tracing::{error, info, warn};

pub enum StopWatchingEvent {
    ClientStop,
    StreamEnded,
    GenericError,
}

pub struct WindowsClient {
    net_info: NetInfo,
    events_sender: Sender<StopWatchingEvent>,
    events_receiver: Receiver<StopWatchingEvent>,
    client_connection: Arc<Mutex<Option<ClientConnection>>>,
}

impl WindowsClient {
    pub fn new(
        net_info: NetInfo,
        events_sender: Sender<StopWatchingEvent>,
        events_receiver: Receiver<StopWatchingEvent>,
    ) -> Self {
        let client_connection =
            Arc::new(Mutex::new(Some(ClientConnection::from(net_info.clone()))));
        Self {
            net_info,
            events_sender,
            events_receiver,
            client_connection,
        }
    }

    pub async fn receive(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let streaming_port = self.net_info.stream_port;

        let pipeline = match &self.net_info.connection_mode {
            crate::network::ConnectionMode::Direct => {
                build_client_udp_pipeline(self.net_info.stream_port)
            }
            crate::network::ConnectionMode::Iroh { info } => build_client_iroh_pipeline(),
        };

        self.client_connection
            .lock()
            .await
            .as_mut()
            .unwrap()
            .pipeline = Some(Arc::new(pipeline));

        let pipeline = {
            let guard = self.client_connection.lock().await;
            guard.as_ref().unwrap().pipeline.as_ref().unwrap().clone()
        };

        let bus = pipeline.bus().unwrap();

        self.client_connection
            .lock()
            .await
            .as_mut()
            .unwrap()
            .connect()
            .await;

        let client_connection_recv_clone = self.client_connection.clone();

        let receive_task_handle = tokio::task::spawn(async move {
            client_connection_recv_clone
                .lock()
                .await
                .as_mut()
                .unwrap()
                .receive()
                .await;
        });

        let client_connection_clone = self.client_connection.clone();

        pipeline.set_state(gst::State::Playing)?;
        info!("Receiver online. Listening on port {}...", streaming_port);

        let mut should_break;

        let socket_sender_clone = self.events_sender.clone();

        // Thread checking for socket events that can stop the stream
        // Spawn it before the loop to avoid ownership issues
        tokio::spawn(async move {
            match client_connection_clone
                .as_ref()
                .lock()
                .await
                .as_mut()
                .unwrap()
                .read_event()
            {
                Ok(StreamingEvent::End) => {
                    info!("Received End event, from tcp socket");
                    socket_sender_clone
                        .send(StopWatchingEvent::StreamEnded)
                        .expect("Error on sending the event from the event socket");
                }
                Err(e) => {
                    warn!("Received err: {:?}, from tcp socket", e);
                    socket_sender_clone
                        .send(StopWatchingEvent::GenericError)
                        .expect("Error on sending the event from the event socket");
                }
            }
        });

        loop {
            match self
                .events_receiver
                .recv()
                .expect("Failed to receive an event from recv")
            {
                StopWatchingEvent::ClientStop => {
                    info!("ClientStop received, stopping client");
                    should_break = true;
                }
                StopWatchingEvent::StreamEnded => {
                    info!("StreamEnded received, stopping client");
                    should_break = true;
                }
                StopWatchingEvent::GenericError => {
                    info!("GenericError received, stopping client");
                    should_break = true;
                }
            };

            if let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(100)) {
                //last_activity = std::time::Instant::now();

                match msg.view() {
                    gst::MessageView::Eos(..) => {
                        info!("Eos received, stopping the stream!");
                        should_break = true;
                    }
                    gst::MessageView::Error(_err) => {
                        error!("Error case");
                        should_break = true;
                    }
                    gst::MessageView::Warning(w) => {
                        warn!("{:?}", w);
                    }
                    gst::MessageView::StateChanged(s) => {
                        // if let Some(src) = msg.src()
                        //     && *src == pipeline.clone().upcast::<gst::Object>()
                        // {
                        //     info!("Pipeline state: {:?} -> {:?}", s.old(), s.current());
                        // }
                    }
                    gst::MessageView::Element(e) => {
                        if let Some(_structure) = e.structure() {
                            //info!("Element message: {}", structure.name());
                        }
                    }
                    gst::MessageView::Qos(_qos) => {
                        //println!("{:?}", qos);
                    }
                    _ => {}
                }
            }

            if should_break {
                break;
            }
        }

        pipeline.send_event(gst::event::Eos::new());
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        pipeline.set_state(gst::State::Paused)?;
        let _ = pipeline.state(gst::ClockTime::from_seconds(2));
        pipeline.set_state(gst::State::Ready)?;
        let _ = pipeline.state(gst::ClockTime::from_seconds(2));
        pipeline.set_state(gst::State::Null)?;
        let _ = pipeline.state(gst::ClockTime::from_seconds(5));

        drop(bus);
        drop(pipeline);

        info!("Quitting client receive loop");
        Ok(())
    }
}
