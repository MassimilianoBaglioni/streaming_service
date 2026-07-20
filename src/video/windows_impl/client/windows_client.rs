use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::network::client_connection::ClientConnection;
use crate::network::streaming_event::StreamingEvent;
use crate::network::NetInfo;
use gstreamer as gst;
use gstreamer::prelude::*;
use tracing::info;

pub struct WindowsClient {
    net_info: NetInfo,
    client_connection: Option<ClientConnection>,
}

impl WindowsClient {
    pub fn new(
        net_info: NetInfo,
        events_sender: Sender<StreamingEvent>,
        events_receiver: Receiver<StreamingEvent>,
    ) -> Self {
        let client_connection = Some(ClientConnection::new(
            net_info.clone(),
            Some(events_sender),
            Some(events_receiver),
        ));
        Self {
            net_info,
            client_connection,
        }
    }

    pub async fn receive(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let pipeline = Arc::new(self.net_info.build_pipeline());

        let mut connection = self
            .client_connection
            .take()
            .ok_or("Client connection already taken")
            .expect("Client connection is None");

        connection.pipeline = Some(pipeline.clone());

        pipeline.set_state(gst::State::Playing)?;
        let bus = pipeline.bus().unwrap();

        connection.connect().await;

        let receive_task_handle = tokio::task::spawn(async move {
            connection.receive().await;
            connection
        });

        let (result, current, pending) =
            tokio::task::block_in_place(|| pipeline.state(gst::ClockTime::from_seconds(1)));
        info!(
            "Pipeline transition kicked off: result={:?} current={:?} pending={:?}",
            result, current, pending
        );

        info!("Pre receive await");

        self.client_connection = Some(receive_task_handle.await.expect("Receive task panicked"));

        info!("Post receive await");

        pipeline.send_event(gst::event::Eos::new());

        pipeline.set_state(gst::State::Null)?;
        let _ = pipeline.state(gst::ClockTime::from_seconds(5));

        drop(bus);
        drop(pipeline);

        info!("Quitting client receive");
        Ok(())
    }
}
