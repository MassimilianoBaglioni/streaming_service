use std::sync::Arc;

use crate::network::client_connection::ClientConnection;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer::{Bus, Pipeline};
use tracing::info;

// This is not just a proxy for ClientConnection, allows to spawn tasks that need ClientConnection passed using Arc
pub struct WindowsClient {
    client_connection: Option<ClientConnection>,
}

impl From<ClientConnection> for WindowsClient {
    fn from(client_connection: ClientConnection) -> Self {
        Self {
            client_connection: Some(client_connection),
        }
    }
}

impl WindowsClient {
    pub async fn receive(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let pipeline = Arc::new(
            self.client_connection
                .as_mut()
                .unwrap()
                .connection_mode
                .build_pipeline(),
        );

        let mut connection = self
            .client_connection
            .take()
            .ok_or("Client connection already taken")
            .expect("Client connection is None");

        connection.pipeline = Some(pipeline.clone());

        pipeline.set_state(gst::State::Playing)?;
        let bus = pipeline.bus().unwrap();

        let pipeline_clone = pipeline.clone();
        match connection.connect().await {
            Ok(()) => {}
            Err(e) => {
                Self::cleanup(pipeline, bus)?;
                return Err(Box::new(e));
            }
        };

        let receive_task_handle = tokio::task::spawn(async move {
            connection.receive().await;
            connection
        });

        let (result, current, pending) =
            tokio::task::block_in_place(|| pipeline_clone.state(gst::ClockTime::from_seconds(1)));
        info!(
            "Pipeline transition kicked off: result={:?} current={:?} pending={:?}",
            result, current, pending
        );

        info!("Waiting for receive task to complete");

        self.client_connection = Some(receive_task_handle.await.expect("Receive task panicked"));

        Self::cleanup(pipeline, bus)?;

        Ok(())
    }

    fn cleanup(pipeline: Arc<Pipeline>, bus: Bus) -> Result<(), Box<dyn std::error::Error>> {
        pipeline.send_event(gst::event::Eos::new());

        pipeline.set_state(gst::State::Null)?;
        let _ = pipeline.state(gst::ClockTime::from_seconds(5));

        drop(bus);
        drop(pipeline);

        info!("Quitting client receive");
        Ok(())
    }
}
