use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};

use tokio::sync::Mutex;

use crate::network::iroh::connection::ClientConnection;
use crate::network::streaming_event::StreamingEvent;
use crate::network::NetInfo;
use gstreamer as gst;
use gstreamer::prelude::*;
use tracing::info;
use tracing::log::warn;

pub struct WindowsClient {
    net_info: NetInfo,
    client_connection: Arc<Mutex<Option<ClientConnection>>>,
}

impl WindowsClient {
    pub fn new(
        net_info: NetInfo,
        events_sender: Sender<StreamingEvent>,
        events_receiver: Receiver<StreamingEvent>,
    ) -> Self {
        let client_connection = Arc::new(Mutex::new(Some(ClientConnection::new(
            net_info.clone(),
            Some(events_sender),
            Some(events_receiver),
        ))));
        Self {
            net_info,
            client_connection,
        }
    }

    pub async fn receive(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let pipeline = self.net_info.build_pipeline();

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
        pipeline.set_state(gst::State::Playing)?;

        let bus = pipeline.bus().unwrap();

        // pipeline.set_state(gst::State::Playing)?;
        // pipeline.set_state(gst::State::Paused)?;
        // let _ = pipeline.state(gst::ClockTime::from_seconds(1));
        // pipeline.set_state(gst::State::Ready)?;
        // let _ = pipeline.state(gst::ClockTime::from_seconds(1));
        // info!("about to start receive task handle");

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

        // Live source (udpsrc/rtpjitterbuffer) can't fully preroll to Playing until
        // real RTP data arrives — which only happens after we connect() and the
        // server starts sending. Async here is expected and correct, not a failure.
        let (result, current, pending) =
            tokio::task::block_in_place(|| pipeline.state(gst::ClockTime::from_seconds(1)));
        info!(
            "Pipeline transition kicked off: result={:?} current={:?} pending={:?}",
            result, current, pending
        );

        info!("Pre receive await");

        receive_task_handle.await;

        info!("Post receive await");

        pipeline.send_event(gst::event::Eos::new());
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        pipeline.set_state(gst::State::Null)?;
        let _ = pipeline.state(gst::ClockTime::from_seconds(5));

        drop(bus);
        drop(pipeline);

        info!("Quitting client receive");
        Ok(())
    }
}
