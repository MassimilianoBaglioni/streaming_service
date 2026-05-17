use crate::network::NetInfo;
use crate::network::streaming_event::StreamingEvent;
use crate::network::streaming_events_client::StreamingEventSocketClient;
use gstreamer::prelude::*;
use gstreamer::{self as gst};
use tracing::{error, info, warn};

pub struct WindowsClient {
    net_info: NetInfo,
}

impl WindowsClient {
    pub fn new(net_info: NetInfo) -> Self {
        Self { net_info }
    }

    pub fn receive(&self) -> Result<(), Box<dyn std::error::Error>> {
        let streaming_port = self.net_info.stream_port;
        let streamer_ip = self.net_info.target_ip;
        let tcp_port = self.net_info.tcp_port;

        let pipeline_description = format!(
            "\
            udpsrc port={} buffer-size=8388608 ! \
            application/x-rtp,media=video,clock-rate=90000,encoding-name=H264,payload=96 ! \
            rtpjitterbuffer latency=200 ! \
            queue leaky=downstream max-size-time=1000000000 ! \
            rtph264depay ! \
            h264parse ! \
            d3d11h264dec ! \
            queue leaky=downstream max-size-time=500000000 ! \
            d3d11videosink sync=false",
            streaming_port
        );
        info!("Streaming port: {}", streaming_port);

        let pipeline = gst::parse::launch(&pipeline_description)?;
        let pipeline = pipeline.downcast::<gst::Pipeline>().unwrap();

        let bus = pipeline.bus().unwrap();

        let tcp_address = format!("{}:{}", streamer_ip, tcp_port);
        info!("Socket address: {}", tcp_address);

        let mut socket = StreamingEventSocketClient::connect(&tcp_address)
            .expect("Could not create the tcp socket");

        pipeline.set_state(gst::State::Playing)?;
        info!("Receiver online. Listening on port {}...", streaming_port);

        let mut should_break = false;
        loop {
            match socket.read_event() {
                Ok(StreamingEvent::End) => {
                    info!("Received End event, from tcp socket");
                    should_break = true;
                }
                Err(e) => warn!("Received err: {:?}, from tcp socket", e),
            }

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
                        if let Some(src) = msg.src()
                            && *src == pipeline.clone().upcast::<gst::Object>()
                        {
                            info!("Pipeline state: {:?} -> {:?}", s.old(), s.current());
                        }
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
        std::thread::sleep(std::time::Duration::from_millis(300));

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
