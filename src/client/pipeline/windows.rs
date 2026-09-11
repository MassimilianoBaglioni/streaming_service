use crate::new_client::pipeline::builder::APPSRC_NAME;
use gstreamer as gst;

use crate::error::PipelineError;
use gstreamer::{prelude::*, Pipeline};
use tracing::info;

pub fn windows_iroh() -> Result<Pipeline, PipelineError> {
    let pipeline_description = format!(
        "\
        appsrc name={} max-bytes=8388608 block=false leaky-type=downstream format=time is-live=true do-timestamp=true ! \
        application/x-rtp,media=video,clock-rate=90000,encoding-name=H264,payload=96 ! \
        rtpjitterbuffer latency=200 ! \
        queue leaky=downstream max-size-time=1000000000 ! \
        rtph264depay ! \
        h264parse ! \
        d3d11h264dec ! \
        queue leaky=downstream max-size-time=500000000 ! \
        d3d11videosink sync=false",
        APPSRC_NAME
    );

    let pipeline =
        gst::parse::launch(&pipeline_description).expect("Failed to launch client iroh pipeline");
    pipeline
        .downcast::<Pipeline>()
        .map_err(|_| PipelineError::FailedDowncast)
}

pub fn windows_direct(streaming_port: u16) -> Result<Pipeline, PipelineError> {
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
    info!("Client UDP pipeline description: {}", pipeline_description);

    let pipeline =
        gst::parse::launch(&pipeline_description).expect("Failed to launch client udp pipeline");
    pipeline
        .downcast::<Pipeline>()
        .map_err(|_| PipelineError::FailedDowncast)
}
