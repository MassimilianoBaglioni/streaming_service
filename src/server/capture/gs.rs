use std::net::Ipv4Addr;

use crate::media::scaling;
use crate::server::capture::windows::windows_streaming_settings::VideoStreamingSettings;
use gstreamer as gst;
use gstreamer::{prelude::*, Pipeline};
use tracing::info;

pub static APPSRC_NAME: &str = "src";

pub fn create_direct_windows_pipeline(
    width: u32,
    height: u32,
    client_ip: Ipv4Addr,
    client_port: u16,
    windows_settings: &VideoStreamingSettings,
) -> Pipeline {
    let video_scale_method = windows_settings.scaling_method.as_gst_method();

    let (scaled_width, scaled_height) =
        scaling::scale_dimensions(windows_settings.resolution as f32, height, width);

    let pipeline_description = format!(
        "appsrc name={} is-live=true do-timestamp=true format=time \
     caps=capture/x-raw,format=BGRA,width={},height={} ! \
     videoscale method={} add-borders=true ! \
     capture/x-raw,width={},height={} ! \
     videoconvert ! \
     capture/x-raw,format=NV12 ! \
     mfh264enc bitrate={} ! \
     capture/x-h264,profile=high ! \
     h264parse ! \
     rtph264pay config-interval=-1 pt=96 mtu=1400 ! \
     udpsink host={} port={} sync=false async=false",
        APPSRC_NAME,
        width,
        height,
        video_scale_method,
        scaled_width,
        scaled_height,
        windows_settings.bitrate,
        client_ip,
        client_port,
    );

    info!("Server pipeline description: {}", pipeline_description);

    gstreamer::parse::launch(&pipeline_description)
        .expect("Failed to create pipeline")
        .downcast::<Pipeline>()
        .expect("Failed to downcast to Pipeline")
}

pub fn create_windows_pipeline_with_app_dest(
    width: u32,
    height: u32,
    windows_settings: &VideoStreamingSettings,
) -> Pipeline {
    let video_scale_method = windows_settings.scaling_method.as_gst_method();

    let (scaled_width, scaled_height) =
        scaling::scale_dimensions(windows_settings.resolution as f32, height, width);

    let pipeline_description = format!(
        "appsrc name={} is-live=true do-timestamp=true format=time \
     caps=capture/x-raw,format=BGRA,width={},height={} ! \
     videoscale method={} add-borders=true ! \
     capture/x-raw,width={},height={} ! \
     videoconvert ! \
     capture/x-raw,format=NV12 ! \
     mfh264enc bitrate={} ! \
     capture/x-h264,profile=high ! \
     h264parse ! \
     rtph264pay config-interval=-1 pt=96 mtu=1400 ! \
     appsink name=rtp_sink sync=false async=false",
        APPSRC_NAME,
        width,
        height,
        video_scale_method,
        scaled_width,
        scaled_height,
        windows_settings.bitrate,
    );

    gstreamer::parse::launch(&pipeline_description)
        .expect("Failed to create pipeline")
        .downcast::<Pipeline>()
        .expect("Failed to downcast to Pipeline")
}

pub fn build_client_udp_pipeline(streaming_port: u16) -> Pipeline {
    let pipeline_description = format!(
        "\
            udpsrc port={} buffer-size=8388608 ! \
            application/x-rtp,media=capture,clock-rate=90000,encoding-name=H264,payload=96 ! \
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
    pipeline.downcast::<Pipeline>().unwrap()
}

pub fn build_client_iroh_pipeline() -> Pipeline {
    let pipeline_description = format!(
        "\
        appsrc name={} max-bytes=8388608 block=false leaky-type=downstream format=time is-live=true do-timestamp=true ! \
        application/x-rtp,media=capture,clock-rate=90000,encoding-name=H264,payload=96 ! \
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
    pipeline.downcast::<Pipeline>().unwrap()
}
