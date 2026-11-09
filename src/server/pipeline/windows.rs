use crate::error::PipelineError;
use crate::media::scaling;
use crate::server::network::direct::request::DirectConnectionRequest;
use crate::server::network::iroh::request::IrohConnectionRequest;
use crate::server::pipeline::builder::APPSRC_NAME;
use gstreamer::prelude::Cast;
use gstreamer::Pipeline;

pub fn windows_direct(request: DirectConnectionRequest) -> Result<Pipeline, PipelineError> {
    let video_scale_method = request.scaling_method.as_gst_method();

    let (scaled_width, scaled_height) =
        scaling::scale_dimensions(request.resolution as f32, request.height, request.width);

    let pipeline_description = format!(
        "appsrc name={} is-live=true do-timestamp=true format=time \
            caps=video/x-raw,format=BGRA,width={},height={} ! \
            videoscale method={} add-borders=true ! \
            video/x-raw,width={},height={} ! \
            videoconvert ! \
            video/x-raw,format=NV12 ! \
            mfh264enc bitrate={} ! \
            video/x-h264,profile=high ! \
            h264parse ! \
            rtph264pay config-interval=-1 pt=96 mtu=1400 ! \
            udpsink host={} port={} sync=false async=false",
        APPSRC_NAME,
        request.width,
        request.height,
        video_scale_method,
        scaled_width,
        scaled_height,
        request.bitrate,
        request.client_ip,
        request.client_port,
    );

    Ok(gstreamer::parse::launch(&pipeline_description)?
        .downcast::<Pipeline>()
        .map_err(|_| PipelineError::FailedDowncast)?)
}
pub fn windows_iroh(request: IrohConnectionRequest) -> Result<Pipeline, PipelineError> {
    let video_scale_method = request.scaling_method.as_gst_method();

    let (scaled_width, scaled_height) =
        scaling::scale_dimensions(request.resolution as f32, request.height, request.width);

    let pipeline_description = format!(
        "appsrc name={} is-live=true do-timestamp=true format=time \
            caps=video/x-raw,format=BGRA,width={},height={} ! \
            videoscale method={} add-borders=true ! \
            video/x-raw,width={},height={} ! \
            videoconvert ! \
            video/x-raw,format=NV12 ! \
            mfh264enc bitrate={} ! \
            video/x-h264,profile=high ! \
            h264parse ! \
            rtph264pay config-interval=-1 pt=96 mtu=1400 ! \
            appsink name=rtp_sink sync=false async=false",
        APPSRC_NAME,
        request.width,
        request.height,
        video_scale_method,
        scaled_width,
        scaled_height,
        request.bitrate,
    );

    Ok(gstreamer::parse::launch(&pipeline_description)?
        .downcast::<Pipeline>()
        .map_err(|_| PipelineError::FailedDowncast)?)
}
