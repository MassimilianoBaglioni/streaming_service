use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gstreamer::prelude::*;
use gstreamer::{self as gst};
use tracing::{error, info, warn};

pub fn start_screen_stream(
    node_id: u32,
    stop_streaming_flag: Arc<AtomicBool>,
    host: &str,
    port: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Enable GStreamer debug output (optional, can be noisy)
    // std::env::set_var("GST_DEBUG", "3");

    // let pipeline_description = format!(
    //     "pipewiresrc path={} do-timestamp=true ! \
    //      queue max-size-buffers=3 leaky=downstream ! \
    //      videoconvert ! video/x-raw,format=I420 ! \
    //      x264enc tune=zerolatency bitrate=9000 speed-preset=ultrafast key-int-max=30 threads=4 ! \
    //      h264parse ! \
    //      rtph264pay config-interval=1 pt=96 mtu=1400 ! \
    //      udpsink host={} port={} sync=false async=false buffer-size=2097152",
    //     node_id, host, port
    // );

    // GPU ENCODING, SHOULD WORK EVEN WITH CPU DECODING
    //     let pipeline_description = format!(
    //     "pipewiresrc path={} do-timestamp=true ! \
    //      queue max-size-buffers=3 leaky=downstream ! \
    //      videoconvert ! \
    //      vaapih264enc rate-control=cbr bitrate=4000 keyframe-period=30 quality-level=7 ! \
    //      h264parse ! \
    //      rtph264pay config-interval=1 pt=96 mtu=1400 ! \
    //      udpsink host={} port={} sync=false async=false buffer-size=8388608",
    //     node_id, host, port
    // );

    // GPU ENCODING, SHOULD BE PAIRED WITH GPU DECODING VERSION ON THE CLIENT
    // let pipeline_description = format!(
    //     "pipewiresrc path={} do-timestamp=true ! \
    //  queue max-size-buffers=3 leaky=downstream ! \
    //  videoconvert ! video/x-raw,format=NV12 ! \
    //  vaapih264enc rate-control=cbr bitrate=4000 keyframe-period=30 quality-level=7 ! \
    //  video/x-h264,profile=main ! \
    //  h264parse ! \
    //  rtph264pay config-interval=-1 pt=96 mtu=1400 ! \
    //  udpsink host={} port={} sync=false async=false buffer-size=8388608",
    //     node_id, host, port
    // );

    // WORKING WITH FRAME LIMITATION
    // let pipeline_description = format!(
    //     "pipewiresrc path={} do-timestamp=true ! \
    //  queue max-size-buffers=3 leaky=downstream ! \
    //  videorate ! \
    //  video/x-raw,framerate=30/1 ! \
    //  videoconvert ! \
    //  video/x-raw,format=NV12 ! \
    //  vaapih264enc rate-control=cbr bitrate=12000 keyframe-period=60 quality-level=1 ! \
    //  video/x-h264,profile=main ! \
    //  h264parse ! \
    //  rtph264pay config-interval=-1 pt=96 mtu=1400 ! \
    //  udpsink host={} port={} sync=false async=false buffer-size=8388608",
    //     node_id, host, port
    // );

    //PRE FIX, using vaapi
    // let pipeline_description = format!(
    //     "pipewiresrc path={} do-timestamp=true ! \
    //  videorate drop-only=true ! \
    //  video/x-raw,framerate=60/1 ! \
    //  queue max-size-time=500000000 leaky=downstream ! \
    //  videoconvert ! \
    //  video/x-raw,format=NV12 ! \
    //  vaapih264enc rate-control=cbr bitrate=15000 keyframe-period=90 quality-level=1 ! \
    //  video/x-h264,profile=high ! \
    //  h264parse ! \
    //  rtph264pay config-interval=-1 pt=96 mtu=1400 ! \
    //  queue max-size-time=1000000000 ! \
    //  udpsink host={} port={} sync=false async=false buffer-size=8388608",
    //     node_id, host, port
    // );

    // POST FIX USING VAH v1 with suspicious quality
    let pipeline_description = format!(
        "pipewiresrc path={} do-timestamp=true ! \
     videorate drop-only=true ! \
     video/x-raw,framerate=60/1 ! \
     queue max-size-time=500000000 leaky=downstream ! \
     videoconvert ! \
     video/x-raw,format=NV12 ! \
     vah264enc bitrate=15000000 rate-control=cbr key-int-max=90 ! \
     video/x-h264,profile=high ! \
     h264parse ! \
     rtph264pay config-interval=-1 pt=96 mtu=1400 ! \
     queue max-size-time=1000000000 ! \
     udpsink host={} port={} sync=false async=false buffer-size=8388608",
        node_id, host, port
    );

    // let pipeline_description = format!(
    //     "pipewiresrc path={} do-timestamp=true ! \
    //  video/x-raw,width=1920,height=1080,framerate=60/1 ! \
    //  videorate drop-only=true ! \
    //  queue max-size-time=500000000 leaky=downstream ! \
    //  videoconvert ! \
    //  video/x-raw,format=NV12 ! \
    //  vah264enc bitrate=15000000 rate-control=vbr key-int-max=60 ! \
    //  video/x-h264,profile=high,stream-format=avc ! \
    //  h264parse ! \
    //  rtph264pay config-interval=1 pt=96 mtu=1400 ! \
    //  udpsink host={} port={} sync=false async=false buffer-size=8388608",
    //     node_id, host, port
    // );
    info!("Pipeline: {}", pipeline_description);

    let pipeline = gst::parse::launch(&pipeline_description)?;
    let pipeline = match pipeline.downcast::<gst::Pipeline>() {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to downcast pipeline: {:?}", e);
            return Err("Failed to downcast pipeline".into());
        }
    };

    let bus = match pipeline.bus() {
        Some(b) => b,
        None => {
            error!("Failed to get pipeline bus");
            return Err("Failed to get pipeline bus".into());
        }
    };
    pipeline.set_state(gst::State::Playing)?;

    info!("Pipeline started successfully");
    info!("Streaming to {}:{}", host, port);

    let mut last_print = std::time::Instant::now();
    let mut iteration_count = 0u64;

    loop {
        if stop_streaming_flag.load(Ordering::Relaxed) {
            info!("stop_streaming_flag is true");
            break;
        }

        if let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(100)) {
            match msg.view() {
                gst::MessageView::Eos(..) => {
                    info!("End of stream reached.");
                    break;
                }
                gst::MessageView::Error(err) => {
                    error!(
                        "ERROR from {:?}: {} - Debug: {:?}",
                        err.src().map(|s| s.path_string()),
                        err.error(),
                        err.debug()
                    );
                    break;
                }
                gst::MessageView::Warning(w) => {
                    warn!(
                        "WARNING from {:?}: {} - Debug: {:?}",
                        w.src().map(|s| s.path_string()),
                        w,
                        w.debug()
                    );
                }
                gst::MessageView::Info(i) => {
                    info!("INFO: {:?}", i);
                }
                gst::MessageView::StateChanged(s) => {
                    if let Some(src) = msg.src() {
                        if *src == pipeline.clone().upcast::<gst::Object>() {
                            info!("Pipeline state changed: {:?} -> {:?}", s.old(), s.current());
                        }
                    }
                }
                gst::MessageView::Element(e) => {
                    if let Some(structure) = e.structure() {
                        info!("Element message: {}", structure.name());
                    }
                }
                gst::MessageView::Qos(_qos) => {
                    // QoS messages can be noisy, uncomment if needed
                    // info!("QoS message received");
                }
                _ => {}
            }
        }

        // Print stats every 2 seconds
        if last_print.elapsed().as_secs() >= 2 {
            iteration_count += 1;

            // Get stats safely
            if let Some(udpsink) = pipeline.by_name("udpsink0") {
                // Try different properties that might be available
                info!("{:?}", udpsink.property_value("bytes-to-serve"));
            }

            // Get x264enc stats
            if let Some(encoder) = pipeline.by_name("x264enc0") {
                info!("{:?}", encoder.property_value("qos"));
            }

            // info!(
            //     "Still streaming ({} iterations, ~{} seconds)",
            //     iteration_count,
            //     iteration_count * 2
            // );
            last_print = std::time::Instant::now();
        }
    }

    info!("Stopping pipeline");
    pipeline.send_event(gst::event::Eos::new());
    std::thread::sleep(std::time::Duration::from_millis(500));
    pipeline.set_state(gst::State::Null)?;
    info!("Pipeline stopped cleanly");
    Ok(())
}
