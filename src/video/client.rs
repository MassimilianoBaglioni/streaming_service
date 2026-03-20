use crate::network::streaming_events_client::StreamingEventSocketClient;
use gstreamer::prelude::*;
use gstreamer::{self as gst};
use tracing::{error, info, warn};

pub fn receive() -> Result<(), Box<dyn std::error::Error>> {
    // let pipeline_description = "\
    //     udpsrc port=5000 buffer-size=2097152 ! \
    //     application/x-rtp, \
    //         media=(string)video, \
    //         clock-rate=(int)90000, \
    //         encoding-name=(string)H264, \
    //         payload=(int)96 ! \
    //     queue max-size-buffers=10 leaky=downstream ! \
    //     rtph264depay ! \
    //     h264parse ! \
    //     queue max-size-buffers=3 ! \
    //     avdec_h264 max-threads=4 ! \
    //     videoconvert ! \
    //     autovideosink sync=false";

    // WORKING CPU
    // let pipeline_description = "\
    // udpsrc port=5000 buffer-size=8388608 ! \
    // application/x-rtp, \
    //     media=(string)video, \
    //     clock-rate=(int)90000, \
    //     encoding-name=(string)H264, \
    //     payload=(int)96 ! \
    // queue max-size-buffers=30 leaky=downstream ! \
    // rtph264depay ! \
    // h264parse ! \
    // vaapih264dec ! \
    // videoconvert ! \
    // autovideosink sync=false";

    // GPU DECONDING, works also with the frame limited one the server
    // let pipeline_description = "\
    // udpsrc port=5000 buffer-size=8388608 ! \
    // application/x-rtp, \
    //     media=(string)video, \
    //     clock-rate=(int)90000, \
    //     encoding-name=(string)H264, \
    //     payload=(int)96 ! \
    // queue max-size-buffers=30 leaky=downstream ! \
    // rtph264depay ! \
    // h264parse ! \
    // vaapih264dec ! \
    // vaapipostproc ! \
    // videoconvert ! \
    // autovideosink sync=false";
    // println!("Pipeline: {}", pipeline_description);

    // QUALITY
    // let pipeline_description = "\
    // udpsrc port=5000 buffer-size=8388608 ! \
    // application/x-rtp, \
    //     media=(string)video, \
    //     clock-rate=(int)90000, \
    //     encoding-name=(string)H264, \
    //     payload=(int)96 ! \
    // rtpjitterbuffer latency=500 ! \
    // queue max-size-time=1000000000 leaky=downstream ! \
    // rtph264depay ! \
    // h264parse ! \
    // queue max-size-time=500000000 leaky=downstream ! \
    // vaapih264dec ! \
    // vaapipostproc ! \
    // videoconvert ! \
    // autovideosink sync=false";

    // POST FIX USING VAH
    let pipeline_description = "\
    udpsrc port=5000 buffer-size=8388608 ! \
    application/x-rtp, \
        media=(string)video, \
        clock-rate=(int)90000, \
        encoding-name=(string)H264, \
        payload=(int)96 ! \
    rtpjitterbuffer latency=500 ! \
    queue max-size-time=1000000000 leaky=downstream ! \
    rtph264depay ! \
    h264parse ! \
    queue max-size-time=500000000 leaky=downstream ! \
    vah264dec ! \
    vapostproc ! \
    videoconvert ! \
    autovideosink sync=false";

    let pipeline = gst::parse::launch(pipeline_description)?;
    let pipeline = pipeline.downcast::<gst::Pipeline>().unwrap();

    let bus = pipeline.bus().unwrap();

    let tcp_address = String::from("127.0.0.1:8010");
    let mut socket =
        StreamingEventSocketClient::connect(&tcp_address).expect("Could not create the tcp socket");

    pipeline.set_state(gst::State::Playing)?;
    info!("Receiver online. Listening on port 5000...");

    let mut should_break = false;
    loop {
        match socket.read_event() {
            Ok(val) => {
                info!("Received {:?}, from tcp socket", val);
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
                    if let Some(src) = msg.src() {
                        if *src == pipeline.clone().upcast::<gst::Object>() {
                            info!("Pipeline state: {:?} -> {:?}", s.old(), s.current());
                        }
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
