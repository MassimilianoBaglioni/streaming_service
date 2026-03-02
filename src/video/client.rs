use gstreamer as gst;
use gstreamer::prelude::*;

pub fn client_fun() -> String {
    String::from("CLIENT answer")
}

pub fn receive() -> Result<(), Box<dyn std::error::Error>> {
    gst::init()?;

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

    // let bus = pipeline.bus().unwrap();

    pipeline.set_state(gst::State::Playing)?;
    println!("✓ Receiver online. Listening on port 5000...");

    // let mut last_stats = std::time::Instant::now();
    // let mut frame_count = 0;

    loop {
        // if let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(100)) {
        //     match msg.view() {
        //         gst::MessageView::Eos(..) => {
        //             println!("End of stream");
        //             break;
        //         }
        //         gst::MessageView::Error(err) => {
        //             // eprintln!(
        //             //     "ERROR from {:?}: {} - Debug: {:?}",
        //             //     err.src().map(|s| s.path_string()),
        //             //     err.error(),
        //             //     err.debug()
        //             // );
        //             break;
        //         }
        //         gst::MessageView::Warning(w) => {
        //             //eprintln!("{:?}", w);
        //         }
        //         gst::MessageView::StateChanged(s) => {
        //             if let Some(src) = msg.src() {
        //                 if *src == pipeline.clone().upcast::<gst::Object>() {
        //                     //println!("Pipeline state: {:?} -> {:?}", s.old(), s.current());
        //                 }
        //             }
        //         }
        //         gst::MessageView::Element(e) => {
        //             if let Some(structure) = e.structure() {
        //                 //println!("Element message: {}", structure.name());
        //             }
        //         }
        //         gst::MessageView::Qos(qos) => {
        //             //println!("{:?}", qos);
        //         }
        //         _ => {}
        //     }
        // }

        // Print periodic status
        // if last_stats.elapsed().as_secs() >= 2 {
        //     frame_count += 1;
        //     println!("📺 Still receiving... ({} checks)", frame_count);
        //     last_stats = std::time::Instant::now();
        // }
    }

    // pipeline.set_state(gst::State::Null)?;
    // Ok(())
}
