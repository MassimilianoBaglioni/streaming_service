use gstreamer::prelude::{Cast, GstBinExt};
use gstreamer::Pipeline;
use gstreamer_app::{AppSink, AppSrc};

pub fn get_app_src(pipeline: &Pipeline) -> AppSrc {
    pipeline
        .by_name("src")
        .expect("Failed to find appsrc element")
        .downcast::<AppSrc>()
        .expect("Failed to downcast to AppSrc")
}

pub fn get_app_sink(pipeline: &Pipeline, sink_name: &str) -> AppSink {
    pipeline
        .by_name(sink_name)
        .unwrap_or_else(|| panic!("Failed to find appsink element, with name: {}", sink_name))
        .downcast::<AppSink>()
        .expect("Failed to downcast to AppSink")
}
