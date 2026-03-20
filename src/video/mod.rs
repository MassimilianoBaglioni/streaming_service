use std::sync::{Arc, Mutex};

use crate::{video::video_source::VideoSource, wayland::wayland_handles::WaylandHandles};

pub mod client;
pub mod gs;
pub mod pipewire_source;
pub mod utilities;
pub mod video_source;

pub fn create_video_source(handles: WaylandHandles) -> Arc<Mutex<dyn VideoSource + Send + Sync>> {
    #[cfg(target_os = "linux")]
    {
        use pipewire_source::PipewireSource;

        //TODO add display server recognition, for now it just goes directly to wayland
        Arc::new(Mutex::new(PipewireSource::new(
            handles,
            // TODO hard coded address
            String::from("127.0.0.1:8010"),
        )))
    }

    #[cfg(target_os = "windows")]
    {
        panic!("WINDOWS not implemented yet")
    }

    #[cfg(target_os = "macos")]
    {
        panic!("MACOS not implemented yet")
    }
}
