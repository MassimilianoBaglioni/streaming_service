use std::{
    net::Ipv4Addr,
    sync::{Arc, Mutex},
};

use crate::{video::video_source::VideoSource, wayland::wayland_handles::WaylandHandles};

pub mod client;
pub mod gs;
pub mod pipewire_source;
pub mod utilities;
pub mod video_source;

pub fn create_video_source(
    handles: WaylandHandles,
    tcp_port: u16,
    streaming_port: u16,
    host_ip: Ipv4Addr,
) -> Arc<Mutex<dyn VideoSource + Send + Sync>> {
    #[cfg(target_os = "linux")]
    {
        use pipewire_source::PipewireSource;

        //TODO add display server recognition, for now it just goes directly to wayland
        Arc::new(Mutex::new(PipewireSource::new(
            handles,
            tcp_port,
            streaming_port,
            host_ip,
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
