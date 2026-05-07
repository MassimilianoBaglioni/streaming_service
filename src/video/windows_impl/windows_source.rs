use std::net::Ipv4Addr;
#[cfg(target_os = "windows")]
use std::sync::{Arc, Mutex};

#[cfg(target_os = "windows")]
use crate::network::streaming_events_server::StreamingEventSocketServer;
use crate::{network::NetInfo, video::video_source::VideoSourceKind};
use tracing::{error, info};
use windows::{Graphics::Capture::GraphicsCaptureItem, Win32::Foundation::HWND};

#[cfg(target_os = "windows")]
#[derive(Clone)]
pub struct SafeHwnd(pub HWND);

unsafe impl Send for SafeHwnd {}
unsafe impl Sync for SafeHwnd {}
pub struct WindowsSource {
    hwnd: SafeHwnd,
    tcp_socket: Option<StreamingEventSocketServer>,
    tcp_port: u16,
    host_ip: Ipv4Addr,
    streaming_port: u16,
    graphics_capture_item: Option<GraphicsCaptureItem>,
}

impl WindowsSource {
    pub fn new(
        raw: isize,
        tcp_port: u16,
        streaming_port: u16,
        host_ip: Ipv4Addr,
        graphics_capture_item: Option<GraphicsCaptureItem>,
    ) -> Self {
        Self {
            hwnd: SafeHwnd(HWND(raw as *mut std::ffi::c_void)),
            tcp_socket: None,
            tcp_port,
            streaming_port,
            host_ip,
            graphics_capture_item,
        }
    }

    pub fn set_graphics_capture_item(
        &mut self,
        graphics_capture_item: Option<GraphicsCaptureItem>,
    ) {
        self.graphics_capture_item = graphics_capture_item;
    }

    pub fn start_streaming(&mut self) {}

    pub fn stop_streaming(&mut self) {
        error!("stop_streaming not implemented yet");
    }

    pub fn update_network_info(&mut self, net_info: &NetInfo) {}
}

pub fn create_windows_video_source(
    hwnd: isize,
    net_info: &NetInfo,
    graphics_capture_item: Option<GraphicsCaptureItem>,
) -> Arc<Mutex<VideoSourceKind>> {
    Arc::new(Mutex::new(VideoSourceKind::Windows(WindowsSource::new(
        hwnd,
        net_info.tcp_port,
        net_info.stream_port,
        net_info.target_ip,
        graphics_capture_item,
    ))))
}
