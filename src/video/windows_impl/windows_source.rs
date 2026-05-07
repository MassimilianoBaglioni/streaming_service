use std::net::Ipv4Addr;
#[cfg(target_os = "windows")]
use std::sync::{Arc, Mutex};

use crate::network::NetInfo;
#[cfg(target_os = "windows")]
use crate::network::streaming_events_server::StreamingEventSocketServer;
use crate::video::video_source::VideoSource;
use tracing::{error, info};
use windows::Win32::Foundation::HWND;

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
}

impl WindowsSource {
    pub fn new(raw: isize, tcp_port: u16, streaming_port: u16, host_ip: Ipv4Addr) -> Self {
        Self {
            hwnd: SafeHwnd(HWND(raw as *mut std::ffi::c_void)),
            tcp_socket: None,
            tcp_port,
            streaming_port,
            host_ip,
        }
    }
}

impl VideoSource for WindowsSource {
    fn start_streaming(&mut self) {}

    fn stop_streaming(&mut self) {
        error!("stop_streaming not implemented yet");
    }

    fn update_network_info(&mut self, net_info: &NetInfo) {
        // todo!()
    }
}

impl WindowsSource {}

pub fn create_windows_video_source(
    hwnd: isize,
    net_info: &NetInfo,
) -> Arc<Mutex<dyn VideoSource + Send + Sync>> {
    Arc::new(Mutex::new(WindowsSource::new(
        hwnd,
        net_info.tcp_port,
        net_info.stream_port,
        net_info.target_ip,
    )))
}
