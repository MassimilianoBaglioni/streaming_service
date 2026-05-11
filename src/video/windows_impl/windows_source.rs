#[cfg(target_os = "windows")]
use std::sync::{Arc, Mutex};
use std::{net::Ipv4Addr, ptr::null_mut};

#[cfg(target_os = "windows")]
use crate::network::streaming_events_server::StreamingEventSocketServer;
use crate::{
    network::NetInfo,
    video::{
        video_source::VideoSourceKind,
        windows_impl::windows_streaming_settings::WindowsStreamingSettings,
    },
};
use tracing::{error, info};
use windows::{
    Foundation::TypedEventHandler,
    Graphics::Capture::GraphicsCaptureSession,
    Win32::System::WinRT::Direct3D11::CreateDirect3D11DeviceFromDXGIDevice,
    core::{IInspectable, Ref},
};
use windows::{
    Graphics::{
        Capture::{Direct3D11CaptureFramePool, GraphicsCaptureItem},
        DirectX::Direct3D11::IDirect3DDevice,
    },
    Win32::{
        Foundation::{HMODULE, HWND},
        Graphics::{
            Direct3D::D3D_DRIVER_TYPE_HARDWARE,
            Direct3D11::{
                D3D11_CREATE_DEVICE_BGRA_SUPPORT, D3D11_SDK_VERSION, D3D11CreateDevice,
                ID3D11Device,
            },
            Dxgi::IDXGIDevice,
        },
    },
    core::Interface,
};

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
    windows_settings: WindowsStreamingSettings,
    token: Option<i64>,
    frame_pool: Option<Direct3D11CaptureFramePool>,
    graphics_capture_session: Option<GraphicsCaptureSession>,
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
            windows_settings: WindowsStreamingSettings::default(),
            token: None,
            frame_pool: None,
            graphics_capture_session: None,
        }
    }

    pub fn set_graphics_capture_item(
        &mut self,
        graphics_capture_item: Option<GraphicsCaptureItem>,
    ) {
        self.graphics_capture_item = graphics_capture_item;
    }

    pub fn start_streaming(&mut self) {
        let pixel_format = self.windows_settings.pixel_format.clone();
        let buffer_count = self.windows_settings.buffer_count.clone();
        let capture_item = self.graphics_capture_item.as_ref().unwrap().clone();

        let mut device: Option<ID3D11Device> = None;
        unsafe {
            D3D11CreateDevice(
                None,
                D3D_DRIVER_TYPE_HARDWARE,
                HMODULE(null_mut()),
                D3D11_CREATE_DEVICE_BGRA_SUPPORT,
                None,
                D3D11_SDK_VERSION,
                Some(&mut device),
                None,
                None, // don't need context
            )
            .unwrap()
        }

        let dxgi_device: IDXGIDevice = device.unwrap().cast().unwrap();
        let inspectable = unsafe { CreateDirect3D11DeviceFromDXGIDevice(&dxgi_device).unwrap() };
        let winrt_device: IDirect3DDevice = inspectable.cast().unwrap();

        let frame_pool = Direct3D11CaptureFramePool::CreateFreeThreaded(
            &winrt_device,
            pixel_format,
            buffer_count,
            capture_item.Size().expect("Error getting size"),
        )
        .expect("Error creating frame pool");

        let handler = TypedEventHandler::new(
            move |sender: Ref<Direct3D11CaptureFramePool>, _args: Ref<IInspectable>| {
                let frame_pool = sender.unwrap();
                if let Ok(frame) = frame_pool.TryGetNextFrame() {
                    let size = frame.ContentSize()?;
                    info!("Frame arrived: {}x{}", size.Width, size.Height);
                }
                Ok(())
            },
        );

        let _token = frame_pool
            .FrameArrived(&handler)
            .expect("Error registering handler");

        let session = frame_pool
            .CreateCaptureSession(&capture_item)
            .expect("Capture session creation failed");

        session.StartCapture().expect("Error starting capture");

        // store to keep alive
        self.frame_pool = Some(frame_pool);
        self.graphics_capture_session = Some(session);
        self.token = Some(_token);

        info!("Capture started");
    }

    pub fn stop_streaming(&mut self) {
        error!("stop_streaming not implemented yet");
    }

    pub fn update_network_info(&mut self, net_info: &NetInfo) {
        self.host_ip = net_info.target_ip;
        self.tcp_port = net_info.tcp_port;
        self.streaming_port = net_info.stream_port;

        self.tcp_socket = Some(
            StreamingEventSocketServer::bind(&format!("0.0.0.0:{}", self.tcp_port))
                .expect("Failed to bind tcp socket."),
        );
    }
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
