#[cfg(target_os = "windows")]
use std::sync::{Arc, Mutex};
use std::{net::Ipv4Addr, ptr::null_mut};

#[cfg(target_os = "windows")]
use crate::network::streaming_events_server::StreamingEventSocketServer;
use crate::{
    network::NetInfo,
    video::{
        gs, video_source::VideoSourceKind,
        windows_impl::windows_streaming_settings::WindowsStreamingSettings,
    },
};
use gstreamer::prelude::ElementExt;
use tracing::{error, info};
use windows::{
    Foundation::TypedEventHandler,
    Graphics::{Capture::GraphicsCaptureSession, DirectX::DirectXPixelFormat},
    Win32::{
        Graphics::{
            Direct3D11::ID3D11DeviceContext,
            Dxgi::{DXGI_MAP_READ, DXGI_MAPPED_RECT, IDXGISurface},
        },
        System::WinRT::Direct3D11::{
            CreateDirect3D11DeviceFromDXGIDevice, IDirect3DDxgiInterfaceAccess,
        },
    },
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

use windows::Win32::Graphics::Direct3D11::{
    D3D11_BIND_FLAG, D3D11_CPU_ACCESS_READ, D3D11_RESOURCE_MISC_FLAG, D3D11_TEXTURE2D_DESC,
    D3D11_USAGE_STAGING, ID3D11Texture2D,
};
use windows::Win32::Graphics::Dxgi::Common::{DXGI_FORMAT_B8G8R8A8_UNORM, DXGI_SAMPLE_DESC};

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
    app_src: Option<Arc<gstreamer_app::AppSrc>>,
    pipeline: Option<gstreamer::Pipeline>,
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
            app_src: None,
            pipeline: None,
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
        let mut context: Option<ID3D11DeviceContext> = None;
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
                Some(&mut context),
            )
            .unwrap()
        }

        let device = device.unwrap();
        let dxgi_device: IDXGIDevice = device.cast().unwrap();
        let context = context.unwrap();
        let inspectable = unsafe { CreateDirect3D11DeviceFromDXGIDevice(&dxgi_device).unwrap() };
        let winrt_device: IDirect3DDevice = inspectable.cast().unwrap();

        let frame_pool = Direct3D11CaptureFramePool::CreateFreeThreaded(
            &winrt_device,
            pixel_format,
            buffer_count,
            capture_item.Size().expect("Error getting size"),
        )
        .expect("Error creating frame pool");

        let size = capture_item.Size().expect("Failed to get capture item");
        let width = size.Width as u32;
        let height = size.Height as u32;

        use windows::Win32::Graphics::Direct3D11::{
            D3D11_BIND_FLAG, D3D11_CPU_ACCESS_READ, D3D11_TEXTURE2D_DESC, D3D11_USAGE_STAGING,
            ID3D11Texture2D,
        };

        let staging_desc = D3D11_TEXTURE2D_DESC {
            Width: width,
            Height: height,
            MipLevels: 1,
            ArraySize: 1,
            Format: windows::Win32::Graphics::Dxgi::Common::DXGI_FORMAT_B8G8R8A8_UNORM,
            SampleDesc: windows::Win32::Graphics::Dxgi::Common::DXGI_SAMPLE_DESC {
                Count: 1,
                Quality: 0,
            },
            Usage: D3D11_USAGE_STAGING,
            BindFlags: 0,
            CPUAccessFlags: D3D11_CPU_ACCESS_READ.0 as u32,
            MiscFlags: 0,
        };

        let mut staging_texture: Option<ID3D11Texture2D> = None;
        unsafe {
            device
                .CreateTexture2D(&staging_desc, None, Some(&mut staging_texture))
                .unwrap();
        }
        let staging_texture = staging_texture.unwrap();

        self.pipeline = Some(gs::create_windows_pipeline(
            width,
            height,
            self.host_ip,
            self.streaming_port,
        ));

        info!(
            "host_ipL {:?}, port: {:?}",
            self.host_ip, self.streaming_port
        );

        self.app_src = Some(Arc::new(gs::create_app_src(
            self.pipeline.as_ref().unwrap(),
        )));

        let app_src_clone = self.app_src.clone();

        let handler = TypedEventHandler::new(
            move |sender: Ref<Direct3D11CaptureFramePool>, _args: Ref<IInspectable>| {
                let frame_pool = sender.unwrap();

                if let Ok(frame) = frame_pool.TryGetNextFrame() {
                    info!("Try get next frame");
                    let size = match frame.ContentSize() {
                        Ok(s) => s,
                        Err(e) => {
                            error!("ContentSize failed: {:?}", e);
                            return Ok(());
                        }
                    };
                    let appsrc = app_src_clone.clone();

                    // A surface is an interface to the texture stored in VRAM. The lines below copy the data from VRAM to RAM for the CPU.
                    let surface = match frame.Surface() {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Surface failed: {:?}", e);
                            return Ok(());
                        }
                    };

                    let dxgi_access: IDirect3DDxgiInterfaceAccess = match surface.cast() {
                        Ok(a) => a,
                        Err(e) => {
                            error!("Cast to IDirect3DDxgiInterfaceAccess failed: {:?}", e);
                            return Ok(());
                        }
                    };

                    // get the capture texture as ID3D11Texture2D
                    let capture_texture: ID3D11Texture2D =
                        match unsafe { dxgi_access.GetInterface() } {
                            Ok(t) => t,
                            Err(e) => {
                                error!("GetInterface texture failed: {:?}", e);
                                return Ok(());
                            }
                        };

                    // copy from GPU-only capture texture → staging texture (CPU accessible)
                    unsafe { context.CopyResource(&staging_texture, &capture_texture) };

                    // map the STAGING texture, not the capture texture
                    let dxgi_staging: IDXGISurface = match staging_texture.cast() {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Staging cast failed: {:?}", e);
                            return Ok(());
                        }
                    };

                    let mut mapped_rect = DXGI_MAPPED_RECT::default();
                    if let Err(e) = unsafe { dxgi_staging.Map(&mut mapped_rect, DXGI_MAP_READ) } {
                        error!("Map failed: {:?}", e);
                        return Ok(()); // ← early return so we never touch null pointer
                    }
                    info!("Mapped staging surface");

                    let pitch = mapped_rect.Pitch as usize;
                    let data_ptr = mapped_rect.pBits;

                    let width = size.Width as usize;
                    let height = size.Height as usize;
                    let bytes_per_pixel = match pixel_format {
                        DirectXPixelFormat::B8G8R8A8UIntNormalized => 4,
                        DirectXPixelFormat::R16G16B16A16Float => 8,
                        _ => panic!("Unsupported pixel format"),
                    };
                    let row_size = width * bytes_per_pixel;
                    let total_size = row_size * height;

                    // Gstreamer has an intenrla buffers pooling, this is not an allocation per iteration, no optimization needed.
                    let mut gst_buffer = gstreamer::Buffer::with_size(total_size)
                        .expect("Failed to allocate the gstreamer buffer");

                    {
                        let buffer_ref = gst_buffer.get_mut().unwrap();

                        let timestamp = frame
                            .SystemRelativeTime()
                            .expect("Error on systemRelative")
                            .Duration;

                        // Set the timestamp for gstreamer, we are taking the timestamp from windows above
                        buffer_ref
                            .set_pts(gstreamer::ClockTime::from_nseconds(timestamp as u64 * 100));

                        let mut map = buffer_ref
                            .map_writable()
                            .expect("Failed to map GStreamer buffer");
                        let dst = map.as_mut_slice();

                        for row in 0..height {
                            let src_offset = row * pitch;
                            let dst_offset = row * row_size;
                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    data_ptr.add(src_offset),
                                    dst.as_mut_ptr().add(dst_offset),
                                    row_size,
                                );
                            }
                        }
                    }

                    unsafe { dxgi_staging.Unmap().expect("Unmap failed") };

                    appsrc
                        .unwrap()
                        .push_buffer(gst_buffer)
                        .expect("Failed to push buffer");
                    info!("Pushed frame");
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

        self.pipeline
            .as_mut()
            .unwrap()
            .set_state(gstreamer::State::Playing)
            .expect("Failed to set pipeline to Playing");
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
