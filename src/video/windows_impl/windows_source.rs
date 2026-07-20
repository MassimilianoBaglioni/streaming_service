use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{net::Ipv4Addr, ptr::null_mut};
use tokio::sync::Mutex;

use crate::network::server_connection::ServerConnection;
use crate::network::ConnectionMode;
use crate::{
    network::NetInfo,
    video::{
        gs, video_source::VideoSourceKind,
        windows_impl::windows_streaming_settings::WindowsStreamingSettings,
    },
};
use gstreamer::prelude::ElementExt;
use gstreamer::Sample;
use gstreamer_app::{AppSink, AppSrc};
use tracing::{error, info, warn};
use windows::{
    core::{IInspectable, Interface, Ref},
    Foundation::TypedEventHandler,
    Graphics::{
        Capture::{Direct3D11CaptureFramePool, GraphicsCaptureItem, GraphicsCaptureSession},
        DirectX::Direct3D11::IDirect3DDevice,
        DirectX::DirectXPixelFormat,
    },
    Win32::{
        Foundation::HMODULE,
        Graphics::{
            Direct3D::D3D_DRIVER_TYPE_HARDWARE,
            Direct3D11::{
                D3D11CreateDevice, ID3D11Device, ID3D11DeviceContext,
                ID3D11Texture2D, D3D11_CPU_ACCESS_READ, D3D11_CREATE_DEVICE_BGRA_SUPPORT, D3D11_SDK_VERSION,
                D3D11_TEXTURE2D_DESC, D3D11_USAGE_STAGING,
            },
            Dxgi::IDXGIDevice,
            Dxgi::{IDXGISurface, DXGI_MAPPED_RECT, DXGI_MAP_READ},
        },
        System::WinRT::Direct3D11::{
            CreateDirect3D11DeviceFromDXGIDevice, IDirect3DDxgiInterfaceAccess,
        },
    },
};

struct SendWrapper<T>(T);
unsafe impl<T> Send for SendWrapper<T> {}

impl<T> SendWrapper<T> {
    fn new(val: T) -> Self {
        Self(val)
    }
    fn get(&self) -> &T {
        &self.0
    }
    fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
    fn into_inner(self) -> T {
        self.0
    }
}

pub struct WindowsSource {
    connection: Option<Arc<Mutex<ServerConnection>>>,
    tcp_port: u16,
    host_ip: Ipv4Addr,
    streaming_port: u16,
    graphics_capture_item: Option<SendWrapper<GraphicsCaptureItem>>,
    pub windows_settings: WindowsStreamingSettings,
    token: Option<i64>,
    frame_pool: Option<SendWrapper<Direct3D11CaptureFramePool>>,
    graphics_capture_session: Option<SendWrapper<GraphicsCaptureSession>>,
    app_src: Option<Arc<gstreamer_app::AppSrc>>,
    app_sink: Option<Arc<gstreamer_app::AppSink>>,
    pipeline: Option<gstreamer::Pipeline>,
}

impl WindowsSource {
    pub fn new(
        tcp_port: u16,
        streaming_port: u16,
        host_ip: Ipv4Addr,
        graphics_capture_item: Option<GraphicsCaptureItem>,
        windows_settings: WindowsStreamingSettings,
        connection_mode: ConnectionMode,
    ) -> Self {
        let mut connection = ServerConnection::default();
        connection.connection_mode = Some(connection_mode);

        Self {
            connection: None,
            tcp_port,
            streaming_port,
            host_ip,
            graphics_capture_item: graphics_capture_item.map(SendWrapper::new),
            windows_settings,
            token: None,
            frame_pool: None,
            graphics_capture_session: None,
            app_src: None,
            app_sink: None,
            pipeline: None,
        }
    }

    pub fn set_graphics_capture_item(
        &mut self,
        graphics_capture_item: Option<GraphicsCaptureItem>,
    ) {
        self.graphics_capture_item = graphics_capture_item.map(SendWrapper::new);
    }

    pub async fn start_streaming(&mut self) {
        info!("Start streaming video source called");
        // Create the server socket if it doesn't exist yet
        self.connection
            .as_ref()
            .unwrap()
            .lock()
            .await
            .accept()
            .await;

        let is_direct = matches!(
            self.connection
                .as_ref()
                .unwrap()
                .lock()
                .await
                .connection_mode
                .as_ref()
                .unwrap(),
            ConnectionMode::Direct
        );

        let pixel_format = self.windows_settings.pixel_format.clone();
        let buffer_count = self.windows_settings.buffer_count.clone();
        let capture_item = self.graphics_capture_item.as_ref().unwrap().get().clone();

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

        self.init_pipeline(is_direct, width, height);

        let target_frame_interval =
            std::time::Duration::from_secs_f64(1.0 / self.windows_settings.fps as f64);

        let frame_handler = FrameArrivedHandler {
            app_src: self.app_src.as_ref().unwrap().clone(),
            context,
            staging_texture,
            pixel_format,
            target_frame_interval,
            last_frame_nanos: Arc::new(AtomicU64::new(0)),
        };

        let last_frame_nanos = Arc::new(AtomicU64::new(0));
        let last_clone = last_frame_nanos.clone();

        let handler = TypedEventHandler::new(
            move |sender: Ref<Direct3D11CaptureFramePool>, args: Ref<IInspectable>| {
                frame_handler.on_frame_arrived(sender, args)
            },
        );

        let session = frame_pool
            .CreateCaptureSession(&capture_item)
            .expect("Capture session creation failed");

        session.StartCapture().expect("Error starting capture");

        self.pipeline
            .as_mut()
            .unwrap()
            .set_state(gstreamer::State::Playing)
            .expect("Failed to set pipeline to Playing");

        if (!is_direct) {
            // This code must be always after starting the pipeline because we need a playing pipeline to pull, otherwise we get errors
            let app_sink_clone = self.app_sink.clone();

            WindowsSource::start_sending_frames_iroh(
                app_sink_clone.unwrap(),
                self.connection.clone().unwrap(),
            );
        }

        let token: i64 = frame_pool
            .FrameArrived(&handler)
            .expect("Error registering handler");

        self.save_to_keep_alive(frame_pool, session, token);
        info!("Capture started");
    }

    fn save_to_keep_alive(
        &mut self,
        frame_pool: Direct3D11CaptureFramePool,
        capture_session: GraphicsCaptureSession,
        token: i64,
    ) {
        self.frame_pool = Some(SendWrapper::new(frame_pool));
        self.graphics_capture_session = Some(SendWrapper::new(capture_session));
        self.token = Some(token);
    }

    fn start_sending_frames_iroh(app_sink: Arc<AppSink>, connection: Arc<Mutex<ServerConnection>>) {
        let (sender, receiver) = tokio::sync::mpsc::channel::<Sample>(32);

        let frame_sender_thread_handle = tokio::task::spawn_blocking(move || {
            loop {
                match app_sink.pull_sample() {
                    Ok(sample) => {
                        if sender.blocking_send(sample).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        warn!("Error on receiving the sample, before sending");
                    }
                }
            }
        });

        let iroh_sender_task_handle = tokio::task::spawn(async move {
            connection.lock().await.send_frames_iroh(receiver).await;
        });
    }

    fn init_pipeline(&mut self, is_direct: bool, width: u32, height: u32) {
        if is_direct {
            info!(
                "host_ip: {:?}, port: {:?}",
                self.host_ip, self.streaming_port
            );
            self.pipeline = Some(gs::create_windows_pipeline(
                width,
                height,
                self.host_ip,
                &self.windows_settings,
                self.streaming_port,
            ))
        } else {
            self.pipeline = Some(gs::create_windows_pipeline_with_app_dest(
                width,
                height,
                &self.windows_settings,
            ));

            self.app_src = Some(Arc::new(gs::get_app_src(self.pipeline.as_ref().unwrap())));

            self.app_sink = Some(Arc::new(gs::get_app_sink(
                self.pipeline.as_ref().unwrap(),
                "rtp_sink",
            )));
        }

        self.app_src = Some(Arc::new(gs::get_app_src(self.pipeline.as_ref().unwrap())));
    }

    pub async fn stop_streaming(&mut self) {
        // stop capture first so no more frames arrive
        if let Some(session) = &self.graphics_capture_session {
            session.get().Close().ok();
        }

        // unregister the frame handler
        if let (Some(pool), Some(token)) = (&self.frame_pool, self.token) {
            pool.get().RemoveFrameArrived(token).ok();
            pool.get().Close().ok();
        }

        // stop gstreamer pipeline
        if let Some(pipeline) = &self.pipeline {
            pipeline.set_state(gstreamer::State::Null).ok();
        }

        // drop everything
        self.graphics_capture_session = None;
        self.frame_pool = None;
        self.token = None;
        self.pipeline = None;
        self.app_src = None;

        self.connection
            .as_mut()
            .expect("No connection established cannot close it")
            .lock()
            .await
            .send_end_event_and_close_conn();

        info!("Streaming stopped");
    }

    pub fn update_network_info(&mut self, net_info: &NetInfo) {
        self.host_ip = net_info.target_ip;
        self.tcp_port = net_info.tcp_port;
        self.streaming_port = net_info.stream_port;

        match &net_info.connection_mode {
            ConnectionMode::Direct => {
                self.connection = Some(Arc::new(Mutex::new(ServerConnection::from(
                    net_info.tcp_port,
                ))))
            }
            ConnectionMode::Iroh { info } => {
                self.connection = Some(Arc::new(Mutex::new(ServerConnection::from(info.clone()))))
            }
        }
    }
}
pub fn create_windows_video_source(
    net_info: &NetInfo,
    graphics_capture_item: Option<GraphicsCaptureItem>,
    windows_streaming_settings: WindowsStreamingSettings,
) -> Arc<Mutex<VideoSourceKind>> {
    Arc::new(Mutex::new(VideoSourceKind::Windows(WindowsSource::new(
        net_info.tcp_port,
        net_info.stream_port,
        net_info.target_ip,
        graphics_capture_item,
        windows_streaming_settings,
        net_info.connection_mode.clone(),
    ))))
}

struct FrameArrivedHandler {
    app_src: Arc<AppSrc>,
    context: ID3D11DeviceContext,
    staging_texture: ID3D11Texture2D,
    pixel_format: DirectXPixelFormat,
    target_frame_interval: std::time::Duration,
    last_frame_nanos: Arc<AtomicU64>,
}

impl FrameArrivedHandler {
    fn on_frame_arrived(
        &self,
        sender: Ref<Direct3D11CaptureFramePool>,
        _args: Ref<IInspectable>,
    ) -> windows::core::Result<()> {
        let frame_pool = sender.unwrap();
        let Ok(frame) = frame_pool.TryGetNextFrame() else {
            return Ok(());
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let last = self.last_frame_nanos.load(Ordering::Relaxed);
        let interval_nanos = self.target_frame_interval.as_nanos() as u64;

        if now - last < interval_nanos {
            return Ok(());
        }
        self.last_frame_nanos.store(now, Ordering::Relaxed);

        let size = match frame.ContentSize() {
            Ok(s) => s,
            Err(e) => {
                error!("ContentSize failed: {:?}", e);
                return Ok(());
            }
        };

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

        let capture_texture: ID3D11Texture2D = match unsafe { dxgi_access.GetInterface() } {
            Ok(t) => t,
            Err(e) => {
                error!("GetInterface texture failed: {:?}", e);
                return Ok(());
            }
        };

        // copy from GPU-only capture texture → staging texture (CPU accessible)
        unsafe {
            self.context
                .CopyResource(&self.staging_texture, &capture_texture)
        };

        // map the STAGING texture, not the capture texture
        let dxgi_staging: IDXGISurface = match self.staging_texture.cast() {
            Ok(s) => s,
            Err(e) => {
                error!("Staging cast failed: {:?}", e);
                return Ok(());
            }
        };

        let mut mapped_rect = DXGI_MAPPED_RECT::default();
        if let Err(e) = unsafe { dxgi_staging.Map(&mut mapped_rect, DXGI_MAP_READ) } {
            error!("Map failed: {:?}", e);
            return Ok(());
        }

        let pitch = mapped_rect.Pitch as usize;
        let data_ptr = mapped_rect.pBits;

        let width = size.Width as usize;
        let height = size.Height as usize;
        let bytes_per_pixel = match self.pixel_format {
            DirectXPixelFormat::B8G8R8A8UIntNormalized => 4,
            DirectXPixelFormat::R16G16B16A16Float => 8,
            _ => panic!("Unsupported pixel format"),
        };
        let row_size = width * bytes_per_pixel;
        let total_size = row_size * height;

        // Gstreamer has an internal buffers pooling, this is not an allocation per iteration, no optimization needed.
        let mut gst_buffer = gstreamer::Buffer::with_size(total_size)
            .expect("Failed to allocate the gstreamer buffer");

        {
            let buffer_ref = gst_buffer.get_mut().unwrap();

            // let timestamp = frame
            //     .SystemRelativeTime()
            //     .expect("Error on systemRelative")
            //     .Duration;
            // 
            // buffer_ref.set_pts(gstreamer::ClockTime::from_nseconds(timestamp as u64 * 100));

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

        self.app_src
            .push_buffer(gst_buffer)
            .expect("Failed to push buffer");

        Ok(())
    }
}
